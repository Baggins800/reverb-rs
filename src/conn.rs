//! A live WebSocket connection and the state the protocol keeps about it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use axum::extract::ws::Utf8Bytes;
use parking_lot::Mutex;
use rand::Rng;
use tokio::sync::mpsc;

use crate::config::Application;
use crate::events::{AppCounters, EventKind, Telemetry};
use crate::protocol::PusherError;

/// A frame queued for delivery to a client.
///
/// Text frames carry [`Utf8Bytes`], which is refcounted, so broadcasting one
/// message to a channel of N subscribers costs N pointer clones rather than N
/// copies of the payload.
#[derive(Debug, Clone)]
pub enum Outbound {
    Text(Utf8Bytes),
    Ping,
    Close,
}

/// Generate a Pusher-compatible socket ID (`%d.%d`, each in `1..=1_000_000_000`).
fn generate_id() -> String {
    let mut rng = rand::rng();

    format!("{}.{}", rng.random_range(1..=1_000_000_000u32), rng.random_range(1..=1_000_000_000u32))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A fixed-window message counter, matching Laravel's `RateLimiter` semantics
/// over the array cache store.
#[derive(Debug)]
struct RateWindow {
    count: u32,
    started_at: i64,
}

#[derive(Debug)]
pub struct Conn {
    /// The Pusher socket ID handed to the client.
    pub id: String,
    pub app: Arc<Application>,
    pub origin: Option<String>,
    tx: mpsc::Sender<Outbound>,
    last_seen_at: AtomicI64,
    has_been_pinged: AtomicBool,
    uses_control_frames: AtomicBool,
    closed: AtomicBool,
    /// Frames dropped because the client could not keep up.
    dropped: AtomicU64,
    limiter: Mutex<RateWindow>,
    telemetry: Arc<Telemetry>,
    /// Resolved once here so the per-frame path never hashes an app ID.
    counters: Arc<AppCounters>,
}

impl Conn {
    pub fn new(
        app: Arc<Application>,
        origin: Option<String>,
        tx: mpsc::Sender<Outbound>,
        telemetry: Arc<Telemetry>,
    ) -> Self {
        let counters = telemetry.counters(&app.id);

        Self {
            id: generate_id(),
            app,
            origin,
            tx,
            last_seen_at: AtomicI64::new(now()),
            has_been_pinged: AtomicBool::new(false),
            uses_control_frames: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            dropped: AtomicU64::new(0),
            limiter: Mutex::new(RateWindow { count: 0, started_at: now() }),
            telemetry,
            counters,
        }
    }

    /// Queue a text frame. Never blocks: if the client's queue is full it has
    /// fallen too far behind and is disconnected instead.
    pub fn send(&self, message: impl Into<Utf8Bytes>) {
        self.send_shared(&message.into());
    }

    /// Queue an already-encoded frame shared across many recipients.
    pub fn send_shared(&self, message: &Utf8Bytes) {
        self.enqueue(Outbound::Text(message.clone()));

        // Reverb dispatches `MessageSent` from exactly this point.
        self.counters.record_sent(1);

        if self.telemetry.forwards(EventKind::MessageSent) {
            self.telemetry.emit(
                EventKind::MessageSent,
                &self.app.id,
                serde_json::json!({
                    "socket_id": self.id,
                    "origin": self.origin,
                    "message": message.as_str(),
                }),
            );
        }
    }

    /// Record a frame accepted from the client, as Reverb's `MessageReceived`.
    pub fn record_received(&self, message: &str) {
        self.counters.record_received();

        if self.telemetry.forwards(EventKind::MessageReceived) {
            self.telemetry.emit(
                EventKind::MessageReceived,
                &self.app.id,
                serde_json::json!({
                    "socket_id": self.id,
                    "origin": self.origin,
                    "message": message,
                }),
            );
        }
    }

    pub fn send_error(&self, error: PusherError) {
        self.send(error.frame());
    }

    /// Send a low-level WebSocket ping control frame.
    pub fn send_control_ping(&self) {
        self.enqueue(Outbound::Ping);
    }

    fn enqueue(&self, frame: Outbound) {
        if self.closed.load(Ordering::Relaxed) {
            return;
        }

        if self.tx.try_send(frame).is_err() {
            // Either the connection task is gone or its queue is saturated.
            // Both mean this connection is finished.
            if self.dropped.fetch_add(1, Ordering::Relaxed) == 0 && !self.is_closed() {
                tracing::warn!(
                    socket_id = %self.id,
                    "disconnecting a client that fell behind its send queue"
                );
            }

            self.terminate();
        }
    }

    /// Ask the writer task to close the socket.
    pub fn terminate(&self) {
        if self.closed.swap(true, Ordering::SeqCst) {
            return;
        }

        let _ = self.tx.try_send(Outbound::Close);
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Relaxed)
    }

    pub fn dropped_frames(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Record activity on the connection and clear any outstanding ping.
    pub fn touch(&self) {
        self.last_seen_at.store(now(), Ordering::Relaxed);
        self.has_been_pinged.store(false, Ordering::Relaxed);
    }

    /// Mark that a ping has been sent and is awaiting a reply.
    pub fn mark_pinged(&self) {
        self.has_been_pinged.store(true, Ordering::Relaxed);
    }

    pub fn last_seen_at(&self) -> i64 {
        self.last_seen_at.load(Ordering::Relaxed)
    }

    pub fn is_active(&self) -> bool {
        now() < self.last_seen_at() + self.app.ping_interval as i64
    }

    /// Inactive *and* already pinged: the client owes us a pong it never sent.
    pub fn is_stale(&self) -> bool {
        !self.is_active() && self.has_been_pinged.load(Ordering::Relaxed)
    }

    pub fn uses_control_frames(&self) -> bool {
        self.uses_control_frames.load(Ordering::Relaxed)
    }

    /// Note that this client speaks WebSocket ping/pong, so liveness should be
    /// tracked with control frames rather than `pusher:ping` messages.
    pub fn set_uses_control_frames(&self) {
        self.uses_control_frames.store(true, Ordering::Relaxed);
    }

    /// Consume one unit of the message rate limit.
    ///
    /// Returns `false` when the connection is over its limit for the current
    /// window. Mirrors Reverb, which tests the limit *before* recording the
    /// message, so exactly `max_attempts` messages succeed per window.
    pub fn check_rate_limit(&self) -> bool {
        let config = &self.app.rate_limiting;

        if !config.enabled {
            return true;
        }

        let now = now();
        let mut window = self.limiter.lock();

        if now.saturating_sub(window.started_at) >= config.decay_seconds as i64 {
            window.count = 0;
            window.started_at = now;
        }

        if window.count >= config.max_attempts {
            return false;
        }

        window.count += 1;

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ClientEvents, RateLimiting};

    fn app(rate_limiting: RateLimiting) -> Arc<Application> {
        Arc::new(Application {
            id: "app-id".into(),
            key: "app-key".into(),
            secret: "app-secret".into(),
            ping_interval: 60,
            activity_timeout: 30,
            allowed_origins: vec!["*".into()],
            max_message_size: 10_000,
            max_connections: None,
            accept_client_events_from: ClientEvents::Members,
            rate_limiting,
        })
    }

    fn conn(rate_limiting: RateLimiting) -> (Conn, mpsc::Receiver<Outbound>) {
        let (tx, rx) = mpsc::channel(16);

        (Conn::new(app(rate_limiting), None, tx, Arc::new(Telemetry::disabled())), rx)
    }

    #[test]
    fn generates_pusher_compatible_socket_ids() {
        let id = generate_id();
        let (left, right) = id.split_once('.').expect("socket id should contain a dot");

        assert!(left.parse::<u32>().is_ok());
        assert!(right.parse::<u32>().is_ok());
    }

    #[test]
    fn allows_exactly_max_attempts_per_window() {
        let (conn, _rx) = conn(RateLimiting {
            enabled: true,
            max_attempts: 3,
            decay_seconds: 60,
            terminate_on_limit: false,
        });

        assert!(conn.check_rate_limit());
        assert!(conn.check_rate_limit());
        assert!(conn.check_rate_limit());
        assert!(!conn.check_rate_limit());
    }

    #[test]
    fn does_not_rate_limit_when_disabled() {
        let (conn, _rx) = conn(RateLimiting::default());

        for _ in 0..1_000 {
            assert!(conn.check_rate_limit());
        }
    }

    #[test]
    fn disconnects_a_client_that_falls_behind() {
        let (tx, rx) = mpsc::channel(2);
        let conn =
            Conn::new(app(RateLimiting::default()), None, tx, Arc::new(Telemetry::disabled()));

        conn.send("one");
        conn.send("two");
        conn.send("three"); // Queue is full, so the connection is terminated.

        assert!(conn.is_closed());
        assert_eq!(conn.dropped_frames(), 1);
        drop(rx);
    }

    #[test]
    fn tracks_liveness() {
        let (conn, _rx) = conn(RateLimiting::default());

        assert!(conn.is_active());
        assert!(!conn.is_stale());

        conn.last_seen_at.store(now() - 120, Ordering::Relaxed);
        assert!(!conn.is_active());
        assert!(!conn.is_stale(), "an un-pinged connection is never stale");

        conn.mark_pinged();
        assert!(conn.is_stale());

        conn.touch();
        assert!(conn.is_active());
        assert!(!conn.is_stale());
    }
}
