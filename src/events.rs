//! Telemetry: per-application counters and the relay that carries Reverb's
//! events back to Laravel.
//!
//! Reverb dispatches five events onto Laravel's event bus from inside the
//! server process. `reverb-rs` is a separate process, so it publishes the same
//! events to Redis instead; the companion `reverb-rs:relay` command
//! re-dispatches them in your application, where Pulse recorders, Telescope
//! and your own listeners pick them up unchanged. Two more, `member_added` and
//! `member_removed`, report presence changes Reverb only ever tells clients.
//!
//! Message events fire once per frame, so they are counted always and relayed
//! only when explicitly enabled. They share one ordered queue with everything
//! else, but only up to their own budget: the rest of the queue is kept for
//! the rare events a listener cannot afford to miss.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use rand::Rng;
use serde_json::{Value, json};
use tokio::sync::mpsc;

/// Queue slots only lifecycle and presence events may use, so a flood of
/// message events can never crowd them out.
const RESERVED_SLOTS: usize = 10_000;

/// Bytes charged to a message event on top of its frame.
const EVENT_OVERHEAD: usize = 64;

/// Seconds between warnings about dropped events.
const DROP_WARNING_INTERVAL: i64 = 10;

/// Pauses before each attempt to publish a batch. Redis restarting is the
/// common failure, and a batch can carry presence changes nothing else
/// reports, so it is worth a short wait before giving one up.
const PUBLISH_ATTEMPTS: [Duration; 3] =
    [Duration::ZERO, Duration::from_millis(250), Duration::from_secs(1)];

/// The events relayed to Laravel, named as they appear on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    MessageSent,
    MessageReceived,
    ChannelCreated,
    ChannelRemoved,
    ConnectionPruned,
    /// A user's first connection joined a presence channel. Not a Reverb event.
    MemberAdded,
    /// A user's last connection left a presence channel. Not a Reverb event.
    MemberRemoved,
}

impl EventKind {
    pub const ALL: [EventKind; 7] = [
        Self::MessageSent,
        Self::MessageReceived,
        Self::ChannelCreated,
        Self::ChannelRemoved,
        Self::ConnectionPruned,
        Self::MemberAdded,
        Self::MemberRemoved,
    ];

    /// The low-volume events, safe to relay without thinking about it.
    pub const LIFECYCLE: [EventKind; 5] = [
        Self::ChannelCreated,
        Self::ChannelRemoved,
        Self::ConnectionPruned,
        Self::MemberAdded,
        Self::MemberRemoved,
    ];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MessageSent => "message_sent",
            Self::MessageReceived => "message_received",
            Self::ChannelCreated => "channel_created",
            Self::ChannelRemoved => "channel_removed",
            Self::ConnectionPruned => "connection_pruned",
            Self::MemberAdded => "member_added",
            Self::MemberRemoved => "member_removed",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value.trim())
    }

    /// The two events that fire once per frame.
    pub fn is_message(&self) -> bool {
        matches!(self, Self::MessageSent | Self::MessageReceived)
    }

    fn bit(&self) -> u8 {
        match self {
            Self::MessageSent => 1,
            Self::MessageReceived => 1 << 1,
            Self::ChannelCreated => 1 << 2,
            Self::ChannelRemoved => 1 << 3,
            Self::ConnectionPruned => 1 << 4,
            Self::MemberAdded => 1 << 5,
            Self::MemberRemoved => 1 << 6,
        }
    }
}

/// A set of event kinds, as a bitmask so the hot path is a single `AND`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EventSet(u8);

impl EventSet {
    pub fn none() -> Self {
        Self(0)
    }

    pub fn of(kinds: impl IntoIterator<Item = EventKind>) -> Self {
        Self(kinds.into_iter().fold(0, |mask, kind| mask | kind.bit()))
    }

    /// Parse a comma-separated list, e.g. `channel_created,message_sent`.
    /// `all` selects every event and `none` selects nothing.
    pub fn parse(value: &str) -> Self {
        match value.trim() {
            "all" => Self::of(EventKind::ALL),
            "none" | "" => Self::none(),
            list => Self::of(list.split(',').filter_map(EventKind::parse)),
        }
    }

    pub fn contains(&self, kind: EventKind) -> bool {
        self.0 & kind.bit() != 0
    }

    pub fn is_empty(&self) -> bool {
        self.0 == 0
    }

    pub fn names(&self) -> Vec<&'static str> {
        EventKind::ALL.into_iter().filter(|k| self.contains(*k)).map(|kind| kind.as_str()).collect()
    }
}

/// Cumulative message counts for one application, since the process started.
#[derive(Debug, Default)]
pub struct AppCounters {
    sent: AtomicU64,
    received: AtomicU64,
}

impl AppCounters {
    /// Record `count` frames delivered to clients.
    pub fn record_sent(&self, count: u64) {
        self.sent.fetch_add(count, Ordering::Relaxed);
    }

    /// Record one frame accepted from a client.
    pub fn record_received(&self) {
        self.received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn sent(&self) -> u64 {
        self.sent.load(Ordering::Relaxed)
    }

    pub fn received(&self) -> u64 {
        self.received.load(Ordering::Relaxed)
    }
}

/// One event on its way to Laravel.
#[derive(Debug, Clone)]
pub struct RelayedEvent {
    pub kind: EventKind,
    pub app_id: String,
    pub body: Value,
}

impl RelayedEvent {
    /// The envelope published to Redis.
    ///
    /// This is the contract the `reverb-rs/laravel` package decodes, so
    /// changing its shape is a breaking change on both sides.
    pub fn to_json(&self) -> Value {
        json!({
            "event": self.kind.as_str(),
            "application": self.app_id,
            "payload": self.body,
        })
    }

    /// What a message event costs against the byte budget: the frame it
    /// carries, which is the part that grows.
    fn weight(&self) -> usize {
        EVENT_OVERHEAD + self.body.get("message").and_then(Value::as_str).map_or(0, str::len)
    }
}

/// How the relay is fed, from `REVERB_EVENTS_*`.
#[derive(Debug, Clone)]
pub struct RelayOptions {
    pub forward: EventSet,
    /// Fraction of the two message events to relay, 0.0 to 1.0.
    pub message_sample_rate: f64,
    /// Whether message events carry the frame itself.
    pub message_bodies: bool,
    /// How many message events, and how many bytes of them, may be queued.
    pub queue_depth: usize,
    pub queue_bytes: usize,
}

impl RelayOptions {
    pub fn new(forward: EventSet) -> Self {
        Self {
            forward,
            message_sample_rate: 1.0,
            message_bodies: true,
            queue_depth: 100_000,
            queue_bytes: 64 * 1024 * 1024,
        }
    }
}

impl From<&crate::config::EventsConfig> for RelayOptions {
    fn from(config: &crate::config::EventsConfig) -> Self {
        Self {
            forward: config.forward,
            message_sample_rate: config.message_sample_rate,
            message_bodies: config.message_bodies,
            queue_depth: config.queue_depth,
            queue_bytes: config.queue_bytes,
        }
    }
}

/// Bookkeeping shared by both ends of the relay queue.
#[derive(Debug, Default)]
struct Backlog {
    /// Message events queued, and the bytes they hold.
    messages: AtomicUsize,
    bytes: AtomicUsize,
    /// Events lost to a full queue or to Redis refusing a batch.
    dropped: AtomicU64,
}

impl Backlog {
    fn release(&self, event: &RelayedEvent) {
        if event.kind.is_message() {
            self.messages.fetch_sub(1, Ordering::Relaxed);
            self.bytes.fetch_sub(event.weight(), Ordering::Relaxed);
        }
    }
}

/// The sending half of the relay queue.
#[derive(Debug)]
struct RelaySender {
    tx: mpsc::Sender<RelayedEvent>,
    backlog: Arc<Backlog>,
    max_messages: usize,
    max_bytes: usize,
}

/// The receiving half of the relay queue, drained by [`relay_loop`].
#[derive(Debug)]
pub struct RelayReceiver {
    rx: mpsc::Receiver<RelayedEvent>,
    backlog: Arc<Backlog>,
}

impl RelayReceiver {
    pub async fn recv(&mut self) -> Option<RelayedEvent> {
        let event = self.rx.recv().await?;
        self.backlog.release(&event);

        Some(event)
    }

    pub fn try_recv(&mut self) -> Result<RelayedEvent, mpsc::error::TryRecvError> {
        let event = self.rx.try_recv()?;
        self.backlog.release(&event);

        Ok(event)
    }

    fn record_lost(&self, count: usize) {
        self.backlog.dropped.fetch_add(count as u64, Ordering::Relaxed);
    }
}

/// Counters plus the outbound side of the event relay.
#[derive(Debug)]
pub struct Telemetry {
    counters: DashMap<String, Arc<AppCounters>>,
    relay: Option<RelaySender>,
    forward: EventSet,
    /// Fraction of message events to relay, as a threshold over `u32::MAX`.
    ///
    /// The two message events fire once per frame. Relaying all of them is
    /// exact but bounded by how fast Redis accepts publishes; sampling trades
    /// exactness for headroom. Lifecycle events are never sampled.
    message_sample: u32,
    message_bodies: bool,
    /// When a dropped event was last reported, in Unix seconds.
    last_drop_warning: AtomicI64,
}

impl Default for Telemetry {
    fn default() -> Self {
        Self::disabled()
    }
}

impl Telemetry {
    /// Counters only: nothing is relayed to Laravel.
    pub fn disabled() -> Self {
        Self {
            counters: DashMap::new(),
            relay: None,
            forward: EventSet::none(),
            message_sample: u32::MAX,
            message_bodies: true,
            last_drop_warning: AtomicI64::new(0),
        }
    }

    /// Relay the events `options` forwards, returning the queue's receiving
    /// end for [`relay_loop`] to drain.
    pub fn relayed(options: RelayOptions) -> (Self, RelayReceiver) {
        let max_messages = options.queue_depth.max(1);
        let (tx, rx) = mpsc::channel(max_messages + RESERVED_SLOTS);
        let backlog = Arc::new(Backlog::default());

        let telemetry = Self {
            counters: DashMap::new(),
            relay: Some(RelaySender {
                tx,
                backlog: backlog.clone(),
                max_messages,
                max_bytes: options.queue_bytes,
            }),
            forward: options.forward,
            message_sample: (options.message_sample_rate.clamp(0.0, 1.0) * u32::MAX as f64) as u32,
            message_bodies: options.message_bodies,
            last_drop_warning: AtomicI64::new(0),
        };

        (telemetry, RelayReceiver { rx, backlog })
    }

    /// Whether this message event survives sampling.
    fn sampled_in(&self, kind: EventKind) -> bool {
        if self.message_sample == u32::MAX || !kind.is_message() {
            return true;
        }

        rand::rng().random::<u32>() < self.message_sample
    }

    /// The counters for an application, created on first use.
    ///
    /// Resolve this once per connection rather than per frame — the hot path
    /// should never hash an application ID.
    pub fn counters(&self, app_id: &str) -> Arc<AppCounters> {
        if let Some(existing) = self.counters.get(app_id) {
            return existing.clone();
        }

        self.counters.entry(app_id.to_string()).or_default().clone()
    }

    pub fn forwards(&self, kind: EventKind) -> bool {
        self.forward.contains(kind)
    }

    pub fn forwarded(&self) -> EventSet {
        self.forward
    }

    /// Events lost to a saturated queue or to Redis refusing a batch.
    pub fn dropped(&self) -> u64 {
        self.relay.as_ref().map_or(0, |relay| relay.backlog.dropped.load(Ordering::Relaxed))
    }

    /// Queue an event for Laravel. Never blocks: under a backlog the event is
    /// dropped and counted rather than allowed to slow the protocol down.
    pub fn emit(&self, kind: EventKind, app_id: &str, body: Value) {
        if self.forwards(kind) && self.sampled_in(kind) {
            self.enqueue(kind, app_id, body);
        }
    }

    /// Queue an event that has already passed the forwarding and sampling
    /// checks.
    fn enqueue(&self, kind: EventKind, app_id: &str, body: Value) {
        let Some(relay) = &self.relay else { return };

        let event = RelayedEvent { kind, app_id: app_id.to_string(), body };

        // Message events may fill only their own share of the queue; the
        // remainder is kept for the events listeners cannot do without.
        if kind.is_message() {
            let weight = event.weight();
            let queued = relay.backlog.messages.fetch_add(1, Ordering::Relaxed);
            let bytes = relay.backlog.bytes.fetch_add(weight, Ordering::Relaxed);

            if queued >= relay.max_messages || bytes + weight > relay.max_bytes {
                relay.backlog.release(&event);
                self.record_drop(relay, kind);
                return;
            }
        }

        if let Err(error) = relay.tx.try_send(event) {
            let event = match error {
                mpsc::error::TrySendError::Full(event)
                | mpsc::error::TrySendError::Closed(event) => event,
            };

            relay.backlog.release(&event);
            self.record_drop(relay, kind);
        }
    }

    /// Queue a message event, building its body only if it will be relayed.
    pub fn emit_message(
        &self,
        kind: EventKind,
        app_id: &str,
        socket_id: &str,
        origin: Option<&str>,
        message: &str,
    ) {
        if !self.forwards(kind) || !self.sampled_in(kind) {
            return;
        }

        let body = match self.message_bodies {
            true => json!({ "socket_id": socket_id, "origin": origin, "message": message }),
            false => json!({ "socket_id": socket_id, "origin": origin }),
        };

        self.enqueue(kind, app_id, body);
    }

    /// Convenience for the two channel lifecycle events.
    pub fn emit_channel(&self, kind: EventKind, app_id: &str, channel: &str) {
        if self.forwards(kind) {
            self.emit(kind, app_id, json!({ "channel": channel }));
        }
    }

    /// Count a lost event, warning at most every few seconds rather than
    /// only for the first loss of the process's life.
    fn record_drop(&self, relay: &RelaySender, kind: EventKind) {
        let total = relay.backlog.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        let now = unix_now();
        let last = self.last_drop_warning.load(Ordering::Relaxed);

        if now - last >= DROP_WARNING_INTERVAL
            && self
                .last_drop_warning
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            tracing::warn!(
                event = kind.as_str(),
                dropped_total = total,
                "event relay queue is saturated; events are being dropped"
            );
        }
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Batch queued events onto Redis until `shutdown` resolves, then publish
/// whatever is still queued and return.
///
/// Batching matters: relaying message events one publish at a time would put a
/// round trip in front of every frame. Draining on shutdown matters too: the
/// `channel_removed` and `member_removed` events a shutdown produces are the
/// last thing a listener hears about every connection.
pub async fn relay_loop(
    mut rx: RelayReceiver,
    mut manager: redis::aio::ConnectionManager,
    channel: String,
    batch_size: usize,
    flush_interval: Duration,
    shutdown: impl Future<Output = ()>,
) {
    let batch_size = batch_size.max(1);
    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);

    tokio::pin!(shutdown);

    let mut stopping = false;

    while !stopping {
        // Block until there is something to send, then sweep up whatever else
        // has arrived rather than publishing one event at a time.
        let first = tokio::select! {
            event = rx.recv() => match event {
                Some(event) => event,
                None => return,
            },
            () = &mut shutdown => break,
        };

        batch.push(first.to_json());

        let deadline = tokio::time::Instant::now() + flush_interval;

        while batch.len() < batch_size {
            tokio::select! {
                next = tokio::time::timeout_at(deadline, rx.recv()) => match next {
                    Ok(Some(event)) => batch.push(event.to_json()),
                    Ok(None) | Err(_) => break,
                },
                () = &mut shutdown => {
                    stopping = true;
                    break;
                }
            }
        }

        publish(&mut manager, &channel, std::mem::take(&mut batch), &rx).await;
    }

    while let Ok(event) = rx.try_recv() {
        batch.push(event.to_json());

        if batch.len() == batch_size {
            publish(&mut manager, &channel, std::mem::take(&mut batch), &rx).await;
        }
    }

    if !batch.is_empty() {
        publish(&mut manager, &channel, batch, &rx).await;
    }
}

/// Publish one batch, retrying briefly before giving it up.
async fn publish(
    manager: &mut redis::aio::ConnectionManager,
    channel: &str,
    batch: Vec<Value>,
    rx: &RelayReceiver,
) {
    let count = batch.len();
    let payload = Value::Array(batch).to_string();

    for (attempt, pause) in PUBLISH_ATTEMPTS.iter().enumerate() {
        if !pause.is_zero() {
            tokio::time::sleep(*pause).await;
        }

        let result: Result<u64, _> =
            redis::cmd("PUBLISH").arg(channel).arg(&payload).query_async(manager).await;

        match result {
            Ok(_) => return,
            Err(error) => tracing::warn!(%error, attempt, "failed to publish events to redis"),
        }
    }

    rx.record_lost(count);

    tracing::error!(events = count, "gave up publishing a batch of events to redis");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_event_lists() {
        let set = EventSet::parse("channel_created, connection_pruned");

        assert!(set.contains(EventKind::ChannelCreated));
        assert!(set.contains(EventKind::ConnectionPruned));
        assert!(!set.contains(EventKind::MessageSent));

        assert_eq!(EventSet::parse("all").names().len(), 7);
        assert!(EventSet::parse("none").is_empty());
        assert!(EventSet::parse("").is_empty());
        assert!(EventSet::parse("nonsense").is_empty());
    }

    #[test]
    fn counts_messages_per_application() {
        let telemetry = Telemetry::disabled();
        let counters = telemetry.counters("app-one");

        counters.record_sent(5);
        counters.record_received();

        assert_eq!(counters.sent(), 5);
        assert_eq!(counters.received(), 1);

        // The same application resolves to the same counters.
        assert_eq!(telemetry.counters("app-one").sent(), 5);
        assert_eq!(telemetry.counters("app-two").sent(), 0);
    }

    fn relayed(forward: EventSet) -> (Telemetry, RelayReceiver) {
        Telemetry::relayed(RelayOptions::new(forward))
    }

    #[test]
    fn does_not_relay_events_that_are_not_forwarded() {
        let (telemetry, mut rx) = relayed(EventSet::of([EventKind::ChannelCreated]));

        telemetry.emit_channel(EventKind::ChannelCreated, "app", "test-channel");
        telemetry.emit_channel(EventKind::ChannelRemoved, "app", "test-channel");

        let event = rx.try_recv().expect("the forwarded event");

        assert_eq!(event.kind, EventKind::ChannelCreated);
        assert_eq!(event.body["channel"], "test-channel");
        assert!(rx.try_recv().is_err(), "the unforwarded event should be dropped");
    }

    #[test]
    fn drops_events_rather_than_blocking_when_saturated() {
        let (telemetry, _rx) = relayed(EventSet::of([EventKind::ChannelCreated]));

        for _ in 0..RESERVED_SLOTS + 110_000 {
            telemetry.emit_channel(EventKind::ChannelCreated, "app", "test-channel");
        }

        assert_eq!(telemetry.dropped(), 10_000);
    }

    #[test]
    fn keeps_room_for_lifecycle_events_behind_a_message_flood() {
        let options =
            RelayOptions { queue_depth: 4, ..RelayOptions::new(EventSet::of(EventKind::ALL)) };
        let (telemetry, mut rx) = Telemetry::relayed(options);

        for _ in 0..100 {
            telemetry.emit_message(EventKind::MessageSent, "app", "1.1", None, "frame");
        }

        telemetry.emit_channel(EventKind::ChannelRemoved, "app", "presence-servers.1");

        assert_eq!(telemetry.dropped(), 96, "only the message events beyond their share go");

        let kinds: Vec<EventKind> =
            std::iter::from_fn(|| rx.try_recv().ok()).map(|e| e.kind).collect();

        // Order is preserved: the removal still follows the messages before it.
        assert_eq!(kinds.len(), 5);
        assert_eq!(kinds.last(), Some(&EventKind::ChannelRemoved));

        // Draining frees the share for more messages.
        telemetry.emit_message(EventKind::MessageSent, "app", "1.1", None, "frame");
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn bounds_queued_message_events_by_bytes() {
        let options =
            RelayOptions { queue_bytes: 1_000, ..RelayOptions::new(EventSet::of(EventKind::ALL)) };
        let (telemetry, mut rx) = Telemetry::relayed(options);
        let frame = "x".repeat(400);

        for _ in 0..5 {
            telemetry.emit_message(EventKind::MessageSent, "app", "1.1", None, &frame);
        }

        assert_eq!(telemetry.dropped(), 3, "two 464-byte events fit in 1,000 bytes");

        rx.try_recv().expect("first");
        rx.try_recv().expect("second");
        telemetry.emit_message(EventKind::MessageSent, "app", "1.1", None, &frame);
        assert!(rx.try_recv().is_ok(), "consumed events release their bytes");
    }

    #[test]
    fn can_relay_message_events_without_their_frames() {
        let options = RelayOptions {
            message_bodies: false,
            ..RelayOptions::new(EventSet::of(EventKind::ALL))
        };
        let (telemetry, mut rx) = Telemetry::relayed(options);

        telemetry.emit_message(EventKind::MessageSent, "app", "1.1", Some("o"), "secret output");

        let event = rx.try_recv().expect("the event is still relayed");

        assert_eq!(event.body, json!({ "socket_id": "1.1", "origin": "o" }));
    }

    #[test]
    fn samples_message_events_but_never_lifecycle_ones() {
        let options = RelayOptions {
            message_sample_rate: 0.0,
            ..RelayOptions::new(EventSet::of(EventKind::ALL))
        };
        let (telemetry, mut rx) = Telemetry::relayed(options);

        for _ in 0..100 {
            telemetry.emit_message(EventKind::MessageSent, "app", "1.1", None, "{}");
        }

        assert!(rx.try_recv().is_err(), "a zero sample rate relays no message events");

        telemetry.emit_channel(EventKind::ChannelCreated, "app", "test-channel");

        assert_eq!(rx.try_recv().expect("lifecycle event").kind, EventKind::ChannelCreated);
    }

    #[test]
    fn serializes_an_event_for_the_relay() {
        let event = RelayedEvent {
            kind: EventKind::MessageSent,
            app_id: "app-id".into(),
            body: json!({ "socket_id": "1.2", "message": "{}" }),
        };

        assert_eq!(
            event.to_json(),
            json!({
                "event": "message_sent",
                "application": "app-id",
                "payload": { "socket_id": "1.2", "message": "{}" },
            })
        );
    }
}
