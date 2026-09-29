//! Telemetry: per-application counters and the relay that carries Reverb's
//! events back to Laravel.
//!
//! Reverb dispatches five events onto Laravel's event bus from inside the
//! server process. `reverb-rs` is a separate process, so it publishes the same
//! events to Redis instead; the companion `reverb-rs:relay` command
//! re-dispatches them in your application, where Pulse recorders, Telescope
//! and your own listeners pick them up unchanged.
//!
//! Message events fire once per frame, so they are counted always and relayed
//! only when explicitly enabled.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use rand::Rng;
use serde_json::{Value, json};
use tokio::sync::mpsc;

/// The events Reverb dispatches, named as they appear on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    MessageSent,
    MessageReceived,
    ChannelCreated,
    ChannelRemoved,
    ConnectionPruned,
}

impl EventKind {
    pub const ALL: [EventKind; 5] = [
        Self::MessageSent,
        Self::MessageReceived,
        Self::ChannelCreated,
        Self::ChannelRemoved,
        Self::ConnectionPruned,
    ];

    /// The low-volume events, safe to relay without thinking about it.
    pub const LIFECYCLE: [EventKind; 3] =
        [Self::ChannelCreated, Self::ChannelRemoved, Self::ConnectionPruned];

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MessageSent => "message_sent",
            Self::MessageReceived => "message_received",
            Self::ChannelCreated => "channel_created",
            Self::ChannelRemoved => "channel_removed",
            Self::ConnectionPruned => "connection_pruned",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value.trim())
    }

    fn bit(&self) -> u8 {
        match self {
            Self::MessageSent => 1,
            Self::MessageReceived => 1 << 1,
            Self::ChannelCreated => 1 << 2,
            Self::ChannelRemoved => 1 << 3,
            Self::ConnectionPruned => 1 << 4,
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
}

/// Counters plus the outbound side of the event relay.
#[derive(Debug)]
pub struct Telemetry {
    counters: DashMap<String, Arc<AppCounters>>,
    relay: Option<mpsc::Sender<RelayedEvent>>,
    forward: EventSet,
    /// Fraction of message events to relay, as a threshold over `u32::MAX`.
    ///
    /// The two message events fire once per frame. Relaying all of them is
    /// exact but bounded by how fast Redis accepts publishes; sampling trades
    /// exactness for headroom. Lifecycle events are never sampled.
    message_sample: u32,
    /// Events discarded because the relay queue was saturated.
    dropped: AtomicU64,
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
            dropped: AtomicU64::new(0),
        }
    }

    pub fn with_relay(forward: EventSet, relay: mpsc::Sender<RelayedEvent>) -> Self {
        Self::sampled(forward, relay, 1.0)
    }

    /// As [`with_relay`], relaying only `sample_rate` of the message events.
    pub fn sampled(forward: EventSet, relay: mpsc::Sender<RelayedEvent>, sample_rate: f64) -> Self {
        Self {
            counters: DashMap::new(),
            relay: Some(relay),
            forward,
            message_sample: (sample_rate.clamp(0.0, 1.0) * u32::MAX as f64) as u32,
            dropped: AtomicU64::new(0),
        }
    }

    /// Whether this message event survives sampling.
    fn sampled_in(&self, kind: EventKind) -> bool {
        if self.message_sample == u32::MAX {
            return true;
        }

        if !matches!(kind, EventKind::MessageSent | EventKind::MessageReceived) {
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

    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Queue an event for Laravel. Never blocks: under a backlog the event is
    /// dropped and counted rather than allowed to slow the protocol down.
    pub fn emit(&self, kind: EventKind, app_id: &str, body: Value) {
        if !self.forwards(kind) || !self.sampled_in(kind) {
            return;
        }

        let Some(relay) = &self.relay else { return };

        let event = RelayedEvent { kind, app_id: app_id.to_string(), body };

        if relay.try_send(event).is_err() && self.dropped.fetch_add(1, Ordering::Relaxed) == 0 {
            tracing::warn!("event relay queue is saturated; events are being dropped");
        }
    }

    /// Convenience for the two channel lifecycle events.
    pub fn emit_channel(&self, kind: EventKind, app_id: &str, channel: &str) {
        if self.forwards(kind) {
            self.emit(kind, app_id, json!({ "channel": channel }));
        }
    }
}

/// Batch queued events onto Redis.
///
/// Batching matters: relaying message events one publish at a time would put a
/// round trip in front of every frame.
pub async fn relay_loop(
    mut rx: mpsc::Receiver<RelayedEvent>,
    mut manager: redis::aio::ConnectionManager,
    channel: String,
    batch_size: usize,
    flush_interval: Duration,
) {
    let mut batch: Vec<Value> = Vec::with_capacity(batch_size);

    loop {
        // Block until there is something to send, then sweep up whatever else
        // has arrived rather than publishing one event at a time.
        let Some(first) = rx.recv().await else { break };

        batch.push(first.to_json());

        let deadline = tokio::time::Instant::now() + flush_interval;

        while batch.len() < batch_size {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(event)) => batch.push(event.to_json()),
                Ok(None) => break,
                Err(_) => break,
            }
        }

        let payload = Value::Array(std::mem::take(&mut batch)).to_string();
        batch = Vec::with_capacity(batch_size);

        let result: Result<u64, _> =
            redis::cmd("PUBLISH").arg(&channel).arg(&payload).query_async(&mut manager).await;

        if let Err(error) = result {
            tracing::warn!(%error, "failed to publish events to redis");
        }
    }
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

        assert_eq!(EventSet::parse("all").names().len(), 5);
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

    #[test]
    fn does_not_relay_events_that_are_not_forwarded() {
        let (tx, mut rx) = mpsc::channel(8);
        let telemetry = Telemetry::with_relay(EventSet::of([EventKind::ChannelCreated]), tx);

        telemetry.emit_channel(EventKind::ChannelCreated, "app", "test-channel");
        telemetry.emit_channel(EventKind::ChannelRemoved, "app", "test-channel");

        let event = rx.try_recv().expect("the forwarded event");

        assert_eq!(event.kind, EventKind::ChannelCreated);
        assert_eq!(event.body["channel"], "test-channel");
        assert!(rx.try_recv().is_err(), "the unforwarded event should be dropped");
    }

    #[test]
    fn drops_events_rather_than_blocking_when_saturated() {
        let (tx, _rx) = mpsc::channel(1);
        let telemetry = Telemetry::with_relay(EventSet::of([EventKind::ChannelCreated]), tx);

        for _ in 0..10 {
            telemetry.emit_channel(EventKind::ChannelCreated, "app", "test-channel");
        }

        assert!(telemetry.dropped() > 0);
    }

    #[test]
    fn samples_message_events_but_never_lifecycle_ones() {
        let (tx, mut rx) = mpsc::channel(4096);
        let telemetry = Telemetry::sampled(EventSet::of(EventKind::ALL), tx, 0.0);

        for _ in 0..100 {
            telemetry.emit(EventKind::MessageSent, "app", json!({}));
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
