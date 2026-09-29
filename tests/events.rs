//! The events reverb-rs relays back to Laravel.
//!
//! Reverb dispatches five events onto Laravel's event bus from inside the
//! server process. These tests assert that reverb-rs emits the same five at
//! the same moments, with the payloads the companion package needs to rebuild
//! the real event objects.

mod support;

use std::sync::Arc;

use reverb_rs::events::{EventKind, EventSet, RelayedEvent};
use serde_json::json;
use support::*;

/// Drain whatever has been relayed so far.
fn drain(rx: &mut tokio::sync::mpsc::Receiver<RelayedEvent>) -> Vec<RelayedEvent> {
    let mut events = Vec::new();

    while let Ok(event) = rx.try_recv() {
        events.push(event);
    }

    events
}

fn kinds(events: &[RelayedEvent]) -> Vec<EventKind> {
    events.iter().map(|event| event.kind).collect()
}

#[tokio::test]
async fn relays_nothing_that_was_not_asked_for() {
    let (server, mut rx) = start_with_events(application(), EventSet::none()).await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn relays_channel_lifecycle() {
    let (server, mut rx) =
        start_with_events(application(), EventSet::of(EventKind::LIFECYCLE)).await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let created = drain(&mut rx);

    assert_eq!(kinds(&created), vec![EventKind::ChannelCreated]);
    assert_eq!(created[0].app_id, APP_ID);
    assert_eq!(created[0].body["channel"], "test-channel");

    // The channel is announced as removed when its last subscriber leaves.
    drop(socket);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let removed = drain(&mut rx);

    assert_eq!(kinds(&removed), vec![EventKind::ChannelRemoved]);
    assert_eq!(removed[0].body["channel"], "test-channel");
}

#[tokio::test]
async fn announces_a_channel_once_however_many_subscribers_arrive() {
    let (server, mut rx) =
        start_with_events(application(), EventSet::of(EventKind::LIFECYCLE)).await;

    for _ in 0..3 {
        let (mut socket, id) = server.connect().await;
        subscribe(&mut socket, &id, "test-channel").await;
        std::mem::forget(socket);
    }

    assert_eq!(kinds(&drain(&mut rx)), vec![EventKind::ChannelCreated]);
}

#[tokio::test]
async fn relays_messages_in_both_directions() {
    let (server, mut rx) = start_with_events(
        application(),
        EventSet::of([EventKind::MessageSent, EventKind::MessageReceived]),
    )
    .await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let events = drain(&mut rx);

    // The acknowledgement and the subscription reply are both sends; the
    // subscribe frame is the one receive, recorded after it was handled.
    let sent: Vec<_> = events.iter().filter(|e| e.kind == EventKind::MessageSent).collect();
    let received: Vec<_> = events.iter().filter(|e| e.kind == EventKind::MessageReceived).collect();

    assert_eq!(sent.len(), 2, "connection_established and subscription_succeeded");
    assert_eq!(received.len(), 1);

    assert_eq!(sent[0].body["socket_id"], id);
    assert!(
        sent[0].body["message"].as_str().unwrap().contains("connection_established"),
        "the first send is the acknowledgement"
    );
    assert!(received[0].body["message"].as_str().unwrap().contains("pusher:subscribe"));
}

#[tokio::test]
async fn does_not_relay_a_message_that_was_rejected() {
    let (server, mut rx) =
        start_with_events(application(), EventSet::of([EventKind::MessageReceived])).await;

    let (mut socket, _) = server.connect().await;

    send(&mut socket, json!({ "event": "pusher:nonsense" })).await;
    next_text(&mut socket).await; // the error frame

    // Reverb dispatches MessageReceived only once handling succeeded.
    assert!(drain(&mut rx).is_empty());
}

#[tokio::test]
async fn relays_a_pruned_connection_with_its_presence_data() {
    let mut app = application();
    // Every connection counts as inactive the moment it is seen.
    app.ping_interval = 0;

    let (server, mut rx) =
        start_with_events(app, EventSet::of([EventKind::ConnectionPruned])).await;

    let (mut socket, id) = server.connect().await;
    subscribe_with_data(&mut socket, &id, "presence-test", Some(json!({ "user_id": 77 }))).await;

    let state = Arc::new(server);

    // The first sweep pings, the second prunes what never replied.
    reverb_rs::sweep(&state.server);
    assert!(drain(&mut rx).is_empty(), "a connection is not stale until it owes a pong");

    reverb_rs::sweep(&state.server);

    let pruned = drain(&mut rx);

    assert_eq!(kinds(&pruned), vec![EventKind::ConnectionPruned]);
    assert_eq!(pruned[0].body["socket_id"], id);
    assert_eq!(pruned[0].body["data"]["user_id"], 77);
}

#[tokio::test]
async fn counts_messages_whether_or_not_they_are_relayed() {
    let (server, _rx) = start_with_events(application(), EventSet::none()).await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let counters = server.server.telemetry.counters(APP_ID);

    assert_eq!(counters.sent(), 2, "connection_established and subscription_succeeded");
    assert_eq!(counters.received(), 1);
}

/// The envelope the `reverb-rs/laravel` package decodes.
///
/// These are deliberately literal. The PHP side reads these exact keys, and it
/// is not compiled against this crate, so nothing else would catch a rename.
mod wire_contract {
    use super::*;
    use reverb_rs::events::RelayedEvent;

    fn envelope(kind: EventKind, body: serde_json::Value) -> serde_json::Value {
        RelayedEvent { kind, app_id: "123456".into(), body }.to_json()
    }

    #[test]
    fn names_each_event_the_way_the_relay_expects() {
        let names: Vec<&str> = EventKind::ALL.iter().map(|k| k.as_str()).collect();

        assert_eq!(
            names,
            vec![
                "message_sent",
                "message_received",
                "channel_created",
                "channel_removed",
                "connection_pruned",
            ]
        );
    }

    #[test]
    fn wraps_every_event_in_the_same_envelope() {
        assert_eq!(
            envelope(EventKind::ChannelCreated, json!({ "channel": "test-channel" })),
            json!({
                "event": "channel_created",
                "application": "123456",
                "payload": { "channel": "test-channel" },
            })
        );
    }

    #[tokio::test]
    async fn message_events_carry_the_socket_origin_and_frame() {
        let (server, mut rx) = start_with_events(
            application(),
            EventSet::of([EventKind::MessageSent, EventKind::MessageReceived]),
        )
        .await;

        let (mut socket, id) = server.connect_with_origin(Some("https://laravel.test")).await;
        subscribe(&mut socket, &id, "test-channel").await;

        for event in drain(&mut rx) {
            let body = &event.to_json()["payload"];

            assert_eq!(body["socket_id"], id);
            assert_eq!(body["origin"], "https://laravel.test");
            assert!(body["message"].is_string(), "the frame is relayed verbatim");
        }
    }

    #[tokio::test]
    async fn a_pruned_connection_carries_its_channel_data() {
        let mut app = application();
        app.ping_interval = 0;

        let (server, mut rx) =
            start_with_events(app, EventSet::of([EventKind::ConnectionPruned])).await;

        let (mut socket, id) = server.connect().await;
        subscribe_with_data(&mut socket, &id, "presence-test", Some(json!({ "user_id": 5 }))).await;

        let state = Arc::new(server);
        reverb_rs::sweep(&state.server);
        reverb_rs::sweep(&state.server);

        let pruned = drain(&mut rx);
        let body = &pruned[0].to_json()["payload"];

        assert_eq!(body["socket_id"], id);
        assert_eq!(body["data"], json!({ "user_id": 5 }));
    }
}
