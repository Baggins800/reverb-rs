//! End-to-end protocol tests.
//!
//! The expected frames are taken verbatim from Laravel Reverb's own test suite
//! (`tests/Feature/Protocols/Pusher/Reverb/ServerTest.php`), so a passing run
//! means a client cannot tell the two servers apart.

mod support;

use serde_json::{Value, json};
use support::*;

#[tokio::test]
async fn acknowledges_a_new_connection() {
    let server = start().await;
    let mut socket = server.open(APP_KEY, None).await;

    let frame = next_json(&mut socket).await;
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();

    assert_eq!(frame["event"], "pusher:connection_established");
    assert_eq!(data["activity_timeout"], 30);
    assert!(
        data["socket_id"].as_str().unwrap().contains('.'),
        "socket ids take Pusher's `%d.%d` form"
    );
}

#[tokio::test]
async fn rejects_an_unknown_application_key() {
    let server = start().await;
    let mut socket = server.open("does-not-exist", None).await;

    let frame = next_json(&mut socket).await;

    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(
        frame["data"].as_str().unwrap(),
        r#"{"code":4001,"message":"Application does not exist"}"#
    );
}

#[tokio::test]
async fn subscribes_to_a_public_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    assert_eq!(
        subscribe(&mut socket, &id, "test-channel").await,
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"test-channel"}"#
    );
    assert_eq!(server.server.registry.for_app(APP_ID).find("test-channel").unwrap().len(), 1);
}

#[tokio::test]
async fn subscribes_to_a_private_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    assert_eq!(
        subscribe(&mut socket, &id, "private-test-channel").await,
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-test-channel"}"#
    );
}

#[tokio::test]
async fn rejects_a_private_channel_with_an_invalid_signature() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    send(
        &mut socket,
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": "private-test-channel", "auth": "reverb-key:not-a-signature" },
        }),
    )
    .await;

    let frame = next_json(&mut socket).await;

    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(
        frame["data"].as_str().unwrap(),
        r#"{"code":4009,"message":"Connection is unauthorized"}"#
    );
}

#[tokio::test]
async fn subscribes_to_a_presence_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    let data = json!({ "user_id": 1, "user_info": { "name": "Test User" } });
    let response = subscribe_with_data(&mut socket, &id, "presence-test-channel", Some(data)).await;

    assert!(response.contains("pusher_internal:subscription_succeeded"));
    assert!(response.contains(r#"\"hash\":{\"1\":{\"name\":\"Test User\"}}"#));
}

#[tokio::test]
async fn notifies_presence_subscribers_when_a_member_joins() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    let data = json!({ "user_id": 1, "user_info": { "name": "Test User 1" } });
    subscribe_with_data(&mut one, &one_id, "presence-test-channel", Some(data)).await;

    let (mut two, two_id) = server.connect().await;
    let data = json!({ "user_id": 2, "user_info": { "name": "Test User 2" } });
    subscribe_with_data(&mut two, &two_id, "presence-test-channel", Some(data)).await;

    assert_eq!(
        next_text(&mut one).await,
        r#"{"event":"pusher_internal:member_added","data":"{\"user_id\":2,\"user_info\":{\"name\":\"Test User 2\"}}","channel":"presence-test-channel"}"#
    );
}

#[tokio::test]
async fn notifies_presence_subscribers_when_a_member_leaves() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    let data = json!({ "user_id": 1, "user_info": { "name": "Test User 1" } });
    subscribe_with_data(&mut one, &one_id, "presence-test-channel", Some(data)).await;

    let (mut two, two_id) = server.connect().await;
    let data = json!({ "user_id": 2, "user_info": { "name": "Test User 2" } });
    subscribe_with_data(&mut two, &two_id, "presence-test-channel", Some(data)).await;

    // Consume the join notice before the departure.
    next_text(&mut one).await;

    drop(two);

    assert_eq!(
        next_text(&mut one).await,
        r#"{"event":"pusher_internal:member_removed","data":"{\"user_id\":2}","channel":"presence-test-channel"}"#
    );
}

#[tokio::test]
async fn does_not_announce_a_user_who_is_already_present() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    let data = json!({ "user_id": 1, "user_info": { "name": "Test User" } });
    subscribe_with_data(&mut one, &one_id, "presence-test-channel", Some(data.clone())).await;

    // The same user joining from a second device is not a new member.
    let (mut two, two_id) = server.connect().await;
    subscribe_with_data(&mut two, &two_id, "presence-test-channel", Some(data)).await;

    assert_silent(&mut one).await;
}

#[tokio::test]
async fn reports_a_cache_miss_on_an_empty_cache_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    assert_eq!(
        subscribe(&mut socket, &id, "cache-test-channel").await,
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"cache-test-channel"}"#
    );
    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"pusher:cache_miss","channel":"cache-test-channel"}"#
    );
}

#[tokio::test]
async fn replays_the_last_payload_when_joining_a_cache_channel() {
    let server = start().await;

    let (mut first, first_id) = server.connect().await;
    subscribe(&mut first, &first_id, "cache-test-channel").await;
    next_text(&mut first).await; // cache_miss

    server.trigger("cache-test-channel", "App\\Events\\TestEvent", json!({ "foo": "bar" })).await;

    next_text(&mut first).await; // the broadcast itself

    let (mut second, second_id) = server.connect().await;
    subscribe(&mut second, &second_id, "cache-test-channel").await;

    assert_eq!(
        next_text(&mut second).await,
        r#"{"event":"App\\Events\\TestEvent","data":"{\"foo\":\"bar\"}","channel":"cache-test-channel"}"#
    );
}

#[tokio::test]
async fn presence_bookkeeping_does_not_overwrite_a_cached_payload() {
    let server = start().await;

    let (mut first, first_id) = server.connect().await;
    let data = json!({ "user_id": 1 });
    subscribe_with_data(&mut first, &first_id, "presence-cache-test-channel", Some(data)).await;
    next_text(&mut first).await; // cache_miss

    server
        .trigger("presence-cache-test-channel", "App\\Events\\TestEvent", json!({ "foo": "bar" }))
        .await;
    next_text(&mut first).await;

    // A second member triggers `member_added`, which must not become the cache.
    let (mut second, second_id) = server.connect().await;
    let data = json!({ "user_id": 2 });
    subscribe_with_data(&mut second, &second_id, "presence-cache-test-channel", Some(data)).await;

    assert_eq!(
        next_text(&mut second).await,
        r#"{"event":"App\\Events\\TestEvent","data":"{\"foo\":\"bar\"}","channel":"presence-cache-test-channel"}"#
    );
}

#[tokio::test]
async fn unsubscribes_from_a_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    subscribe(&mut socket, &id, "test-channel").await;

    send(
        &mut socket,
        json!({ "event": "pusher:unsubscribe", "data": { "channel": "test-channel" } }),
    )
    .await;

    // The channel is dropped once its last subscriber leaves.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(server.server.registry.for_app(APP_ID).find("test-channel").is_none());
}

#[tokio::test]
async fn responds_to_a_ping() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    send(&mut socket, json!({ "event": "pusher:ping" })).await;

    assert_eq!(next_text(&mut socket).await, r#"{"event":"pusher:pong"}"#);
}

#[tokio::test]
async fn rejects_a_malformed_message() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    socket_send_raw(&mut socket, "not json at all").await;

    let frame = next_json(&mut socket).await;

    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(
        frame["data"].as_str().unwrap(),
        r#"{"code":4200,"message":"Invalid message format"}"#
    );
}

#[tokio::test]
async fn rejects_an_unknown_pusher_event() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    send(&mut socket, json!({ "event": "pusher:nonsense" })).await;

    assert_eq!(next_json(&mut socket).await["event"], "pusher:error");
}

#[tokio::test]
async fn accepts_data_sent_as_a_json_string() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    // Some clients encode `data` as a string rather than an object.
    send(
        &mut socket,
        json!({ "event": "pusher:subscribe", "data": r#"{"channel":"test-channel"}"# }),
    )
    .await;

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"test-channel"}"#
    );
}

#[tokio::test]
async fn whispers_a_client_event_to_other_members() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe_with_data(&mut one, &one_id, "presence-test-channel", Some(json!({ "user_id": 1 })))
        .await;

    let (mut two, two_id) = server.connect().await;
    subscribe_with_data(&mut two, &two_id, "presence-test-channel", Some(json!({ "user_id": 2 })))
        .await;

    next_text(&mut one).await; // member_added

    send(
        &mut two,
        json!({
            "event": "client-typing",
            "channel": "presence-test-channel",
            "data": { "typing": true },
        }),
    )
    .await;

    assert_eq!(
        next_text(&mut one).await,
        r#"{"event":"client-typing","channel":"presence-test-channel","data":{"typing":true},"user_id":2}"#
    );
    assert_silent(&mut two).await;
}

#[tokio::test]
async fn rejects_a_client_event_from_a_non_member() {
    let server = start().await;
    let (mut socket, _) = server.connect().await;

    send(
        &mut socket,
        json!({ "event": "client-typing", "channel": "private-test-channel", "data": {} }),
    )
    .await;

    let frame = next_json(&mut socket).await;

    assert_eq!(
        frame["data"].as_str().unwrap(),
        r#"{"code":4009,"message":"The client is not a member of the specified channel."}"#
    );
}

#[tokio::test]
async fn rejects_client_events_when_the_application_disables_them() {
    let mut app = application();
    app.accept_client_events_from = reverb_rs::config::ClientEvents::Disabled;

    let server = start_with(app).await;
    let (mut socket, id) = server.connect().await;

    subscribe(&mut socket, &id, "test-channel").await;

    send(&mut socket, json!({ "event": "client-typing", "channel": "test-channel", "data": {} }))
        .await;

    let frame = next_json(&mut socket).await;

    assert_eq!(
        frame["data"].as_str().unwrap(),
        r#"{"code":4301,"message":"The app does not have client messaging enabled."}"#
    );
}

#[tokio::test]
async fn enforces_the_message_rate_limit() {
    let mut app = application();
    app.rate_limiting = reverb_rs::config::RateLimiting {
        enabled: true,
        max_attempts: 2,
        decay_seconds: 60,
        terminate_on_limit: false,
    };

    let server = start_with(app).await;
    let (mut socket, _) = server.connect().await;

    for _ in 0..2 {
        send(&mut socket, json!({ "event": "pusher:ping" })).await;
        assert_eq!(next_text(&mut socket).await, r#"{"event":"pusher:pong"}"#);
    }

    send(&mut socket, json!({ "event": "pusher:ping" })).await;

    assert_eq!(
        next_json(&mut socket).await["data"].as_str().unwrap(),
        r#"{"code":4301,"message":"Rate limit exceeded"}"#
    );
}

#[tokio::test]
async fn rejects_a_connection_from_a_disallowed_origin() {
    let mut app = application();
    app.allowed_origins = vec!["laravel.com".into()];

    let server = start_with(app).await;
    let mut socket = server.open(APP_KEY, Some("http://evil.com")).await;

    assert_eq!(
        next_json(&mut socket).await["data"].as_str().unwrap(),
        r#"{"code":4009,"message":"Origin not allowed"}"#
    );
}

#[tokio::test]
async fn accepts_a_connection_from_an_allowed_origin() {
    let mut app = application();
    app.allowed_origins = vec!["*.laravel.com".into()];

    let server = start_with(app).await;
    let (_socket, _id) = server.connect_with_origin(Some("https://app.laravel.com")).await;
}

#[tokio::test]
async fn enforces_the_connection_limit() {
    let mut app = application();
    app.max_connections = Some(1);

    let server = start_with(app).await;
    let (_first, _) = server.connect().await;

    let mut second = server.open(APP_KEY, None).await;

    assert_eq!(
        next_json(&mut second).await["data"].as_str().unwrap(),
        r#"{"code":4004,"message":"Application is over connection quota"}"#
    );
}

/// Send a frame that is deliberately not valid JSON.
async fn socket_send_raw(socket: &mut Socket, raw: &str) {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;

    socket.send(Message::Text(raw.into())).await.expect("send");
}
