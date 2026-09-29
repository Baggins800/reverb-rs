//! Horizontal scaling tests.
//!
//! Two nodes share one Redis channel: an event triggered on either must reach
//! subscribers on both, and the metrics endpoints must report the whole
//! cluster rather than one node's share of it.
//!
//! Skipped unless `REVERB_TEST_REDIS_URL` is set, for example:
//!
//!     REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test --test scaling

mod support;

use std::time::Duration;

use serde_json::json;
use support::*;

/// Bring up a two-node cluster on a channel unique to this test.
async fn cluster(name: &str) -> Option<(TestServer, TestServer)> {
    let url = std::env::var("REVERB_TEST_REDIS_URL").ok()?;
    let channel = format!("reverb-test-{name}-{}", unix_time());

    let one = start_scaled(&url, &channel).await;
    let two = start_scaled(&url, &channel).await;

    // Both subscribers must be attached before anything is published.
    tokio::time::sleep(Duration::from_millis(400)).await;

    Some((one, two))
}

#[tokio::test]
async fn broadcasts_across_nodes() {
    let Some((one, two)) = cluster("broadcast").await else {
        eprintln!("skipped: REVERB_TEST_REDIS_URL is not set");
        return;
    };

    let (mut socket, id) = two.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    // Triggered on the first node, delivered by the second.
    one.trigger("test-channel", "App\\Events\\TestEvent", json!({ "foo": "bar" })).await;

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"App\\Events\\TestEvent","data":"{\"foo\":\"bar\"}","channel":"test-channel"}"#
    );
}

#[tokio::test]
async fn does_not_echo_to_the_excluded_socket() {
    let Some((one, two)) = cluster("except").await else {
        return;
    };

    let (mut sender, sender_id) = one.connect().await;
    subscribe(&mut sender, &sender_id, "test-channel").await;

    let (mut other, other_id) = two.connect().await;
    subscribe(&mut other, &other_id, "test-channel").await;

    let body = json!({
        "name": "App\\Events\\TestEvent",
        "channel": "test-channel",
        "data": "{}",
        "socket_id": sender_id,
    })
    .to_string();

    two.api("POST", &format!("/apps/{APP_ID}/events"), &[], Some(&body)).await;

    assert_eq!(next_json(&mut other).await["event"], "App\\Events\\TestEvent");
    assert_silent(&mut sender).await;
}

#[tokio::test]
async fn counts_connections_across_the_cluster() {
    let Some((one, two)) = cluster("connections").await else {
        return;
    };

    let (mut first, first_id) = one.connect().await;
    subscribe(&mut first, &first_id, "test-channel").await;

    let (mut second, second_id) = two.connect().await;
    subscribe(&mut second, &second_id, "test-channel").await;

    for node in [&one, &two] {
        let response = node.api("GET", &format!("/apps/{APP_ID}/connections"), &[], None).await;

        assert_eq!(response.text().await.unwrap(), r#"{"connections":2}"#);
    }
}

#[tokio::test]
async fn sums_channel_information_across_the_cluster() {
    let Some((one, two)) = cluster("channel-info").await else {
        return;
    };

    let (mut first, first_id) = one.connect().await;
    subscribe(&mut first, &first_id, "test-channel").await;

    let (mut second, second_id) = two.connect().await;
    subscribe(&mut second, &second_id, "test-channel").await;

    let response = one
        .api(
            "GET",
            &format!("/apps/{APP_ID}/channels/test-channel"),
            &[("info", "subscription_count")],
            None,
        )
        .await;

    assert_eq!(response.text().await.unwrap(), r#"{"occupied":true,"subscription_count":2}"#);
}

#[tokio::test]
async fn merges_presence_rosters_across_the_cluster() {
    let Some((one, two)) = cluster("presence").await else {
        return;
    };

    let (mut first, first_id) = one.connect().await;
    subscribe_with_data(&mut first, &first_id, "presence-test", Some(json!({ "user_id": 1 })))
        .await;

    let (mut second, second_id) = two.connect().await;
    subscribe_with_data(&mut second, &second_id, "presence-test", Some(json!({ "user_id": 2 })))
        .await;

    let response =
        two.api("GET", &format!("/apps/{APP_ID}/channels/presence-test/users"), &[], None).await;

    let users: serde_json::Value =
        serde_json::from_str(&response.text().await.unwrap()).expect("json");
    let mut ids: Vec<u64> =
        users["users"].as_array().unwrap().iter().map(|u| u["id"].as_u64().unwrap()).collect();

    ids.sort_unstable();

    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
async fn terminates_a_user_across_the_cluster() {
    let Some((one, two)) = cluster("terminate").await else {
        return;
    };

    let (mut socket, id) = two.connect().await;
    subscribe_with_data(&mut socket, &id, "presence-test", Some(json!({ "user_id": 99 }))).await;

    // Requested on the first node, enforced by the second.
    one.api("POST", &format!("/apps/{APP_ID}/users/99/terminate_connections"), &[], Some("{}"))
        .await;

    let closed = tokio::time::timeout(Duration::from_secs(5), async {
        use futures_util::StreamExt;

        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                return true;
            }
        }

        true
    })
    .await;

    assert_eq!(closed, Ok(true), "the connection should have been terminated");
}
