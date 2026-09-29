//! HTTP API tests.
//!
//! Expected bodies follow Reverb's controller tests under
//! `tests/Feature/Protocols/Pusher/Reverb/`.

mod support;

use serde_json::json;
use support::*;

#[tokio::test]
async fn serves_the_health_check_unauthenticated() {
    let server = start().await;

    let response = reqwest::get(format!("http://{}/up", server.addr)).await.expect("health check");

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), r#"{"health":"OK"}"#);
}

#[tokio::test]
async fn triggers_an_event_on_a_channel() {
    let server = start().await;
    let (mut socket, id) = server.connect().await;

    subscribe(&mut socket, &id, "test-channel").await;

    let response =
        server.trigger("test-channel", "App\\Events\\TestEvent", json!({ "foo": "bar" })).await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "{}");

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"App\\Events\\TestEvent","data":"{\"foo\":\"bar\"}","channel":"test-channel"}"#
    );
}

#[tokio::test]
async fn excludes_the_triggering_socket() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel").await;

    let (mut two, two_id) = server.connect().await;
    subscribe(&mut two, &two_id, "test-channel").await;

    let body = json!({
        "name": "App\\Events\\TestEvent",
        "channel": "test-channel",
        "data": r#"{"foo":"bar"}"#,
        "socket_id": one_id,
    })
    .to_string();

    server.api("POST", &format!("/apps/{APP_ID}/events"), &[], Some(&body)).await;

    assert_eq!(next_json(&mut two).await["event"], "App\\Events\\TestEvent");
    assert_silent(&mut one).await;
}

#[tokio::test]
async fn triggers_an_event_on_several_channels() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel-one").await;

    let (mut two, two_id) = server.connect().await;
    subscribe(&mut two, &two_id, "test-channel-two").await;

    let body = json!({
        "name": "App\\Events\\TestEvent",
        "channels": ["test-channel-one", "test-channel-two"],
        "data": r#"{"foo":"bar"}"#,
    })
    .to_string();

    server.api("POST", &format!("/apps/{APP_ID}/events"), &[], Some(&body)).await;

    assert_eq!(next_json(&mut one).await["channel"], "test-channel-one");
    assert_eq!(next_json(&mut two).await["channel"], "test-channel-two");
}

#[tokio::test]
async fn triggers_a_batch_of_events() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let body = json!({
        "batch": [
            { "name": "First", "channel": "test-channel", "data": r#"{"n":1}"# },
            { "name": "Second", "channel": "test-channel", "data": r#"{"n":2}"# },
        ]
    })
    .to_string();

    let response =
        server.api("POST", &format!("/apps/{APP_ID}/batch_events"), &[], Some(&body)).await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), r#"{"batch":{}}"#);

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"First","channel":"test-channel","data":"{\"n\":1}"}"#
    );
    assert_eq!(next_json(&mut socket).await["event"], "Second");
}

#[tokio::test]
async fn returns_per_event_info_for_a_batch() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let body = json!({
        "batch": [
            { "name": "First", "channel": "test-channel", "data": "{}", "info": "subscription_count" },
            { "name": "Second", "channel": "test-channel", "data": "{}" },
        ]
    })
    .to_string();

    let response =
        server.api("POST", &format!("/apps/{APP_ID}/batch_events"), &[], Some(&body)).await;

    assert_eq!(response.text().await.unwrap(), r#"{"batch":[{"subscription_count":1},{}]}"#);
}

#[tokio::test]
async fn returns_information_for_all_occupied_channels() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel-one").await;

    let (mut two, two_id) = server.connect().await;
    subscribe_with_data(
        &mut two,
        &two_id,
        "presence-test-channel-two",
        Some(json!({ "user_id": 1 })),
    )
    .await;

    let response = server
        .api("GET", &format!("/apps/{APP_ID}/channels"), &[("info", "user_count")], None)
        .await;

    assert_eq!(response.status(), 200);
    // Channels are reported in name order, which Reverb leaves to insertion order.
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"channels":{"presence-test-channel-two":{"user_count":1},"test-channel-one":{}}}"#
    );
}

#[tokio::test]
async fn filters_channels_by_prefix() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel-one").await;
    subscribe(&mut one, &one_id, "other-channel").await;

    let response = server
        .api("GET", &format!("/apps/{APP_ID}/channels"), &[("filter_by_prefix", "test-")], None)
        .await;

    assert_eq!(response.text().await.unwrap(), r#"{"channels":{"test-channel-one":{}}}"#);
}

#[tokio::test]
async fn omits_channels_that_have_emptied() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel-one").await;

    let (two, two_id) = server.connect().await;
    let mut two = two;
    subscribe(&mut two, &two_id, "test-channel-two").await;
    drop(two);

    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let response = server.api("GET", &format!("/apps/{APP_ID}/channels"), &[], None).await;

    assert_eq!(response.text().await.unwrap(), r#"{"channels":{"test-channel-one":{}}}"#);
}

#[tokio::test]
async fn returns_information_for_a_single_channel() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel-one").await;

    let (mut two, two_id) = server.connect().await;
    subscribe(&mut two, &two_id, "test-channel-one").await;

    let response = server
        .api(
            "GET",
            &format!("/apps/{APP_ID}/channels/test-channel-one"),
            &[("info", "user_count,subscription_count,cache")],
            None,
        )
        .await;

    assert_eq!(response.text().await.unwrap(), r#"{"occupied":true,"subscription_count":2}"#);
}

#[tokio::test]
async fn reports_an_unoccupied_channel() {
    let server = start().await;

    let response = server
        .api(
            "GET",
            &format!("/apps/{APP_ID}/channels/test-channel-one"),
            &[("info", "user_count,subscription_count,cache")],
            None,
        )
        .await;

    assert_eq!(response.text().await.unwrap(), r#"{"occupied":false}"#);
}

#[tokio::test]
async fn reports_only_the_requested_attributes() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel-one").await;

    let path = format!("/apps/{APP_ID}/channels/test-channel-one");

    let response = server.api("GET", &path, &[("info", "cache")], None).await;
    assert_eq!(response.text().await.unwrap(), r#"{"occupied":true}"#);

    let response =
        server.api("GET", &path, &[("info", "subscription_count,user_count")], None).await;
    assert_eq!(response.text().await.unwrap(), r#"{"occupied":true,"subscription_count":1}"#);
}

#[tokio::test]
async fn reports_presence_channel_attributes() {
    let server = start().await;

    // Two connections for the same user count as one user.
    for _ in 0..2 {
        let (mut socket, id) = server.connect().await;
        subscribe_with_data(
            &mut socket,
            &id,
            "presence-test-channel",
            Some(json!({ "user_id": 123 })),
        )
        .await;
        std::mem::forget(socket);
    }

    let response = server
        .api(
            "GET",
            &format!("/apps/{APP_ID}/channels/presence-test-channel"),
            &[("info", "user_count,subscription_count,cache")],
            None,
        )
        .await;

    assert_eq!(response.text().await.unwrap(), r#"{"occupied":true,"user_count":1}"#);
}

#[tokio::test]
async fn reports_the_cached_payload_of_a_cache_channel() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "cache-test-channel").await;

    server.trigger("cache-test-channel", "TestEvent", json!({ "some": "data" })).await;

    let response = server
        .api(
            "GET",
            &format!("/apps/{APP_ID}/channels/cache-test-channel"),
            &[("info", "subscription_count,cache")],
            None,
        )
        .await;

    // Symfony's JsonResponse hex-escapes quotes, and so do we.
    assert_eq!(
        response.text().await.unwrap(),
        r#"{"occupied":true,"subscription_count":1,"cache":{"event":"TestEvent","data":"{\u0022some\u0022:\u0022data\u0022}","channel":"cache-test-channel"}}"#
    );
}

#[tokio::test]
async fn counts_connections() {
    let server = start().await;

    let (mut one, one_id) = server.connect().await;
    subscribe(&mut one, &one_id, "test-channel").await;

    let (mut two, two_id) = server.connect().await;
    subscribe(&mut two, &two_id, "test-channel").await;

    let response = server.api("GET", &format!("/apps/{APP_ID}/connections"), &[], None).await;

    assert_eq!(response.text().await.unwrap(), r#"{"connections":2}"#);
}

#[tokio::test]
async fn lists_the_users_of_a_presence_channel() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe_with_data(&mut socket, &id, "presence-test-channel", Some(json!({ "user_id": 7 })))
        .await;

    let response = server
        .api("GET", &format!("/apps/{APP_ID}/channels/presence-test-channel/users"), &[], None)
        .await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), r#"{"users":[{"id":7}]}"#);
}

#[tokio::test]
async fn rejects_listing_users_of_a_non_presence_channel() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "test-channel").await;

    let response =
        server.api("GET", &format!("/apps/{APP_ID}/channels/test-channel/users"), &[], None).await;

    assert_eq!(response.status(), 400);
}

#[tokio::test]
async fn returns_not_found_for_an_unknown_channel_users_request() {
    let server = start().await;

    let response = server
        .api("GET", &format!("/apps/{APP_ID}/channels/presence-nothing/users"), &[], None)
        .await;

    assert_eq!(response.status(), 404);
}

#[tokio::test]
async fn terminates_a_users_connections() {
    let server = start().await;

    let (mut socket, id) = server.connect().await;
    subscribe_with_data(&mut socket, &id, "presence-test-channel", Some(json!({ "user_id": 42 })))
        .await;

    let response = server
        .api("POST", &format!("/apps/{APP_ID}/users/42/terminate_connections"), &[], Some("{}"))
        .await;

    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await.unwrap(), "{}");

    // The socket is closed, so the stream ends.
    let closed = tokio::time::timeout(std::time::Duration::from_secs(2), async {
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

#[tokio::test]
async fn rejects_an_invalid_signature() {
    let server = start().await;

    let url = format!(
        "http://{}/apps/{APP_ID}/channels?auth_key={APP_KEY}&auth_timestamp={}&auth_version=1.0&auth_signature=deadbeef",
        server.addr,
        unix_time()
    );

    let response = reqwest::get(url).await.expect("request");

    assert_eq!(response.status(), 401);
    assert_eq!(response.text().await.unwrap(), "Authentication signature invalid.");
}

#[tokio::test]
async fn rejects_signatures_outside_the_timestamp_tolerance() {
    let server = start().await;
    let path = format!("/apps/{APP_ID}/channels");

    for offset in [-3600, 3600] {
        let response = server.api_at("GET", &path, &[], None, unix_time() + offset).await;

        assert_eq!(response.status(), 401, "offset {offset} should be rejected");
    }
}

#[tokio::test]
async fn rejects_an_unknown_application_id() {
    let server = start().await;

    let response = server.api("GET", "/apps/nope/channels", &[], None).await;

    assert_eq!(response.status(), 404);
    assert_eq!(response.text().await.unwrap(), "No matching application for ID [nope].");
}

#[tokio::test]
async fn rejects_a_tampered_body() {
    let server = start().await;

    // Sign one body, send another.
    let signed = json!({ "name": "A", "channel": "c", "data": "{}" }).to_string();
    let tampered = json!({ "name": "B", "channel": "c", "data": "{}" }).to_string();

    let timestamp = unix_time().to_string();
    let body_md5 = format!("{:x}", {
        use md5::Digest;
        md5::Md5::digest(signed.as_bytes())
    });

    let query = format!(
        "auth_key={APP_KEY}&auth_timestamp={timestamp}&auth_version=1.0&body_md5={body_md5}"
    );
    let signature =
        reverb_rs::server::sign(APP_SECRET, &format!("POST\n/apps/{APP_ID}/events\n{query}"));

    let response = reqwest::Client::new()
        .post(format!(
            "http://{}/apps/{APP_ID}/events?{query}&auth_signature={signature}",
            server.addr
        ))
        .body(tampered)
        .send()
        .await
        .expect("request");

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn rejects_a_body_over_the_request_size_limit() {
    let server = start().await;

    let oversized = json!({
        "name": "Big",
        "channel": "test-channel",
        "data": "x".repeat(50_000),
    })
    .to_string();

    let response =
        server.api("POST", &format!("/apps/{APP_ID}/events"), &[], Some(&oversized)).await;

    assert_eq!(response.status(), 413);
    assert_eq!(response.text().await.unwrap(), "Payload too large.");
}

#[tokio::test]
async fn reports_an_unrouted_path_the_way_reverb_does() {
    let server = start().await;

    let response = reqwest::get(format!("http://{}/nope", server.addr)).await.expect("request");

    assert_eq!(response.status(), 404);
    assert_eq!(response.text().await.unwrap(), "Not found.");
}

#[tokio::test]
async fn reports_a_wrong_method_the_way_reverb_does() {
    let server = start().await;

    let response = reqwest::Client::new()
        .post(format!("http://{}/apps/{APP_ID}/channels", server.addr))
        .send()
        .await
        .expect("request");

    assert_eq!(response.status(), 405);
    assert!(response.headers().contains_key("allow"));
    assert_eq!(response.text().await.unwrap(), "Method not allowed.");
}

#[tokio::test]
async fn treats_an_empty_application_id_as_unrouted() {
    let server = start().await;

    let response =
        reqwest::get(format!("http://{}/apps//channels", server.addr)).await.expect("request");

    assert_eq!(response.status(), 404);
    assert_eq!(response.text().await.unwrap(), "Not found.");
}
