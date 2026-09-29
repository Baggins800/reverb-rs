//! The Laravel relay, tested end to end from Rust.
//!
//! These drive the real companion package: a PHP process boots Laravel,
//! subscribes to Redis and dispatches Reverb's own event classes, while the
//! Rust side exercises the protocol and asserts on what Laravel actually saw.
//! That covers the PHP code — the event factory, the relayed connection, the
//! command's error handling — without a second test framework.
//!
//! Skipped unless both are set:
//!
//!     REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 \
//!     REVERB_TEST_PHP_APP=/path/to/app-with-reverb-installed \
//!     cargo test --test relay

mod support;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use reverb_rs::events::{EventKind, EventSet};
use serde_json::{Value, json};
use support::*;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};

/// A running PHP relay, plus everything its listeners have reported.
struct Relay {
    child: Child,
    observed: Arc<Mutex<Vec<Value>>>,
    channel: String,
}

impl Drop for Relay {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

impl Relay {
    /// Every observation matching a predicate, in the order Laravel saw them.
    fn matching(&self, predicate: impl Fn(&Value) -> bool) -> Vec<Value> {
        self.observed.lock().iter().filter(|v| predicate(v)).cloned().collect()
    }

    fn of_kind(&self, event: &str) -> Vec<Value> {
        self.matching(|v| v["event"] == event)
    }

    /// Wait until a predicate holds, or fail with what was actually seen.
    async fn wait_for(&self, what: &str, predicate: impl Fn(&[Value]) -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);

        while tokio::time::Instant::now() < deadline {
            if predicate(&self.observed.lock().clone()) {
                return;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let seen: Vec<String> = self
            .observed
            .lock()
            .iter()
            .map(|v| v["event"].as_str().unwrap_or("?").to_string())
            .collect();

        panic!("timed out waiting for {what}; Laravel saw: {seen:?}");
    }
}

/// The environment these tests need, or `None` to skip.
fn environment() -> Option<(String, String)> {
    let redis = std::env::var("REVERB_TEST_REDIS_URL").ok()?;
    let app = std::env::var("REVERB_TEST_PHP_APP").ok()?;

    if !std::path::Path::new(&app).join("vendor/autoload.php").is_file() {
        eprintln!("skipped: REVERB_TEST_PHP_APP has no vendor/autoload.php");
        return None;
    }

    Some((redis, app))
}

/// Boot the PHP relay against a channel of its own and wait until it is ready.
async fn start_relay(php_app: &str, channel: &str, throwing: bool) -> Relay {
    let harness = concat!(env!("CARGO_MANIFEST_DIR"), "/laravel/tests/relay-harness.php");

    let mut command = Command::new("php");

    command
        .arg(harness)
        .env("REVERB_TEST_PHP_APP", php_app)
        .env("REVERB_EVENTS_CHANNEL", channel)
        .env("REVERB_APP_ID", APP_ID)
        .env("REVERB_APP_KEY", APP_KEY)
        .env("REVERB_APP_SECRET", APP_SECRET)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    if throwing {
        command.env("RELAY_HARNESS_THROW", "1");
    }

    let mut child = command.spawn().expect("failed to run php");
    let stdout = child.stdout.take().expect("piped stdout");

    let observed = Arc::new(Mutex::new(Vec::new()));
    let collector = observed.clone();

    tokio::spawn(async move {
        let mut lines = BufReader::new(stdout).lines();

        while let Ok(Some(line)) = lines.next_line().await {
            // The harness prefixes its observations; Laravel prints other things.
            if let Some(payload) = line.strip_prefix("@@")
                && let Ok(value) = serde_json::from_str::<Value>(payload)
            {
                collector.lock().push(value);
            }
        }
    });

    let relay = Relay { child, observed, channel: channel.to_string() };

    relay.wait_for("the relay to boot", |seen| seen.iter().any(|v| v["event"] == "Ready")).await;

    // The Ready line is printed just before the Redis subscription opens.
    tokio::time::sleep(Duration::from_millis(500)).await;

    relay
}

/// Publish a raw envelope, bypassing the server, to test the PHP side's
/// handling of input it should reject.
async fn publish_raw(redis_url: &str, channel: &str, payload: &str) {
    let client = redis::Client::open(redis_url).expect("redis client");
    let mut conn = client.get_multiplexed_async_connection().await.expect("redis connection");

    let _: i64 = redis::cmd("PUBLISH")
        .arg(channel)
        .arg(payload)
        .query_async(&mut conn)
        .await
        .expect("publish");
}

fn unique_channel(name: &str) -> String {
    format!("reverb-rs-test-{name}-{}", unix_time())
}

#[tokio::test]
async fn relays_every_event_to_laravel() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("all");
    let relay = start_relay(&php_app, &channel, false).await;

    let mut app = application();
    // Makes every connection immediately inactive, so two sweeps prune it.
    app.ping_interval = 0;

    let server = start_with_redis_relay(app, EventSet::of(EventKind::ALL), &redis, &channel).await;

    let (mut socket, id) = server.connect().await;
    subscribe_with_data(&mut socket, &id, "presence-relay", Some(json!({ "user_id": 77 }))).await;

    relay
        .wait_for("the subscription to be seen", |seen| {
            seen.iter().any(|v| v["event"] == "ChannelCreated")
                && seen.iter().any(|v| v["event"] == "MessageReceived")
        })
        .await;

    let created = relay.of_kind("ChannelCreated");
    assert_eq!(created[0]["channel"], "presence-relay");
    assert_eq!(
        created[0]["class"], "Laravel\\Reverb\\Protocols\\Pusher\\Channels\\PresenceChannel",
        "the relay rebuilds the channel as its real Reverb subclass"
    );

    let sent = relay.of_kind("MessageSent");
    assert_eq!(sent[0]["connection"]["app"], APP_ID);
    assert_eq!(sent[0]["connection"]["socket"], id);
    assert!(sent[0]["message"].as_str().unwrap().contains("connection_established"));

    let received = relay.of_kind("MessageReceived");
    assert!(received[0]["message"].as_str().unwrap().contains("pusher:subscribe"));

    // Two sweeps: the first pings, the second prunes what never answered.
    reverb_rs::sweep(&server.server);
    reverb_rs::sweep(&server.server);

    relay
        .wait_for("the pruned connection", |seen| {
            seen.iter().any(|v| v["event"] == "ConnectionPruned")
        })
        .await;

    let pruned = relay.of_kind("ConnectionPruned");
    assert_eq!(pruned[0]["socket"], id);
    assert_eq!(pruned[0]["user_id"], 77, "presence data survives the round trip");

    relay
        .wait_for("the emptied channel", |seen| seen.iter().any(|v| v["event"] == "ChannelRemoved"))
        .await;

    assert_eq!(relay.of_kind("ChannelRemoved")[0]["channel"], "presence-relay");
}

#[tokio::test]
async fn rebuilds_each_channel_as_its_reverb_class() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("classes");
    let relay = start_relay(&php_app, &channel, false).await;

    let server = start_with_redis_relay(
        application(),
        EventSet::of([EventKind::ChannelCreated]),
        &redis,
        &channel,
    )
    .await;

    let (mut socket, id) = server.connect().await;

    for name in ["plain-relay", "private-relay", "cache-relay", "private-cache-relay"] {
        subscribe(&mut socket, &id, name).await;
    }

    relay
        .wait_for("all four channels", |seen| {
            seen.iter().filter(|v| v["event"] == "ChannelCreated").count() == 4
        })
        .await;

    let classes: Vec<String> = relay
        .of_kind("ChannelCreated")
        .iter()
        .map(|v| v["class"].as_str().unwrap().rsplit('\\').next().unwrap().to_string())
        .collect();

    assert_eq!(classes, vec!["Channel", "PrivateChannel", "CacheChannel", "PrivateCacheChannel"]);
}

#[tokio::test]
async fn a_relayed_connection_cannot_be_written_to() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("write");
    let relay = start_relay(&php_app, &channel, false).await;

    let server = start_with_redis_relay(
        application(),
        EventSet::of([EventKind::MessageSent]),
        &redis,
        &channel,
    )
    .await;

    let (_socket, _id) = server.connect().await;

    relay.wait_for("a sent message", |seen| seen.iter().any(|v| v["event"] == "MessageSent")).await;

    let write = relay.of_kind("MessageSent")[0]["connection"]["write"].clone();

    assert_eq!(write["threw"], true, "the socket lives in another process");
    assert!(
        write["message"].as_str().unwrap().contains("reverb-rs server process"),
        "the failure explains itself: {}",
        write["message"]
    );
}

#[tokio::test]
async fn survives_a_listener_that_throws() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("throwing");
    let relay = start_relay(&php_app, &channel, true).await;

    let server = start_with_redis_relay(
        application(),
        EventSet::of([EventKind::ChannelCreated]),
        &redis,
        &channel,
    )
    .await;

    let (mut socket, id) = server.connect().await;

    // Each of these trips the throwing listener; the relay must keep going.
    for name in ["throwing-one", "throwing-two", "throwing-three"] {
        subscribe(&mut socket, &id, name).await;
    }

    relay
        .wait_for("every channel despite the failures", |seen| {
            seen.iter().filter(|v| v["event"] == "ChannelCreated").count() == 3
        })
        .await;
}

#[tokio::test]
async fn ignores_events_for_an_unknown_application() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("unknown-app");
    let relay = start_relay(&php_app, &channel, false).await;

    publish_raw(
        &redis,
        &relay.channel,
        &json!([{
            "event": "channel_created",
            "application": "no-such-app",
            "payload": { "channel": "orphan" },
        }])
        .to_string(),
    )
    .await;

    // Then something valid, to prove the relay is still listening.
    let server = start_with_redis_relay(
        application(),
        EventSet::of([EventKind::ChannelCreated]),
        &redis,
        &channel,
    )
    .await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "after-orphan").await;

    relay
        .wait_for("the valid event", |seen| seen.iter().any(|v| v["channel"] == "after-orphan"))
        .await;

    assert!(
        relay.matching(|v| v["channel"] == "orphan").is_empty(),
        "an event for an unconfigured application is discarded"
    );
}

#[tokio::test]
async fn discards_malformed_payloads() {
    let Some((redis, php_app)) = environment() else { return };

    let channel = unique_channel("malformed");
    let relay = start_relay(&php_app, &channel, false).await;

    for payload in [
        "not json at all",
        "{\"not\":\"an array of events\"}",
        "[{\"event\":\"nonsense\",\"application\":\"123456\"}]",
        "[\"a bare string\"]",
        "[]",
    ] {
        publish_raw(&redis, &relay.channel, payload).await;
    }

    let server = start_with_redis_relay(
        application(),
        EventSet::of([EventKind::ChannelCreated]),
        &redis,
        &channel,
    )
    .await;

    let (mut socket, id) = server.connect().await;
    subscribe(&mut socket, &id, "after-garbage").await;

    relay
        .wait_for("the relay to still be working", |seen| {
            seen.iter().any(|v| v["channel"] == "after-garbage")
        })
        .await;

    assert_eq!(
        relay.of_kind("ChannelCreated").len(),
        1,
        "none of the malformed payloads produced an event"
    );
}
