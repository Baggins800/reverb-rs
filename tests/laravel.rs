//! Laravel's own broadcaster, driven against reverb-rs.
//!
//! These use `Broadcast::connection('reverb')` and the Pusher SDK behind it —
//! the exact path a real application takes — so a green run means an app's
//! broadcasts, channel authorization and API calls work unchanged.
//!
//! Skipped unless `REVERB_TEST_PHP_APP` is set; see `laravel/tests/setup-test-app.sh`.

mod support;

use std::process::Stdio;

use serde_json::{Value, json};
use support::*;
use tokio::process::Command;

/// The Laravel application these tests broadcast from, or `None` to skip.
fn php_app() -> Option<String> {
    let app = std::env::var("REVERB_TEST_PHP_APP").ok()?;

    if !std::path::Path::new(&app).join("vendor/autoload.php").is_file() {
        eprintln!("skipped: REVERB_TEST_PHP_APP has no vendor/autoload.php");
        return None;
    }

    Some(app)
}

/// Run the broadcast harness and return its JSON result.
async fn laravel(app: &str, port: u16, args: &[&str]) -> Value {
    let harness = concat!(env!("CARGO_MANIFEST_DIR"), "/laravel/tests/broadcast-harness.php");

    let output = Command::new("php")
        .arg(harness)
        .args(args)
        .env("REVERB_TEST_PHP_APP", app)
        .env("REVERB_HOST", "127.0.0.1")
        .env("REVERB_PORT", port.to_string())
        .env("REVERB_SCHEME", "http")
        .env("REVERB_APP_ID", APP_ID)
        .env("REVERB_APP_KEY", APP_KEY)
        .env("REVERB_APP_SECRET", APP_SECRET)
        .stderr(Stdio::null())
        .output()
        .await
        .expect("failed to run php");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let line = stdout.lines().last().unwrap_or_default();

    let result: Value = serde_json::from_str(line)
        .unwrap_or_else(|_| panic!("harness did not return JSON, got: {stdout}"));

    assert_eq!(result["ok"], true, "the harness failed: {}", result["error"]);

    result
}

#[tokio::test]
async fn a_laravel_broadcast_reaches_a_subscriber() {
    let Some(app) = php_app() else { return };

    let server = start().await;
    let (mut socket, id) = server.connect().await;

    subscribe(&mut socket, &id, "orders").await;

    laravel(
        &app,
        server.addr.port(),
        &["broadcast", "orders", "App\\Events\\OrderShipped", r#"{"order":42}"#],
    )
    .await;

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"App\\Events\\OrderShipped","data":"{\"order\":42}","channel":"orders"}"#
    );
}

#[tokio::test]
async fn a_broadcast_honours_to_others_exclusion() {
    let Some(app) = php_app() else { return };

    let server = start().await;

    let (mut sender, sender_id) = server.connect().await;
    subscribe(&mut sender, &sender_id, "orders").await;

    let (mut other, other_id) = server.connect().await;
    subscribe(&mut other, &other_id, "orders").await;

    // `toOthers()` passes the originating socket, which must not receive it.
    laravel(
        &app,
        server.addr.port(),
        &["broadcast", "orders", "App\\Events\\OrderShipped", r#"{"order":1}"#, &sender_id],
    )
    .await;

    assert_eq!(next_json(&mut other).await["event"], "App\\Events\\OrderShipped");
    assert_silent(&mut sender).await;
}

#[tokio::test]
async fn laravels_private_channel_authorization_is_accepted() {
    let Some(app) = php_app() else { return };

    let server = start().await;
    let (mut socket, id) = server.connect().await;

    // Exactly what /broadcasting/auth hands back to the browser.
    let response =
        laravel(&app, server.addr.port(), &["auth-private", "private-orders", &id]).await;
    let auth = response["auth"].as_str().expect("an auth signature");

    send(
        &mut socket,
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": "private-orders", "auth": auth },
        }),
    )
    .await;

    assert_eq!(
        next_text(&mut socket).await,
        r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"private-orders"}"#
    );
}

#[tokio::test]
async fn laravels_presence_channel_authorization_is_accepted() {
    let Some(app) = php_app() else { return };

    let server = start().await;
    let (mut socket, id) = server.connect().await;

    let response =
        laravel(&app, server.addr.port(), &["auth-presence", "presence-orders", &id, "7"]).await;

    send(
        &mut socket,
        json!({
            "event": "pusher:subscribe",
            "data": {
                "channel": "presence-orders",
                "auth": response["auth"],
                "channel_data": response["channel_data"],
            },
        }),
    )
    .await;

    let frame = next_text(&mut socket).await;

    assert!(frame.contains("pusher_internal:subscription_succeeded"));
    assert!(
        frame.contains(r#"\"ids\":[\"7\"]"#),
        "the presence roster carries Laravel's user id: {frame}"
    );
}

#[tokio::test]
async fn the_pusher_sdks_info_endpoints_work() {
    let Some(app) = php_app() else { return };

    let server = start().await;
    let (mut socket, id) = server.connect().await;

    subscribe(&mut socket, &id, "orders").await;

    let response = laravel(&app, server.addr.port(), &["info", "orders"]).await;

    assert_eq!(response["channel"]["occupied"], true);
    assert_eq!(response["channel"]["subscription_count"], 1);
    assert!(
        response["channels"]["channels"].get("orders").is_some(),
        "the channel listing includes it: {}",
        response["channels"]
    );
}
