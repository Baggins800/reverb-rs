//! Configuration exported from a Laravel application, and reverb:restart.
//!
//! `reverb-rs` reads the environment, which covers a stock `config/reverb.php`
//! because that file is entirely `env()` calls. These tests cover what the
//! environment cannot express, by running the real export command against a
//! Laravel app and starting a server from its output.
//!
//! Skipped unless `REVERB_TEST_PHP_APP` is set.

mod support;

use std::process::Stdio;
use std::time::Duration;

use reverb_rs::restart::{RESTART_KEY, RestartWatch};
use tokio::process::Command;



fn php_app() -> Option<String> {
    let app = std::env::var("REVERB_TEST_PHP_APP").ok()?;

    if !std::path::Path::new(&app).join("vendor/autoload.php").is_file() {
        eprintln!("skipped: REVERB_TEST_PHP_APP has no vendor/autoload.php");
        return None;
    }

    Some(app)
}

/// Run `reverb-rs:config` in a Laravel app and write the export to a file.
async fn export(app: &str, cache_path: &std::path::Path, to: &std::path::Path) {
    let harness = concat!(env!("CARGO_MANIFEST_DIR"), "/laravel/tests/config-harness.php");

    let output = Command::new("php")
        .arg(harness)
        .arg(cache_path)
        .env("REVERB_TEST_PHP_APP", app)
        .stderr(Stdio::null())
        .output()
        .await
        .expect("failed to run php");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let json = stdout
        .lines()
        .rev()
        .find(|line| line.starts_with('{'))
        .unwrap_or_else(|| panic!("no config exported, got: {stdout}"));

    std::fs::write(to, json).expect("write the exported config");
}

/// A temporary directory unique to one test.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir()
        .join(format!("reverb-rs-config-{name}-{}-{}", std::process::id(), unix_time()));

    std::fs::create_dir_all(&dir).expect("create scratch dir");

    dir
}

fn unix_time() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[tokio::test]
async fn serves_applications_the_environment_could_not_express() {
    let Some(app) = php_app() else { return };

    let dir = scratch("apps");
    let config = dir.join("reverb-rs.json");

    export(&app, &dir.join("cache"), &config).await;

    let server = support::start_from_config(&config).await;

    // Both applications from config/reverb.php are served, which a single set
    // of REVERB_APP_* variables cannot do.
    for key in ["primary-key", "secondary-key"] {
        let mut socket = server.open(key, None).await;
        let frame = support::next_json(&mut socket).await;

        assert_eq!(
            frame["event"], "pusher:connection_established",
            "application key [{key}] should be served"
        );
    }

    // Per-application settings come through too: the second app was given a
    // different activity timeout.
    let mut socket = server.open("secondary-key", None).await;
    let frame = support::next_json(&mut socket).await;
    let data: serde_json::Value =
        serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();

    assert_eq!(data["activity_timeout"], 45);

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn rejects_an_application_key_that_was_not_exported() {
    let Some(app) = php_app() else { return };

    let dir = scratch("unknown");
    let config = dir.join("reverb-rs.json");

    export(&app, &dir.join("cache"), &config).await;

    let server = support::start_from_config(&config).await;
    let mut socket = server.open("not-a-configured-key", None).await;

    assert_eq!(
        support::next_json(&mut socket).await["data"].as_str().unwrap(),
        r#"{"code":4001,"message":"Application does not exist"}"#
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn watches_the_cache_key_reverb_restart_writes() {
    let cache = scratch("restart");
    let watch = RestartWatch::file(&cache, RESTART_KEY);

    let RestartWatch::File { path } = &watch else { panic!("expected a file watch") };

    // Nothing written yet: a server starting now has no signal to react to.
    assert_eq!(watch.read().await, None);

    std::fs::create_dir_all(path.parent().unwrap()).expect("create dirs");
    std::fs::write(path, "9999999999i:100;").expect("write");

    let first = watch.read().await;
    assert_eq!(first.as_deref(), Some("i:100;"));

    // `php artisan reverb:restart` writes a fresh timestamp.
    std::fs::write(path, "9999999999i:200;").expect("rewrite");

    assert_ne!(watch.read().await, first, "a new timestamp is a restart signal");

    std::fs::remove_dir_all(&cache).ok();
}

#[tokio::test]
async fn shuts_down_when_the_restart_signal_changes() {
    let cache = scratch("signal");
    let watch = RestartWatch::file(&cache, RESTART_KEY);

    let RestartWatch::File { path } = watch.clone() else { panic!("expected a file watch") };

    std::fs::create_dir_all(path.parent().unwrap()).expect("create dirs");
    std::fs::write(&path, "9999999999i:100;").expect("write");

    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(reverb_rs::restart::watch(watch, move || {
        let _ = tx.send(());
    }));

    // Let the watcher record the starting value before it changes.
    tokio::time::sleep(Duration::from_millis(200)).await;
    std::fs::write(&path, "9999999999i:200;").expect("rewrite");

    // Reverb polls every five seconds, so allow for one full interval.
    let stopped = tokio::time::timeout(Duration::from_secs(15), rx).await;

    assert!(stopped.is_ok(), "the server should have been asked to stop");

    std::fs::remove_dir_all(&cache).ok();
}

#[tokio::test]
async fn the_artisan_start_command_runs_the_server() {
    let Some(app) = php_app() else { return };

    let harness = concat!(env!("CARGO_MANIFEST_DIR"), "/laravel/tests/config-harness.php");
    let binary = concat!(env!("CARGO_MANIFEST_DIR"), "/target/release/reverb-rs");

    if !std::path::Path::new(binary).is_file() {
        eprintln!("skipped: run cargo build --release first");
        return;
    }

    let cache = scratch("start");
    let port = 8123;

    // `php artisan reverb-rs:start` execs the server in place, so this child
    // process becomes it.
    let mut child = Command::new("php")
        .arg(harness)
        .arg(&cache)
        .env("REVERB_TEST_PHP_APP", &app)
        .env("HARNESS_COMMAND", "reverb-rs:start")
        .env("HARNESS_OPTIONS", format!("binary={binary},host=127.0.0.1,port={port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("failed to run php");

    // Wait for it to come up.
    let mut ready = false;

    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            ready = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(ready, "reverb-rs:start did not bring the server up");

    // It is serving the applications config/reverb.php defines, not anything
    // from the environment.
    let health = reqwest::get(format!("http://127.0.0.1:{port}/up")).await.expect("health");
    assert_eq!(health.text().await.unwrap(), r#"{"health":"OK"}"#);

    let (socket, _) = tokio_tungstenite::connect_async(format!(
        "ws://127.0.0.1:{port}/app/secondary-key"
    ))
    .await
    .expect("connect with an application only the Laravel config knows about");

    drop(socket);

    let _ = child.kill().await;
    std::fs::remove_dir_all(&cache).ok();
}
