//! A differential conformance harness.
//!
//! Drives a running Pusher-protocol server through a fixed script and prints
//! every frame and response body it produces, with the random socket IDs
//! masked. Run it against Laravel Reverb and against `reverb-rs` and diff the
//! two transcripts: an empty diff means the servers are indistinguishable.
//!
//!     cargo run --release --example conformance -- --addr 127.0.0.1:8080

use std::time::Duration;

use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use reverb_rs::server::sign;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    #[arg(long, default_value = "123456")]
    app_id: String,

    #[arg(long, default_value = "reverb-key")]
    key: String,

    #[arg(long, default_value = "reverb-secret")]
    secret: String,
}

/// A connection plus the socket ID to mask out of its transcript.
struct Client {
    socket: Socket,
    id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    section("connection");
    let mut one = open(&args, "connection_established").await?;

    section("public channel");
    subscribe(&mut one, &args, "test-channel", None).await?;
    drain(&mut one, "subscribe public").await;

    section("private channel");
    subscribe(&mut one, &args, "private-test-channel", None).await?;
    drain(&mut one, "subscribe private").await;

    section("private channel with a bad signature");
    send(
        &mut one,
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": "private-nope", "auth": "reverb-key:0000" },
        }),
    )
    .await?;
    drain(&mut one, "bad auth").await;

    section("presence channel");
    let data = json!({ "user_id": 1, "user_info": { "name": "Test User" } }).to_string();
    subscribe(&mut one, &args, "presence-test-channel", Some(&data)).await?;
    drain(&mut one, "subscribe presence").await;

    section("cache channel, empty");
    subscribe(&mut one, &args, "cache-test-channel", None).await?;
    drain(&mut one, "subscribe cache").await;

    section("trigger an event over the HTTP API");
    let body = json!({
        "name": "App\\Events\\TestEvent",
        "channel": "cache-test-channel",
        "data": r#"{"foo":"bar"}"#,
    })
    .to_string();
    api(&args, "POST", &format!("/apps/{}/events", args.app_id), &[], Some(&body)).await;
    drain(&mut one, "broadcast").await;

    section("cache channel, replayed");
    let mut two = open(&args, "connection_established (second client)").await?;
    subscribe(&mut two, &args, "cache-test-channel", None).await?;
    drain(&mut two, "subscribe cache again").await;

    section("presence membership");
    let data = json!({ "user_id": 2, "user_info": { "name": "Second User" } }).to_string();
    subscribe(&mut two, &args, "presence-test-channel", Some(&data)).await?;
    drain(&mut two, "second presence subscribe").await;
    drain(&mut one, "member_added seen by the first client").await;

    section("client event");
    send(
        &mut two,
        json!({
            "event": "client-typing",
            "channel": "presence-test-channel",
            "data": { "typing": true },
        }),
    )
    .await?;
    drain(&mut one, "whisper seen by the first client").await;
    drain(&mut two, "whisper echoed to the sender").await;

    section("client event from a non-member");
    send(
        &mut one,
        json!({ "event": "client-typing", "channel": "private-nope", "data": {} }),
    )
    .await?;
    drain(&mut one, "non-member whisper").await;

    section("ping");
    send(&mut one, json!({ "event": "pusher:ping" })).await?;
    drain(&mut one, "ping").await;

    section("unknown pusher event");
    send(&mut one, json!({ "event": "pusher:nonsense" })).await?;
    drain(&mut one, "unknown event").await;

    section("malformed frame");
    one.socket.send(Message::Text("not json".into())).await?;
    drain(&mut one, "malformed").await;

    section("unsubscribe");
    send(
        &mut one,
        json!({ "event": "pusher:unsubscribe", "data": { "channel": "test-channel" } }),
    )
    .await?;
    drain(&mut one, "unsubscribe").await;

    section("HTTP API");
    api(&args, "GET", "/up", &[], None).await;
    api(&args, "GET", &format!("/apps/{}/connections", args.app_id), &[], None).await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels", args.app_id),
        &[("info", "user_count")],
        None,
    )
    .await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels/presence-test-channel", args.app_id),
        &[("info", "user_count,subscription_count,cache")],
        None,
    )
    .await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels/cache-test-channel", args.app_id),
        &[("info", "subscription_count,cache")],
        None,
    )
    .await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels/does-not-exist", args.app_id),
        &[("info", "subscription_count")],
        None,
    )
    .await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels/presence-test-channel/users", args.app_id),
        &[],
        None,
    )
    .await;
    api(
        &args,
        "GET",
        &format!("/apps/{}/channels/test-channel/users", args.app_id),
        &[],
        None,
    )
    .await;

    section("HTTP API failures");
    api(&args, "GET", "/apps/999999/channels", &[], None).await;
    unsigned(&args, &format!("/apps/{}/channels", args.app_id)).await;

    section("batch events");
    let body = json!({
        "batch": [
            { "name": "First", "channel": "presence-test-channel", "data": "{}", "info": "user_count" },
            { "name": "Second", "channel": "presence-test-channel", "data": "{}" },
        ]
    })
    .to_string();
    api(&args, "POST", &format!("/apps/{}/batch_events", args.app_id), &[], Some(&body)).await;

    section("routing and limits");
    api(&args, "GET", "/nope", &[], None).await;
    api(&args, "GET", "/apps//channels", &[], None).await;
    wrong_method(&args, &format!("/apps/{}/channels", args.app_id)).await;
    oversized(&args, &format!("/apps/{}/events", args.app_id)).await;

    section("unknown application key");
    let mut stray = raw_open(&args.addr, "nope").await?;
    if let Some(frame) = next_text(&mut stray).await {
        println!("  {frame}");
    }

    Ok(())
}

fn section(title: &str) {
    println!("\n## {title}");
}

async fn open(args: &Args, label: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let mut socket = raw_open(&args.addr, &args.key).await?;

    let frame = next_text(&mut socket).await.ok_or("no connection_established")?;
    let parsed: Value = serde_json::from_str(&frame)?;
    let data: Value = serde_json::from_str(parsed["data"].as_str().unwrap_or("{}"))?;
    let id = data["socket_id"].as_str().unwrap_or_default().to_string();

    println!("  {label}: {}", mask(&frame, &id));

    Ok(Client { socket, id })
}

async fn raw_open(addr: &str, key: &str) -> Result<Socket, Box<dyn std::error::Error>> {
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/app/{key}")).await?;

    Ok(socket)
}

async fn send(client: &mut Client, message: Value) -> Result<(), Box<dyn std::error::Error>> {
    client.socket.send(Message::Text(message.to_string().into())).await?;

    Ok(())
}

async fn subscribe(
    client: &mut Client,
    args: &Args,
    channel: &str,
    channel_data: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut signed = format!("{}:{}", client.id, channel);

    if let Some(data) = channel_data {
        signed.push(':');
        signed.push_str(data);
    }

    let mut data = json!({
        "channel": channel,
        "auth": format!("{}:{}", args.key, sign(&args.secret, &signed)),
    });

    if let Some(channel_data) = channel_data {
        data["channel_data"] = json!(channel_data);
    }

    send(client, json!({ "event": "pusher:subscribe", "data": data })).await
}

/// Print every frame that arrives within a short window.
async fn drain(client: &mut Client, label: &str) {
    let mut frames = Vec::new();

    while let Ok(Some(frame)) =
        tokio::time::timeout(Duration::from_millis(400), next_text(&mut client.socket)).await
    {
        frames.push(mask(&frame, &client.id));
    }

    if frames.is_empty() {
        println!("  {label}: <no frames>");
        return;
    }

    for frame in frames {
        println!("  {label}: {frame}");
    }
}

async fn next_text(socket: &mut Socket) -> Option<String> {
    loop {
        match socket.next().await? {
            Ok(Message::Text(text)) => return Some(text.to_string()),
            Ok(Message::Ping(_) | Message::Pong(_)) => continue,
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => continue,
        }
    }
}

/// Replace the random socket ID so transcripts compare cleanly.
fn mask(frame: &str, id: &str) -> String {
    if id.is_empty() {
        return frame.to_string();
    }

    frame.replace(id, "<SOCKET_ID>")
}

/// Issue a signed API request and print its status and body.
async fn api(args: &Args, method: &str, path: &str, params: &[(&str, &str)], body: Option<&str>) {
    let timestamp =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();

    let mut signed: Vec<(String, String)> = vec![
        ("auth_key".into(), args.key.clone()),
        ("auth_timestamp".into(), timestamp.to_string()),
        ("auth_version".into(), "1.0".into()),
    ];

    if let Some(body) = body {
        use md5::Digest;

        signed.push(("body_md5".into(), format!("{:x}", md5::Md5::digest(body.as_bytes()))));
    }

    for (key, value) in params {
        signed.push(((*key).to_string(), (*value).to_string()));
    }

    signed.sort_by(|a, b| a.0.cmp(&b.0));

    let query = signed.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");
    let signature = sign(&args.secret, &format!("{method}\n{path}\n{query}"));

    let encoded = signed
        .iter()
        .map(|(k, v)| {
            format!("{k}={}", form_urlencoded::byte_serialize(v.as_bytes()).collect::<String>())
        })
        .collect::<Vec<_>>()
        .join("&");

    let url = format!("http://{}{path}?{encoded}&auth_signature={signature}", args.addr);
    let client = reqwest::Client::new();

    let request = match method {
        "POST" => client.post(&url).body(body.unwrap_or("").to_string()),
        _ => client.get(&url),
    };

    match request.send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();

            println!("  {method} {path} -> {status} {body}");
        }
        Err(error) => println!("  {method} {path} -> transport error: {error}"),
    }
}

/// Hit a known route with a method it does not serve.
async fn wrong_method(args: &Args, path: &str) {
    match reqwest::Client::new().post(format!("http://{}{path}", args.addr)).send().await {
        Ok(response) => {
            let status = response.status().as_u16();
            let allow = response
                .headers()
                .get("allow")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_string();
            let body = response.text().await.unwrap_or_default();

            println!("  POST {path} (wrong method) -> {status} [allow: {allow}] {body}");
        }
        Err(error) => println!("  POST {path} (wrong method) -> transport error: {error}"),
    }
}

/// Send a body past `max_request_size` to compare the rejection.
async fn oversized(args: &Args, path: &str) {
    let body = json!({ "name": "Big", "channel": "c", "data": "x".repeat(50_000) }).to_string();

    match reqwest::Client::new()
        .post(format!("http://{}{path}", args.addr))
        .body(body)
        .send()
        .await
    {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();

            println!("  POST {path} (50 KB body) -> {status} {body}");
        }
        Err(error) => println!("  POST {path} (50 KB body) -> transport error: {error}"),
    }
}

/// Request without a signature, to compare rejection behaviour.
async fn unsigned(args: &Args, path: &str) {
    match reqwest::get(format!("http://{}{path}", args.addr)).await {
        Ok(response) => {
            let status = response.status().as_u16();
            let body = response.text().await.unwrap_or_default();

            println!("  GET {path} (unsigned) -> {status} {body}");
        }
        Err(error) => println!("  GET {path} (unsigned) -> transport error: {error}"),
    }
}
