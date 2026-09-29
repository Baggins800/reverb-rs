//! Harness for driving a live server the way a real Pusher client would.

// Shared by several test binaries, each of which uses a subset of it.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use reverb_rs::config::{
    Application, ClientEvents, EventsConfig, RateLimiting, ScalingConfig, ServerConfig,
};
use reverb_rs::events::EventSet;
use reverb_rs::server::{Server, sign};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

pub const APP_ID: &str = "reverb-app-id";
pub const APP_KEY: &str = "reverb-key";
pub const APP_SECRET: &str = "reverb-secret";

/// How long a test waits for a frame before declaring the server silent.
const FRAME_TIMEOUT: Duration = Duration::from_secs(2);

pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub struct TestServer {
    pub addr: SocketAddr,
    pub server: Arc<Server>,
}

/// Build an application with the defaults Reverb's own test suite uses.
pub fn application() -> Application {
    Application {
        id: APP_ID.into(),
        key: APP_KEY.into(),
        secret: APP_SECRET.into(),
        ping_interval: 60,
        activity_timeout: 30,
        allowed_origins: vec!["*".into()],
        max_message_size: 10_000,
        max_connections: None,
        accept_client_events_from: ClientEvents::Members,
        rate_limiting: RateLimiting::default(),
    }
}

pub async fn start() -> TestServer {
    start_with(application()).await
}

/// Start a node that relays the given events, returning the relay's receiving
/// end so a test can assert on what Laravel would have been sent.
pub async fn start_with_events(
    app: Application,
    forward: EventSet,
) -> (TestServer, tokio::sync::mpsc::Receiver<reverb_rs::events::RelayedEvent>) {
    let (tx, rx) = tokio::sync::mpsc::channel(4096);
    let telemetry = Arc::new(reverb_rs::events::Telemetry::with_relay(forward, tx));
    let server = Arc::new(Server::with_telemetry(config_for(app), telemetry));

    (serve(server).await, rx)
}

/// Start a server configured entirely from an exported Laravel config.
pub async fn start_from_config(path: &std::path::Path) -> TestServer {
    // SAFETY: the config is read immediately below, on this thread.
    unsafe { std::env::set_var("REVERB_CONFIG_FILE", path) };

    let mut config = ServerConfig::load().expect("load the exported config");

    unsafe { std::env::remove_var("REVERB_CONFIG_FILE") };

    // The export names the port the Laravel app would use; the test binds its own.
    config.port = 0;
    config.host = "127.0.0.1".into();

    serve(Arc::new(Server::new(config))).await
}

/// Start a node whose relay publishes to Redis, the way production does.
pub async fn start_with_redis_relay(
    app: Application,
    forward: EventSet,
    redis_url: &str,
    channel: &str,
) -> TestServer {
    let client = redis::Client::open(redis_url).expect("redis client");
    let manager = redis::aio::ConnectionManager::new(client).await.expect("redis connection");

    let (tx, rx) = tokio::sync::mpsc::channel(4096);

    tokio::spawn(reverb_rs::events::relay_loop(
        rx,
        manager,
        channel.to_string(),
        128,
        Duration::from_millis(25),
    ));

    let telemetry = Arc::new(reverb_rs::events::Telemetry::with_relay(forward, tx));

    serve(Arc::new(Server::with_telemetry(config_for(app), telemetry))).await
}

/// Start a node that shares state with its peers over Redis.
pub async fn start_scaled(redis_url: &str, channel: &str) -> TestServer {
    let mut config = config_for(application());

    config.scaling = ScalingConfig {
        enabled: true,
        channel: channel.to_string(),
        redis_url: redis_url.to_string(),
    };

    let server = Arc::new(Server::new(config));

    let pubsub = reverb_rs::pubsub::PubSub::connect(redis_url, channel.to_string(), server.clone())
        .await
        .expect("connect to redis");

    server.attach_pubsub(pubsub);

    serve(server).await
}

/// Start a server on an ephemeral port serving a single application.
pub async fn start_with(app: Application) -> TestServer {
    serve(Arc::new(Server::new(config_for(app)))).await
}

fn config_for(app: Application) -> ServerConfig {
    ServerConfig {
        host: "127.0.0.1".into(),
        port: 0,
        path: String::new(),
        hostname: None,
        max_request_size: 10_000,
        tls: None,
        scaling: ScalingConfig {
            enabled: false,
            channel: "reverb".into(),
            redis_url: String::new(),
        },
        events: EventsConfig {
            enabled: false,
            channel: "reverb-rs:events".into(),
            redis_url: String::new(),
            forward: EventSet::none(),
            message_sample_rate: 1.0,
            batch_size: 500,
            flush_interval_ms: 100,
            queue_depth: 1024,
        },
        apps: vec![Arc::new(app)],
        send_queue_depth: 1024,
        ws_read_buffer_size: 4096,
        ws_write_buffer_size: 4096,
        maintenance_interval: 60,
        listen_backlog: 4096,
        restart: reverb_rs::restart::RestartWatch::Off,
    }
}

/// Bind an ephemeral port and serve the given server on it.
async fn serve(server: Arc<Server>) -> TestServer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let router = reverb_rs::router(server.clone());

    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });

    TestServer { addr, server }
}

impl TestServer {
    /// Open a connection and consume the `connection_established` frame,
    /// returning the socket and its assigned socket ID.
    pub async fn connect(&self) -> (Socket, String) {
        self.connect_with_origin(None).await
    }

    pub async fn connect_with_origin(&self, origin: Option<&str>) -> (Socket, String) {
        let mut socket = self.open(APP_KEY, origin).await;
        let frame = next_json(&mut socket).await;

        assert_eq!(frame["event"], "pusher:connection_established");

        let data: Value =
            serde_json::from_str(frame["data"].as_str().expect("data")).expect("data json");

        let socket_id = data["socket_id"].as_str().expect("socket_id").to_string();

        (socket, socket_id)
    }

    /// Open a raw connection without reading anything from it.
    pub async fn open(&self, key: &str, origin: Option<&str>) -> Socket {
        let url = format!("ws://{}/app/{}", self.addr, key);
        let mut request = url.into_client_request().expect("request");

        if let Some(origin) = origin {
            request.headers_mut().insert("origin", origin.parse().expect("origin header"));
        }

        let (socket, _) = tokio_tungstenite::connect_async(request).await.expect("connect");

        socket
    }

    /// Issue a signed HTTP API request, as `pusher-php-server` would.
    pub async fn api(
        &self,
        method: &str,
        path: &str,
        params: &[(&str, &str)],
        body: Option<&str>,
    ) -> reqwest::Response {
        self.api_at(method, path, params, body, unix_time()).await
    }

    /// As [`api`], but with an explicit `auth_timestamp`.
    pub async fn api_at(
        &self,
        method: &str,
        path: &str,
        params: &[(&str, &str)],
        body: Option<&str>,
        timestamp: i64,
    ) -> reqwest::Response {
        let timestamp = timestamp.to_string();
        let body_md5 = body.map(|b| format!("{:x}", md5_of(b.as_bytes())));

        let mut signed: Vec<(String, String)> = vec![
            ("auth_key".into(), APP_KEY.into()),
            ("auth_timestamp".into(), timestamp.clone()),
            ("auth_version".into(), "1.0".into()),
        ];

        if let Some(hash) = &body_md5 {
            signed.push(("body_md5".into(), hash.clone()));
        }

        for (key, value) in params {
            signed.push(((*key).to_string(), (*value).to_string()));
        }

        signed.sort_by(|a, b| a.0.cmp(&b.0));

        let query = signed.iter().map(|(k, v)| format!("{k}={v}")).collect::<Vec<_>>().join("&");

        let signature = sign(APP_SECRET, &format!("{method}\n{path}\n{query}"));

        let url = format!(
            "http://{}{}?{}&auth_signature={}",
            self.addr,
            path,
            urlencode_pairs(&signed),
            signature
        );

        let client = reqwest::Client::new();
        let request = match method {
            "POST" => client.post(&url),
            _ => client.get(&url),
        };

        let request = match body {
            Some(body) => request.body(body.to_string()),
            None => request,
        };

        request.send().await.expect("api request")
    }

    /// Trigger an event through the HTTP API, as a Laravel broadcast would.
    pub async fn trigger(&self, channel: &str, event: &str, data: Value) -> reqwest::Response {
        let body =
            json!({ "name": event, "channel": channel, "data": data.to_string() }).to_string();

        self.api("POST", &format!("/apps/{APP_ID}/events"), &[], Some(&body)).await
    }
}

/// Percent-encode signed parameters for the request URL.
fn urlencode_pairs(pairs: &[(String, String)]) -> String {
    pairs
        .iter()
        .map(|(k, v)| {
            format!("{}={}", k, form_urlencoded::byte_serialize(v.as_bytes()).collect::<String>())
        })
        .collect::<Vec<_>>()
        .join("&")
}

fn md5_of(bytes: &[u8]) -> md5::digest::Output<md5::Md5> {
    use md5::Digest;

    md5::Md5::digest(bytes)
}

pub fn unix_time() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// -- Client-side protocol helpers --------------------------------------------

pub async fn send(socket: &mut Socket, message: Value) {
    socket.send(Message::Text(message.to_string().into())).await.expect("send");
}

/// Read the next text frame, failing the test if none arrives in time.
pub async fn next_text(socket: &mut Socket) -> String {
    loop {
        let message = tokio::time::timeout(FRAME_TIMEOUT, socket.next())
            .await
            .expect("timed out waiting for a frame")
            .expect("stream ended")
            .expect("frame");

        match message {
            Message::Text(text) => return text.to_string(),
            // Control frames are handled by the client library; keep reading.
            Message::Ping(_) | Message::Pong(_) => continue,
            other => panic!("expected a text frame, got {other:?}"),
        }
    }
}

pub async fn next_json(socket: &mut Socket) -> Value {
    serde_json::from_str(&next_text(socket).await).expect("frame json")
}

/// Assert that nothing arrives within a short window.
pub async fn assert_silent(socket: &mut Socket) {
    let result = tokio::time::timeout(Duration::from_millis(250), socket.next()).await;

    if let Ok(Some(Ok(Message::Text(text)))) = result {
        panic!("expected no frame, received {text}");
    }
}

/// The `auth` value a client computes for a private or presence subscription.
pub fn auth_for(socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
    let mut signed = format!("{socket_id}:{channel}");

    if let Some(data) = channel_data {
        signed.push(':');
        signed.push_str(data);
    }

    format!("{APP_KEY}:{}", sign(APP_SECRET, &signed))
}

/// Subscribe and return the `subscription_succeeded` frame.
pub async fn subscribe(socket: &mut Socket, socket_id: &str, channel: &str) -> String {
    subscribe_with_data(socket, socket_id, channel, None).await
}

pub async fn subscribe_with_data(
    socket: &mut Socket,
    socket_id: &str,
    channel: &str,
    channel_data: Option<Value>,
) -> String {
    let encoded = channel_data.map(|d| d.to_string());
    let mut data = json!({ "channel": channel });

    if reverb_rs::channel::ChannelKind::of(channel).requires_auth() {
        data["auth"] = json!(auth_for(socket_id, channel, encoded.as_deref()));
    }

    if let Some(encoded) = &encoded {
        data["channel_data"] = json!(encoded);
    }

    send(socket, json!({ "event": "pusher:subscribe", "data": data })).await;

    next_text(socket).await
}
