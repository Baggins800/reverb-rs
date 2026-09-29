//! Load generation and measurement, shared by the `bench` and `benchmark`
//! examples.
//!
//! Opens N subscribers on one channel, publishes M events through the Pusher
//! HTTP API, and measures what it cost the server to deliver the resulting
//! N×M frames: throughput, bandwidth, latency, CPU and memory.

// Shared by two examples, each of which uses a subset of it.
#![allow(dead_code)]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::stream;
use futures_util::{SinkExt, StreamExt};
use reverb_rs::server::sign;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A server to put under load.
#[derive(Debug, Clone)]
pub struct Target {
    pub addr: String,
    pub app_id: String,
    pub key: String,
    pub secret: String,
    /// The server's process ID, for CPU and memory accounting.
    pub pid: Option<u32>,
}

/// One unit of work, applied identically to every server being compared.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: &'static str,
    pub title: &'static str,
    pub connections: usize,
    pub events: usize,
    pub payload_bytes: usize,
    /// How many connections to open at once, or all of them when absent.
    pub connect_concurrency: Option<usize>,
    /// How many API publishes to keep in flight.
    pub publish_concurrency: usize,
    /// Pin the server to these CPUs, as a `taskset -c` list.
    ///
    /// Reverb is single-threaded by design, so its wall-clock results reflect
    /// having one core whatever the machine has. Pinning both servers to one
    /// core removes that difference and compares the runtimes directly.
    pub cpus: Option<&'static str>,
    /// Report only memory for this scenario.
    ///
    /// A scenario that delivers almost nothing finishes in milliseconds, and
    /// `/proc` accounts CPU in 10ms ticks — the throughput and CPU figures
    /// would be quantisation noise rather than a measurement.
    pub memory_only: bool,
}

impl Scenario {
    pub fn new(
        name: &'static str,
        title: &'static str,
        connections: usize,
        events: usize,
        payload_bytes: usize,
    ) -> Self {
        Self {
            name,
            title,
            connections,
            events,
            payload_bytes,
            connect_concurrency: None,
            publish_concurrency: 16,
            cpus: None,
            memory_only: false,
        }
    }

    /// Run this scenario with the server pinned to one core.
    pub fn on_one_core(mut self) -> Self {
        self.cpus = Some("0");
        self
    }

    /// A scenario that only reports memory.
    pub fn memory_only(mut self) -> Self {
        self.memory_only = true;
        self
    }
}

/// What one run cost.
#[derive(Debug, Clone, Default)]
pub struct Measurement {
    pub subscribers_served: usize,
    pub frames: usize,
    pub seconds: f64,
    pub frames_per_second: f64,
    pub megabits_per_second: f64,
    pub kilobytes_per_second: f64,
    pub connect_per_second: f64,
    pub publish_per_second: f64,
    pub latency_p50_ms: f64,
    pub latency_p99_ms: f64,
    pub latency_max_ms: f64,
    pub cpu_seconds: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub cpu_us_per_frame: Option<f64>,
    pub rss_idle_mb: Option<f64>,
    pub rss_connected_mb: Option<f64>,
    pub rss_peak_mb: Option<f64>,
    pub rss_kb_per_connection: Option<f64>,
}

impl Measurement {
    /// Every metric by name, for reporting and for taking medians.
    pub fn metrics(&self) -> Vec<(&'static str, Option<f64>)> {
        vec![
            ("frames_per_second", Some(self.frames_per_second)),
            ("megabits_per_second", Some(self.megabits_per_second)),
            ("kilobytes_per_second", Some(self.kilobytes_per_second)),
            ("publish_per_second", Some(self.publish_per_second)),
            ("connect_per_second", Some(self.connect_per_second)),
            ("seconds", Some(self.seconds)),
            ("cpu_seconds", self.cpu_seconds),
            ("cpu_percent", self.cpu_percent),
            ("cpu_us_per_frame", self.cpu_us_per_frame),
            ("latency_p50_ms", Some(self.latency_p50_ms)),
            ("latency_p99_ms", Some(self.latency_p99_ms)),
            ("latency_max_ms", Some(self.latency_max_ms)),
            ("rss_idle_mb", self.rss_idle_mb),
            ("rss_connected_mb", self.rss_connected_mb),
            ("rss_peak_mb", self.rss_peak_mb),
            ("rss_kb_per_connection", self.rss_kb_per_connection),
        ]
    }

    pub fn to_json(&self, scenario: &Scenario, label: &str) -> Value {
        let mut out = json!({
            "scenario": scenario.name,
            "label": label,
            "connections": scenario.connections,
            "events": scenario.events,
            "payload_bytes": scenario.payload_bytes,
            "subscribers_served": self.subscribers_served,
            "frames": self.frames,
        });

        for (key, value) in self.metrics() {
            out[key] = match value {
                Some(v) => json!(v),
                None => Value::Null,
            };
        }

        out
    }
}

/// Run one scenario against one server.
pub async fn measure(
    target: &Target,
    scenario: &Scenario,
    progress: bool,
) -> Result<Measurement, Box<dyn std::error::Error + Send + Sync>> {
    let target = Arc::new(target.clone());
    let scenario = Arc::new(scenario.clone());

    let baseline_rss = target.pid.and_then(resident_kb);

    // -- Connect ------------------------------------------------------------
    let (ready_tx, mut ready_rx) = mpsc::channel(scenario.connections.max(1));
    let connect_started = Instant::now();

    let mut subscribers = Vec::with_capacity(scenario.connections);
    let batch = scenario.connect_concurrency.unwrap_or(scenario.connections).max(1);
    let mut opened = 0;

    while opened < scenario.connections {
        let wave = batch.min(scenario.connections - opened);

        for _ in 0..wave {
            let (target, scenario, ready) = (target.clone(), scenario.clone(), ready_tx.clone());

            subscribers.push(tokio::spawn(async move {
                subscribe_and_collect(target, scenario, ready).await
            }));
        }

        for _ in 0..wave {
            ready_rx.recv().await.ok_or("a subscriber failed to connect")?;
        }

        opened += wave;
    }

    drop(ready_tx);

    let connect_elapsed = connect_started.elapsed();
    let connected_rss = target.pid.and_then(resident_kb);

    if progress {
        eprintln!(
            "  {} subscribers ready in {:.2}s",
            scenario.connections,
            connect_elapsed.as_secs_f64()
        );
    }

    // Let the server settle before the measured window opens.
    tokio::time::sleep(Duration::from_millis(250)).await;

    // -- Publish and collect ------------------------------------------------
    // One pooled client for every publish: building one per request would
    // measure the load generator rather than the server.
    let http = Arc::new(reqwest::Client::builder().pool_max_idle_per_host(64).build()?);

    let cpu_before = target.pid.and_then(cpu_seconds);
    let started = Instant::now();

    stream::iter(0..scenario.events)
        .map(|sequence| {
            let (target, scenario, http) = (target.clone(), scenario.clone(), http.clone());

            async move { trigger(&http, &target, &scenario, sequence).await }
        })
        .buffer_unordered(scenario.publish_concurrency)
        .for_each(|result| async {
            if let Err(error) = result {
                eprintln!("  publish failed: {error}");
            }
        })
        .await;

    let publish_elapsed = started.elapsed();

    let mut latencies = Vec::with_capacity(scenario.connections * scenario.events);
    let mut served = 0;
    let mut bytes = 0u64;

    for handle in subscribers {
        match handle.await {
            Ok(Some((samples, received))) => {
                served += 1;
                latencies.extend(samples);
                bytes += received;
            }
            _ => eprintln!("  a subscriber did not receive every frame"),
        }
    }

    let elapsed = started.elapsed();
    let cpu_after = target.pid.and_then(cpu_seconds);
    let peak_rss = target.pid.and_then(peak_resident_kb);

    latencies.sort_unstable();

    let seconds = elapsed.as_secs_f64().max(f64::MIN_POSITIVE);
    let frames = latencies.len();
    let cpu_used = cpu_before.zip(cpu_after).map(|(before, after)| after - before);

    Ok(Measurement {
        subscribers_served: served,
        frames,
        seconds,
        frames_per_second: frames as f64 / seconds,
        megabits_per_second: (bytes as f64 * 8.0) / 1_000_000.0 / seconds,
        kilobytes_per_second: bytes as f64 / 1024.0 / seconds,
        connect_per_second: scenario.connections as f64 / connect_elapsed.as_secs_f64(),
        publish_per_second: scenario.events as f64 / publish_elapsed.as_secs_f64(),
        latency_p50_ms: percentile(&latencies, 0.50) as f64 / 1e6,
        latency_p99_ms: percentile(&latencies, 0.99) as f64 / 1e6,
        latency_max_ms: latencies.last().copied().unwrap_or(0) as f64 / 1e6,
        cpu_seconds: cpu_used,
        cpu_percent: cpu_used.map(|cpu| cpu / seconds * 100.0),
        cpu_us_per_frame: cpu_used.map(|cpu| cpu * 1_000_000.0 / frames.max(1) as f64),
        rss_idle_mb: baseline_rss.map(mb),
        rss_connected_mb: connected_rss.map(mb),
        rss_peak_mb: peak_rss.map(mb),
        rss_kb_per_connection: peak_rss
            .zip(baseline_rss)
            .map(|(peak, base)| peak.saturating_sub(base) as f64 / scenario.connections as f64),
    })
}

/// Open one connection, subscribe, and record every frame's latency and size.
async fn subscribe_and_collect(
    target: Arc<Target>,
    scenario: Arc<Scenario>,
    ready: mpsc::Sender<()>,
) -> Option<(Vec<u64>, u64)> {
    let url = format!("ws://{}/app/{}", target.addr, target.key);
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.ok()?;

    next_text(&mut socket).await?; // connection_established

    let subscribe =
        json!({ "event": "pusher:subscribe", "data": { "channel": scenario.channel() } })
            .to_string();

    socket.send(Message::Text(subscribe.into())).await.ok()?;
    next_text(&mut socket).await?;

    ready.send(()).await.ok()?;
    drop(ready);

    let mut latencies = Vec::with_capacity(scenario.events);
    let mut bytes = 0u64;

    while latencies.len() < scenario.events {
        let frame =
            tokio::time::timeout(Duration::from_secs(120), next_text(&mut socket)).await.ok()??;

        // Payload plus the unmasked server frame header.
        bytes += frame.len() as u64 + if frame.len() < 126 { 2 } else { 4 };

        let Ok(parsed) = serde_json::from_str::<Value>(&frame) else { continue };
        let Some(data) = parsed.get("data").and_then(Value::as_str) else { continue };
        let Ok(data) = serde_json::from_str::<Value>(data) else { continue };
        let Some(sent_at) = data.get("t").and_then(Value::as_u64) else { continue };

        latencies.push(now_nanos().saturating_sub(sent_at));
    }

    Some((latencies, bytes))
}

impl Scenario {
    /// A channel name unique to the scenario, so runs cannot bleed together.
    fn channel(&self) -> String {
        format!("bench-{}", self.name)
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

/// Publish one event through the signed HTTP API.
async fn trigger(
    http: &reqwest::Client,
    target: &Target,
    scenario: &Scenario,
    sequence: usize,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Pad to the requested size so bandwidth varies independently of message
    // rate. The publish timestamp rides along so latency stays measurable.
    let stub = json!({ "n": sequence, "t": now_nanos(), "pad": "" }).to_string();
    let padding = scenario.payload_bytes.saturating_sub(stub.len());

    let body = json!({
        "name": "BenchEvent",
        "channel": scenario.channel(),
        "data": json!({ "n": sequence, "t": now_nanos(), "pad": "x".repeat(padding) })
            .to_string(),
    })
    .to_string();

    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let body_md5 = {
        use md5::Digest;

        format!("{:x}", md5::Md5::digest(body.as_bytes()))
    };

    let path = format!("/apps/{}/events", target.app_id);
    let query = format!(
        "auth_key={}&auth_timestamp={}&auth_version=1.0&body_md5={}",
        target.key, timestamp, body_md5
    );
    let signature = sign(&target.secret, &format!("POST\n{path}\n{query}"));

    let response = http
        .post(format!("http://{}{}?{}&auth_signature={}", target.addr, path, query, signature))
        .body(body)
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("trigger failed: {}", response.status()).into());
    }

    Ok(())
}

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn mb(kb: u64) -> f64 {
    kb as f64 / 1024.0
}

pub fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }

    sorted[((sorted.len() - 1) as f64 * fraction).round() as usize]
}

/// Resident set size in KB.
pub fn resident_kb(pid: u32) -> Option<u64> {
    proc_status_kb(pid, "VmRSS:")
}

/// Peak resident set size in KB, since the process started.
pub fn peak_resident_kb(pid: u32) -> Option<u64> {
    proc_status_kb(pid, "VmHWM:")
}

fn proc_status_kb(pid: u32, field: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()?
        .lines()
        .find(|line| line.starts_with(field))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

/// CPU seconds a process has consumed, user plus system.
///
/// `/proc/<pid>/stat` reports these in USER_HZ, which the kernel fixes at 100
/// for userspace regardless of the configured tick rate.
pub fn cpu_seconds(pid: u32) -> Option<f64> {
    const USER_HZ: f64 = 100.0;

    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;

    // The process name can contain spaces and parentheses, so fields are
    // counted from after the closing parenthesis.
    let fields: Vec<&str> = stat[stat.rfind(')')? + 1..].split_whitespace().collect();

    // After the state field, utime and stime are the 12th and 13th entries.
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;

    Some((utime + stime) / USER_HZ)
}
