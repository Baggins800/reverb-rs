//! Put one Pusher-protocol server under load and report what it cost.
//!
//!     cargo run --release --example bench -- --addr 127.0.0.1:8080 --connections 500
//!
//! To compare two servers head to head, use the `benchmark` example instead,
//! which starts and stops them for you and writes `benchmark.md`.

#[path = "support/loadgen.rs"]
mod loadgen;

use clap::Parser;
use loadgen::{Scenario, Target};

#[derive(Parser, Debug)]
#[command(about = "Benchmark a Pusher-protocol WebSocket server")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,

    #[arg(long, default_value = "123456")]
    app_id: String,

    #[arg(long, default_value = "reverb-key")]
    key: String,

    #[arg(long, default_value = "reverb-secret")]
    secret: String,

    /// Number of concurrent subscribers.
    #[arg(long, default_value_t = 500)]
    connections: usize,

    /// Number of events to broadcast to them.
    #[arg(long, default_value_t = 100)]
    events: usize,

    /// Size of each event's payload, in bytes.
    #[arg(long, default_value_t = 100)]
    payload_bytes: usize,

    /// How many connections to open at once. Defaults to all of them, which
    /// also tests how the server copes with a reconnect storm; lower it to
    /// measure steady-state capacity independently of the listen backlog.
    #[arg(long)]
    connect_concurrency: Option<usize>,

    /// How many API publishes to keep in flight at once.
    #[arg(long, default_value_t = 16)]
    publish_concurrency: usize,

    /// Report the resident memory and CPU of this process alongside the results.
    #[arg(long)]
    pid: Option<u32>,

    /// Print one JSON object instead of a human-readable table.
    #[arg(long)]
    json: bool,

    /// Label for the results.
    #[arg(long, default_value = "server")]
    label: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    let target = Target {
        addr: args.addr.clone(),
        app_id: args.app_id.clone(),
        key: args.key.clone(),
        secret: args.secret.clone(),
        pid: args.pid,
    };

    let mut scenario =
        Scenario::new("adhoc", "ad hoc run", args.connections, args.events, args.payload_bytes);

    scenario.connect_concurrency = args.connect_concurrency;
    scenario.publish_concurrency = args.publish_concurrency;

    if !args.json {
        eprintln!("[{}] opening {} connections to {}", args.label, args.connections, args.addr);
    }

    let result = loadgen::measure(&target, &scenario, !args.json).await?;

    if args.json {
        println!("{}", result.to_json(&scenario, &args.label));

        return Ok(());
    }

    println!("\n=== {} ===", args.label);
    println!("connections                {}", args.connections);
    println!("events published           {}", args.events);
    println!("payload per event          {} bytes", args.payload_bytes);
    println!("subscribers fully served   {}/{}", result.subscribers_served, args.connections);
    println!("frames delivered           {}", result.frames);
    println!("connect + subscribe        {:.0} conn/s", result.connect_per_second);
    println!("publish rate               {:.0} req/s", result.publish_per_second);
    println!(
        "fan-out throughput         {:.0} msg/s over {:.2}s",
        result.frames_per_second, result.seconds
    );
    println!(
        "wire throughput            {:.1} Mbit/s ({:.0} KB/s)",
        result.megabits_per_second, result.kilobytes_per_second
    );
    println!(
        "delivery latency           p50 {:.2}ms  p99 {:.2}ms  max {:.2}ms",
        result.latency_p50_ms, result.latency_p99_ms, result.latency_max_ms
    );

    if let (Some(cpu), Some(percent), Some(per_frame)) =
        (result.cpu_seconds, result.cpu_percent, result.cpu_us_per_frame)
    {
        println!(
            "server cpu                 {cpu:.2}s ({percent:.0}% of one core) — {per_frame:.2} us/msg"
        );
    }

    if let (Some(idle), Some(connected), Some(peak)) =
        (result.rss_idle_mb, result.rss_connected_mb, result.rss_peak_mb)
    {
        println!(
            "resident memory            {idle:.1} MB idle -> {connected:.1} MB connected -> {peak:.1} MB peak"
        );
    }

    if let Some(per_connection) = result.rss_kb_per_connection {
        println!("memory per connection      {per_connection:.1} KB");
    }

    Ok(())
}
