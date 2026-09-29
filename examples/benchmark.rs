//! Compare Laravel Reverb and reverb-rs, and write `benchmark.md`.
//!
//!     cargo run --release --example benchmark -- --reverb-php /path/to/reverb-checkout
//!
//! Both servers are restarted before every measurement so idle memory is
//! genuinely idle, each scenario runs several times and the median is
//! reported, and both are given exactly the same work so wall time, CPU and
//! memory are comparable.
//!
//! The Reverb checkout needs `composer install` to have been run in it.

#[path = "support/loadgen.rs"]
mod loadgen;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use clap::Parser;
use loadgen::{Measurement, Scenario, Target};
use tokio::net::TcpStream;
use tokio::process::{Child, Command};

const APP_ID: &str = "123456";
const APP_KEY: &str = "reverb-key";
const APP_SECRET: &str = "reverb-secret";

/// Sized so the slower server runs for several seconds: CPU accounting in
/// `/proc` has 10ms granularity, so short runs would be mostly quantisation.
fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario::new(
            "small-payload",
            "500 subscribers, 1000 events, 100 B payload",
            500,
            1000,
            100,
        ),
        Scenario::new("large-payload", "500 subscribers, 300 events, 4 KB payload", 500, 300, 4096),
        Scenario::new(
            "many-connections",
            "800 subscribers, 500 events, 256 B payload",
            800,
            500,
            256,
        ),
        Scenario::new("idle", "800 idle subscribers (memory only)", 800, 1, 100).memory_only(),
        // The same work with both servers confined to a single core.
        Scenario::new(
            "one-core",
            "500 subscribers, 500 events, 100 B payload — one core each",
            500,
            500,
            100,
        )
        .on_one_core(),
    ]
}

#[derive(Parser, Debug)]
#[command(about = "Compare Laravel Reverb and reverb-rs")]
struct Args {
    /// A Laravel Reverb checkout with `composer install` already run.
    #[arg(long)]
    reverb_php: PathBuf,

    /// How many times to run each scenario. The median is reported.
    #[arg(long, default_value_t = 3)]
    repeats: usize,

    #[arg(long, default_value_t = 8090)]
    php_port: u16,

    #[arg(long, default_value_t = 8091)]
    rust_port: u16,

    #[arg(long, default_value = "benchmark.md")]
    out: PathBuf,
}

/// A server process, started fresh for each measurement and torn down after.
struct Server {
    child: Child,
    pid: u32,
}

impl Server {
    /// Spawn a command and wait for it to accept connections.
    async fn start(
        mut command: Command,
        port: u16,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let child =
            command.stdout(Stdio::null()).stderr(Stdio::null()).kill_on_drop(true).spawn()?;

        // The command is the server itself, so its own PID is the one to
        // account against.
        let pid = child.id().ok_or("the server exited immediately")?;

        wait_for_port(port, Duration::from_secs(30)).await?;

        // Let allocation settle before idle memory is sampled.
        tokio::time::sleep(Duration::from_secs(1)).await;

        Ok(Self { child, pid })
    }

    async fn stop(mut self, port: u16) {
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;

        // Wait for the port to be released before the next server binds it.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

        while tokio::time::Instant::now() < deadline {
            if TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                return;
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

async fn wait_for_port(
    port: u16,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deadline = tokio::time::Instant::now() + timeout;

    while tokio::time::Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    Err(format!("nothing listening on port {port} after {timeout:?}").into())
}

/// Build a command, optionally confined to a set of CPUs.
fn command_on(program: &str, args: &[&str], cpus: Option<&str>) -> Command {
    let mut command = match cpus {
        Some(list) => {
            // `taskset` sets affinity and then execs, so the process keeps its
            // PID and CPU accounting still points at the right place.
            let mut c = Command::new("taskset");
            c.arg("-c").arg(list).arg(program);
            c
        }
        None => Command::new(program),
    };

    command.args(args);
    command
}

fn reverb_rs_command(port: u16, cpus: Option<&str>) -> Command {
    let mut command = command_on("./target/release/reverb-rs", &[], cpus);

    // Match the runtime to the cores it is allowed to use, so a pinned server
    // is not paying to schedule threads it cannot run.
    if let Some(list) = cpus {
        command.env("TOKIO_WORKER_THREADS", list.split(',').count().to_string());
    }

    command
        .env("REVERB_APP_ID", APP_ID)
        .env("REVERB_APP_KEY", APP_KEY)
        .env("REVERB_APP_SECRET", APP_SECRET)
        .env("REVERB_SERVER_HOST", "127.0.0.1")
        .env("REVERB_SERVER_PORT", port.to_string());

    command
}

fn reverb_php_command(checkout: &Path, port: u16, cpus: Option<&str>) -> Command {
    let mut command = command_on("php", &["bench/reverb-php-serve.php"], cpus);

    command
        .env("REVERB_CHECKOUT", checkout)
        .env("BENCH_HOST", "127.0.0.1")
        .env("BENCH_PORT", port.to_string());

    command
}

/// Every scenario against one server, restarting it between each run.
async fn run(
    label: &str,
    port: u16,
    build: impl Fn(u16, Option<&str>) -> Command,
    repeats: usize,
) -> BTreeMap<String, Option<BTreeMap<String, f64>>> {
    let mut results = BTreeMap::new();

    for scenario in scenarios() {
        let mut runs: Vec<Measurement> = Vec::new();

        for attempt in 1..=repeats {
            eprintln!("  {label}: {} ({attempt}/{repeats})", scenario.name);

            let server = match Server::start(build(port, scenario.cpus), port).await {
                Ok(server) => server,
                Err(error) => {
                    eprintln!("    could not start: {error}");
                    continue;
                }
            };

            let target = Target {
                addr: format!("127.0.0.1:{port}"),
                app_id: APP_ID.into(),
                key: APP_KEY.into(),
                secret: APP_SECRET.into(),
                pid: Some(server.pid),
            };

            match loadgen::measure(&target, &scenario, false).await {
                Ok(measurement) => runs.push(measurement),
                Err(error) => eprintln!("    failed: {error}"),
            }

            server.stop(port).await;
        }

        results.insert(scenario.name.to_string(), medians(&runs));
    }

    results
}

/// The median of each metric across the runs, or `None` if all of them failed.
fn medians(runs: &[Measurement]) -> Option<BTreeMap<String, f64>> {
    if runs.is_empty() {
        return None;
    }

    let mut out = BTreeMap::new();

    for (key, _) in runs[0].metrics() {
        let mut values: Vec<f64> = runs
            .iter()
            .filter_map(|run| run.metrics().into_iter().find(|(k, _)| *k == key)?.1)
            .collect();

        if values.is_empty() {
            continue;
        }

        values.sort_by(|a, b| a.partial_cmp(b).unwrap());
        out.insert(key.to_string(), values[values.len() / 2]);
    }

    Some(out)
}

type Results = BTreeMap<String, Option<BTreeMap<String, f64>>>;

fn value(results: &Results, scenario: &str, key: &str) -> Option<f64> {
    results.get(scenario)?.as_ref()?.get(key).copied()
}

fn cell(value: Option<f64>, decimals: usize, suffix: &str) -> String {
    match value {
        Some(v) => format!("{v:.decimals$}{suffix}"),
        None => "—".into(),
    }
}

/// How much better reverb-rs is, or worse if it loses.
fn ratio(rust: Option<f64>, php: Option<f64>, higher_is_better: bool) -> String {
    let (Some(rust), Some(php)) = (rust, php) else { return "—".into() };

    if rust == 0.0 || php == 0.0 {
        return "—".into();
    }

    let factor = if higher_is_better { rust / php } else { php / rust };

    if factor >= 1.0 {
        format!("**{factor:.1}×**")
    } else {
        format!("{:.1}× worse", 1.0 / factor)
    }
}

struct Meta {
    date: String,
    cores: usize,
    kernel: String,
    php_version: String,
    rust_version: String,
    reverb_version: String,
    repeats: usize,
    caveats: Vec<String>,
}

fn report(php: &Results, rust: &Results, meta: &Meta) -> String {
    // (title, key, decimals, suffix, higher is better)
    let rows: &[(&str, &str, usize, &str, bool)] = &[
        ("Messages delivered", "frames_per_second", 0, " msg/s", true),
        ("Wire throughput", "megabits_per_second", 1, " Mbit/s", true),
        ("Wire throughput (KB/s)", "kilobytes_per_second", 0, " KB/s", true),
        ("HTTP publish rate", "publish_per_second", 0, " req/s", true),
        ("Wall time for the run", "seconds", 2, " s", false),
        ("Server CPU used", "cpu_seconds", 2, " s", false),
        ("CPU per message", "cpu_us_per_frame", 2, " µs", false),
        ("Latency p50", "latency_p50_ms", 1, " ms", false),
        ("Latency p99", "latency_p99_ms", 1, " ms", false),
        ("Memory idle", "rss_idle_mb", 1, " MB", false),
        ("Memory with subscribers", "rss_connected_mb", 1, " MB", false),
        ("Memory peak", "rss_peak_mb", 1, " MB", false),
        ("Memory per connection", "rss_kb_per_connection", 1, " KB", false),
    ];

    let mut out = String::new();

    out.push_str(&format!(
        "# Benchmark: Laravel Reverb vs reverb-rs\n\n\
         Generated by `cargo run --release --example benchmark` on {}.\n\n\
         ## Method\n\n\
         Both servers were given identical work: the same number of subscribers on one\n\
         channel, the same number of events, the same payload size. Each server was\n\
         restarted before every measurement, each scenario ran {} times, and the median is\n\
         reported. The load generator is the same Rust client in both cases, and it pools\n\
         its HTTP connections so it is not itself the bottleneck.\n\n\
         CPU is read from `/proc/<pid>/stat` (user + system) across the measured window.\n\
         Memory is `VmRSS` when idle and `VmHWM` at peak. Wire throughput counts payload\n\
         plus WebSocket frame headers, not TCP/IP overhead.\n\n\
         | | |\n|---|---|\n\
         | Host | {} cores, Linux {} |\n\
         | Laravel Reverb | {} on PHP {} |\n\
         | reverb-rs | {} |\n\
         | Repeats per scenario | {} (median reported) |\n\n",
        meta.date,
        meta.repeats,
        meta.cores,
        meta.kernel,
        meta.reverb_version,
        meta.php_version,
        meta.rust_version,
        meta.repeats,
    ));

    if !meta.caveats.is_empty() {
        out.push_str("> **Caveats.** ");
        out.push_str(&meta.caveats.join(" "));
        out.push_str("\n\n");
    }

    for scenario in scenarios() {
        out.push_str(&format!("## {}\n\n", scenario.title));

        let php_ok = php.get(scenario.name).and_then(Option::as_ref).is_some();
        let rust_ok = rust.get(scenario.name).and_then(Option::as_ref).is_some();

        if !php_ok || !rust_ok {
            let failed = if php_ok { "reverb-rs" } else { "Laravel Reverb" };

            out.push_str(&format!("{failed} could not complete this scenario.\n\n"));

            if !php_ok && rust_ok {
                out.push_str(&format!(
                    "reverb-rs completed it: {} at {} peak.\n\n",
                    cell(value(rust, scenario.name, "frames_per_second"), 0, " msg/s"),
                    cell(value(rust, scenario.name, "rss_peak_mb"), 1, " MB"),
                ));
            }

            continue;
        }

        if let Some(cpus) = scenario.cpus {
            out.push_str(&format!(
                "Both servers are pinned to CPU {cpus} with `taskset`, and the Rust runtime is \
                 given one worker thread to match. Reverb is single-threaded whatever the \
                 machine has, so this is the comparison with that advantage removed: it is \
                 the two runtimes doing the same work with the same resources.\n\n",
            ));
        }

        if scenario.memory_only {
            out.push_str(concat!(
                "Connections are opened and held, then memory is sampled. ",
                "Throughput and CPU are not reported: the run is too short for ",
                "`/proc`'s 10ms accounting to say anything meaningful about them.\n\n",
            ));
        }

        out.push_str("| Metric | Laravel Reverb | reverb-rs | |\n|---|---|---|---|\n");

        for (title, key, decimals, suffix, higher) in rows {
            if scenario.memory_only && !key.starts_with("rss_") {
                continue;
            }

            let (p, r) = (value(php, scenario.name, key), value(rust, scenario.name, key));

            if p.is_none() && r.is_none() {
                continue;
            }

            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                title,
                cell(p, *decimals, suffix),
                cell(r, *decimals, suffix),
                ratio(r, p, *higher),
            ));
        }

        out.push('\n');
    }

    out.push_str(
        "## Reading these numbers\n\n\
         **CPU per message** is the metric that survives a change of hardware. It is the\n\
         work each server does to put one frame on one socket, independent of how many\n\
         cores are available — Reverb is single-threaded by design, so its wall-clock\n\
         results also reflect having only one core to use.\n\n\
         **Wire throughput** is measured over loopback, so the figures are far above what\n\
         any real network interface would carry. Read them as a ceiling the server does not\n\
         impose, not as a rate you will see in production.\n\n\
         **Memory per connection** is peak RSS minus idle RSS, divided by the connection\n\
         count. PHP's allocator reuses a pool, so its figure moves around between runs more\n\
         than the Rust one does.\n\n\
         **HTTP publish rate** is how fast the server accepts events on its Pusher API. In\n\
         the fan-out scenarios this bounds the measured window: a server that accepts\n\
         events slowly cannot deliver them quickly either.\n\n\
         Reproduce with:\n\n\
         ```bash\n\
         cargo build --release\n\
         cargo run --release --example benchmark -- --reverb-php /path/to/reverb-checkout\n\
         ```\n",
    );

    out
}

async fn command_output(program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .output()
        .await
        .ok()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let args = Args::parse();

    if !args.reverb_php.join("vendor/autoload.php").is_file() {
        return Err(format!("run composer install in {} first", args.reverb_php.display()).into());
    }

    if !Path::new("./target/release/reverb-rs").is_file() {
        return Err("run cargo build --release first".into());
    }

    eprintln!("Laravel Reverb:");
    let checkout = args.reverb_php.clone();
    let php = run(
        "reverb",
        args.php_port,
        move |port, cpus| reverb_php_command(&checkout, port, cpus),
        args.repeats,
    )
    .await;

    eprintln!("reverb-rs:");
    let rust = run("reverb-rs", args.rust_port, reverb_rs_command, args.repeats).await;

    let mut caveats = vec![
        "Both servers ran on loopback on the same host as the load generator, so network \
         latency is excluded and CPU is shared with the client."
            .to_string(),
    ];

    if !command_output("php", &["-m"]).await.to_lowercase().contains("opcache") {
        caveats.push(
            "PHP ran without the opcache extension, which also rules out JIT; for a \
             long-lived daemon opcache mostly affects startup rather than steady-state \
             throughput, but the gap would narrow somewhat with it enabled."
                .to_string(),
        );
    }

    let meta = Meta {
        date: command_output("date", &["+%Y-%m-%d"]).await,
        cores: std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
        kernel: command_output("uname", &["-r"]).await,
        php_version: command_output("php", &["-r", "echo PHP_VERSION;"]).await,
        rust_version: command_output("rustc", &["--version"]).await,
        reverb_version: {
            let rev = command_output(
                "git",
                &["-C", args.reverb_php.to_str().unwrap_or("."), "rev-parse", "--short", "HEAD"],
            )
            .await;

            if rev == "unknown" { "working copy".into() } else { rev }
        },
        repeats: args.repeats,
        caveats,
    };

    std::fs::write(&args.out, report(&php, &rust, &meta))?;

    eprintln!("\nwrote {}", args.out.display());

    Ok(())
}
