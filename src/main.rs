//! `reverb-rs` — the Reverb WebSocket server, in Rust.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::accept::NoDelayAcceptor;
use axum_server::tls_rustls::{RustlsAcceptor, RustlsConfig};
use axum_server::Handle;
use clap::Parser;
use tokio::net::{TcpSocket, TcpStream};
use reverb_rs::config::ServerConfig;
use reverb_rs::events::Telemetry;
use reverb_rs::pubsub::PubSub;
use reverb_rs::server::Server;
use tracing_subscriber::EnvFilter;

/// How long in-flight requests get to finish once a shutdown signal arrives.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

#[derive(Parser, Debug)]
#[command(name = "reverb-rs", about = "Start the Reverb server", version)]
struct Cli {
    /// The IP address the server should bind to.
    #[arg(long)]
    host: Option<String>,

    /// The port the server should listen on.
    #[arg(long)]
    port: Option<u16>,

    /// The path the server should prefix to all routes.
    #[arg(long)]
    path: Option<String>,

    /// The hostname the server is accessible from.
    #[arg(long)]
    hostname: Option<String>,

    /// Display debug messages in the terminal.
    #[arg(long)]
    debug: bool,

    /// Load environment variables from this file before starting.
    #[arg(long, default_value = ".env")]
    env_file: String,

    /// Probe a running server instead of starting one, then exit 0 if healthy.
    ///
    /// Container images here carry no shell or HTTP client, so the binary
    /// doubles as its own health probe.
    #[arg(long)]
    healthcheck: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // A missing env file is fine: the environment may already carry everything.
    let _ = dotenvy::from_filename(&cli.env_file);

    let default_level = if cli.debug { "debug" } else { "info" };

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new(format!("reverb_rs={default_level},warn"))),
        )
        .init();

    let mut config = ServerConfig::load().context("failed to load configuration")?;

    // Command line options win over the environment, as in `reverb:start`.
    if let Some(host) = cli.host {
        config.host = host;
    }
    if let Some(port) = cli.port {
        config.port = port;
    }
    if let Some(path) = cli.path {
        config.path = if path.trim_matches('/').is_empty() {
            String::new()
        } else {
            format!("/{}", path.trim_matches('/'))
        };
    }
    if let Some(hostname) = cli.hostname {
        config.hostname = Some(hostname);
    }

    if config.tls.is_some() {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .ok()
            .context("a rustls crypto provider is already installed")
            .ok();
    }

    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .with_context(|| format!("invalid bind address [{}:{}]", config.host, config.port))?;

    if cli.healthcheck {
        return healthcheck(addr, &config.path, config.tls.is_some()).await;
    }

    let backlog = config.listen_backlog;
    let restart = config.restart.clone();
    let scaling = config.scaling.clone();
    let events = config.events.clone();
    let tls = config.tls.clone();
    let path = config.path.clone();
    let apps = config.apps.len();

    let telemetry = match events.enabled && !events.forward.is_empty() {
        true => {
            let client = redis::Client::open(events.redis_url.as_str())
                .context("invalid Redis URL for the event relay")?;

            let manager = redis::aio::ConnectionManager::new(client)
                .await
                .context("failed to connect to Redis for the event relay")?;

            let (tx, rx) = tokio::sync::mpsc::channel(events.queue_depth);

            tokio::spawn(reverb_rs::events::relay_loop(
                rx,
                manager,
                events.channel.clone(),
                events.batch_size,
                Duration::from_millis(events.flush_interval_ms),
            ));

            tracing::info!(
                channel = %events.channel,
                forwarding = ?events.forward.names(),
                message_sample_rate = events.message_sample_rate,
                "relaying events to laravel"
            );

            Arc::new(Telemetry::sampled(events.forward, tx, events.message_sample_rate))
        }
        false => Arc::new(Telemetry::disabled()),
    };

    let server = Arc::new(Server::with_telemetry(config, telemetry));

    if scaling.enabled {
        let pubsub =
            PubSub::connect(&scaling.redis_url, scaling.channel.clone(), server.clone())
                .await
                .context("failed to connect to Redis for horizontal scaling")?;

        server.attach_pubsub(pubsub);

        tracing::info!(channel = %scaling.channel, "horizontal scaling enabled");
    }

    tokio::spawn(reverb_rs::maintain(server.clone()));

    let app = reverb_rs::router(server.clone());
    let handle = Handle::new();

    tokio::spawn(shutdown(handle.clone(), server.clone()));

    // `php artisan reverb:restart` signals through the Laravel cache rather
    // than the process, so watch for it the way Reverb does.
    if restart.is_enabled() {
        let (handle, server) = (handle.clone(), server.clone());

        tokio::spawn(reverb_rs::restart::watch(restart, move || {
            reverb_rs::disconnect_all(&server);
            handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
        }));
    }

    tracing::info!(
        %addr,
        apps,
        secure = tls.is_some(),
        path = %if path.is_empty() { "/".into() } else { path.clone() },
        "starting reverb-rs"
    );

    let service = app.into_make_service();
    let listener = bind(addr, backlog)?;

    // WebSocket frames are small and latency-sensitive, so disable Nagle's
    // algorithm rather than let small writes wait to be coalesced.
    match tls {
        Some(tls) => {
            let rustls = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
                .await
                .context("failed to load the TLS certificate and key")?;

            axum_server::from_tcp(listener)
                .handle(handle)
                .acceptor(RustlsAcceptor::new(rustls).acceptor(NoDelayAcceptor::new()))
                .serve(service)
                .await?;
        }
        None => {
            axum_server::from_tcp(listener)
                .handle(handle)
                .acceptor(NoDelayAcceptor::new())
                .serve(service)
                .await?;
        }
    }

    Ok(())
}

/// Ask a running server whether it is healthy.
///
/// Speaks just enough HTTP/1.1 to fetch `/up`, so the image needs no client.
/// A TLS-terminating server is only checked for reachability, since the probe
/// does not negotiate a session.
async fn healthcheck(addr: SocketAddr, path: &str, secure: bool) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // A wildcard bind address is not connectable; talk to the loopback instead.
    let target = match addr.ip().is_unspecified() {
        true => SocketAddr::from(([127, 0, 0, 1], addr.port())),
        false => addr,
    };

    let mut stream = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(target))
        .await
        .context("health check timed out connecting")?
        .with_context(|| format!("health check could not connect to {target}"))?;

    if secure {
        println!("{target} is accepting connections (TLS, body not checked)");

        return Ok(());
    }

    let request =
        format!("GET {path}/up HTTP/1.1\r\nHost: {target}\r\nConnection: close\r\n\r\n");

    stream.write_all(request.as_bytes()).await.context("health check could not send")?;

    let mut response = String::new();

    tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response))
        .await
        .context("health check timed out reading")?
        .context("health check could not read")?;

    if !response.starts_with("HTTP/1.1 200") || !response.contains("\"health\":\"OK\"") {
        anyhow::bail!("unhealthy: {}", response.lines().next().unwrap_or("no response"));
    }

    println!("healthy");

    Ok(())
}

/// Bind the listening socket with an explicit backlog.
///
/// `TcpListener::bind` would queue only 1024 pending connections, which a
/// reconnect storm overruns; each dropped SYN then costs the client a full
/// second waiting to retransmit.
fn bind(addr: SocketAddr, backlog: u32) -> Result<std::net::TcpListener> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4(),
        SocketAddr::V6(_) => TcpSocket::new_v6(),
    }
    .context("failed to create the listening socket")?;

    socket.set_reuseaddr(true)?;
    socket.bind(addr).with_context(|| format!("failed to bind {addr}"))?;

    Ok(socket.listen(backlog)?.into_std()?)
}

/// Wait for a termination signal, disconnect clients, then stop accepting.
async fn shutdown(handle: Handle, server: Arc<Server>) {
    let interrupt = async {
        tokio::signal::ctrl_c().await.expect("failed to listen for ctrl-c");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }

    tracing::info!("gracefully terminating connections");

    reverb_rs::disconnect_all(&server);

    handle.graceful_shutdown(Some(SHUTDOWN_GRACE));
}
