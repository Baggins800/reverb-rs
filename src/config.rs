//! Configuration, mirroring the shape of Laravel Reverb's `config/reverb.php`.
//!
//! Values are read from the environment using the same variable names Reverb
//! uses, so an existing `.env` works unchanged. Deployments running the
//! multi-app `config` provider can point `REVERB_APPS_FILE` at a JSON export of
//! the `reverb.apps.apps` array instead.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use serde::Deserialize;

/// Who the application accepts `client-*` events from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientEvents {
    All,
    Members,
    Disabled,
}

impl ClientEvents {
    fn parse(value: &str) -> Self {
        match value {
            "all" => Self::All,
            "members" => Self::Members,
            _ => Self::Disabled,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiting {
    pub enabled: bool,
    pub max_attempts: u32,
    pub decay_seconds: u64,
    pub terminate_on_limit: bool,
}

impl Default for RateLimiting {
    fn default() -> Self {
        Self { enabled: false, max_attempts: 60, decay_seconds: 60, terminate_on_limit: false }
    }
}

/// A Reverb application: the unit of tenancy, keyed by `app_id` and `key`.
#[derive(Debug)]
pub struct Application {
    pub id: String,
    pub key: String,
    pub secret: String,
    pub ping_interval: u64,
    pub activity_timeout: u64,
    pub allowed_origins: Vec<String>,
    pub max_message_size: usize,
    pub max_connections: Option<usize>,
    pub accept_client_events_from: ClientEvents,
    pub rate_limiting: RateLimiting,
}

impl Application {
    /// Whether `origin` is permitted to open a connection to this application.
    ///
    /// Mirrors Reverb: the host is extracted from the `Origin` header and
    /// matched against each pattern with `*` acting as a multi-character
    /// wildcard. A missing `Origin` is rejected unless `*` is allowed.
    pub fn origin_allowed(&self, origin: Option<&str>) -> bool {
        if self.allowed_origins.iter().any(|o| o == "*") {
            return true;
        }

        let Some(host) = origin.and_then(host_of) else {
            return false;
        };

        self.allowed_origins.iter().any(|pattern| wildcard_match(pattern, &host))
    }
}

/// Extract the host component of a URL the way `parse_url($origin, PHP_URL_HOST)` does.
fn host_of(origin: &str) -> Option<String> {
    let rest = match origin.find("://") {
        Some(i) => &origin[i + 3..],
        None => origin,
    };

    if rest.is_empty() {
        return None;
    }

    // Strip userinfo, then path/query/fragment, then the port.
    let rest = rest.rsplit_once('@').map(|(_, h)| h).unwrap_or(rest);
    let rest = rest.split(['/', '?', '#']).next().unwrap_or(rest);

    // An IPv6 literal keeps its brackets, and its inner colons are not a port.
    let host = match rest.strip_prefix('[').and_then(|inner| inner.find(']')) {
        Some(closing) => &rest[..closing + 2],
        None => rest.split(':').next().unwrap_or(rest),
    };

    if host.is_empty() { None } else { Some(host.to_string()) }
}

/// `Str::is()` semantics: `*` matches any run of characters, everything else is literal.
fn wildcard_match(pattern: &str, value: &str) -> bool {
    if pattern == value {
        return true;
    }

    let mut parts = pattern.split('*');
    let Some(first) = parts.next() else { return false };

    if !value.starts_with(first) {
        return false;
    }

    let mut cursor = first.len();
    let mut last: Option<&str> = None;

    for part in parts {
        last = Some(part);
        if part.is_empty() {
            continue;
        }
        match value[cursor..].find(part) {
            Some(i) => cursor += i + part.len(),
            None => return false,
        }
    }

    match last {
        // The pattern contained no `*` at all and the literal comparison failed.
        None => false,
        // A trailing literal must anchor to the end of the value.
        Some(tail) if !tail.is_empty() => value.ends_with(tail) && cursor <= value.len(),
        // The pattern ended in `*`, so whatever remains is absorbed.
        Some(_) => true,
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl TlsConfig {
    /// Look for a locally trusted certificate for `hostname`, in the same
    /// Herd and Valet directories Reverb searches.
    pub fn for_hostname(hostname: &str) -> Option<Self> {
        let host = host_of(hostname).unwrap_or_else(|| hostname.to_string());
        let home = PathBuf::from(std::env::var("HOME").ok()?);

        let roots = [
            // Herd, macOS.
            home.join("Library/Application Support/Herd/config/valet/Certificates"),
            // Herd, Linux and Windows.
            home.join(".config/herd/config/valet/Certificates"),
            home.join(".config/valet/Certificates"),
        ];

        roots.iter().find_map(|root| {
            let cert = root.join(format!("{host}.crt"));
            let key = root.join(format!("{host}.key"));

            (cert.is_file() && key.is_file()).then_some(Self { cert, key })
        })
    }
}

/// Relaying Reverb's events back to a Laravel application.
#[derive(Debug, Clone)]
pub struct EventsConfig {
    pub enabled: bool,
    pub channel: String,
    pub redis_url: String,
    /// Which of Reverb's five events to forward.
    pub forward: crate::events::EventSet,
    /// Fraction of the two per-frame message events to relay, 0.0 to 1.0.
    pub message_sample_rate: f64,
    /// Events per Redis publish, and how long to wait to fill a batch.
    pub batch_size: usize,
    pub flush_interval_ms: u64,
    pub queue_depth: usize,
}

#[derive(Debug, Clone)]
pub struct ScalingConfig {
    pub enabled: bool,
    pub channel: String,
    pub redis_url: String,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub path: String,
    pub hostname: Option<String>,
    pub max_request_size: usize,
    pub tls: Option<TlsConfig>,
    pub scaling: ScalingConfig,
    pub events: EventsConfig,
    pub apps: Vec<Arc<Application>>,
    /// Per-connection outbound queue depth. A client that falls this far behind
    /// is disconnected rather than allowed to consume unbounded memory.
    pub send_queue_depth: usize,
    /// Bytes reserved per direction for each WebSocket's framing buffers.
    ///
    /// Pusher frames are small, so the library default of 128 KiB per direction
    /// would dominate memory long before the connections themselves did.
    pub ws_buffer_size: usize,
    /// Seconds between prune-and-ping sweeps. Reverb's is fixed at 60.
    pub maintenance_interval: u64,
    /// Pending-connection queue depth passed to `listen(2)`.
    ///
    /// Clients reconnecting en masse — a deploy, a network blip — arrive as one
    /// burst; too small a backlog drops their SYNs and costs each a one-second
    /// retransmit. Capped by `net.core.somaxconn`.
    pub listen_backlog: u32,
}

fn env(key: &str) -> Option<String> {
    match std::env::var(key) {
        Ok(v) if !v.is_empty() && v != "null" => Some(v),
        _ => None,
    }
}

fn env_or(key: &str, default: &str) -> String {
    env(key).unwrap_or_else(|| default.to_string())
}

fn env_parse<T: std::str::FromStr>(key: &str, default: T) -> T {
    env(key).and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    match env(key) {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "on" | "yes"),
        None => default,
    }
}

/// The JSON shape of one entry in `reverb.apps.apps`, for `REVERB_APPS_FILE`.
#[derive(Debug, Deserialize)]
struct AppFileEntry {
    app_id: String,
    key: String,
    secret: String,
    #[serde(default)]
    allowed_origins: Option<Vec<String>>,
    #[serde(default)]
    ping_interval: Option<u64>,
    #[serde(default)]
    activity_timeout: Option<u64>,
    #[serde(default)]
    max_message_size: Option<usize>,
    #[serde(default)]
    max_connections: Option<usize>,
    #[serde(default)]
    accept_client_events_from: Option<String>,
    #[serde(default)]
    rate_limiting: Option<RateLimitingFile>,
}

#[derive(Debug, Deserialize)]
struct RateLimitingFile {
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    max_attempts: Option<u32>,
    #[serde(default)]
    decay_seconds: Option<u64>,
    #[serde(default)]
    terminate_on_limit: Option<bool>,
}

impl ServerConfig {
    /// Build the configuration from the environment, applying the same
    /// defaults as the stock `config/reverb.php`.
    pub fn from_env() -> Result<Self> {
        let apps = match env("REVERB_APPS_FILE") {
            Some(path) => Self::apps_from_file(&path)?,
            None => vec![Arc::new(Self::app_from_env()?)],
        };

        let hostname = env("REVERB_HOST");

        let tls = match (env("REVERB_SERVER_TLS_CERT"), env("REVERB_SERVER_TLS_KEY")) {
            (Some(cert), Some(key)) => Some(TlsConfig { cert: cert.into(), key: key.into() }),
            (Some(_), None) | (None, Some(_)) => {
                bail!("REVERB_SERVER_TLS_CERT and REVERB_SERVER_TLS_KEY must be set together")
            }
            // Fall back to a Herd or Valet certificate for the hostname, which
            // is how Reverb turns on TLS for local development.
            (None, None) => hostname.as_deref().and_then(TlsConfig::for_hostname),
        };

        Ok(Self {
            host: env_or("REVERB_SERVER_HOST", "0.0.0.0"),
            port: env_parse("REVERB_SERVER_PORT", 8080),
            path: normalize_path(&env_or("REVERB_SERVER_PATH", "")),
            hostname,
            max_request_size: env_parse("REVERB_MAX_REQUEST_SIZE", 10_000),
            tls,
            scaling: ScalingConfig {
                enabled: env_bool("REVERB_SCALING_ENABLED", false),
                channel: env_or("REVERB_SCALING_CHANNEL", "reverb"),
                redis_url: redis_url(),
            },
            events: EventsConfig {
                enabled: env_bool("REVERB_EVENTS_ENABLED", false),
                channel: env_or("REVERB_EVENTS_CHANNEL", "reverb-rs:events"),
                redis_url: redis_url(),
                // Message events fire once per delivered frame, so only the
                // lifecycle events are forwarded unless asked for explicitly.
                forward: match env("REVERB_EVENTS_TYPES") {
                    Some(list) => crate::events::EventSet::parse(&list),
                    None => crate::events::EventSet::of(crate::events::EventKind::LIFECYCLE),
                },
                message_sample_rate: env_parse("REVERB_EVENTS_SAMPLE_RATE", 1.0),
                batch_size: env_parse("REVERB_EVENTS_BATCH_SIZE", 500),
                flush_interval_ms: env_parse("REVERB_EVENTS_FLUSH_MS", 100),
                queue_depth: env_parse("REVERB_EVENTS_QUEUE_DEPTH", 100_000),
            },
            apps,
            send_queue_depth: env_parse("REVERB_SEND_QUEUE_DEPTH", 1024),
            ws_buffer_size: env_parse("REVERB_WS_BUFFER_SIZE", 4096),
            maintenance_interval: env_parse("REVERB_MAINTENANCE_INTERVAL", 60),
            listen_backlog: env_parse("REVERB_LISTEN_BACKLOG", 4096),
        })
    }

    fn app_from_env() -> Result<Application> {
        let key = env("REVERB_APP_KEY").context("REVERB_APP_KEY is not set")?;
        let secret = env("REVERB_APP_SECRET").context("REVERB_APP_SECRET is not set")?;
        let id = env("REVERB_APP_ID").context("REVERB_APP_ID is not set")?;

        Ok(Application {
            id,
            key,
            secret,
            ping_interval: env_parse("REVERB_APP_PING_INTERVAL", 60),
            activity_timeout: env_parse("REVERB_APP_ACTIVITY_TIMEOUT", 30),
            allowed_origins: env("REVERB_APP_ALLOWED_ORIGINS")
                .map(|v| v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect())
                .unwrap_or_else(|| vec!["*".to_string()]),
            max_message_size: env_parse("REVERB_APP_MAX_MESSAGE_SIZE", 10_000),
            max_connections: env("REVERB_APP_MAX_CONNECTIONS").and_then(|v| v.parse().ok()),
            accept_client_events_from: ClientEvents::parse(&env_or(
                "REVERB_APP_ACCEPT_CLIENT_EVENTS_FROM",
                "members",
            )),
            rate_limiting: RateLimiting {
                enabled: env_bool("REVERB_APP_RATE_LIMITING_ENABLED", false),
                max_attempts: env_parse("REVERB_APP_RATE_LIMIT_MAX_ATTEMPTS", 60),
                decay_seconds: env_parse("REVERB_APP_RATE_LIMIT_DECAY_SECONDS", 60),
                terminate_on_limit: env_bool("REVERB_APP_RATE_LIMIT_TERMINATE", false),
            },
        })
    }

    fn apps_from_file(path: &str) -> Result<Vec<Arc<Application>>> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read REVERB_APPS_FILE [{path}]"))?;

        let entries: Vec<AppFileEntry> = serde_json::from_str(&raw)
            .with_context(|| format!("failed to parse REVERB_APPS_FILE [{path}]"))?;

        if entries.is_empty() {
            bail!("REVERB_APPS_FILE [{path}] defines no applications");
        }

        Ok(entries
            .into_iter()
            .map(|e| {
                let rl = e.rate_limiting;
                Arc::new(Application {
                    id: e.app_id,
                    key: e.key,
                    secret: e.secret,
                    ping_interval: e.ping_interval.unwrap_or(60),
                    activity_timeout: e.activity_timeout.unwrap_or(30),
                    allowed_origins: e.allowed_origins.unwrap_or_else(|| vec!["*".into()]),
                    max_message_size: e.max_message_size.unwrap_or(10_000),
                    max_connections: e.max_connections,
                    // Reverb's config provider defaults this to "all" when the
                    // key is absent from the app definition.
                    accept_client_events_from: ClientEvents::parse(
                        e.accept_client_events_from.as_deref().unwrap_or("all"),
                    ),
                    rate_limiting: match rl {
                        Some(rl) => RateLimiting {
                            enabled: rl.enabled,
                            max_attempts: rl.max_attempts.unwrap_or(60),
                            decay_seconds: rl.decay_seconds.unwrap_or(60),
                            terminate_on_limit: rl.terminate_on_limit.unwrap_or(false),
                        },
                        None => RateLimiting::default(),
                    },
                })
            })
            .collect())
    }
}

/// Assemble a Redis URL from the same variables Reverb's scaling block reads.
fn redis_url() -> String {
    if let Some(url) = env("REDIS_URL") {
        return url;
    }

    let host = env_or("REDIS_HOST", "127.0.0.1");
    let port = env_or("REDIS_PORT", "6379");
    let db = env_or("REDIS_DB", "0");
    let username = env("REDIS_USERNAME").unwrap_or_default();
    let password = env("REDIS_PASSWORD");

    let auth = match password {
        Some(pass) => format!("{username}:{pass}@"),
        None => String::new(),
    };

    format!("redis://{auth}{host}:{port}/{db}")
}

/// Normalize the route prefix to either `""` or `"/segment"` with no trailing slash.
fn normalize_path(path: &str) -> String {
    let trimmed = path.trim().trim_matches('/');

    if trimmed.is_empty() { String::new() } else { format!("/{trimmed}") }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_the_host_from_an_origin() {
        assert_eq!(host_of("http://laravel.com"), Some("laravel.com".into()));
        assert_eq!(host_of("https://laravel.com:8080/path"), Some("laravel.com".into()));
        assert_eq!(host_of("https://user:pw@laravel.com"), Some("laravel.com".into()));
        assert_eq!(host_of("laravel.com"), Some("laravel.com".into()));
        assert_eq!(host_of("http://[::1]:8080"), Some("[::1]".into()));
        assert_eq!(host_of(""), None);
    }

    #[test]
    fn matches_origin_wildcards() {
        assert!(wildcard_match("laravel.com", "laravel.com"));
        assert!(!wildcard_match("laravel.com", "evil.com"));
        assert!(wildcard_match("*.laravel.com", "app.laravel.com"));
        assert!(!wildcard_match("*.laravel.com", "laravel.com.evil.com"));
        assert!(wildcard_match("*", "anything"));
        assert!(wildcard_match("app.*.com", "app.laravel.com"));
        assert!(!wildcard_match("app.*.com", "app.laravel.net"));
    }

    #[test]
    fn finds_a_locally_trusted_certificate_for_a_hostname() {
        let home = std::env::temp_dir().join(format!("reverb-rs-tls-{}", std::process::id()));
        let certs = home.join(".config/valet/Certificates");

        std::fs::create_dir_all(&certs).expect("create cert dir");
        std::fs::write(certs.join("reverb.test.crt"), "cert").expect("write cert");
        std::fs::write(certs.join("reverb.test.key"), "key").expect("write key");

        // SAFETY: single-threaded test, and the value is restored below.
        let previous = std::env::var("HOME").ok();
        unsafe { std::env::set_var("HOME", &home) };

        let resolved = TlsConfig::for_hostname("https://reverb.test");
        let missing = TlsConfig::for_hostname("nothing.test");

        unsafe {
            match previous {
                Some(home) => std::env::set_var("HOME", home),
                None => std::env::remove_var("HOME"),
            }
        }

        std::fs::remove_dir_all(&home).ok();

        assert_eq!(resolved.map(|tls| tls.cert), Some(certs.join("reverb.test.crt")));
        assert!(missing.is_none());
    }

    #[test]
    fn normalizes_the_route_prefix() {
        assert_eq!(normalize_path(""), "");
        assert_eq!(normalize_path("/"), "");
        assert_eq!(normalize_path("reverb"), "/reverb");
        assert_eq!(normalize_path("/reverb/"), "/reverb");
    }
}
