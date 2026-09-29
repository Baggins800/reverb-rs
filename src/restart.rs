//! Support for `php artisan reverb:restart`.
//!
//! Reverb does not restart itself: the command writes a timestamp to the
//! Laravel cache, and the running server polls that key and stops when it
//! changes. Deploy scripts rely on it, so this reads the same key from the
//! same cache store and shuts down the same way.

use std::path::{Path, PathBuf};
use std::time::Duration;

use sha1::{Digest, Sha1};

/// The Laravel cache key Reverb's restart command writes.
pub const RESTART_KEY: &str = "laravel:reverb:restart";

/// Where to look for the restart signal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum RestartWatch {
    /// Not watching; stop the server with a signal instead.
    #[default]
    Off,
    /// Laravel's file cache store.
    File { path: PathBuf },
    /// Laravel's Redis cache store.
    Redis { url: String, key: String },
}

impl RestartWatch {
    /// Resolve the file a Laravel file-cache key is stored in.
    ///
    /// Laravel hashes the key with SHA-1 and nests it two levels deep by the
    /// first four hex characters.
    pub fn file(cache_path: &Path, key: &str) -> Self {
        let digest = hex::encode(Sha1::digest(key.as_bytes()));

        Self::File { path: cache_path.join(&digest[0..2]).join(&digest[2..4]).join(&digest) }
    }

    /// The Redis key for a Laravel cache key under the configured prefix.
    pub fn redis(url: &str, prefix: &str, key: &str) -> Self {
        Self::Redis { url: url.to_string(), key: format!("{prefix}{key}") }
    }

    pub fn is_enabled(&self) -> bool {
        !matches!(self, Self::Off)
    }

    /// Read the current signal, or `None` if it has never been written.
    ///
    /// Only changes matter, so the raw stored bytes are compared rather than
    /// decoded — Laravel writes a bare integer to Redis but a serialized one
    /// to a file, and neither needs interpreting.
    pub async fn read(&self) -> Option<String> {
        match self {
            Self::Off => None,
            Self::File { path } => tokio::fs::read(path).await.ok().map(|bytes| {
                // The first ten bytes are the expiry, which changes on every
                // write even when the value does not.
                String::from_utf8_lossy(bytes.get(10..).unwrap_or_default()).into_owned()
            }),
            Self::Redis { url, key } => {
                let client = redis::Client::open(url.as_str()).ok()?;
                let mut connection = client.get_multiplexed_async_connection().await.ok()?;

                redis::cmd("GET")
                    .arg(key)
                    .query_async::<Option<String>>(&mut connection)
                    .await
                    .ok()?
            }
        }
    }
}

/// Poll for the restart signal, calling `stop` once it changes.
///
/// Reverb polls every five seconds, so this does too.
pub async fn watch<F>(watch: RestartWatch, stop: F)
where
    F: FnOnce(),
{
    if !watch.is_enabled() {
        return;
    }

    let initial = watch.read().await;

    tracing::info!(?watch, "watching for reverb:restart");

    let mut ticker = tokio::time::interval(Duration::from_secs(5));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        let current = watch.read().await;

        // A key that has never been written stays absent; only a change from
        // what was there at boot means a restart was asked for.
        if current.is_some() && current != initial {
            tracing::info!("reverb:restart signalled");

            stop();

            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashes_a_cache_key_the_way_laravels_file_store_does() {
        let watch = RestartWatch::file(Path::new("/cache"), RESTART_KEY);

        // php > echo sha1('laravel:reverb:restart');
        let expected = "/cache/4f/e4/4fe44729b03e0a0a2586ebe0d445f50c083d0ff3";

        assert_eq!(watch, RestartWatch::File { path: PathBuf::from(expected) });
    }

    #[test]
    fn prefixes_the_redis_key() {
        assert_eq!(
            RestartWatch::redis("redis://localhost", "laravel_cache_:", RESTART_KEY),
            RestartWatch::Redis {
                url: "redis://localhost".into(),
                key: "laravel_cache_:laravel:reverb:restart".into(),
            }
        );
    }

    #[tokio::test]
    async fn reads_a_file_cache_entry_without_its_expiry() {
        let dir = std::env::temp_dir().join(format!("reverb-rs-restart-{}", std::process::id()));
        let watch = RestartWatch::file(&dir, RESTART_KEY);

        let RestartWatch::File { path } = &watch else { panic!("expected a file watch") };

        std::fs::create_dir_all(path.parent().unwrap()).expect("create dirs");
        std::fs::write(path, "9999999999i:1790684343;").expect("write");

        assert_eq!(watch.read().await.as_deref(), Some("i:1790684343;"));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reports_nothing_when_the_key_has_never_been_written() {
        let watch = RestartWatch::file(Path::new("/nonexistent"), RESTART_KEY);

        assert_eq!(watch.read().await, None);
    }
}
