//! The channel registry: which sockets exist, and which channels they are in.
//!
//! Reverb's `ArrayChannelManager` is a plain nested PHP array, safe only
//! because the server is single-threaded. Here the same structure is sharded
//! across a `DashMap` so the Tokio runtime can broadcast from every worker
//! thread at once.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;

use crate::channel::{Channel, Member};
use crate::conn::Conn;
use crate::events::{EventKind, Telemetry};

/// All state belonging to a single application.
#[derive(Debug)]
pub struct AppRegistry {
    app_id: String,
    /// Every live socket, whether or not it has subscribed to anything.
    sockets: DashMap<String, Arc<Conn>>,
    channels: DashMap<String, Arc<Channel>>,
    telemetry: Arc<Telemetry>,
}

impl AppRegistry {
    pub fn new(app_id: String, telemetry: Arc<Telemetry>) -> Self {
        Self {
            app_id,
            sockets: DashMap::new(),
            channels: DashMap::new(),
            telemetry,
        }
    }

    pub fn add_socket(&self, conn: Arc<Conn>) {
        self.sockets.insert(conn.id.clone(), conn);
    }

    pub fn remove_socket(&self, socket_id: &str) {
        self.sockets.remove(socket_id);
    }

    pub fn socket(&self, socket_id: &str) -> Option<Arc<Conn>> {
        self.sockets.get(socket_id).map(|c| c.clone())
    }

    /// Every live socket for the application.
    pub fn sockets(&self) -> Vec<Arc<Conn>> {
        self.sockets.iter().map(|entry| entry.value().clone()).collect()
    }

    /// The number of live sockets, used to enforce `max_connections`.
    ///
    /// Reverb counts only sockets that have joined at least one channel, which
    /// lets an idle client slip past the quota; this counts every socket.
    pub fn socket_count(&self) -> usize {
        self.sockets.len()
    }

    pub fn find(&self, name: &str) -> Option<Arc<Channel>> {
        self.channels.get(name).map(|c| c.clone())
    }

    pub fn find_or_create(&self, name: &str) -> Arc<Channel> {
        if let Some(channel) = self.find(name) {
            return channel;
        }

        let mut created = false;

        let channel = self
            .channels
            .entry(name.to_string())
            .or_insert_with(|| {
                created = true;
                Arc::new(Channel::new(name.to_string()))
            })
            .clone();

        // Announced only by the caller that actually created it, so a race
        // cannot report the same channel twice.
        if created {
            self.telemetry.emit_channel(EventKind::ChannelCreated, &self.app_id, name);
        }

        channel
    }

    pub fn channels(&self) -> Vec<Arc<Channel>> {
        self.channels.iter().map(|entry| entry.value().clone()).collect()
    }

    /// Drop a channel once its last subscriber leaves.
    pub fn remove_channel(&self, name: &str) {
        // Re-check emptiness while holding the shard lock so a subscription
        // racing with the removal is never discarded.
        if self.channels.remove_if(name, |_, channel| channel.is_empty()).is_some() {
            self.telemetry.emit_channel(EventKind::ChannelRemoved, &self.app_id, name);
        }
    }

    /// Remove a connection from every channel it belongs to, returning the
    /// channels it was actually subscribed to along with its membership.
    pub fn unsubscribe_from_all(&self, socket_id: &str) -> Vec<(Arc<Channel>, Member)> {
        let mut removed = Vec::new();

        for channel in self.channels() {
            if let Some(member) = channel.unsubscribe(socket_id) {
                removed.push((channel.clone(), member));
            }

            if channel.is_empty() {
                self.remove_channel(&channel.name);
            }
        }

        removed
    }

    /// The connections visible to the metrics endpoints: those subscribed to at
    /// least one channel, deduplicated by socket ID.
    ///
    /// When a socket appears in several channels the entry carrying presence
    /// data wins, so `user_id` lookups behave the way Reverb's do.
    pub fn subscribed_connections(&self) -> HashMap<String, Member> {
        let mut result: HashMap<String, Member> = HashMap::new();

        for channel in self.channels() {
            for member in channel.members() {
                match result.get(&member.conn.id) {
                    Some(existing) if existing.user_id().is_some() => continue,
                    _ => {
                        result.insert(member.conn.id.clone(), member);
                    }
                }
            }
        }

        result
    }
}

/// The registry for every configured application.
#[derive(Debug)]
pub struct Registry {
    apps: DashMap<String, Arc<AppRegistry>>,
    telemetry: Arc<Telemetry>,
}

impl Registry {
    pub fn new(telemetry: Arc<Telemetry>) -> Self {
        Self { apps: DashMap::new(), telemetry }
    }

    /// Scope the registry to an application, creating its entry on first use.
    pub fn for_app(&self, app_id: &str) -> Arc<AppRegistry> {
        if let Some(existing) = self.apps.get(app_id) {
            return existing.clone();
        }

        self.apps
            .entry(app_id.to_string())
            .or_insert_with(|| {
                Arc::new(AppRegistry::new(app_id.to_string(), self.telemetry.clone()))
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Application, ClientEvents, RateLimiting};
    use serde_json::json;
    use tokio::sync::mpsc;

    fn app() -> Arc<Application> {
        Arc::new(Application {
            id: "app-id".into(),
            key: "app-key".into(),
            secret: "app-secret".into(),
            ping_interval: 60,
            activity_timeout: 30,
            allowed_origins: vec!["*".into()],
            max_message_size: 10_000,
            max_connections: None,
            accept_client_events_from: ClientEvents::Members,
            rate_limiting: RateLimiting::default(),
        })
    }

    fn conn() -> Arc<Conn> {
        let (tx, rx) = mpsc::channel(64);
        std::mem::forget(rx);

        Arc::new(Conn::new(app(), None, tx, Arc::new(Telemetry::disabled())))
    }

    #[test]
    fn creates_channels_on_demand_and_reuses_them() {
        let registry = AppRegistry::new("app-id".into(), Arc::new(Telemetry::disabled()));

        let first = registry.find_or_create("test-channel");
        let second = registry.find_or_create("test-channel");

        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(registry.channels().len(), 1);
    }

    #[test]
    fn removes_a_channel_once_it_empties() {
        let registry = AppRegistry::new("app-id".into(), Arc::new(Telemetry::disabled()));
        let conn = conn();

        registry.find_or_create("test-channel").subscribe(conn.clone(), json!(null));
        assert_eq!(registry.channels().len(), 1);

        let removed = registry.unsubscribe_from_all(&conn.id);

        assert_eq!(removed.len(), 1);
        assert_eq!(registry.channels().len(), 0);
    }

    #[test]
    fn keeps_a_channel_that_still_has_subscribers() {
        let registry = AppRegistry::new("app-id".into(), Arc::new(Telemetry::disabled()));
        let (one, two) = (conn(), conn());
        let channel = registry.find_or_create("test-channel");

        channel.subscribe(one.clone(), json!(null));
        channel.subscribe(two.clone(), json!(null));

        registry.unsubscribe_from_all(&one.id);

        assert_eq!(registry.channels().len(), 1);
        assert_eq!(channel.len(), 1);
    }

    #[test]
    fn prefers_the_membership_carrying_presence_data() {
        let registry = AppRegistry::new("app-id".into(), Arc::new(Telemetry::disabled()));
        let conn = conn();

        registry.find_or_create("test-channel").subscribe(conn.clone(), json!(null));
        registry
            .find_or_create("presence-test-channel")
            .subscribe(conn.clone(), json!({ "user_id": 7 }));

        let connections = registry.subscribed_connections();

        assert_eq!(connections.len(), 1);
        assert_eq!(connections[&conn.id].user_id().as_deref(), Some("7"));
    }

    #[test]
    fn counts_every_socket_not_just_subscribed_ones() {
        let registry = AppRegistry::new("app-id".into(), Arc::new(Telemetry::disabled()));
        let conn = conn();

        registry.add_socket(conn.clone());

        assert_eq!(registry.socket_count(), 1);
        assert_eq!(registry.subscribed_connections().len(), 0);
    }
}
