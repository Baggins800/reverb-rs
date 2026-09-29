//! Channels and their subscribers.
//!
//! Reverb models the six Pusher channel flavours as a class hierarchy; here a
//! single [`Channel`] carries a [`ChannelKind`] describing which behaviours
//! apply — auth, presence bookkeeping, and last-payload caching.

use std::sync::Arc;

use axum::extract::ws::Utf8Bytes;
use indexmap::IndexMap;
use parking_lot::RwLock;
use serde_json::{Map, Value, json};

use crate::conn::Conn;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Public,
    Private,
    Presence,
    Cache,
    PrivateCache,
    PresenceCache,
}

impl ChannelKind {
    /// Classify a channel by name, in the same order Reverb's `ChannelBroker`
    /// tests its prefixes. Note that `cache` and `private` are matched without
    /// a trailing dash, exactly as upstream does.
    pub fn of(name: &str) -> Self {
        if name.starts_with("private-cache-") {
            Self::PrivateCache
        } else if name.starts_with("presence-cache-") {
            Self::PresenceCache
        } else if name.starts_with("cache") {
            Self::Cache
        } else if name.starts_with("private") {
            Self::Private
        } else if name.starts_with("presence") {
            Self::Presence
        } else {
            Self::Public
        }
    }

    /// Whether subscribing requires a valid auth signature.
    pub fn requires_auth(&self) -> bool {
        !matches!(self, Self::Public | Self::Cache)
    }

    pub fn is_presence(&self) -> bool {
        matches!(self, Self::Presence | Self::PresenceCache)
    }

    pub fn is_cache(&self) -> bool {
        matches!(self, Self::Cache | Self::PrivateCache | Self::PresenceCache)
    }
}

/// One connection's membership of one channel.
#[derive(Debug, Clone)]
pub struct Member {
    pub conn: Arc<Conn>,
    /// The decoded `channel_data`, or `null` when none was supplied.
    pub data: Value,
    pub subscribed_at: f64,
}

impl Member {
    /// The member's `user_id` as a string, however it was encoded in the
    /// original `channel_data`.
    pub fn user_id(&self) -> Option<String> {
        Self::key_of(self.data.get("user_id")?)
    }

    pub fn user_info(&self) -> Option<&Value> {
        self.data.get("user_info")
    }

    /// Render a JSON scalar the way PHP would when used as an array key, so
    /// that `1` and `"1"` identify the same user.
    pub fn key_of(value: &Value) -> Option<String> {
        match value {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            Value::Bool(b) => Some(if *b { "1".into() } else { "".into() }),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct Channel {
    pub name: String,
    pub kind: ChannelKind,
    /// Subscribers in subscription order, keyed by socket ID.
    members: RwLock<IndexMap<String, Member>>,
    /// The last payload broadcast to a cache channel.
    cached: RwLock<Option<Value>>,
}

impl Channel {
    pub fn new(name: String) -> Self {
        Self {
            kind: ChannelKind::of(&name),
            name,
            members: RwLock::new(IndexMap::new()),
            cached: RwLock::new(None),
        }
    }

    pub fn subscribe(&self, conn: Arc<Conn>, data: Value) {
        let subscribed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);

        self.members
            .write()
            .insert(conn.id.clone(), Member { conn, data, subscribed_at });
    }

    /// Remove a subscriber, returning its membership if it had one.
    pub fn unsubscribe(&self, socket_id: &str) -> Option<Member> {
        self.members.write().shift_remove(socket_id)
    }

    pub fn find(&self, socket_id: &str) -> Option<Member> {
        self.members.read().get(socket_id).cloned()
    }

    pub fn members(&self) -> Vec<Member> {
        self.members.read().values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.members.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.read().is_empty()
    }

    /// Whether any *other* connection is subscribed under the same user ID.
    pub fn user_is_subscribed(&self, user_id: &str, excluding: Option<&str>) -> bool {
        self.members.read().values().any(|member| {
            if Some(member.conn.id.as_str()) == excluding {
                return false;
            }

            member.user_id().as_deref() == Some(user_id)
        })
    }

    /// Send a pre-encoded frame to every subscriber, optionally skipping one.
    ///
    /// Delivery happens under the read lock. That is safe because queueing a
    /// frame never blocks and never touches channel state — a client that
    /// cannot keep up is dropped rather than waited on — and it avoids
    /// cloning every subscriber's handle on each broadcast.
    pub fn broadcast(&self, message: &Utf8Bytes, except: Option<&str>) {
        let members = self.members.read();

        for member in members.values() {
            if Some(member.conn.id.as_str()) == except {
                continue;
            }

            member.conn.send_shared(message);
        }
    }

    pub fn cache(&self, payload: Value) {
        *self.cached.write() = Some(payload);
    }

    pub fn cached_payload(&self) -> Option<Value> {
        self.cached.read().clone()
    }

    /// The `pusher_internal:subscription_succeeded` body for this channel.
    ///
    /// Non-presence channels have no body. Presence channels report the unique
    /// members; if *any* subscriber lacks a `user_id` the roster is reported as
    /// empty, matching Reverb.
    pub fn data(&self) -> Option<Value> {
        if !self.kind.is_presence() {
            return None;
        }

        let members = self.members.read();

        let mut ids: Vec<Value> = Vec::new();
        let mut hash = Map::new();

        for member in members.values() {
            let Some(user_id) = member.user_id() else {
                return Some(json!({ "presence": { "count": 0, "ids": [], "hash": {} } }));
            };

            if hash.contains_key(&user_id) {
                continue;
            }

            ids.push(member.data.get("user_id").cloned().unwrap_or(Value::Null));
            hash.insert(user_id, member.user_info().cloned().unwrap_or_else(|| json!({})));
        }

        Some(json!({ "presence": { "count": ids.len(), "ids": ids, "hash": hash } }))
    }

    /// The number of distinct users subscribed, for `user_count` channel info.
    pub fn user_count(&self) -> usize {
        let members = self.members.read();
        let mut seen: Vec<Option<String>> = Vec::new();

        for member in members.values() {
            let id = member.user_id();

            if !seen.contains(&id) {
                seen.push(id);
            }
        }

        seen.len()
    }

    /// Every connection subscribed under `user_id`, with the time it joined.
    /// Used to decide which node announces a presence member.
    pub fn connections_for_user(&self, user_id: &str) -> Vec<(String, f64)> {
        self.members
            .read()
            .values()
            .filter(|m| m.user_id().as_deref() == Some(user_id))
            .map(|m| (m.conn.id.clone(), m.subscribed_at))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_channels_by_prefix() {
        assert_eq!(ChannelKind::of("test-channel"), ChannelKind::Public);
        assert_eq!(ChannelKind::of("private-test"), ChannelKind::Private);
        assert_eq!(ChannelKind::of("presence-test"), ChannelKind::Presence);
        assert_eq!(ChannelKind::of("cache-test"), ChannelKind::Cache);
        assert_eq!(ChannelKind::of("private-cache-test"), ChannelKind::PrivateCache);
        assert_eq!(ChannelKind::of("presence-cache-test"), ChannelKind::PresenceCache);
    }

    #[test]
    fn matches_upstream_prefix_matching_without_a_dash() {
        // Reverb tests `Str::startsWith($name, 'cache')`, not `'cache-'`.
        assert_eq!(ChannelKind::of("cacheless"), ChannelKind::Cache);
        assert_eq!(ChannelKind::of("privateer"), ChannelKind::Private);
    }

    #[test]
    fn reports_channel_capabilities() {
        assert!(!ChannelKind::Public.requires_auth());
        assert!(!ChannelKind::Cache.requires_auth());
        assert!(ChannelKind::Private.requires_auth());
        assert!(ChannelKind::PresenceCache.requires_auth());

        assert!(ChannelKind::PresenceCache.is_presence());
        assert!(ChannelKind::PresenceCache.is_cache());
        assert!(!ChannelKind::Private.is_cache());
    }

    #[test]
    fn non_presence_channels_have_no_subscription_body() {
        assert!(Channel::new("test-channel".into()).data().is_none());
    }
}
