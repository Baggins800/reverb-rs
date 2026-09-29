//! Channel statistics for the HTTP API.
//!
//! On a single node these are read straight from the registry. When scaling is
//! enabled every node answers the same question about its own connections and
//! the results are merged here, mirroring Reverb's `MetricsHandler`.

use std::sync::Arc;

use serde_json::{Map, Value, json};

use crate::config::Application;
use crate::server::Server;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricType {
    Connections,
    Channel,
    Channels,
    ChannelUsers,
    PresenceData,
    PresenceConnections,
}

/// A metrics question, either answered locally or broadcast to the cluster.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricRequest {
    pub kind: MetricType,
    pub app_id: String,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub channels: Option<Vec<String>>,
    #[serde(default)]
    pub info: Option<String>,
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
}

impl MetricRequest {
    pub fn new(kind: MetricType, app_id: &str) -> Self {
        Self {
            kind,
            app_id: app_id.to_string(),
            channel: None,
            channels: None,
            info: None,
            filter: None,
            user_id: None,
        }
    }

    pub fn channel(mut self, channel: impl Into<String>) -> Self {
        self.channel = Some(channel.into());
        self
    }

    pub fn channels(mut self, channels: Vec<String>) -> Self {
        self.channels = Some(channels);
        self
    }

    pub fn info(mut self, info: impl Into<String>) -> Self {
        self.info = Some(info.into());
        self
    }

    pub fn filter(mut self, filter: Option<String>) -> Self {
        self.filter = filter;
        self
    }

    pub fn user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }
}

/// Answer a metrics request from this node's registry alone.
pub fn local(server: &Server, app: &Arc<Application>, request: &MetricRequest) -> Value {
    let registry = server.registry.for_app(&app.id);
    let requested = request.info.as_deref().unwrap_or("");

    match request.kind {
        MetricType::Connections => {
            let ids: Vec<Value> = registry
                .subscribed_connections()
                .into_keys()
                .map(Value::String)
                .collect();

            Value::Array(ids)
        }

        MetricType::Channel => {
            let name = request.channel.as_deref().unwrap_or("");

            Value::Object(channel_info(server, app, name, requested))
        }

        MetricType::Channels => {
            let names = match &request.channels {
                Some(names) => names.clone(),
                None => {
                    let mut names: Vec<String> = registry
                        .channels()
                        .into_iter()
                        // Reverb reports only occupied channels.
                        .filter(|channel| !channel.is_empty())
                        .filter(|channel| match &request.filter {
                            Some(prefix) => channel.name.starts_with(prefix),
                            None => true,
                        })
                        .map(|channel| channel.name.clone())
                        .collect();

                    names.sort();
                    names
                }
            };

            let mut out = Map::new();

            for name in names {
                out.insert(name.clone(), Value::Object(channel_info(server, app, &name, requested)));
            }

            Value::Object(out)
        }

        MetricType::ChannelUsers => {
            let Some(channel) = request.channel.as_deref().and_then(|c| registry.find(c)) else {
                return json!([]);
            };

            let mut seen: Vec<String> = Vec::new();
            let mut users = Vec::new();

            for member in channel.members() {
                let Some(key) = member.user_id() else { continue };

                if seen.contains(&key) {
                    continue;
                }

                seen.push(key);
                users.push(json!({ "id": member.data.get("user_id").cloned().unwrap_or(Value::Null) }));
            }

            Value::Array(users)
        }

        MetricType::PresenceData => request
            .channel
            .as_deref()
            .and_then(|c| registry.find(c))
            .and_then(|channel| channel.data())
            .unwrap_or_else(|| json!({})),

        MetricType::PresenceConnections => {
            let (Some(channel), Some(user_id)) =
                (request.channel.as_deref().and_then(|c| registry.find(c)), request.user_id.as_deref())
            else {
                return json!([]);
            };

            let connections: Vec<Value> = channel
                .connections_for_user(user_id)
                .into_iter()
                .map(|(id, subscribed_at)| json!({ "id": id, "subscribed_at": subscribed_at }))
                .collect();

            Value::Array(connections)
        }
    }
}

/// Meta information for one channel, in Reverb's member order, omitting
/// anything that was not requested or does not apply.
fn channel_info(server: &Server, app: &Arc<Application>, name: &str, info: &str) -> Map<String, Value> {
    let requested: Vec<&str> = info.split(',').collect();
    let wants = |key: &str| requested.contains(&key);

    let mut out = Map::new();

    let Some(channel) = server.registry.for_app(&app.id).find(name) else {
        if wants("occupied") {
            out.insert("occupied".into(), Value::Bool(false));
        }

        return out;
    };

    let count = channel.len();

    if wants("occupied") {
        out.insert("occupied".into(), Value::Bool(count > 0));
    }

    if wants("user_count") && channel.kind.is_presence() {
        out.insert("user_count".into(), json!(channel.user_count()));
    }

    if wants("subscription_count") && !channel.kind.is_presence() {
        out.insert("subscription_count".into(), json!(count));
    }

    if wants("cache")
        && channel.kind.is_cache()
        && let Some(cached) = channel.cached_payload()
    {
        out.insert("cache".into(), cached);
    }

    out
}

/// Combine one answer per node into a single result set.
pub fn merge(kind: MetricType, answers: Vec<Value>) -> Value {
    match kind {
        MetricType::Connections | MetricType::ChannelUsers | MetricType::PresenceConnections => {
            let mut out: Vec<Value> = Vec::new();

            for answer in answers {
                for item in answer.as_array().cloned().unwrap_or_default() {
                    // Connections and users are deduplicated across nodes;
                    // presence connections are a flat concatenation.
                    if kind == MetricType::PresenceConnections || !out.contains(&item) {
                        out.push(item);
                    }
                }
            }

            Value::Array(out)
        }

        MetricType::Channel => {
            let mut merged = Map::new();

            for answer in answers {
                if let Value::Object(map) = answer {
                    merge_channel_into(&mut merged, map);
                }
            }

            Value::Object(merged)
        }

        MetricType::Channels => {
            let mut merged: Map<String, Value> = Map::new();

            for answer in answers {
                let Value::Object(channels) = answer else { continue };

                for (name, info) in channels {
                    let Value::Object(info) = info else { continue };

                    match merged.get_mut(&name) {
                        Some(Value::Object(existing)) => merge_channel_into(existing, info),
                        _ => {
                            merged.insert(name, Value::Object(info));
                        }
                    }
                }
            }

            Value::Object(merged)
        }

        MetricType::PresenceData => {
            let mut ids: Vec<Value> = Vec::new();
            let mut hash = Map::new();

            for answer in answers {
                let Some(presence) = answer.get("presence") else { continue };

                for id in presence.get("ids").and_then(Value::as_array).cloned().unwrap_or_default() {
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }

                if let Some(Value::Object(node)) = presence.get("hash") {
                    for (key, info) in node {
                        hash.entry(key.clone()).or_insert_with(|| info.clone());
                    }
                }
            }

            json!({ "presence": { "count": ids.len(), "ids": ids, "hash": hash } })
        }
    }
}

/// Fold one node's channel info into the running total: counts add up and
/// `occupied` is true if it is true anywhere.
fn merge_channel_into(target: &mut Map<String, Value>, source: Map<String, Value>) {
    for (key, value) in source {
        match key.as_str() {
            "occupied" => {
                let existing = target.get("occupied").and_then(Value::as_bool).unwrap_or(false);

                target.insert(key, Value::Bool(existing || value.as_bool().unwrap_or(false)));
            }
            "user_count" | "subscription_count" => {
                let existing = target.get(&key).and_then(Value::as_u64).unwrap_or(0);

                target.insert(key, json!(existing + value.as_u64().unwrap_or(0)));
            }
            _ => {
                target.insert(key, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sums_counts_and_ors_occupancy_across_nodes() {
        let merged = merge(
            MetricType::Channel,
            vec![
                json!({ "occupied": false, "subscription_count": 0 }),
                json!({ "occupied": true, "subscription_count": 3 }),
            ],
        );

        assert_eq!(merged, json!({ "occupied": true, "subscription_count": 3 }));
    }

    #[test]
    fn merges_channel_sets() {
        let merged = merge(
            MetricType::Channels,
            vec![
                json!({ "one": { "occupied": true, "subscription_count": 1 } }),
                json!({
                    "one": { "occupied": true, "subscription_count": 2 },
                    "two": { "occupied": true, "subscription_count": 5 },
                }),
            ],
        );

        assert_eq!(
            merged,
            json!({
                "one": { "occupied": true, "subscription_count": 3 },
                "two": { "occupied": true, "subscription_count": 5 },
            })
        );
    }

    #[test]
    fn deduplicates_connections_but_not_presence_connections() {
        assert_eq!(
            merge(MetricType::Connections, vec![json!(["1.1", "2.2"]), json!(["2.2", "3.3"])]),
            json!(["1.1", "2.2", "3.3"])
        );

        assert_eq!(
            merge(
                MetricType::PresenceConnections,
                vec![json!([{ "id": "1.1" }]), json!([{ "id": "1.1" }])]
            ),
            json!([{ "id": "1.1" }, { "id": "1.1" }])
        );
    }

    #[test]
    fn unions_presence_rosters() {
        let merged = merge(
            MetricType::PresenceData,
            vec![
                json!({ "presence": { "count": 1, "ids": [1], "hash": { "1": { "name": "A" } } } }),
                json!({ "presence": { "count": 2, "ids": [1, 2], "hash": { "1": {}, "2": { "name": "B" } } } }),
            ],
        );

        assert_eq!(
            merged,
            json!({
                "presence": {
                    "count": 2,
                    "ids": [1, 2],
                    "hash": { "1": { "name": "A" }, "2": { "name": "B" } },
                }
            })
        );
    }
}
