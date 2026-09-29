//! Horizontal scaling over Redis pub/sub.
//!
//! Every node publishes the events it receives and subscribes to the events its
//! peers publish, so a client connected to one node sees broadcasts triggered
//! on any other. Metrics requests use the same channel: the publisher learns
//! how many nodes received its question from Redis' `PUBLISH` reply, then waits
//! for that many answers before merging them.
//!
//! The envelope mirrors Reverb's (`type`, `payload`, `socket_id`) but carries
//! the application as its ID rather than a PHP-serialized object, so a cluster
//! must be all-Rust or all-PHP, not a mix of both.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use dashmap::DashMap;
use futures_util::StreamExt;
use rand::Rng;
use serde_json::{Map, Value, json};
use tokio::sync::mpsc;

use crate::config::Application;
use crate::metrics::{self, MetricRequest, MetricType};
use crate::server::Server;

/// How long to wait for peers to answer a metrics request before falling back
/// to whatever has arrived.
const METRICS_TIMEOUT: Duration = Duration::from_secs(10);

pub struct PubSub {
    channel: String,
    manager: redis::aio::ConnectionManager,
    outbound: mpsc::Sender<String>,
    /// In-flight metrics requests, keyed by their correlation ID.
    pending: DashMap<String, mpsc::UnboundedSender<Value>>,
}

impl PubSub {
    /// Connect the publisher and subscriber, and start pumping both.
    pub async fn connect(url: &str, channel: String, server: Arc<Server>) -> Result<Arc<Self>> {
        let client = redis::Client::open(url)?;
        let manager = redis::aio::ConnectionManager::new(client.clone()).await?;

        let (tx, rx) = mpsc::channel(8192);

        let pubsub = Arc::new(Self {
            channel: channel.clone(),
            manager: manager.clone(),
            outbound: tx,
            pending: DashMap::new(),
        });

        tokio::spawn(publish_loop(rx, manager, channel.clone()));
        tokio::spawn(subscribe_loop(client, channel, server, pubsub.clone()));

        Ok(pubsub)
    }

    /// Broadcast a payload to the cluster and apply it locally.
    ///
    /// Publishing is fire-and-forget so a Redis stall never blocks a client's
    /// message loop.
    pub fn publish_message(
        &self,
        app: &Arc<Application>,
        payload: Map<String, Value>,
        except: Option<&str>,
    ) {
        let mut envelope = json!({
            "type": "message",
            "application": app.id,
            "payload": Value::Object(payload),
        });

        if let Some(socket_id) = except {
            envelope["socket_id"] = json!(socket_id);
        }

        self.enqueue(envelope);
    }

    /// Ask every node to disconnect a user's connections.
    pub fn publish_terminate(&self, app: &Arc<Application>, user_id: &str) {
        self.enqueue(json!({
            "type": "terminate",
            "application": app.id,
            "payload": { "user_id": user_id },
        }));
    }

    fn enqueue(&self, envelope: Value) {
        if self.outbound.try_send(envelope.to_string()).is_err() {
            tracing::warn!("redis publish queue is full; dropping an event");
        }
    }

    /// Gather a metric from every node and merge the answers.
    pub async fn gather(&self, request: &MetricRequest) -> Value {
        let key = correlation_key();
        let (tx, mut rx) = mpsc::unbounded_channel();

        self.pending.insert(key.clone(), tx);

        let envelope = json!({
            "type": "metrics",
            "key": key,
            "request": request,
        });

        let expected = match self.publish_counted(&envelope.to_string()).await {
            Ok(count) => count,
            Err(error) => {
                tracing::warn!(%error, "failed to request metrics from peers");
                self.pending.remove(&key);

                return Value::Array(Vec::new());
            }
        };

        let mut answers = Vec::new();

        if expected > 0 {
            let collect = async {
                while answers.len() < expected as usize {
                    match rx.recv().await {
                        Some(answer) => answers.push(answer),
                        None => break,
                    }
                }
            };

            // A node that dies mid-request must not stall the HTTP response;
            // merge whatever arrived when the deadline passes.
            if tokio::time::timeout(METRICS_TIMEOUT, collect).await.is_err() {
                tracing::warn!(
                    received = answers.len(),
                    expected,
                    "timed out gathering metrics from peers"
                );
            }
        }

        self.pending.remove(&key);

        metrics::merge(request.kind, answers)
    }

    /// Publish and return how many subscribers received the message.
    async fn publish_counted(&self, payload: &str) -> Result<u64> {
        let mut manager = self.manager.clone();

        let received: u64 =
            redis::cmd("PUBLISH").arg(&self.channel).arg(payload).query_async(&mut manager).await?;

        Ok(received)
    }

    /// Route a peer's answer back to the request that is waiting for it.
    fn deliver_answer(&self, key: &str, payload: Value) {
        if let Some(sender) = self.pending.get(key) {
            let _ = sender.send(payload);
        }
    }
}

fn correlation_key() -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    let mut rng = rand::rng();

    (0..10).map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char).collect()
}

/// Drain queued envelopes onto Redis, reconnecting implicitly via the manager.
async fn publish_loop(
    mut rx: mpsc::Receiver<String>,
    mut manager: redis::aio::ConnectionManager,
    channel: String,
) {
    while let Some(payload) = rx.recv().await {
        let result: Result<u64, _> =
            redis::cmd("PUBLISH").arg(&channel).arg(&payload).query_async(&mut manager).await;

        if let Err(error) = result {
            tracing::warn!(%error, "failed to publish to redis");
        }
    }
}

/// Consume the cluster channel, reconnecting on failure.
async fn subscribe_loop(
    client: redis::Client,
    channel: String,
    server: Arc<Server>,
    pubsub: Arc<PubSub>,
) {
    loop {
        match run_subscriber(&client, &channel, &server, &pubsub).await {
            Ok(()) => tracing::warn!("redis subscription closed; reconnecting"),
            Err(error) => tracing::warn!(%error, "redis subscription failed; reconnecting"),
        }

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn run_subscriber(
    client: &redis::Client,
    channel: &str,
    server: &Arc<Server>,
    pubsub: &Arc<PubSub>,
) -> Result<()> {
    let mut subscriber = client.get_async_pubsub().await?;
    subscriber.subscribe(channel).await?;

    tracing::info!(channel, "subscribed to the reverb scaling channel");

    let mut stream = subscriber.on_message();

    while let Some(message) = stream.next().await {
        let Ok(payload) = message.get_payload::<String>() else {
            continue;
        };

        if let Err(error) = handle_envelope(&payload, server, pubsub).await {
            tracing::warn!(%error, "failed to handle a message from the scaling channel");
        }
    }

    Ok(())
}

async fn handle_envelope(raw: &str, server: &Arc<Server>, pubsub: &Arc<PubSub>) -> Result<()> {
    let envelope: Value = serde_json::from_str(raw)?;

    let Some(kind) = envelope.get("type").and_then(Value::as_str) else {
        return Ok(());
    };

    match kind {
        "message" => {
            let Some(app) = resolve_app(server, &envelope) else { return Ok(()) };

            if let Some(Value::Object(payload)) = envelope.get("payload") {
                let except = envelope.get("socket_id").and_then(Value::as_str);

                server.dispatch_locally(&app, payload, except);
            }
        }

        "terminate" => {
            let Some(app) = resolve_app(server, &envelope) else { return Ok(()) };

            if let Some(user_id) = envelope.pointer("/payload/user_id").and_then(Value::as_str) {
                server.terminate_user(&app, user_id);
            }
        }

        "metrics" => {
            let (Some(key), Some(request)) = (
                envelope.get("key").and_then(Value::as_str),
                envelope.get("request").cloned(),
            ) else {
                return Ok(());
            };

            let request: MetricRequest = serde_json::from_value(request)?;

            // A node that does not serve this application still answers, so
            // the requester's reply count always adds up.
            let answer = match server.app_by_id(&request.app_id) {
                Some(app) => metrics::local(server, &app, &request),
                None => empty_answer(request.kind),
            };

            pubsub.enqueue(json!({ "type": key, "payload": answer }));
        }

        // Anything else is an answer addressed to a request we published.
        key => {
            if let Some(payload) = envelope.get("payload") {
                pubsub.deliver_answer(key, payload.clone());
            }
        }
    }

    Ok(())
}

fn resolve_app(server: &Server, envelope: &Value) -> Option<Arc<Application>> {
    server.app_by_id(envelope.get("application")?.as_str()?)
}

/// The shape a node returns when it knows nothing about the application.
fn empty_answer(kind: MetricType) -> Value {
    match kind {
        MetricType::Connections
        | MetricType::ChannelUsers
        | MetricType::PresenceConnections => json!([]),
        _ => json!({}),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_keys_are_ten_characters_and_unique() {
        let first = correlation_key();
        let second = correlation_key();

        assert_eq!(first.len(), 10);
        assert_ne!(first, second);
    }

    #[test]
    fn empty_answers_match_their_metric_shape() {
        assert_eq!(empty_answer(MetricType::Connections), json!([]));
        assert_eq!(empty_answer(MetricType::Channels), json!({}));
    }
}
