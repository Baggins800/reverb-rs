//! The Pusher protocol server: connection lifecycle, subscriptions and fan-out.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::Utf8Bytes;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value, json};
use sha2::Sha256;

use crate::channel::{Channel, ChannelKind, Member};
use crate::config::{Application, ClientEvents, ServerConfig};
use crate::conn::Conn;
use crate::events::Telemetry;
use crate::protocol::{self, PusherError};
use crate::pubsub::PubSub;
use crate::registry::{AppRegistry, Registry};

pub type HmacSha256 = Hmac<Sha256>;

/// Shared server state: configuration, the application catalogue and the
/// channel registry.
pub struct Server {
    pub config: ServerConfig,
    pub registry: Registry,
    pub telemetry: Arc<Telemetry>,
    by_key: HashMap<String, Arc<Application>>,
    by_id: HashMap<String, Arc<Application>>,
    pubsub: parking_lot::RwLock<Option<Arc<PubSub>>>,
}

impl Server {
    pub fn new(config: ServerConfig) -> Self {
        Self::with_telemetry(config, Arc::new(Telemetry::disabled()))
    }

    pub fn with_telemetry(config: ServerConfig, telemetry: Arc<Telemetry>) -> Self {
        let by_key = config.apps.iter().map(|a| (a.key.clone(), a.clone())).collect();
        let by_id = config.apps.iter().map(|a| (a.id.clone(), a.clone())).collect();

        // The registry announces channel lifecycle events, so it needs the
        // same telemetry handle the connections use.
        let registry = Registry::new(telemetry.clone());

        Self { config, registry, telemetry, by_key, by_id, pubsub: parking_lot::RwLock::new(None) }
    }

    pub fn app_by_key(&self, key: &str) -> Option<Arc<Application>> {
        self.by_key.get(key).cloned()
    }

    pub fn app_by_id(&self, id: &str) -> Option<Arc<Application>> {
        self.by_id.get(id).cloned()
    }

    pub fn attach_pubsub(&self, pubsub: Arc<PubSub>) {
        *self.pubsub.write() = Some(pubsub);
    }

    pub fn pubsub(&self) -> Option<Arc<PubSub>> {
        self.pubsub.read().clone()
    }

    fn app_registry(&self, app: &Application) -> Arc<AppRegistry> {
        self.registry.for_app(&app.id)
    }

    // -- Connection lifecycle -------------------------------------------------

    /// Handle a newly upgraded connection: enforce quotas and origin rules,
    /// then acknowledge it.
    ///
    /// Reverb reports these failures without closing the socket, and this does
    /// the same so misbehaving clients see identical behaviour.
    pub fn open(&self, conn: &Arc<Conn>) {
        let registry = self.app_registry(&conn.app);

        if let Some(limit) = conn.app.max_connections
            && registry.socket_count() > limit
        {
            conn.send_error(PusherError::ConnectionLimitExceeded);
            return;
        }

        if !conn.app.origin_allowed(conn.origin.as_deref()) {
            conn.send_error(PusherError::InvalidOrigin);
            return;
        }

        conn.touch();

        let body = json!({
            "socket_id": conn.id,
            "activity_timeout": conn.app.activity_timeout,
        });

        conn.send(protocol::payload("connection_established", Some(&body), None));

        tracing::debug!(socket_id = %conn.id, app = %conn.app.id, "connection established");
    }

    /// Handle a text frame from a client.
    pub fn message(&self, conn: &Arc<Conn>, message: &str) {
        conn.touch();

        if !conn.check_rate_limit() {
            if conn.app.rate_limiting.terminate_on_limit {
                conn.terminate();
            }

            conn.send_error(PusherError::RateLimitExceeded);
            return;
        }

        match self.handle_message(conn, message) {
            // Reverb dispatches `MessageReceived` only once handling succeeded.
            Ok(()) => conn.record_received(message),
            Err(error) => {
                tracing::debug!(socket_id = %conn.id, ?error, "message rejected");
                conn.send_error(error);
            }
        }
    }

    fn handle_message(&self, conn: &Arc<Conn>, message: &str) -> Result<(), PusherError> {
        let parsed: Value =
            serde_json::from_str(message).map_err(|_| PusherError::InvalidMessageFormat)?;

        let Value::Object(mut event) = parsed else {
            return Err(PusherError::InvalidMessageFormat);
        };

        // Clients may send `data` either as an object or as a JSON string;
        // normalize the latter before dispatching.
        if let Some(Value::String(raw)) = event.get("data")
            && let Ok(decoded) = serde_json::from_str::<Value>(raw)
        {
            event.insert("data".into(), decoded);
        }

        let name = event
            .get("event")
            .and_then(Value::as_str)
            .ok_or(PusherError::InvalidMessageFormat)?
            .to_string();

        match name.strip_prefix("pusher:") {
            Some(event_name) => {
                let payload = match event.get("data") {
                    Some(Value::Object(map)) => map.clone(),
                    _ => Map::new(),
                };

                self.handle_pusher_event(conn, event_name, &payload)
            }
            None => self.handle_client_event(conn, &name, &event),
        }
    }

    fn handle_pusher_event(
        &self,
        conn: &Arc<Conn>,
        event: &str,
        payload: &Map<String, Value>,
    ) -> Result<(), PusherError> {
        match event {
            "subscribe" => {
                let channel = payload
                    .get("channel")
                    .and_then(Value::as_str)
                    .ok_or(PusherError::InvalidMessageFormat)?;

                let auth = match payload.get("auth") {
                    Some(Value::String(auth)) => Some(auth.as_str()),
                    Some(Value::Null) | None => None,
                    Some(_) => return Err(PusherError::InvalidMessageFormat),
                };

                let data = match payload.get("channel_data") {
                    Some(Value::String(data)) => Some(data.as_str()),
                    Some(Value::Null) | None => None,
                    Some(_) => return Err(PusherError::InvalidMessageFormat),
                };

                self.subscribe(conn, channel, auth, data)
            }
            "unsubscribe" => {
                let channel = payload
                    .get("channel")
                    .and_then(Value::as_str)
                    .ok_or(PusherError::InvalidMessageFormat)?;

                self.unsubscribe(conn, channel);
                Ok(())
            }
            "ping" => {
                conn.send(protocol::payload("pong", None, None));
                Ok(())
            }
            "pong" => {
                conn.touch();
                Ok(())
            }
            _ => Err(PusherError::InvalidMessageFormat),
        }
    }

    // -- Subscriptions --------------------------------------------------------

    pub fn subscribe(
        &self,
        conn: &Arc<Conn>,
        name: &str,
        auth: Option<&str>,
        data: Option<&str>,
    ) -> Result<(), PusherError> {
        let kind = ChannelKind::of(name);

        if kind.requires_auth() {
            self.verify_subscription(conn, name, auth, data)?;
        }

        // `channel_data` must be a JSON document; Reverb validates this before
        // the channel is touched.
        let parsed = match data.filter(|d| !d.is_empty()) {
            Some(raw) => {
                serde_json::from_str::<Value>(raw).map_err(|_| PusherError::InvalidMessageFormat)?
            }
            None => Value::Null,
        };

        let registry = self.app_registry(&conn.app);
        let channel = registry.find_or_create(name);

        let user_id = if kind.is_presence() {
            parsed
                .get("user_id")
                .and_then(crate::channel::Member::key_of)
                .map(|id| (id, parsed.get("user_id").cloned().unwrap_or(Value::Null)))
        } else {
            None
        };

        // Whether this user was already present decides if a `member_added` is
        // announced, and must be read before the new membership is recorded.
        let already_present =
            user_id.as_ref().is_some_and(|(id, _)| channel.user_is_subscribed(id, None));

        channel.subscribe(conn.clone(), parsed.clone());

        if kind.is_presence() && !already_present {
            self.announce_member_added(conn, &channel, &parsed);
        }

        self.send_subscription_succeeded(conn, &channel);

        Ok(())
    }

    fn send_subscription_succeeded(&self, conn: &Arc<Conn>, channel: &Channel) {
        let data = channel.data();

        conn.send(protocol::internal_payload(
            "subscription_succeeded",
            data.as_ref(),
            Some(&channel.name),
        ));

        if !channel.kind.is_cache() {
            return;
        }

        match channel.cached_payload() {
            Some(payload) => conn.send(payload.to_string()),
            None => conn.send(protocol::payload("cache_miss", None, Some(&channel.name))),
        }
    }

    fn announce_member_added(&self, conn: &Arc<Conn>, channel: &Channel, data: &Value) {
        let body = match data {
            Value::Object(_) => data.clone(),
            _ => json!({}),
        };

        self.dispatch(
            &conn.app,
            payload_map([
                ("event", json!("pusher_internal:member_added")),
                ("data", json!(body.to_string())),
                ("channel", json!(channel.name)),
            ]),
            Some(&conn.id),
        );
    }

    pub fn unsubscribe(&self, conn: &Arc<Conn>, name: &str) {
        let registry = self.app_registry(&conn.app);

        let Some(channel) = registry.find(name) else {
            return;
        };

        let Some(member) = channel.unsubscribe(&conn.id) else {
            return;
        };

        if channel.is_empty() {
            registry.remove_channel(name);
        }

        self.after_unsubscribe(conn, &channel, &member);
    }

    /// Announce a departing presence member once their last connection to the
    /// channel has gone.
    fn after_unsubscribe(&self, conn: &Arc<Conn>, channel: &Channel, member: &Member) {
        if !channel.kind.is_presence() {
            return;
        }

        let Some(user_id) = member.user_id() else {
            return;
        };

        if channel.user_is_subscribed(&user_id, None) {
            return;
        }

        let raw = member.data.get("user_id").cloned().unwrap_or(Value::Null);

        self.dispatch(
            &conn.app,
            payload_map([
                ("event", json!("pusher_internal:member_removed")),
                ("data", json!(json!({ "user_id": raw }).to_string())),
                ("channel", json!(channel.name)),
            ]),
            Some(&conn.id),
        );
    }

    /// Tear down a connection: leave every channel, then drop the socket.
    pub fn close(&self, conn: &Arc<Conn>) {
        let registry = self.app_registry(&conn.app);

        for (channel, member) in registry.unsubscribe_from_all(&conn.id) {
            self.after_unsubscribe(conn, &channel, &member);
        }

        registry.remove_socket(&conn.id);
        conn.terminate();

        tracing::debug!(socket_id = %conn.id, app = %conn.app.id, "connection closed");
    }

    /// Verify a private or presence channel subscription signature.
    ///
    /// The signed string is `socket_id:channel`, with `:channel_data` appended
    /// when presence data is supplied. `auth` arrives as `app_key:signature`.
    fn verify_subscription(
        &self,
        conn: &Arc<Conn>,
        channel: &str,
        auth: Option<&str>,
        data: Option<&str>,
    ) -> Result<(), PusherError> {
        let mut signed = format!("{}:{}", conn.id, channel);

        if let Some(data) = data.filter(|d| !d.is_empty()) {
            signed.push(':');
            signed.push_str(data);
        }

        // `Str::after($auth, ':')` yields the whole string when there is no colon.
        let provided =
            auth.map(|auth| auth.split_once(':').map(|(_, sig)| sig).unwrap_or(auth)).unwrap_or("");

        verify_signature(&conn.app.secret, &signed, provided)
            .then_some(())
            .ok_or(PusherError::Unauthorized)
    }

    // -- Client events --------------------------------------------------------

    fn handle_client_event(
        &self,
        conn: &Arc<Conn>,
        name: &str,
        event: &Map<String, Value>,
    ) -> Result<(), PusherError> {
        let channel_name = event
            .get("channel")
            .and_then(Value::as_str)
            .ok_or(PusherError::InvalidMessageFormat)?;

        match event.get("data") {
            Some(Value::Object(_) | Value::Array(_) | Value::Null) | None => {}
            Some(_) => return Err(PusherError::InvalidMessageFormat),
        }

        if !name.starts_with("client-") {
            return Ok(());
        }

        let policy = conn.app.accept_client_events_from;

        if policy == ClientEvents::Disabled {
            conn.send_error(PusherError::ClientEventsDisabled);
            return Ok(());
        }

        let rebroadcast = if policy == ClientEvents::Members {
            let member = self
                .app_registry(&conn.app)
                .find(channel_name)
                .and_then(|channel| channel.find(&conn.id));

            let Some(member) = member else {
                conn.send_error(PusherError::NotAChannelMember);
                return Ok(());
            };

            // Rebuild the payload so only the expected members survive, plus
            // the authenticated user ID where we have one.
            let mut rebuilt = payload_map([
                ("event", json!(name)),
                ("channel", json!(channel_name)),
                ("data", event.get("data").cloned().unwrap_or(Value::Null)),
            ]);

            // Public channels allow unauthenticated users, so there may be
            // no user ID to attach.
            if let Some(user_id) = member.data.get("user_id").filter(|id| !id.is_null()) {
                rebuilt.insert("user_id".into(), user_id.clone());
            }

            rebuilt
        } else {
            event.clone()
        };

        self.dispatch(&conn.app, rebroadcast, Some(&conn.id));

        Ok(())
    }

    // -- Fan-out --------------------------------------------------------------

    /// Publish a payload to its channels, across the cluster when scaling is on.
    pub fn dispatch(
        &self,
        app: &Arc<Application>,
        payload: Map<String, Value>,
        except: Option<&str>,
    ) {
        match self.pubsub() {
            Some(pubsub) => pubsub.publish_message(app, payload, except),
            None => self.dispatch_locally(app, &payload, except),
        }
    }

    /// Deliver a payload to subscribers connected to *this* node.
    ///
    /// The payload is re-keyed per channel exactly as Reverb does: `channels`
    /// is dropped and `channel` set, which preserves the member ordering the
    /// single- and multi-channel endpoints each produce.
    pub fn dispatch_locally(
        &self,
        app: &Arc<Application>,
        payload: &Map<String, Value>,
        except: Option<&str>,
    ) {
        let registry = self.app_registry(app);
        let internal = payload
            .get("event")
            .and_then(Value::as_str)
            .is_some_and(|e| e.starts_with("pusher_internal:"));

        for name in target_channels(payload) {
            let Some(channel) = registry.find(&name) else {
                continue;
            };

            let mut scoped = payload.clone();
            scoped.shift_remove("channels");
            scoped.insert("channel".into(), Value::String(name));

            // Only externally triggered events refresh a cache channel, so
            // presence bookkeeping never displaces the retained payload.
            if channel.kind.is_cache() && !internal {
                channel.cache(Value::Object(scoped.clone()));
            }

            let encoded: Utf8Bytes = Value::Object(scoped).to_string().into();

            channel.broadcast(&encoded, except);
        }
    }

    /// Disconnect every connection belonging to `user_id`.
    pub fn terminate_user(&self, app: &Arc<Application>, user_id: &str) {
        for member in self.app_registry(app).subscribed_connections().values() {
            if member.user_id().as_deref() == Some(user_id) {
                member.conn.terminate();
            }
        }
    }
}

/// The channels a payload targets, from either `channels` or `channel`.
fn target_channels(payload: &Map<String, Value>) -> Vec<String> {
    match payload.get("channels") {
        Some(Value::Array(items)) => {
            items.iter().filter_map(|c| c.as_str().map(str::to_string)).collect()
        }
        Some(Value::String(one)) => vec![one.clone()],
        _ => match payload.get("channel") {
            Some(Value::String(one)) => vec![one.clone()],
            _ => Vec::new(),
        },
    }
}

/// Build an ordered payload map from literal members.
pub fn payload_map<const N: usize>(entries: [(&str, Value); N]) -> Map<String, Value> {
    let mut map = Map::with_capacity(N);

    for (key, value) in entries {
        map.insert(key.to_string(), value);
    }

    map
}

/// Constant-time comparison of a hex-encoded HMAC-SHA256 signature.
pub fn verify_signature(secret: &str, signed: &str, provided: &str) -> bool {
    let Ok(expected) = hex::decode(provided) else {
        return false;
    };

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(signed.as_bytes());

    mac.verify_slice(&expected).is_ok()
}

/// Hex-encoded HMAC-SHA256, matching PHP's `hash_hmac('sha256', ...)`.
pub fn sign(secret: &str, message: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(message.as_bytes());

    hex::encode(mac.finalize().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signs_and_verifies() {
        let signature = sign("secret", "123.456:private-channel");

        assert!(verify_signature("secret", "123.456:private-channel", &signature));
        assert!(!verify_signature("secret", "123.456:private-other", &signature));
        assert!(!verify_signature("other", "123.456:private-channel", &signature));
        assert!(!verify_signature("secret", "123.456:private-channel", "not-hex"));
    }

    #[test]
    fn matches_php_hash_hmac_output() {
        // php > echo hash_hmac('sha256', 'message', 'key');
        assert_eq!(
            sign("key", "message"),
            "6e9ef29b75fffc5b7abae527d58fdadb2fe42e7219011976917343065f58ed4a"
        );
    }

    #[test]
    fn resolves_target_channels_from_either_member() {
        let single = payload_map([("channel", json!("one"))]);
        assert_eq!(target_channels(&single), vec!["one"]);

        let many = payload_map([("channels", json!(["one", "two"]))]);
        assert_eq!(target_channels(&many), vec!["one", "two"]);

        assert!(target_channels(&payload_map([("event", json!("x"))])).is_empty());
    }

    #[test]
    fn prefers_channels_over_channel() {
        let both = payload_map([("channel", json!("one")), ("channels", json!(["two"]))]);

        assert_eq!(target_channels(&both), vec!["two"]);
    }
}
