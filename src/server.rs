//! The Pusher protocol server: connection lifecycle, subscriptions and fan-out.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::Utf8Bytes;
use hmac::{Hmac, Mac};
use serde_json::{Map, Value, json};
use sha2::Sha256;

use crate::channel::{Channel, ChannelKind};
use crate::config::{Application, ClientEvents, ServerConfig};
use crate::conn::Conn;
use crate::events::{EventKind, Telemetry};
use crate::protocol::{self, PusherError};
use crate::pubsub::PubSub;
use crate::registry::{AppRegistry, Registry};

pub type HmacSha256 = Hmac<Sha256>;

/// The longest channel name accepted, in bytes. Pusher's own limit; Reverb
/// sets none, which lets one subscribe frame name a channel as large as the
/// message size allows.
pub const MAX_CHANNEL_NAME_LENGTH: usize = 164;

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
    /// A refused connection is sent the same error Reverb sends, then closed.
    /// Reverb leaves it open, and a client that ignores the error can go on
    /// subscribing and whispering as if the check had passed.
    pub fn open(&self, conn: &Arc<Conn>) {
        let registry = self.app_registry(&conn.app);

        let refusal = if conn.app.max_connections.is_some_and(|l| registry.socket_count() > l) {
            Some(PusherError::ConnectionLimitExceeded)
        } else if !conn.app.origin_allowed(conn.origin.as_deref()) {
            Some(PusherError::InvalidOrigin)
        } else {
            None
        };

        if let Some(error) = refusal {
            conn.send_error(error);
            conn.terminate();
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
        // A connection on its way out — refused, terminated or fallen behind —
        // has nothing more to say, however much it keeps sending.
        if conn.is_closed() {
            return;
        }

        conn.touch();

        if !conn.check_rate_limit() {
            // The error is queued first so it reaches the client ahead of the
            // close; once terminated, nothing more can be queued.
            conn.send_error(PusherError::RateLimitExceeded);

            if conn.app.rate_limiting.terminate_on_limit {
                conn.terminate();
            }

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

        if name.is_empty() || name.len() > MAX_CHANNEL_NAME_LENGTH {
            return Err(PusherError::InvalidChannelName);
        }

        if !conn.is_tracking(name)
            && conn.channel_count() >= self.config.max_channels_per_connection
        {
            return Err(PusherError::SubscriptionLimitExceeded);
        }

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

        let channel = loop {
            let channel = registry.find_or_create(name);
            let announcing = channel.announcing();

            let first = channel.subscribe(conn.clone(), parsed.clone());

            // The channel emptied and was removed between the lookup and the
            // join, so this membership is in a channel nobody can reach.
            // Leave it and join whichever channel now holds the name.
            if !registry.holds(&channel) {
                channel.unsubscribe(&conn.id);
                continue;
            }

            if kind.is_presence() && first {
                self.announce_member_added(conn, &channel, &parsed);
            }

            drop(announcing);

            break channel;
        };

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

        self.telemetry.emit(
            EventKind::MemberAdded,
            &conn.app.id,
            json!({
                "channel": channel.name,
                "socket_id": conn.id,
                "user_id": data.get("user_id").cloned().unwrap_or(Value::Null),
                "user_info": data.get("user_info").cloned().unwrap_or(Value::Null),
            }),
        );

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

        if let Some(channel) = registry.find(name) {
            self.leave(conn, &registry, &channel);
        }
    }

    /// Remove a connection from one channel, announcing a departing presence
    /// member once their last connection to it has gone.
    fn leave(&self, conn: &Arc<Conn>, registry: &AppRegistry, channel: &Channel) {
        let _announcing = channel.announcing();

        let Some((member, last)) = registry.leave(channel, &conn.id) else {
            return;
        };

        if !channel.kind.is_presence() || !last || member.user_id().is_none() {
            return;
        }

        let raw = member.data.get("user_id").cloned().unwrap_or(Value::Null);

        self.telemetry.emit(
            EventKind::MemberRemoved,
            &conn.app.id,
            json!({ "channel": channel.name, "socket_id": conn.id, "user_id": raw }),
        );

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

        for channel in registry.channels_of(conn) {
            self.leave(conn, &registry, &channel);
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

        let member = self
            .app_registry(&conn.app)
            .find(channel_name)
            .and_then(|channel| channel.find(&conn.id));

        if policy == ClientEvents::Members && member.is_none() {
            conn.send_error(PusherError::NotAChannelMember);
            return Ok(());
        }

        // Rebuild the payload so only the expected members survive, plus the
        // authenticated user ID where we have one. Reverb does this only for
        // `members` and forwards anything else verbatim, which lets a client
        // pick its own `user_id` or fan out to a `channels` list of its
        // choosing; here the one named channel is all a whisper ever reaches.
        let mut rebroadcast = payload_map([
            ("event", json!(name)),
            ("channel", json!(channel_name)),
            ("data", event.get("data").cloned().unwrap_or(Value::Null)),
        ]);

        // Public channels allow unauthenticated users, so there may be no
        // user ID to attach.
        if let Some(user_id) =
            member.as_ref().and_then(|m| m.data.get("user_id")).filter(|id| !id.is_null())
        {
            rebroadcast.insert("user_id".into(), user_id.clone());
        }

        self.dispatch(&conn.app, rebroadcast, Some(&conn.id));

        Ok(())
    }

    // -- Fan-out --------------------------------------------------------------

    /// Deliver a payload to its channels here, then to the rest of the
    /// cluster when scaling is on.
    ///
    /// Reverb publishes and waits for its own echo before delivering locally,
    /// so a Redis outage silences even clients on the same node. Delivering
    /// first keeps a node working on its own; peers skip the echo.
    pub fn dispatch(
        &self,
        app: &Arc<Application>,
        payload: Map<String, Value>,
        except: Option<&str>,
    ) {
        self.dispatch_locally(app, &payload, except);

        if let Some(pubsub) = self.pubsub() {
            pubsub.publish_message(app, payload, except);
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
