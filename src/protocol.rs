//! Pusher wire-format helpers.
//!
//! Reverb builds every frame with `json_encode(array_filter([...]))`, which
//! drops empty members and preserves insertion order. The helpers here produce
//! byte-identical output: keys always appear as `event`, `data`, `channel`, and
//! `data` is itself a JSON-encoded *string*.

use serde_json::{Map, Value};

pub type Payload = Map<String, Value>;

/// Build a `pusher:` frame. `data` is encoded as a nested JSON string and
/// omitted entirely when empty, as is `channel` when absent.
pub fn payload(event: &str, data: Option<&Value>, channel: Option<&str>) -> String {
    frame(&format!("pusher:{event}"), data.map(encode_data), channel)
}

/// Build a `pusher_internal:` frame. Unlike [`payload`], the `data` member is
/// always present — an absent body is encoded as the object `{}`.
pub fn internal_payload(event: &str, data: Option<&Value>, channel: Option<&str>) -> String {
    let encoded = data.map(encode_data).unwrap_or_else(|| "{}".to_string());

    frame(&format!("pusher_internal:{event}"), Some(encoded), channel)
}

/// Encode a payload body the way `json_encode((object) $data)` would: an empty
/// or absent body becomes `{}` rather than `[]` or `null`.
fn encode_data(data: &Value) -> String {
    match data {
        Value::Null => "{}".to_string(),
        Value::Object(map) if map.is_empty() => "{}".to_string(),
        Value::Array(items) if items.is_empty() => "{}".to_string(),
        other => other.to_string(),
    }
}

fn frame(event: &str, data: Option<String>, channel: Option<&str>) -> String {
    let mut out = String::with_capacity(64 + data.as_ref().map_or(0, |d| d.len()));

    out.push_str("{\"event\":");
    write_json_string(&mut out, event);

    // `array_filter` removes empty members, so an empty body drops the key.
    if let Some(data) = data.filter(|d| !d.is_empty()) {
        out.push_str(",\"data\":");
        write_json_string(&mut out, &data);
    }

    if let Some(channel) = channel.filter(|c| !c.is_empty()) {
        out.push_str(",\"channel\":");
        write_json_string(&mut out, channel);
    }

    out.push('}');
    out
}

fn write_json_string(out: &mut String, value: &str) {
    // `Value::String` escaping matches PHP's `json_encode` for the characters
    // that appear in channel names, event names and encoded payload bodies.
    out.push_str(&Value::String(value.to_string()).to_string());
}

/// The Pusher error codes Reverb emits, each carrying its canonical message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PusherError {
    /// Application key did not resolve to a configured application.
    ApplicationDoesNotExist,
    /// Application is over its configured connection quota.
    ConnectionLimitExceeded,
    /// Subscription auth signature did not verify, or the origin was rejected.
    Unauthorized,
    /// Connection origin is not in the application's allow list.
    InvalidOrigin,
    /// The message could not be parsed or handled.
    InvalidMessageFormat,
    /// The connection exceeded its configured message rate limit.
    RateLimitExceeded,
    /// Client events are disabled for this application.
    ClientEventsDisabled,
    /// The client is not a member of the channel it tried to whisper on.
    NotAChannelMember,
    /// No pong was received within the ping window.
    PongNotReceived,
}

impl PusherError {
    pub fn code(&self) -> u16 {
        match self {
            Self::ApplicationDoesNotExist => 4001,
            Self::ConnectionLimitExceeded => 4004,
            Self::Unauthorized | Self::InvalidOrigin | Self::NotAChannelMember => 4009,
            Self::InvalidMessageFormat => 4200,
            Self::PongNotReceived => 4201,
            Self::RateLimitExceeded | Self::ClientEventsDisabled => 4301,
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::ApplicationDoesNotExist => "Application does not exist",
            Self::ConnectionLimitExceeded => "Application is over connection quota",
            Self::Unauthorized => "Connection is unauthorized",
            Self::InvalidOrigin => "Origin not allowed",
            Self::InvalidMessageFormat => "Invalid message format",
            Self::RateLimitExceeded => "Rate limit exceeded",
            Self::ClientEventsDisabled => "The app does not have client messaging enabled.",
            Self::NotAChannelMember => "The client is not a member of the specified channel.",
            Self::PongNotReceived => "Pong reply not received in time",
        }
    }

    /// The full `pusher:error` frame for this error.
    pub fn frame(&self) -> String {
        let body = serde_json::json!({ "code": self.code(), "message": self.message() });

        payload("error", Some(&body), None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn formats_a_connection_established_frame() {
        let data = json!({ "socket_id": "123.456", "activity_timeout": 30 });

        assert_eq!(
            payload("connection_established", Some(&data), None),
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"123.456\",\"activity_timeout\":30}"}"#
        );
    }

    #[test]
    fn omits_an_empty_body_and_absent_channel() {
        assert_eq!(payload("pong", None, None), r#"{"event":"pusher:pong"}"#);
        assert_eq!(
            payload("cache_miss", None, Some("cache-test-channel")),
            r#"{"event":"pusher:cache_miss","channel":"cache-test-channel"}"#
        );
    }

    #[test]
    fn always_emits_a_body_for_internal_frames() {
        assert_eq!(
            internal_payload("subscription_succeeded", None, Some("test-channel")),
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{}","channel":"test-channel"}"#
        );
    }

    #[test]
    fn formats_presence_subscription_data() {
        let data = json!({ "presence": { "count": 1, "ids": [1], "hash": { "1": { "name": "Test User" } } } });

        assert_eq!(
            internal_payload("subscription_succeeded", Some(&data), Some("presence-test-channel")),
            r#"{"event":"pusher_internal:subscription_succeeded","data":"{\"presence\":{\"count\":1,\"ids\":[1],\"hash\":{\"1\":{\"name\":\"Test User\"}}}}","channel":"presence-test-channel"}"#
        );
    }

    #[test]
    fn formats_error_frames() {
        assert_eq!(
            PusherError::InvalidMessageFormat.frame(),
            r#"{"event":"pusher:error","data":"{\"code\":4200,\"message\":\"Invalid message format\"}"}"#
        );
        assert_eq!(
            PusherError::PongNotReceived.frame(),
            r#"{"event":"pusher:error","data":"{\"code\":4201,\"message\":\"Pong reply not received in time\"}"}"#
        );
    }
}
