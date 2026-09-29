//! The Pusher HTTP API.
//!
//! Every endpoint but the health check is authenticated with Pusher's
//! `auth_signature` scheme: an HMAC over the request method, path and sorted
//! query parameters, with the body hashed in when present.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, State};
use axum::http::{HeaderValue, Method, StatusCode, header};
use axum::response::{IntoResponse, Response};
use md5::{Digest, Md5};
use serde_json::{Value, json};

use crate::config::Application;
use crate::metrics::{self, MetricRequest, MetricType};
use crate::server::{Server, payload_map, verify_signature};

/// Seconds either side of now that a request signature remains valid.
const SIGNATURE_TOLERANCE: i64 = 600;

/// An API failure, rendered the way Reverb renders one: a bare status and a
/// plain-text reason.
pub struct ApiError(StatusCode, String);

impl ApiError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self(status, message.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, self.1).into_response()
    }
}

type ApiResult = Result<Response, ApiError>;

/// Symfony's `JsonResponse` — which Reverb returns from every endpoint —
/// encodes with `JSON_HEX_TAG | JSON_HEX_AMP | JSON_HEX_APOS | JSON_HEX_QUOT`,
/// so these five characters appear as `\uXXXX` escapes inside strings rather
/// than in their usual form. Matching it keeps responses byte-identical.
struct SymfonyJson;

impl serde_json::ser::Formatter for SymfonyJson {
    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        let mut start = 0;

        for (index, byte) in fragment.bytes().enumerate() {
            let escape: &[u8] = match byte {
                b'<' => b"\\u003C",
                b'>' => b"\\u003E",
                b'&' => b"\\u0026",
                b'\'' => b"\\u0027",
                _ => continue,
            };

            writer.write_all(&fragment.as_bytes()[start..index])?;
            writer.write_all(escape)?;
            start = index + 1;
        }

        writer.write_all(&fragment.as_bytes()[start..])
    }

    fn write_char_escape<W>(
        &mut self,
        writer: &mut W,
        escape: serde_json::ser::CharEscape,
    ) -> std::io::Result<()>
    where
        W: ?Sized + std::io::Write,
    {
        use serde_json::ser::{CharEscape, CompactFormatter};

        match escape {
            CharEscape::Quote => writer.write_all(b"\\u0022"),
            other => CompactFormatter.write_char_escape(writer, other),
        }
    }
}

/// Serialize a value the way Symfony's `JsonResponse` would.
fn encode(value: &Value) -> String {
    use serde::Serialize;

    let mut out = Vec::with_capacity(64);
    let mut serializer = serde_json::Serializer::with_formatter(&mut out, SymfonyJson);

    value.serialize(&mut serializer).expect("serializing a Value cannot fail");

    String::from_utf8(out).expect("serde_json emits valid UTF-8")
}

/// Render a JSON body with an explicit `Content-Length`, as Reverb does.
fn json_response(status: StatusCode, body: &Value) -> Response {
    let encoded = encode(body);

    let mut response = (status, encoded).into_response();

    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));

    response
}

fn ok(body: Value) -> ApiResult {
    Ok(json_response(StatusCode::OK, &body))
}

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// A request that has passed signature verification.
struct Verified {
    app: Arc<Application>,
    query: BTreeMap<String, Vec<String>>,
}

impl Verified {
    /// The first value of a scalar query parameter.
    fn param(&self, key: &str) -> Option<&str> {
        self.query.get(key)?.first().map(String::as_str)
    }
}

/// Verify the Pusher authentication signature for an incoming request.
fn verify(
    server: &Server,
    app_id: &str,
    method: &Method,
    uri_path: &str,
    raw_query: &str,
    body: &[u8],
) -> Result<Verified, ApiError> {
    // Reverb's router never matches an empty `{appId}` segment, so such a
    // request is simply not found.
    if app_id.is_empty() {
        return Err(ApiError::new(StatusCode::NOT_FOUND, "Not found."));
    }

    let Some(app) = server.app_by_id(app_id) else {
        return Err(ApiError::new(
            StatusCode::NOT_FOUND,
            format!("No matching application for ID [{app_id}]."),
        ));
    };

    let query = parse_query(raw_query);

    let invalid =
        || ApiError::new(StatusCode::UNAUTHORIZED, "Authentication signature invalid.");

    // Everything but the signature itself, the app identifiers and a
    // client-supplied body hash participates in the signature.
    let mut params: BTreeMap<String, String> = query
        .iter()
        .filter(|(key, _)| {
            !matches!(
                key.as_str(),
                "auth_signature" | "body_md5" | "appId" | "appKey" | "channelName"
            )
        })
        .map(|(key, values)| (key.clone(), values.join(",")))
        .collect();

    if !body.is_empty() {
        params.insert("body_md5".into(), hex::encode(Md5::digest(body)));
    }

    let encoded = params
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&");

    let path = strip_prefix(uri_path, &server.config.path);
    let signed = format!("{}\n{}\n{}", method.as_str(), path, encoded);

    let provided = query
        .get("auth_signature")
        .and_then(|values| values.first())
        .ok_or_else(invalid)?;

    if !verify_signature(&app.secret, &signed, provided) {
        return Err(invalid());
    }

    let timestamp: i64 = query
        .get("auth_timestamp")
        .and_then(|values| values.first())
        .and_then(|value| value.parse().ok())
        .ok_or_else(invalid)?;

    if (now() - timestamp).abs() > SIGNATURE_TOLERANCE {
        return Err(invalid());
    }

    Ok(Verified { app, query })
}

/// Remove the server's configured route prefix from a request path.
fn strip_prefix(path: &str, prefix: &str) -> String {
    if prefix.is_empty() {
        return path.to_string();
    }

    match path.split_once(prefix) {
        Some((_, rest)) => format!("/{}", rest.trim_start_matches('/')),
        None => path.to_string(),
    }
}

/// Parse a query string the way PHP's `parse_str` does: repeated scalar keys
/// keep the last value, while `key[]` accumulates into a list.
fn parse_query(raw: &str) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();

    for (key, value) in form_urlencoded::parse(raw.as_bytes()) {
        match key.strip_suffix("[]") {
            Some(name) => out.entry(name.to_string()).or_default().push(value.into_owned()),
            None => {
                out.insert(key.into_owned(), vec![value.into_owned()]);
            }
        }
    }

    out
}

/// Answer a metrics question, consulting peers when scaling is enabled.
async fn gather(server: &Arc<Server>, app: &Arc<Application>, request: MetricRequest) -> Value {
    match server.pubsub() {
        Some(pubsub) => pubsub.gather(&request).await,
        None => metrics::local(server, app, &request),
    }
}

/// Build a 422 body in Laravel's validation-error shape.
fn validation_error(field: &str, message: &str) -> Response {
    json_response(StatusCode::UNPROCESSABLE_ENTITY, &json!({ field: [message] }))
}

// -- Endpoints ----------------------------------------------------------------

pub async fn health_check() -> Response {
    json_response(StatusCode::OK, &json!({ "health": "OK" }))
}

/// Reverb answers an unrouted path with a plain-text reason, not an empty body.
pub async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "Not found.").into_response()
}

/// As above for a known path reached with the wrong method. Axum has already
/// attached the `Allow` header by this point.
pub async fn method_not_allowed() -> Response {
    (StatusCode::METHOD_NOT_ALLOWED, "Method not allowed.").into_response()
}

/// Reject a request whose body exceeds `max_request_size` before reading it.
///
/// Reverb applies the limit while buffering the raw HTTP message; checking
/// `Content-Length` catches every real client, and axum's body limit backstops
/// the chunked case.
pub async fn limit_request_size(
    State(server): State<Arc<Server>>,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let declared = request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok());

    if declared.is_some_and(|length| length > server.config.max_request_size) {
        return (StatusCode::PAYLOAD_TOO_LARGE, "Payload too large.").into_response();
    }

    next.run(request).await
}

/// `POST /apps/{appId}/events`
pub async fn events(
    State(server): State<Arc<Server>>,
    Path(app_id): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &body)?;
    let app = request.app.clone();

    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Bad request."));
    };

    let Some(name) = payload.get("name").and_then(Value::as_str) else {
        return Ok(validation_error("name", "The name field is required."));
    };

    let Some(data) = payload.get("data").and_then(Value::as_str) else {
        return Ok(validation_error("data", "The data field is required."));
    };

    let channels = match (payload.get("channels"), payload.get("channel")) {
        (Some(Value::Array(items)), _) => {
            items.iter().filter_map(|c| c.as_str().map(str::to_string)).collect()
        }
        (_, Some(Value::String(one))) => vec![one.clone()],
        _ => return Ok(validation_error("channel", "The channel field is required.")),
    };

    let socket_id = payload.get("socket_id").and_then(Value::as_str);

    server.dispatch(
        &app,
        payload_map([
            ("event", json!(name)),
            ("channels", json!(channels)),
            ("data", json!(data)),
        ]),
        socket_id,
    );

    let Some(info) = payload.get("info").and_then(Value::as_str) else {
        return ok(json!({}));
    };

    let request = MetricRequest::new(MetricType::Channels, &app.id).info(info).channels(channels);

    ok(json!({ "channels": gather(&server, &app, request).await }))
}

/// `POST /apps/{appId}/batch_events`
pub async fn batch_events(
    State(server): State<Arc<Server>>,
    Path(app_id): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &body)?;
    let app = request.app.clone();

    let Ok(payload) = serde_json::from_slice::<Value>(&body) else {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "Bad request."));
    };

    let Some(batch) = payload.get("batch").and_then(Value::as_array) else {
        return Ok(validation_error("batch", "The batch field is required."));
    };

    let mut requested_info = false;
    let mut pending: Vec<Option<MetricRequest>> = Vec::with_capacity(batch.len());

    for item in batch {
        let (Some(name), Some(data), Some(channel)) = (
            item.get("name").and_then(Value::as_str),
            item.get("data").and_then(Value::as_str),
            item.get("channel").and_then(Value::as_str),
        ) else {
            return Ok(validation_error("batch", "The batch field is invalid."));
        };

        server.dispatch(
            &app,
            payload_map([
                ("event", json!(name)),
                ("channel", json!(channel)),
                ("data", json!(data)),
            ]),
            item.get("socket_id").and_then(Value::as_str),
        );

        match item.get("info").and_then(Value::as_str) {
            Some(info) => {
                requested_info = true;
                pending.push(Some(
                    MetricRequest::new(MetricType::Channel, &app.id).channel(channel).info(info),
                ));
            }
            None => pending.push(None),
        }
    }

    if !requested_info {
        return ok(json!({ "batch": {} }));
    }

    let mut results = Vec::with_capacity(pending.len());

    for request in pending {
        match request {
            Some(request) => results.push(gather(&server, &app, request).await),
            None => results.push(json!({})),
        }
    }

    ok(json!({ "batch": results }))
}

/// `GET /apps/{appId}/connections`
pub async fn connections(
    State(server): State<Arc<Server>>,
    Path(app_id): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &[])?;
    let app = request.app.clone();

    let result =
        gather(&server, &app, MetricRequest::new(MetricType::Connections, &app.id)).await;

    ok(json!({ "connections": result.as_array().map_or(0, Vec::len) }))
}

/// `GET /apps/{appId}/channels`
pub async fn channels(
    State(server): State<Arc<Server>>,
    Path(app_id): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &[])?;
    let app = request.app.clone();

    let metric = MetricRequest::new(MetricType::Channels, &app.id)
        .info(request.param("info").unwrap_or(""))
        .filter(request.param("filter_by_prefix").map(str::to_string));

    ok(json!({ "channels": gather(&server, &app, metric).await }))
}

/// `GET /apps/{appId}/channels/{channel}`
pub async fn channel(
    State(server): State<Arc<Server>>,
    Path((app_id, channel)): Path<(String, String)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &[])?;
    let app = request.app.clone();

    // Occupancy is always reported for a single channel, whatever was asked for.
    let info = match request.param("info") {
        Some(info) if !info.is_empty() => format!("{info},occupied"),
        _ => "occupied".to_string(),
    };

    let metric = MetricRequest::new(MetricType::Channel, &app.id).channel(channel).info(info);

    ok(gather(&server, &app, metric).await)
}

/// `GET /apps/{appId}/channels/{channel}/users`
pub async fn channel_users(
    State(server): State<Arc<Server>>,
    Path((app_id, channel)): Path<(String, String)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &[])?;
    let app = request.app.clone();

    let Some(found) = server.registry.for_app(&app.id).find(&channel) else {
        return Ok(json_response(StatusCode::NOT_FOUND, &json!({})));
    };

    if !found.kind.is_presence() {
        return Ok(json_response(StatusCode::BAD_REQUEST, &json!({})));
    }

    let metric = MetricRequest::new(MetricType::ChannelUsers, &app.id).channel(channel);

    ok(json!({ "users": gather(&server, &app, metric).await }))
}

/// `GET /apps/{appId}/counters`
///
/// A `reverb-rs` extension, not part of the Pusher API. Reports cumulative
/// message counts for the application so a Pulse recorder can poll them the
/// same way `ReverbConnections` polls `/connections`. Counts are per node and
/// reset when the process restarts, so consumers should treat a value lower
/// than the last one they saw as a restart rather than a negative delta.
pub async fn counters(
    State(server): State<Arc<Server>>,
    Path(app_id): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &[])?;
    let counters = server.telemetry.counters(&request.app.id);

    ok(json!({
        "messages_sent": counters.sent(),
        "messages_received": counters.received(),
        "events_dropped": server.telemetry.dropped(),
    }))
}

/// `POST /apps/{appId}/users/{userId}/terminate_connections`
pub async fn terminate_user(
    State(server): State<Arc<Server>>,
    Path((app_id, user_id)): Path<(String, String)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    body: Bytes,
) -> ApiResult {
    let request = verify(&server, &app_id, &method, uri.path(), uri.query().unwrap_or(""), &body)?;
    let app = request.app.clone();

    match server.pubsub() {
        Some(pubsub) => pubsub.publish_terminate(&app, &user_id),
        None => server.terminate_user(&app, &user_id),
    }

    ok(json!({}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_the_configured_route_prefix() {
        assert_eq!(strip_prefix("/apps/1/events", ""), "/apps/1/events");
        assert_eq!(strip_prefix("/reverb/apps/1/events", "/reverb"), "/apps/1/events");
        assert_eq!(strip_prefix("/apps/1/events", "/reverb"), "/apps/1/events");
    }

    #[test]
    fn parses_scalar_and_array_query_parameters() {
        let query = parse_query("info=user_count&auth_key=abc&auth_key=def");

        assert_eq!(query["info"], vec!["user_count"]);
        assert_eq!(query["auth_key"], vec!["def"], "a repeated scalar keeps the last value");

        let arrays = parse_query("channels[]=one&channels[]=two");
        assert_eq!(arrays["channels"], vec!["one", "two"]);
    }

    #[test]
    fn encodes_bodies_the_way_symfony_does() {
        let body = json!({ "cache": { "data": r#"{"foo":"bar"}"# }, "html": "<b>&'x'</b>" });

        assert_eq!(
            encode(&body),
            r#"{"cache":{"data":"{\u0022foo\u0022:\u0022bar\u0022}"},"html":"\u003Cb\u003E\u0026\u0027x\u0027\u003C/b\u003E"}"#
        );
    }

    #[test]
    fn leaves_backslashes_alone_when_escaping() {
        assert_eq!(encode(&json!("App\\Events\\TestEvent")), r#""App\\Events\\TestEvent""#);
    }

    #[test]
    fn decodes_percent_encoded_parameters() {
        let query = parse_query("info=user_count%2Csubscription_count");

        assert_eq!(query["info"], vec!["user_count,subscription_count"]);
    }
}
