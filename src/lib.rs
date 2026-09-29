//! A drop-in replacement for the Laravel Reverb WebSocket server.
//!
//! Speaks the same Pusher protocol on the same routes with the same
//! configuration, so existing Laravel apps, `pusher-js` clients and the
//! `pusher-php-server` broadcaster need no changes.

pub mod channel;
pub mod config;
pub mod conn;
pub mod events;
pub mod http;
pub mod metrics;
pub mod protocol;
pub mod pubsub;
pub mod registry;
pub mod restart;
pub mod server;
pub mod ws;

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};

use crate::events::EventKind;
use crate::protocol::PusherError;
use crate::server::Server;

/// Build the HTTP and WebSocket routes, under the configured path prefix.
pub fn router(server: Arc<Server>) -> Router {
    let prefix = server.config.path.clone();

    let routes = Router::new()
        .route("/app/{appKey}", get(ws::handler))
        .route("/apps/{appId}/events", post(http::events))
        .route("/apps/{appId}/batch_events", post(http::batch_events))
        .route("/apps/{appId}/connections", get(http::connections))
        .route("/apps/{appId}/channels", get(http::channels))
        .route("/apps/{appId}/channels/{channel}", get(http::channel))
        .route("/apps/{appId}/channels/{channel}/users", get(http::channel_users))
        .route("/apps/{appId}/users/{userId}/terminate_connections", post(http::terminate_user))
        .route("/apps/{appId}/counters", get(http::counters))
        .route("/up", get(http::health_check))
        .fallback(http::not_found)
        .method_not_allowed_fallback(http::method_not_allowed)
        .layer(DefaultBodyLimit::max(server.config.max_request_size))
        .layer(axum::middleware::from_fn_with_state(server.clone(), http::limit_request_size))
        .with_state(server);

    if prefix.is_empty() { routes } else { Router::new().nest(&prefix, routes) }
}

/// Periodically ping idle connections and drop the ones that never answer.
pub async fn maintain(server: Arc<Server>) {
    let mut ticker =
        tokio::time::interval(Duration::from_secs(server.config.maintenance_interval.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;

        sweep(&server);
    }
}

/// One maintenance pass: drop connections that owe us a pong, then ping the
/// ones that have gone quiet. Exposed so the behaviour can be driven directly
/// instead of waited on.
pub fn sweep(server: &Arc<Server>) {
    prune_stale_connections(server);
    ping_inactive_connections(server);
}

/// Disconnect connections that were pinged and never replied.
///
/// Reverb only ever visits connections that joined a channel; this sweeps every
/// socket, so an idle client that never subscribed is still reclaimed.
fn prune_stale_connections(server: &Arc<Server>) {
    for app in &server.config.apps {
        for conn in server.registry.for_app(&app.id).sockets() {
            if !conn.is_stale() {
                continue;
            }

            conn.send_error(PusherError::PongNotReceived);

            // Reverb reports the connection's presence data with the event, so
            // read it before the memberships are torn down.
            let data = server
                .registry
                .for_app(&app.id)
                .subscribed_connections()
                .get(&conn.id)
                .map(|member| member.data.clone())
                .unwrap_or(serde_json::Value::Null);

            server.close(&conn);

            server.telemetry.emit(
                EventKind::ConnectionPruned,
                &app.id,
                serde_json::json!({ "socket_id": conn.id, "data": data }),
            );

            tracing::debug!(socket_id = %conn.id, "connection pruned");
        }
    }
}

/// Ping connections that have gone quiet for longer than `ping_interval`.
fn ping_inactive_connections(server: &Arc<Server>) {
    for app in &server.config.apps {
        for conn in server.registry.for_app(&app.id).sockets() {
            if conn.is_active() {
                continue;
            }

            if conn.uses_control_frames() {
                conn.send_control_ping();
            } else {
                conn.send(protocol::payload("ping", None, None));
            }

            conn.mark_pinged();
        }
    }
}

/// Close every connection, giving clients a clean shutdown.
pub fn disconnect_all(server: &Arc<Server>) {
    for app in &server.config.apps {
        for conn in server.registry.for_app(&app.id).sockets() {
            server.close(&conn);
        }
    }
}
