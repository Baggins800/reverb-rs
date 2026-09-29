//! The WebSocket endpoint: `GET /app/{appKey}`.

use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::Response;
use futures_util::SinkExt;
use tokio::sync::mpsc;

use crate::conn::{Conn, Outbound};
use crate::protocol::PusherError;
use crate::server::Server;

pub async fn handler(
    ws: WebSocketUpgrade,
    Path(app_key): Path<String>,
    State(server): State<Arc<Server>>,
    headers: HeaderMap,
) -> Response {
    let origin =
        headers.get("origin").and_then(|value| value.to_str().ok()).map(str::to_string);

    let Some(app) = server.app_by_key(&app_key) else {
        // Reverb completes the handshake before reporting an unknown key, so
        // the client receives a protocol error rather than an HTTP failure.
        return ws.on_upgrade(|mut socket| async move {
            let _ = socket
                .send(Message::Text(PusherError::ApplicationDoesNotExist.frame().into()))
                .await;
            let _ = socket.close().await;
        });
    };

    let max_message_size = app.max_message_size;
    let buffer = server.config.ws_buffer_size;

    ws.max_message_size(max_message_size)
        .max_frame_size(max_message_size)
        .read_buffer_size(buffer)
        .write_buffer_size(buffer)
        .on_upgrade(move |socket| serve(socket, server, app, origin))
}

async fn serve(
    mut socket: WebSocket,
    server: Arc<Server>,
    app: Arc<crate::config::Application>,
    origin: Option<String>,
) {
    let (tx, mut rx) = mpsc::channel(server.config.send_queue_depth);

    let conn = Arc::new(Conn::new(app, origin, tx, server.telemetry.clone()));
    let registry = server.registry.for_app(&conn.app.id);

    registry.add_socket(conn.clone());
    server.open(&conn);

    // Reads and writes share one task so the connection applies natural
    // backpressure: a client that cannot drain its queue also stops being read.
    loop {
        tokio::select! {
            outgoing = rx.recv() => {
                let Some(frame) = outgoing else { break };

                let message = match frame {
                    Outbound::Text(text) => Message::Text(text),
                    Outbound::Ping => Message::Ping(Default::default()),
                    Outbound::Close => break,
                };

                if socket.send(message).await.is_err() {
                    break;
                }
            }

            incoming = socket.recv() => {
                let Some(Ok(message)) = incoming else { break };

                match message {
                    Message::Text(text) => server.message(&conn, text.as_str()),

                    // Reverb feeds binary frames through the same JSON decoder,
                    // which always rejects them.
                    Message::Binary(_) => conn.send_error(PusherError::InvalidMessageFormat),

                    // A client that speaks control frames has its liveness
                    // tracked with them instead of `pusher:ping` messages.
                    Message::Ping(_) | Message::Pong(_) => {
                        conn.set_uses_control_frames();
                        conn.touch();
                    }

                    Message::Close(_) => break,
                }
            }
        }
    }

    server.close(&conn);

    let _ = socket.close().await;
}
