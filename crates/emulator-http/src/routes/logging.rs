use std::time::Duration;

use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    response::Response,
    routing::get,
    BoxError, Router,
};
use emulator_tracing::{DynamicSubscriber, Tracing};
use tracing::{debug, error};

pub(crate) fn router<S: Tracing>() -> Router<&'static S> {
    Router::new().route("/", get(logging_route))
}

async fn logging_route(
    State(tracing): State<&'static impl Tracing>,
    ws: WebSocketUpgrade,
) -> Response {
    ws.on_upgrade(move |mut socket| async move {
        if let Err(err) = logging_handler(&mut socket, tracing).await {
            error!(error = err, "error in logging socket");
            let _unused = socket.send(Message::Text(format!("ERROR: {err}"))).await;
        };
    })
}

async fn logging_handler(
    socket: &mut WebSocket,
    tracing: &'static impl Tracing,
) -> Result<(), BoxError> {
    let Some(cmd) = socket.recv().await else {
        // Closed before receiving the first message
        return Ok(());
    };
    let Message::Text(level) = cmd? else {
        return Err("unexpected command".into());
    };

    let subscription = tracing.subscribe(&level)?;

    let mut interval = tokio::time::interval(Duration::from_millis(100));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                let logs = subscription.consume();
                if !logs.is_empty() {
                    socket.send(Message::Text(String::from_utf8_lossy(&logs).into_owned())).await?;
                }
            }
            cmd = socket.recv() => {
                let Some(cmd) = cmd else {
                    return Ok(());
                };
                let Message::Text(cmd) = cmd? else {
                    return Err("unexpected command".into());
                };
                if cmd != "STOP" {
                    return Err("unexpected command".into());
                }
                debug!("Received STOP command, flushing logs");
                socket.send(Message::Text(String::from_utf8_lossy(&subscription.consume()).into_owned())).await?;
                return Ok(());
            }
        }
    }
}
