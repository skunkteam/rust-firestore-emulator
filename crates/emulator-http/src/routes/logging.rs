use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::post,
    Router,
};
use emulator_tracing::{DynamicSubscriber, Tracing};

use crate::error::{RestError, Result};

struct TracingState<S: Tracing + 'static> {
    tracing:     &'static S,
    subscribers: Arc<Mutex<HashMap<String, Box<dyn DynamicSubscriber>>>>,
}

impl<S: Tracing + 'static> Clone for TracingState<S> {
    fn clone(&self) -> Self {
        Self {
            tracing:     self.tracing,
            subscribers: self.subscribers.clone(),
        }
    }
}

pub(crate) fn router(tracing: &'static impl Tracing) -> Router {
    Router::new()
        .route(
            "/:name",
            post(start_capture_or_poll_logs).delete(stop_capture),
        )
        .with_state(TracingState {
            tracing,
            subscribers: Default::default(),
        })
}

async fn start_capture_or_poll_logs<S: Tracing + 'static>(
    State(tracing): State<TracingState<S>>,
    Path(name): Path<String>,
    body: String,
) -> Result<impl IntoResponse> {
    if let Some(name) = name.strip_suffix(":poll") {
        return poll_logs(tracing, name);
    }
    let subscriber = tracing.tracing.subscribe(&body)?;
    let replaced = tracing
        .subscribers
        .lock()
        .unwrap()
        .insert(name, Box::new(subscriber))
        .is_some();
    let msg = if replaced { "Restarted." } else { "Started." };
    Ok(msg.as_bytes().to_vec())
}

fn poll_logs<S: Tracing + 'static>(tracing: TracingState<S>, name: &str) -> Result<Vec<u8>> {
    let mut subscribers = tracing.subscribers.lock().unwrap();
    subscribers
        .get_mut(name)
        .ok_or_else(RestError::not_found)
        .map(|subscriber| subscriber.consume())
}

async fn stop_capture<S: Tracing + 'static>(
    State(tracing): State<TracingState<S>>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let mut subscribers = tracing.subscribers.lock().unwrap();
    subscribers
        .remove(&name)
        .ok_or_else(RestError::not_found)
        .map(|mut subscriber| subscriber.consume())
}
