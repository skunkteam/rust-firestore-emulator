use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    body::Bytes,
    extract::{Path, State},
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
) -> Result<Bytes> {
    if let Some(name) = name.strip_suffix(":poll") {
        return poll_logs(tracing, name);
    }
    let mut subscribers = tracing.subscribers.lock().unwrap();
    let subscriber = tracing.tracing.subscribe(&body)?;
    let current_logs = subscribers
        .insert(name, Box::new(subscriber))
        .map(|mut subscriber| subscriber.drain())
        .unwrap_or_default();
    Ok(current_logs)
}

fn poll_logs<S: Tracing + 'static>(tracing: TracingState<S>, name: &str) -> Result<Bytes> {
    let mut subscribers = tracing.subscribers.lock().unwrap();
    subscribers
        .get_mut(name)
        .ok_or_else(RestError::not_found)
        .map(|subscriber| subscriber.drain())
}

async fn stop_capture<S: Tracing + 'static>(
    State(tracing): State<TracingState<S>>,
    Path(name): Path<String>,
) -> Result<Bytes> {
    let mut subscribers = tracing.subscribers.lock().unwrap();
    subscribers
        .remove(&name)
        .ok_or_else(RestError::not_found)
        .map(|mut subscriber| subscriber.drain())
}
