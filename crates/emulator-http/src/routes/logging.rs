use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    routing::post,
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
            "/{name}",
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

#[cfg(test)]
mod tests {
    use axum::{
        Router,
        body::{Body, to_bytes},
        http::{Method, Request, StatusCode},
    };
    use emulator_database::{FirestoreConfig, FirestoreProject};
    use emulator_tracing::DefaultTracing;
    use googletest::prelude::*;
    use tower::ServiceExt;

    use crate::RouterBuilder;

    async fn send_request(
        router: &Router,
        method: Method,
        uri: &str,
        body: &str,
    ) -> Result<(StatusCode, String)> {
        let resp = router
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::from(body.to_owned()))?,
            )
            .await?;
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await?;
        Ok((status, String::from_utf8(bytes.to_vec())?))
    }

    #[gtest]
    #[tokio::test]
    async fn test_dynamic_tracing() -> Result<()> {
        let project = FirestoreProject::new(FirestoreConfig::default());
        let project = Box::leak(Box::new(project));
        let tracing = DefaultTracing::start();
        let router = RouterBuilder::new(project)
            .add_dynamic_tracing(tracing)
            .build();

        let handle_uri = "/emulator/v1/logging/my-handle";
        let poll_uri = "/emulator/v1/logging/my-handle:poll";

        // Start capturing logging of all tower_http DEBUG messages
        let (status, logs) =
            send_request(&router, Method::POST, handle_uri, "tower_http=debug").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(logs, eq(""), "we start with no logging captured");

        let (status, logs) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            ),
            "the first poll shows some DEBUG messages from the tower_http crate"
        );

        // Now we will adjust the logging directives of this capture handle...
        let (status, logs) = send_request(&router, Method::POST, handle_uri, "warn").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            ),
            "we still receive the logs that were captured up until now"
        );

        let (status, logs) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(status, StatusCode::OK);
        expect_that!(
            logs,
            eq(""),
            "but this time there is no logging, because no WARN-level logs have been captured"
        );

        // Stop the capture
        let (status, _) = send_request(&router, Method::DELETE, handle_uri, "").await?;
        expect_eq!(status, StatusCode::OK);

        let (unknown_poll_status, _) = send_request(&router, Method::POST, poll_uri, "").await?;
        expect_eq!(
            unknown_poll_status,
            StatusCode::NOT_FOUND,
            "polling on an unknown handle gives a 404"
        );
        Ok(())
    }
}
