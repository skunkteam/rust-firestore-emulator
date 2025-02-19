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

#[cfg(test)]
mod tests {
    use emulator_database::{FirestoreConfig, FirestoreProject};
    use emulator_tracing::DefaultTracing;
    use googletest::prelude::*;
    use reqwest::Client;
    use tokio::net::TcpListener;

    use crate::RouterBuilder;

    #[gtest]
    #[tokio::test]
    async fn test_dynamic_tracing() -> Result<()> {
        let project = FirestoreProject::new(FirestoreConfig::default());
        let project = Box::leak(Box::new(project));
        let tracing = DefaultTracing::start();
        let router = RouterBuilder::new(project)
            .add_dynamic_tracing(tracing)
            .build();

        let tcp_listener = TcpListener::bind("127.0.0.1:0").await?;
        let host = tcp_listener.local_addr()?;
        tokio::spawn(async {
            axum::serve(tcp_listener, router).await.unwrap();
        });

        let client = Client::new();
        let handle_url = format!("http://{host}/emulator/v1/logging/my-handle");
        let poll_url = format!("{handle_url}:poll");

        // Start capturing logging of all tower_http DEBUG messages
        let logs = client
            .post(&handle_url)
            .body("tower_http=debug")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await;

        expect_that!(logs, ok(eq("")), "we start with no logging captured");

        let logs = client
            .post(&poll_url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await;
        expect_that!(
            logs,
            ok(all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            )),
            "the first poll shows some DEBUG messages from the tower_http crate"
        );

        // Now we will adjust the logging directives of this capture handle...
        let logs = client
            .post(&handle_url)
            .body("warn")
            .send()
            .await?
            .error_for_status()?
            .text()
            .await;
        expect_that!(
            logs,
            ok(all!(
                contains_substring("DEBUG"),
                contains_substring("tower_http::trace")
            )),
            "we still receive the logs that were captured up until now"
        );

        let logs = client
            .post(&poll_url)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await;
        expect_that!(
            logs,
            ok(eq("")),
            "but this time there is no logging, because no WARN-level logs have been captured"
        );

        // Stop the capture
        client
            .delete(&handle_url)
            .send()
            .await?
            .error_for_status()?;

        let unknown_poll_status = client.post(&poll_url).send().await?.status();
        expect_that!(
            unknown_poll_status,
            eq(404),
            "polling on an unknown handle gives a 404"
        );
        Ok(())
    }
}
