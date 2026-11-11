use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{Method, Uri},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get},
};
use emulator_database::{FirestoreProject, event::ProjectEvent, reference::Ref};
use serde_json::json;
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use crate::error::{RestError, Result};

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new()
        .route("/", get(list_databases))
        .route("/events", get(sse_events))
        .route(
            "/{*ref}",
            delete(delete_by_ref).fallback(unmatched_emulator_route),
        )
        .fallback(unmatched_emulator_route)
}

async fn list_databases(State(project): State<&'static FirestoreProject>) -> impl IntoResponse {
    Json(project.database_names().await)
}

async fn sse_events(
    State(project): State<&'static FirestoreProject>,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let rx = project.subscribe_project_events();
    let stream = BroadcastStream::new(rx).filter_map(|item| match item {
        Ok(event) => {
            let payload = match event {
                ProjectEvent::DatabaseCreated { database } => json!({
                    "type": "database_created",
                    "database": database.to_string(),
                }),
                ProjectEvent::DatabaseCleared { database } => json!({
                    "type": "database_cleared",
                    "database": database.to_string(),
                }),
                ProjectEvent::DatabaseConfigChanged { database } => json!({
                    "type": "database_config_changed",
                    "database": database.to_string(),
                }),
                ProjectEvent::DocumentsChanged {
                    database,
                    paths,
                    update_time,
                } => json!({
                    "type": "documents_changed",
                    "database": database.to_string(),
                    "paths": paths,
                    "updateTime": update_time,
                }),
            };
            Event::default()
                .event("change")
                .json_data(payload)
                .ok()
                .map(Ok)
        }
        Err(BroadcastStreamRecvError::Lagged(skipped)) => {
            tracing::warn!(skipped, "SSE client lagged behind project event stream");
            Event::default()
                .event("resync")
                .json_data(json!({
                    "type": "resync",
                    "skipped": skipped,
                }))
                .ok()
                .map(Ok)
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("heartbeat"),
    )
}

async fn delete_by_ref(
    State(project): State<&'static FirestoreProject>,
    Path(raw_ref): Path<String>,
) -> Result<Json<serde_json::Value>> {
    let r: Ref = raw_ref.parse()?;
    match r {
        Ref::Root(r) => {
            project.clear_database(&r).await;
        }
        Ref::Collection(c) => {
            let db = project.database(&c.root_ref).await;
            let _deleted = db.delete_prefix(&Ref::Collection(c)).await?;
        }
        Ref::Document(d) => {
            let db = project.database(&d.collection_ref.root_ref).await;
            let _deleted = db.delete_prefix(&Ref::Document(d)).await?;
        }
    }
    Ok(Json(json!({})))
}

async fn unmatched_emulator_route(method: Method, uri: Uri) -> Response {
    RestError::not_found_msg(format!(
        "Unknown emulator endpoint: {method} /emulator/v1{}",
        uri.path()
    ))
    .into_response()
}
