use std::{convert::Infallible, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::{Method, Uri},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get},
};
use emulator_database::{
    ContentionSnapshot, ContentionSubscription, FirestoreProject,
    event::ProjectEvent,
    reference::{Ref, RootRef},
};
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
        .route("/contention", get(get_contention).delete(delete_contention))
        .route("/contention/events", get(sse_contention_events))
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
            json_sse_event("change", payload)
        }
        Err(BroadcastStreamRecvError::Lagged(skipped)) => {
            lagged_resync_event(skipped, "project event stream")
        }
    });

    Sse::new(stream).keep_alive(sse_keep_alive())
}

fn json_sse_event(
    event_name: &'static str,
    payload: impl serde::Serialize,
) -> Option<std::result::Result<Event, Infallible>> {
    Event::default()
        .event(event_name)
        .json_data(payload)
        .ok()
        .map(Ok)
}

fn lagged_resync_event(
    skipped: u64,
    stream_name: &str,
) -> Option<std::result::Result<Event, Infallible>> {
    tracing::warn!(
        skipped,
        stream = stream_name,
        "SSE client lagged behind stream"
    );
    json_sse_event(
        "resync",
        json!({
            "type": "resync",
            "skipped": skipped,
        }),
    )
}

fn sse_keep_alive() -> KeepAlive {
    KeepAlive::new()
        .interval(Duration::from_secs(15))
        .text("heartbeat")
}

fn parse_database_query(query: Option<&str>) -> Result<Option<RootRef>> {
    let Some(query) = query else {
        return Ok(None);
    };
    let mut database = None;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = super::projects::percent_decode(raw_k)?;
        let val = super::projects::percent_decode(raw_v)?;
        if key == "database" {
            if database.is_some() {
                return Err(RestError::bad_request(
                    "Duplicate database query parameter is not supported",
                ));
            }
            if val.is_empty() {
                return Err(RestError::bad_request("database cannot be empty"));
            }
            let root: RootRef = val.parse().map_err(|_| {
                RestError::bad_request(format!("Invalid database resource name: {val}"))
            })?;
            database = Some(root);
        } else {
            return Err(RestError::bad_request(format!(
                "Unsupported query parameter: {key}"
            )));
        }
    }
    Ok(database)
}

async fn get_contention(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
) -> Result<Json<ContentionSnapshot>> {
    let database = parse_database_query(uri.query())?;
    Ok(Json(project.contention_snapshot(database.as_ref()).await))
}

async fn delete_contention(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
) -> Result<Json<serde_json::Value>> {
    let database = parse_database_query(uri.query())?;
    project.clear_contention_history(database.as_ref()).await;
    Ok(Json(json!({})))
}

async fn sse_contention_events(
    State(project): State<&'static FirestoreProject>,
) -> Sse<impl Stream<Item = std::result::Result<Event, Infallible>>> {
    let ContentionSubscription { rx, guard } = project.subscribe_contention_events();
    let stream = BroadcastStream::new(rx).filter_map(move |item| {
        let _keep_alive = &guard;
        match item {
            Ok(event) => json_sse_event("contention", event),
            Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                lagged_resync_event(skipped, "contention event stream")
            }
        }
    });

    Sse::new(stream).keep_alive(sse_keep_alive())
}

async fn delete_by_ref(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
) -> Result<Json<serde_json::Value>> {
    let raw_path = uri.path().strip_prefix('/').unwrap_or(uri.path());
    let decoded_ref = super::projects::decode_raw_path_segments(raw_path)?;
    let r: Ref = decoded_ref.parse()?;
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
