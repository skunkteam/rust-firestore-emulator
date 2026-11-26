use std::{convert::Infallible, str::FromStr, time::Duration};

use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, Uri, header},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{delete, get, post},
};
use emulator_database::{
    ContentionSnapshot, ContentionSubscription, DatabaseConcurrencyMode, DatabaseEdition,
    FirestoreProject,
    event::ProjectEvent,
    reference::{Ref, RootRef},
    snapshot::{ImportSummary, SnapshotError},
};
use googleapis::google::protobuf::Timestamp;
use serde::Serialize;
use serde_json::json;
use tokio_stream::{
    Stream, StreamExt,
    wrappers::{BroadcastStream, errors::BroadcastStreamRecvError},
};

use crate::error::{RestError, Result, method_not_allowed_fallback};

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new()
        .route(
            "/",
            get(list_databases).fallback(method_not_allowed_fallback("/emulator/v1", "GET, HEAD")),
        )
        .route(
            "/events",
            get(sse_events).fallback(method_not_allowed_fallback("/emulator/v1", "GET, HEAD")),
        )
        .route(
            "/export",
            get(export_snapshot).fallback(method_not_allowed_fallback("/emulator/v1", "GET, HEAD")),
        )
        .route(
            "/import",
            post(import_snapshot)
                .layer(DefaultBodyLimit::disable())
                .fallback(method_not_allowed_fallback("/emulator/v1", "POST")),
        )
        .route(
            "/contention",
            get(get_contention)
                .delete(delete_contention)
                .fallback(method_not_allowed_fallback(
                    "/emulator/v1",
                    "GET, HEAD, DELETE",
                )),
        )
        .route(
            "/contention/events",
            get(sse_contention_events)
                .fallback(method_not_allowed_fallback("/emulator/v1", "GET, HEAD")),
        )
        .route(
            "/{*ref}",
            delete(delete_by_ref).fallback(unmatched_emulator_route),
        )
        .fallback(unmatched_emulator_route)
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportedDatabaseResponse {
    name: String,
    project_id: String,
    database_id: String,
    database_edition: DatabaseEdition,
    concurrency_mode: DatabaseConcurrencyMode,
    document_count: u64,
    replaced: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImportSummaryResponse {
    databases: Vec<ImportedDatabaseResponse>,
    total_databases: usize,
    total_documents: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    import_id: Option<u64>,
}

impl From<ImportSummary> for ImportSummaryResponse {
    fn from(summary: ImportSummary) -> Self {
        let total_databases = summary.total_databases();
        let total_documents = summary.total_documents();
        let import_id = summary.import_id;
        let databases = summary
            .databases
            .into_iter()
            .map(|db| ImportedDatabaseResponse {
                name: db.name.to_string(),
                project_id: db.name.project_id.to_string(),
                database_id: db.name.database_id.to_string(),
                database_edition: db.settings.edition,
                concurrency_mode: db.settings.concurrency_mode,
                document_count: db.document_count,
                replaced: db.replaced,
            })
            .collect();
        Self {
            databases,
            total_databases,
            total_documents,
            import_id,
        }
    }
}

fn sanitize_filename_segment(segment: &str) -> String {
    segment
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

fn default_export_filename(single_target: Option<&RootRef>, snapshot_time: Timestamp) -> String {
    const MAX_PREFIX_BYTES: usize = 200;
    let utc = snapshot_time.format_utc_compact();
    match single_target {
        Some(root) => {
            let mut prefix = format!(
                "{}-{}",
                sanitize_filename_segment(&root.project_id),
                sanitize_filename_segment(&root.database_id)
            );
            prefix.truncate(MAX_PREFIX_BYTES);
            format!("{prefix}-{utc}.fsemu")
        }
        None => format!("firestore-emulator-snapshot-{utc}.fsemu"),
    }
}

async fn export_snapshot(
    State(project): State<&'static FirestoreProject>,
    method: Method,
    uri: Uri,
) -> Result<Response> {
    let mut targets = Vec::new();
    if let Some(raw_query) = uri.query() {
        for (key, val) in super::projects::parse_query_pairs(raw_query)? {
            if key != "database" {
                return Err(RestError::bad_request(format!(
                    "Unsupported query parameter for export: {key}"
                )));
            }
            if val.is_empty() {
                return Err(RestError::bad_request(
                    "database query parameter cannot be empty",
                ));
            }
            targets.push(RootRef::from_str(&val)?);
        }
    }

    let plan = if targets.is_empty() {
        project.prepare_export(None).await?
    } else {
        project.prepare_export(Some(&targets)).await?
    };

    let single_target = (!targets.is_empty())
        .then(|| plan.single_target())
        .flatten();
    let filename = default_export_filename(single_target, plan.snapshot_time());
    let disposition = HeaderValue::from_str(&format!("attachment; filename=\"{filename}\""))
        .map_err(|err| RestError::internal(format!("Invalid Content-Disposition header: {err}")))?;

    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from_stream(plan.into_stream())
    };

    Ok((
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            ),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        body,
    )
        .into_response())
}

fn resolve_request_host<'a>(headers: &'a HeaderMap, uri: &'a Uri) -> &'a str {
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .or_else(|| {
            uri.authority()
                .map(|a| a.as_str().trim())
                .filter(|h| !h.is_empty())
        })
        .unwrap_or("127.0.0.1:8080")
}

fn ensure_octet_stream_content_type(headers: &HeaderMap, uri: &Uri) -> Result<()> {
    let mut content_types = headers.get_all(header::CONTENT_TYPE).iter();
    let found = match (content_types.next(), content_types.next()) {
        (None, _) => "<missing>",
        (Some(_), Some(_)) => "<multiple>",
        (Some(val), None) => match val.to_str() {
            Ok(raw) => {
                let media_type = raw.split(';').next().unwrap_or("").trim();
                if media_type.eq_ignore_ascii_case("application/octet-stream") {
                    return Ok(());
                }
                raw
            }
            Err(_) => "<non-utf8>",
        },
    };
    let host = resolve_request_host(headers, uri);
    Err(RestError::unsupported_media_type(format!(
        "Unsupported or missing Content-Type '{found}'. Snapshot import requires Content-Type: \
         application/octet-stream (for example: curl -X POST --data-binary @snapshot.fsemu -H \
         \"Content-Type: application/octet-stream\" http://{host}/emulator/v1/import)."
    )))
}

async fn import_snapshot(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Result<Json<ImportSummaryResponse>> {
    if uri.query().is_some_and(|q| !q.is_empty()) {
        return Err(RestError::bad_request(
            "Snapshot import does not accept query parameters",
        ));
    }
    ensure_octet_stream_content_type(&headers, &uri)?;

    let stream = body
        .into_data_stream()
        .map(|res| res.map_err(|err| SnapshotError::Stream(err.to_string())));
    let summary = project.import_snapshot(stream).await?;
    Ok(Json(ImportSummaryResponse::from(summary)))
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
                ProjectEvent::DatabaseCreated {
                    database,
                    import_id,
                } => match import_id {
                    Some(id) => json!({
                        "type": "database_created",
                        "database": database.to_string(),
                        "importId": id,
                    }),
                    None => json!({
                        "type": "database_created",
                        "database": database.to_string(),
                    }),
                },
                ProjectEvent::DatabaseCleared { database } => json!({
                    "type": "database_cleared",
                    "database": database.to_string(),
                }),
                ProjectEvent::DatabaseReplaced {
                    database,
                    import_id,
                } => json!({
                    "type": "database_replaced",
                    "database": database.to_string(),
                    "importId": import_id,
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
    payload: impl Serialize,
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
    for (key, val) in super::projects::parse_query_pairs(query)? {
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
        Ref::Collection(_) | Ref::Document(_) => {
            if let Some(db) = project.get_existing_database(r.root()).await {
                let _deleted = db.delete_prefix(&r).await?;
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_export_filename_sanitizes_truncates_and_formats_utc() {
        let ts = Timestamp {
            seconds: 1_764_151_205, // 2025-11-26T10:00:05Z
            nanos:   123_456_789,
        };
        assert_eq!(
            default_export_filename(None, ts),
            "firestore-emulator-snapshot-20251126T100005Z.fsemu"
        );

        let default_db = RootRef::try_new("my project", "(default)").unwrap();
        assert_eq!(
            default_export_filename(Some(&default_db), ts),
            "my_project-_default_-20251126T100005Z.fsemu"
        );

        let unicode_db = RootRef::try_new("projé_1.0-ok", "db@2").unwrap();
        assert_eq!(
            default_export_filename(Some(&unicode_db), ts),
            "proj__1.0-ok-db_2-20251126T100005Z.fsemu"
        );

        let long_proj = "p".repeat(150);
        let long_db = "d".repeat(150);
        let long_root = RootRef::try_new(&long_proj, &long_db).unwrap();
        let filename = default_export_filename(Some(&long_root), ts);
        assert!(filename.len() <= 255);
        let prefix = filename.strip_suffix("-20251126T100005Z.fsemu").unwrap();
        assert_eq!(prefix.len(), 200);
        assert!(prefix.is_ascii());

        let out_of_range = Timestamp {
            seconds: i64::MAX,
            nanos:   0,
        };
        assert_eq!(
            default_export_filename(None, out_of_range),
            "firestore-emulator-snapshot-19700101T000000Z.fsemu"
        );
    }
}
