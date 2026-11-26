use axum::{
    Router,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, Uri},
    response::Response,
    routing::{any, get, post},
};
use emulator_database::{
    FirestoreProject,
    reference::{Ref, RootRef},
};
use query_params::percent_decode_path_segment;

use crate::error::{RestBytes, RestError, Result, method_not_allowed_fallback};

mod admin;
mod documents;
mod query_params;

pub(super) use query_params::{decode_raw_path_segments, parse_query_pairs};

const SUBPATH_CUSTOM_VERBS: &[&str] = &[
    "listCollectionIds",
    "runQuery",
    "runAggregationQuery",
    "partitionQuery",
];

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new()
        .route(
            "/{project_id}/databases/{database_id}",
            get(admin::get_database)
                .patch(admin::patch_database)
                .fallback(method_not_allowed_fallback(
                    "/v1/projects",
                    "GET, HEAD, PATCH",
                )),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents",
            any(dispatch_documents_root),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents:listCollectionIds",
            post(dispatch_root_list_collection_ids)
                .fallback(method_not_allowed_fallback("/v1/projects", "POST")),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents:runQuery",
            post(dispatch_root_run_query)
                .fallback(method_not_allowed_fallback("/v1/projects", "POST")),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents:runAggregationQuery",
            post(dispatch_root_run_aggregation_query)
                .fallback(method_not_allowed_fallback("/v1/projects", "POST")),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents/{*tail}",
            any(dispatch_documents_tail),
        )
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .fallback(documents::unmatched_projects_route)
}

async fn dispatch_root_request(
    project: &'static FirestoreProject,
    verb: Option<&str>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let root = parse_raw_root_ref(uri.path())?;
    documents::dispatch_documents(project, Ref::Root(root), verb, method, &uri, &headers, body)
        .await
}

async fn dispatch_documents_root(
    State(project): State<&'static FirestoreProject>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    RestBytes(body): RestBytes,
) -> Result<Response> {
    dispatch_root_request(project, None, method, uri, headers, &body).await
}

async fn dispatch_root_list_collection_ids(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    RestBytes(body): RestBytes,
) -> Result<Response> {
    dispatch_root_request(
        project,
        Some("listCollectionIds"),
        Method::POST,
        uri,
        headers,
        &body,
    )
    .await
}

async fn dispatch_root_run_query(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    RestBytes(body): RestBytes,
) -> Result<Response> {
    dispatch_root_request(project, Some("runQuery"), Method::POST, uri, headers, &body).await
}

async fn dispatch_root_run_aggregation_query(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    RestBytes(body): RestBytes,
) -> Result<Response> {
    dispatch_root_request(
        project,
        Some("runAggregationQuery"),
        Method::POST,
        uri,
        headers,
        &body,
    )
    .await
}

async fn dispatch_documents_tail(
    State(project): State<&'static FirestoreProject>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    RestBytes(body): RestBytes,
) -> Result<Response> {
    let root = parse_raw_root_ref(uri.path())?;
    let raw_tail = extract_raw_tail(uri.path())?;
    let (clean_raw_tail, verb) = split_raw_custom_verb(raw_tail, &method);
    let tail = decode_raw_path_segments(clean_raw_tail)?;
    let parsed_ref: Ref = if tail.is_empty() {
        Ref::Root(root)
    } else {
        format!("{root}/{tail}").parse()?
    };
    documents::dispatch_documents(project, parsed_ref, verb, method, &uri, &headers, &body).await
}

pub(super) fn parse_raw_root_ref(raw_path: &str) -> Result<RootRef> {
    let (project_id, database_id) = extract_raw_project_and_database(raw_path)?;
    parse_root_ref(&project_id, &database_id)
}

pub(super) fn parse_root_ref(project_id: &str, database_id: &str) -> Result<RootRef> {
    Ok(RootRef::try_new(project_id, database_id)?)
}

fn extract_raw_project_and_database(raw_path: &str) -> Result<(String, String)> {
    let mut parts = raw_path.strip_prefix('/').unwrap_or(raw_path).split('/');
    let (Some(raw_proj), Some("databases"), Some(raw_db)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return Err(RestError::bad_request(format!(
            "Invalid Firestore project/database path: {raw_path}"
        )));
    };
    let project_id = percent_decode_path_segment(raw_proj)?;
    let database_id = percent_decode_path_segment(raw_db)?;
    Ok((project_id, database_id))
}

fn extract_raw_tail(raw_path: &str) -> Result<&str> {
    let mut parts = raw_path
        .strip_prefix('/')
        .unwrap_or(raw_path)
        .splitn(5, '/');
    let (_proj, db_kw, _db, docs_kw, tail) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    );
    match (db_kw, docs_kw, tail) {
        (Some("databases"), Some("documents"), Some(tail)) if !tail.is_empty() => Ok(tail),
        _ => Err(RestError::bad_request(format!(
            "Invalid Firestore documents path: {raw_path}"
        ))),
    }
}

fn split_raw_custom_verb<'a>(
    raw_tail: &'a str,
    method: &Method,
) -> (&'a str, Option<&'static str>) {
    if *method == Method::POST {
        for &verb in SUBPATH_CUSTOM_VERBS {
            if let Some(prefix) = raw_tail
                .strip_suffix(verb)
                .and_then(|s| s.strip_suffix(':'))
                && !prefix.is_empty()
                && !prefix.ends_with('/')
            {
                return (prefix, Some(verb));
            }
        }
    }
    (raw_tail, None)
}
