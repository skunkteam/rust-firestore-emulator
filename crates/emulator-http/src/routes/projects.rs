use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, Method, Uri},
    response::{IntoResponse, Response},
    routing::{any, get, post},
};
use emulator_database::{
    FirestoreDatabase, FirestoreProject,
    projection::{Project, Projection},
    query::Query,
    read_consistency::ReadConsistency,
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
};
use googleapis::google::{
    firestore::v1::{
        Document, DocumentMask, ListCollectionIdsRequest, ListCollectionIdsResponse,
        ListDocumentsResponse, Precondition, RunQueryRequest, RunQueryResponse, precondition,
        run_query_request,
    },
    protobuf::Timestamp,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{Level, instrument};

use crate::error::{ApiJson, RestError, Result, parse_json_body};

const PHASE2_VERBS: &[&str] = &[
    "commit",
    "batchGet",
    "batchWrite",
    "beginTransaction",
    "rollback",
    "runAggregationQuery",
    "executePipeline",
    "partitionQuery",
];

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
            get(get_database).patch(patch_database),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents",
            any(dispatch_documents_root),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents:listCollectionIds",
            post(dispatch_root_list_collection_ids),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents:runQuery",
            post(dispatch_root_run_query),
        )
        .route(
            "/{project_id}/databases/{database_id}/documents/{*tail}",
            any(dispatch_documents_tail),
        )
        .layer(DefaultBodyLimit::max(50 * 1024 * 1024))
        .fallback(unmatched_projects_route)
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum DatabaseEdition {
    Standard,
    Enterprise,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum ConcurrencyMode {
    Pessimistic,
    Optimistic,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseMetadata {
    name: String,
    uid: &'static str,
    r#type: &'static str,
    database_edition: DatabaseEdition,
    concurrency_mode: ConcurrencyMode,
}

impl DatabaseMetadata {
    fn new(name: &RootRef, enterprise_edition: bool, optimistic_concurrency: bool) -> Self {
        Self {
            name: name.to_string(),
            uid: "emulator-uid",
            r#type: "FIRESTORE_NATIVE",
            database_edition: if enterprise_edition {
                DatabaseEdition::Enterprise
            } else {
                DatabaseEdition::Standard
            },
            concurrency_mode: if optimistic_concurrency {
                ConcurrencyMode::Optimistic
            } else {
                ConcurrencyMode::Pessimistic
            },
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchDatabase {
    #[serde(alias = "database_edition")]
    database_edition: Option<DatabaseEdition>,
    #[serde(alias = "concurrency_mode")]
    concurrency_mode: Option<ConcurrencyMode>,
    #[serde(alias = "firestore_data_access_mode")]
    firestore_data_access_mode: Option<serde_json::Value>,
    #[serde(alias = "mongodb_compatible_data_access_mode")]
    mongodb_compatible_data_access_mode: Option<serde_json::Value>,
    #[serde(alias = "realtime_updates_mode")]
    realtime_updates_mode: Option<serde_json::Value>,
}

async fn get_database(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
) -> Result<Json<DatabaseMetadata>> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    let name = parse_root_ref(&project_id, &database_id)?;

    let db = project.database(&name).await;
    Ok(Json(DatabaseMetadata::new(
        &name,
        db.enterprise_edition(),
        db.optimistic_concurrency(),
    )))
}

async fn patch_database(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    ApiJson(payload): ApiJson<PatchDatabase>,
) -> Result<Json<DatabaseMetadata>> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    let name = parse_root_ref(&project_id, &database_id)?;
    let update_mask = parse_patch_database_mask(uri.query())?;

    if update_mask.is_none() {
        for (name, val) in [
            (
                "firestore_data_access_mode",
                &payload.firestore_data_access_mode,
            ),
            (
                "mongodb_compatible_data_access_mode",
                &payload.mongodb_compatible_data_access_mode,
            ),
            ("realtime_updates_mode", &payload.realtime_updates_mode),
        ] {
            if val.is_some() {
                return Err(RestError::not_implemented(format!(
                    "Database.{name} is not supported yet"
                )));
            }
        }
    }

    let db = project.database(&name).await;

    let apply_edition = update_mask.as_ref().is_none_or(|m| m.database_edition);
    let apply_concurrency = update_mask.as_ref().is_none_or(|m| m.concurrency_mode);

    let effective_edition = if apply_edition {
        payload.database_edition
    } else {
        None
    };
    let effective_concurrency = if apply_concurrency {
        payload.concurrency_mode
    } else {
        None
    };

    let prev_enterprise = db.enterprise_edition();
    let target_enterprise = effective_edition
        .map(|edition| matches!(edition, DatabaseEdition::Enterprise))
        .unwrap_or(prev_enterprise);
    let target_optimistic = if let Some(mode) = effective_concurrency {
        matches!(mode, ConcurrencyMode::Optimistic)
    } else if target_enterprise != prev_enterprise {
        // Derive optimistic concurrency mode automatically when edition changes
        target_enterprise
    } else {
        db.optimistic_concurrency()
    };

    db.set_config(target_enterprise, target_optimistic);

    Ok(Json(DatabaseMetadata::new(
        &name,
        db.enterprise_edition(),
        db.optimistic_concurrency(),
    )))
}

async fn dispatch_documents_root(
    State(project): State<&'static FirestoreProject>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    dispatch_documents(
        project,
        &project_id,
        &database_id,
        None,
        None,
        method,
        &uri,
        &headers,
        &body,
    )
    .await
}

async fn dispatch_root_list_collection_ids(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    dispatch_documents(
        project,
        &project_id,
        &database_id,
        None,
        Some("listCollectionIds"),
        Method::POST,
        &uri,
        &headers,
        &body,
    )
    .await
}

async fn dispatch_root_run_query(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    dispatch_documents(
        project,
        &project_id,
        &database_id,
        None,
        Some("runQuery"),
        Method::POST,
        &uri,
        &headers,
        &body,
    )
    .await
}

async fn dispatch_documents_tail(
    State(project): State<&'static FirestoreProject>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (project_id, database_id) = extract_raw_project_and_database(uri.path())?;
    parse_root_ref(&project_id, &database_id)?;
    let raw_tail = extract_raw_tail(uri.path())?;
    let (clean_raw_tail, verb) = split_raw_custom_verb(raw_tail, &method);
    let tail = decode_raw_path_segments(clean_raw_tail)?;
    let subpath = (!tail.is_empty()).then_some(tail.as_str());
    dispatch_documents(
        project,
        &project_id,
        &database_id,
        subpath,
        verb,
        method,
        &uri,
        &headers,
        &body,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_documents(
    project: &'static FirestoreProject,
    project_id: &str,
    database_id: &str,
    subpath: Option<&str>,
    verb: Option<&str>,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let root = parse_root_ref(project_id, database_id)?;
    let full_path = match subpath {
        Some(tail) => format!("{root}/{tail}"),
        None => root.to_string(),
    };
    let parsed_ref: Ref = full_path.parse()?;
    let params = DocumentQueryParams::parse(uri.query())?;
    let db = project.database(parsed_ref.root()).await;

    match (method, parsed_ref, verb) {
        (Method::POST, parent @ (Ref::Root(_) | Ref::Document(_)), Some("listCollectionIds")) => {
            handle_list_collection_ids(&db, parent, headers, body).await
        }
        (Method::POST, parent @ (Ref::Root(_) | Ref::Document(_)), Some("runQuery")) => {
            handle_run_query(&db, parent, headers, body).await
        }
        (Method::POST, Ref::Root(_) | Ref::Document(_), Some(v)) if PHASE2_VERBS.contains(&v) => {
            Err(RestError::not_implemented(format!(
                "Firestore REST custom verb :{v} is not implemented yet"
            )))
        }
        (Method::GET, Ref::Collection(col_ref), None) => {
            handle_list_documents(&db, col_ref, params).await
        }
        (Method::POST, Ref::Collection(col_ref), None) => {
            handle_create_document(&db, col_ref, params, headers, body).await
        }
        (Method::GET, Ref::Document(doc_ref), None) => {
            handle_get_document(&db, doc_ref, params).await
        }
        (Method::PATCH, Ref::Document(doc_ref), None) => {
            handle_update_document(&db, doc_ref, params, headers, body).await
        }
        (Method::DELETE, Ref::Document(doc_ref), None) => {
            handle_delete_document(&db, doc_ref, params).await
        }
        (method, target_ref, verb) => {
            let suffix = verb.map(|v| format!(":{v}")).unwrap_or_default();
            Err(RestError::not_found_msg(format!(
                "Unsupported Firestore REST operation: {method} {target_ref}{suffix}"
            )))
        }
    }
}

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err)]
async fn handle_get_document(
    db: &Arc<FirestoreDatabase>,
    doc_ref: DocumentRef,
    params: DocumentQueryParams,
) -> Result<Response> {
    let consistency = params.read_consistency()?;
    let projection = params.mask().map(Projection::try_from).transpose()?;
    let doc = db
        .get_doc(&doc_ref, consistency)
        .await?
        .ok_or_else(|| RestError::not_found_msg(format!("Document not found: {doc_ref}")))?;
    Ok(Json(projection.project(&doc)).into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(collection = %col_ref), err)]
async fn handle_list_documents(
    db: &Arc<FirestoreDatabase>,
    col_ref: CollectionRef,
    params: DocumentQueryParams,
) -> Result<Response> {
    let consistency = params.read_consistency()?;
    let parent = col_ref.parent();
    let leaf_collection_id = col_ref
        .collection_id
        .rsplit('/')
        .next()
        .unwrap_or(&col_ref.collection_id)
        .to_string();
    let mask = params.mask();
    let (documents, next_page_token) = db
        .list_docs(
            parent,
            leaf_collection_id,
            params.page_size.unwrap_or(0),
            params.page_token.unwrap_or_default(),
            params.order_by.unwrap_or_default(),
            mask,
            params.show_missing.unwrap_or(false),
            consistency,
        )
        .await?;
    Ok(Json(ListDocumentsResponse {
        documents,
        next_page_token,
    })
    .into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(collection = %col_ref), err)]
async fn handle_create_document(
    db: &Arc<FirestoreDatabase>,
    col_ref: CollectionRef,
    params: DocumentQueryParams,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let doc: Document = parse_json_body(headers, body)?;
    if !doc.name.is_empty() {
        return Err(RestError::bad_request(
            "document.name must not be set on CreateDocument",
        ));
    }
    let mask = params.mask();
    let doc_id = params.document_id.as_deref().unwrap_or("");
    if doc_id.contains('/') {
        return Err(RestError::bad_request("documentId cannot contain '/'"));
    }
    let created = db.create_doc(&col_ref, doc_id, doc.fields, mask).await?;
    Ok(Json(created).into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err)]
async fn handle_update_document(
    db: &Arc<FirestoreDatabase>,
    doc_ref: DocumentRef,
    params: DocumentQueryParams,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let doc: Document = parse_json_body(headers, body)?;
    if !doc.name.is_empty() && doc.name != doc_ref.to_string() {
        return Err(RestError::bad_request(
            "document.name does not match target URL path",
        ));
    }
    let current_document = params.precondition()?;
    let update_mask = params.update_mask();
    let mask = params.mask();
    let updated = db
        .update_doc(&doc_ref, doc.fields, update_mask, current_document, mask)
        .await?;
    Ok(Json(updated).into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err)]
async fn handle_delete_document(
    db: &Arc<FirestoreDatabase>,
    doc_ref: DocumentRef,
    params: DocumentQueryParams,
) -> Result<Response> {
    let current_document = params.precondition()?;
    db.delete_doc(&doc_ref, current_document).await?;
    Ok(Json(json!({})).into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(parent = %parent), err)]
async fn handle_list_collection_ids(
    db: &Arc<FirestoreDatabase>,
    parent: Ref,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let req: ListCollectionIdsRequest = parse_json_body(headers, body)?;
    if req.consistency_selector.is_some() {
        return Err(RestError::not_implemented(
            "ListCollectionIds with readTime over REST is not implemented yet",
        ));
    }
    if req.page_size < 0 {
        return Err(RestError::bad_request("pageSize must be non-negative"));
    }
    let mut collection_ids: Vec<String> =
        db.get_collection_ids(&parent).await?.into_iter().collect();
    if !req.page_token.is_empty() {
        collection_ids.retain(|id| id.as_str() > req.page_token.as_str());
    }
    let next_page_token = if req.page_size > 0 && collection_ids.len() > req.page_size as usize {
        collection_ids.truncate(req.page_size as usize);
        collection_ids.last().cloned().unwrap_or_default()
    } else {
        String::new()
    };
    Ok(Json(ListCollectionIdsResponse {
        collection_ids,
        next_page_token,
    })
    .into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(parent = %parent), err)]
async fn handle_run_query(
    db: &Arc<FirestoreDatabase>,
    parent: Ref,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let raw: serde_json::Value = parse_json_body(headers, body)?;
    if let Some(sq) = raw
        .get("structuredQuery")
        .or_else(|| raw.get("structured_query"))
        && let Some(fn_val) = sq.get("findNearest").or_else(|| sq.get("find_nearest"))
        && !fn_val.is_null()
    {
        if fn_val.is_object() {
            return Err(RestError::not_implemented(
                "StructuredQuery.findNearest over REST is not implemented",
            ));
        }
        return Err(RestError::bad_request(
            "invalid findNearest in StructuredQuery",
        ));
    }
    let req: RunQueryRequest =
        serde_json::from_value(raw).map_err(|e| RestError::bad_request(e.to_string()))?;
    let Some(run_query_request::QueryType::StructuredQuery(structured_query)) = req.query_type
    else {
        return Err(RestError::bad_request(
            "missing structuredQuery in RunQueryRequest",
        ));
    };
    let read_consistency = match req.consistency_selector {
        None => ReadConsistency::Default,
        Some(run_query_request::ConsistencySelector::ReadTime(ts)) => ReadConsistency::ReadTime(ts),
        Some(_) => {
            return Err(RestError::not_implemented(
                "transactional runQuery over REST is not implemented yet",
            ));
        }
    };
    let mut query = Query::from_structured(
        parent,
        structured_query,
        read_consistency,
        db.enterprise_edition(),
    )?;
    let (read_time, docs) = db.run_query(&mut query).await?;
    let responses: Vec<RunQueryResponse> = if docs.is_empty() {
        vec![RunQueryResponse {
            read_time: Some(read_time),
            ..Default::default()
        }]
    } else {
        docs.into_iter()
            .map(|doc| RunQueryResponse {
                document: Some(doc),
                read_time: Some(read_time),
                ..Default::default()
            })
            .collect()
    };
    Ok(Json(responses).into_response())
}

async fn unmatched_projects_route(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if method == Method::POST {
        match parse_root_phase2_verb_path(path) {
            Ok(Some((project_id, database_id, verb))) => {
                if let Err(err) = parse_root_ref(&project_id, &database_id) {
                    return err.into_response();
                }
                return RestError::not_implemented(format!(
                    "Firestore REST custom verb :{verb} is not implemented yet"
                ))
                .into_response();
            }
            Ok(None) => {}
            Err(err) => return err.into_response(),
        }
    }
    RestError::not_found_msg(format!(
        "Unknown projects endpoint: {method} /v1/projects{path}"
    ))
    .into_response()
}

fn parse_root_ref(project_id: &str, database_id: &str) -> Result<RootRef> {
    if project_id.is_empty()
        || project_id.contains('/')
        || database_id.is_empty()
        || database_id.contains('/')
    {
        return Err(RestError::bad_request("Invalid project_id or database_id"));
    }
    Ok(format!("projects/{project_id}/databases/{database_id}").parse()?)
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

fn parse_root_phase2_verb_path(raw_path: &str) -> Result<Option<(String, String, &'static str)>> {
    let mut parts = raw_path.strip_prefix('/').unwrap_or(raw_path).split('/');
    let (Some(raw_proj), Some("databases"), Some(raw_db), Some(last), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Ok(None);
    };
    let Some(raw_verb) = last.strip_prefix("documents:") else {
        return Ok(None);
    };
    let Some(&verb) = PHASE2_VERBS.iter().find(|&&v| v == raw_verb) else {
        return Ok(None);
    };
    let project_id = percent_decode_path_segment(raw_proj)?;
    let database_id = percent_decode_path_segment(raw_db)?;
    Ok(Some((project_id, database_id, verb)))
}

pub(super) fn decode_raw_path_segments(raw_path: &str) -> Result<String> {
    let mut decoded_segments = Vec::new();
    for raw_seg in raw_path.split('/') {
        let seg = percent_decode_path_segment(raw_seg)?;
        if seg.contains('/') {
            return Err(RestError::bad_request("Path segment cannot contain '/'"));
        }
        decoded_segments.push(seg);
    }
    Ok(decoded_segments.join("/"))
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

#[derive(Debug, Default)]
struct PatchDatabaseMask {
    database_edition: bool,
    concurrency_mode: bool,
}

fn parse_patch_database_mask(raw_query: Option<&str>) -> Result<Option<PatchDatabaseMask>> {
    let Some(query) = raw_query else {
        return Ok(None);
    };
    let mut mask = PatchDatabaseMask::default();
    let mut saw_mask = false;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
        let key = percent_decode(raw_k)?;
        let val = percent_decode(raw_v)?;
        if matches!(
            key.as_str(),
            "updateMask" | "update_mask" | "updateMask.paths" | "update_mask.paths"
        ) {
            saw_mask = true;
            for raw_field in val.split(',') {
                let field = raw_field.trim();
                match field {
                    "" => {
                        return Err(RestError::bad_request(
                            "updateMask cannot contain empty field paths",
                        ));
                    }
                    "databaseEdition" | "database_edition" => mask.database_edition = true,
                    "concurrencyMode" | "concurrency_mode" => mask.concurrency_mode = true,
                    "firestoreDataAccessMode"
                    | "firestore_data_access_mode"
                    | "mongodbCompatibleDataAccessMode"
                    | "mongodb_compatible_data_access_mode"
                    | "realtimeUpdatesMode"
                    | "realtime_updates_mode" => {
                        return Err(RestError::not_implemented(format!(
                            "Database.{field} is not supported yet"
                        )));
                    }
                    other => {
                        return Err(RestError::bad_request(format!(
                            "Unsupported updateMask field for Database: {other}"
                        )));
                    }
                }
            }
        }
    }
    Ok(saw_mask.then_some(mask))
}

#[derive(Debug, Default)]
struct DocumentQueryParams {
    document_id: Option<String>,
    page_size: Option<i32>,
    page_token: Option<String>,
    order_by: Option<String>,
    show_missing: Option<bool>,
    mask: Vec<String>,
    has_mask_param: bool,
    update_mask: Vec<String>,
    has_update_mask_param: bool,
    current_document_exists: Option<bool>,
    current_document_update_time: Option<Timestamp>,
    read_time: Option<Timestamp>,
    transaction: Option<String>,
}

impl DocumentQueryParams {
    fn parse(raw_query: Option<&str>) -> Result<Self> {
        let Some(query) = raw_query else {
            return Ok(Self::default());
        };
        let mut params = Self::default();
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_k, raw_v) = pair.split_once('=').unwrap_or((pair, ""));
            let key = percent_decode(raw_k)?;
            let val = percent_decode(raw_v)?;

            match key.as_str() {
                "documentId" | "document_id" => params.document_id = Some(val),
                "pageSize" | "page_size" => {
                    let parsed = val.parse::<i32>().map_err(|_| {
                        RestError::bad_request(format!("Invalid pageSize value: {val}"))
                    })?;
                    params.page_size = Some(parsed);
                }
                "pageToken" | "page_token" => params.page_token = Some(val),
                "orderBy" | "order_by" => params.order_by = Some(val),
                "showMissing" | "show_missing" => {
                    params.show_missing = Some(parse_bool_param("showMissing", &val)?);
                }
                "mask.fieldPaths" | "mask.field_paths" => {
                    params.has_mask_param = true;
                    params.mask.push(val);
                }
                "updateMask.fieldPaths"
                | "updateMask.field_paths"
                | "update_mask.fieldPaths"
                | "update_mask.field_paths" => {
                    params.has_update_mask_param = true;
                    params.update_mask.push(val);
                }
                "currentDocument.exists" | "current_document.exists" => {
                    params.current_document_exists =
                        Some(parse_bool_param("currentDocument.exists", &val)?);
                }
                "currentDocument.updateTime"
                | "currentDocument.update_time"
                | "current_document.updateTime"
                | "current_document.update_time" => {
                    let ts: Timestamp = serde_json::from_value(serde_json::Value::String(val))
                        .map_err(|err| {
                            RestError::bad_request(format!(
                                "Invalid currentDocument.updateTime: {err}"
                            ))
                        })?;
                    params.current_document_update_time = Some(ts);
                }
                "readTime" | "read_time" => {
                    let ts: Timestamp = serde_json::from_value(serde_json::Value::String(val))
                        .map_err(|err| {
                            RestError::bad_request(format!("Invalid readTime: {err}"))
                        })?;
                    params.read_time = Some(ts);
                }
                "transaction" => {
                    if val.is_empty() {
                        return Err(RestError::bad_request("transaction cannot be empty"));
                    }
                    params.transaction = Some(val);
                }
                "requestOptions.requestTags"
                | "requestOptions.request_tags"
                | "request_options.requestTags"
                | "request_options.request_tags" => {
                    return Err(RestError::not_implemented(
                        "request_options is not supported yet",
                    ));
                }
                _ => {}
            }
        }
        Ok(params)
    }

    fn mask(&self) -> Option<DocumentMask> {
        self.has_mask_param.then(|| DocumentMask {
            field_paths: if self.mask.iter().all(String::is_empty) {
                Vec::new()
            } else {
                self.mask.clone()
            },
        })
    }

    fn update_mask(&self) -> Option<DocumentMask> {
        self.has_update_mask_param.then(|| DocumentMask {
            field_paths: if self.update_mask.iter().all(String::is_empty) {
                Vec::new()
            } else {
                self.update_mask.clone()
            },
        })
    }

    fn read_consistency(&self) -> Result<ReadConsistency> {
        match (self.read_time, self.transaction.as_deref()) {
            (Some(_), Some(_)) => Err(RestError::bad_request(
                "Cannot specify both readTime and transaction",
            )),
            (Some(ts), None) => Ok(ReadConsistency::ReadTime(ts)),
            (None, Some(_)) => Err(RestError::not_implemented(
                "transactional reads over REST are not implemented yet",
            )),
            (None, None) => Ok(ReadConsistency::Default),
        }
    }

    fn precondition(&self) -> Result<Option<Precondition>> {
        match (
            self.current_document_exists,
            self.current_document_update_time,
        ) {
            (Some(_), Some(_)) => Err(RestError::bad_request(
                "currentDocument cannot specify both exists and updateTime",
            )),
            (Some(exists), None) => Ok(Some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(exists)),
            })),
            (None, Some(ts)) => Ok(Some(Precondition {
                condition_type: Some(precondition::ConditionType::UpdateTime(ts)),
            })),
            (None, None) => Ok(None),
        }
    }
}

fn parse_bool_param(name: &str, val: &str) -> Result<bool> {
    match val {
        "true" | "1" => Ok(true),
        "false" | "0" => Ok(false),
        _ => Err(RestError::bad_request(format!(
            "Invalid boolean query parameter {name}: {val}"
        ))),
    }
}

fn percent_decode(input: &str) -> Result<String> {
    percent_decode_inner(input, true)
}

fn percent_decode_path_segment(input: &str) -> Result<String> {
    percent_decode_inner(input, false)
}

fn percent_decode_inner(input: &str, plus_as_space: bool) -> Result<String> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(RestError::bad_request(format!(
                        "Incomplete percent-encoding in string: {input}"
                    )));
                }
                let hi = hex_val(bytes[i + 1]).ok_or_else(|| {
                    RestError::bad_request(format!("Invalid percent-encoding in: {input}"))
                })?;
                let lo = hex_val(bytes[i + 2]).ok_or_else(|| {
                    RestError::bad_request(format!("Invalid percent-encoding in: {input}"))
                })?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out)
        .map_err(|_| RestError::bad_request("URL component contains invalid UTF-8"))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
