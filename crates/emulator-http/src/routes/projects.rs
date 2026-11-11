use std::sync::Arc;

use axum::{
    Json, Router,
    body::Bytes,
    extract::{Path, State},
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
    database_edition: Option<DatabaseEdition>,
    concurrency_mode: Option<ConcurrencyMode>,
}

async fn get_database(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
) -> Result<Json<DatabaseMetadata>> {
    let name: RootRef = format!("projects/{project_id}/databases/{database_id}").parse()?;

    let db = project.database(&name).await;
    Ok(Json(DatabaseMetadata::new(
        &name,
        db.enterprise_edition(),
        db.optimistic_concurrency(),
    )))
}

async fn patch_database(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
    ApiJson(payload): ApiJson<PatchDatabase>,
) -> Result<Json<DatabaseMetadata>> {
    let name: RootRef = format!("projects/{project_id}/databases/{database_id}").parse()?;

    let db = project.database(&name).await;

    if let Some(edition) = &payload.database_edition {
        let is_enterprise = matches!(edition, DatabaseEdition::Enterprise);
        db.set_enterprise_edition(is_enterprise);
        // Derive optimistic concurrency mode automatically from enterprise edition by default
        db.set_optimistic_concurrency(is_enterprise);
    }

    if let Some(mode) = &payload.concurrency_mode {
        db.set_optimistic_concurrency(matches!(mode, ConcurrencyMode::Optimistic));
    }

    Ok(Json(DatabaseMetadata::new(
        &name,
        db.enterprise_edition(),
        db.optimistic_concurrency(),
    )))
}

async fn dispatch_documents_root(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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
    Path((project_id, database_id)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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
    Path((project_id, database_id)): Path<(String, String)>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
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
    Path((project_id, database_id, tail)): Path<(String, String, String)>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response> {
    let (clean_tail, verb) = split_custom_verb(&tail, &method);
    let subpath = (!clean_tail.is_empty()).then_some(clean_tail);
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
    if let Some(v) = verb
        && PHASE2_VERBS.contains(&v)
    {
        return Err(RestError::not_implemented(format!(
            "Firestore REST custom verb :{v} is not implemented yet"
        )));
    }

    let full_path = match subpath {
        Some(tail) => format!("projects/{project_id}/databases/{database_id}/documents/{tail}"),
        None => format!("projects/{project_id}/databases/{database_id}/documents"),
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
    let projection = params.mask().map(Projection::try_from).transpose()?;
    let doc = db
        .get_doc(&doc_ref, ReadConsistency::Default)
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
            ReadConsistency::Default,
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
    let req: RunQueryRequest = parse_json_body(headers, body)?;
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
        for verb in PHASE2_VERBS {
            if path.ends_with(&format!(":{verb}")) {
                return RestError::not_implemented(format!(
                    "Firestore REST custom verb :{verb} is not implemented yet"
                ))
                .into_response();
            }
        }
    }
    RestError::not_found_msg(format!(
        "Unknown projects endpoint: {method} /v1/projects{path}"
    ))
    .into_response()
}

fn split_custom_verb<'a>(tail: &'a str, method: &Method) -> (&'a str, Option<&'static str>) {
    if *method == Method::POST {
        for &verb in &["listCollectionIds", "runQuery"] {
            if let Some(prefix) = tail.strip_suffix(verb).and_then(|s| s.strip_suffix(':')) {
                return (prefix, Some(verb));
            }
        }
        for &verb in PHASE2_VERBS {
            if let Some(prefix) = tail.strip_suffix(verb).and_then(|s| s.strip_suffix(':')) {
                return (prefix, Some(verb));
            }
        }
    }
    (tail, None)
}

#[derive(Debug, Default)]
struct DocumentQueryParams {
    document_id: Option<String>,
    page_size: Option<i32>,
    page_token: Option<String>,
    order_by: Option<String>,
    show_missing: Option<bool>,
    mask: Vec<String>,
    update_mask: Vec<String>,
    has_update_mask_param: bool,
    current_document_exists: Option<bool>,
    current_document_update_time: Option<Timestamp>,
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
                "documentId" => params.document_id = Some(val),
                "pageSize" => {
                    let parsed = val.parse::<i32>().map_err(|_| {
                        RestError::bad_request(format!("Invalid pageSize value: {val}"))
                    })?;
                    params.page_size = Some(parsed);
                }
                "pageToken" => params.page_token = Some(val),
                "orderBy" => params.order_by = Some(val),
                "showMissing" => {
                    params.show_missing = Some(parse_bool_param("showMissing", &val)?);
                }
                "mask.fieldPaths" => {
                    if !val.is_empty() {
                        params.mask.push(val);
                    }
                }
                "updateMask.fieldPaths" => {
                    params.has_update_mask_param = true;
                    if !val.is_empty() {
                        params.update_mask.push(val);
                    }
                }
                "currentDocument.exists" => {
                    params.current_document_exists =
                        Some(parse_bool_param("currentDocument.exists", &val)?);
                }
                "currentDocument.updateTime" => {
                    let ts: Timestamp = serde_json::from_value(serde_json::Value::String(val))
                        .map_err(|err| {
                            RestError::bad_request(format!(
                                "Invalid currentDocument.updateTime: {err}"
                            ))
                        })?;
                    params.current_document_update_time = Some(ts);
                }
                _ => {}
            }
        }
        Ok(params)
    }

    fn mask(&self) -> Option<DocumentMask> {
        (!self.mask.is_empty()).then(|| DocumentMask {
            field_paths: self.mask.clone(),
        })
    }

    fn update_mask(&self) -> Option<DocumentMask> {
        self.has_update_mask_param.then(|| DocumentMask {
            field_paths: self.update_mask.clone(),
        })
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
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err(RestError::bad_request(format!(
                        "Incomplete percent-encoding in query string: {input}"
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
        .map_err(|_| RestError::bad_request("Query string contains invalid UTF-8"))
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
