use std::sync::Arc;

use axum::{
    Json,
    http::{HeaderMap, Method, Uri},
    response::{IntoResponse, Response},
};
use emulator_database::{
    FirestoreDatabase, FirestoreProject,
    projection::{Project, Projection},
    query::Query,
    read_consistency::ReadConsistency,
    reference::{CollectionRef, DocumentRef, Ref},
};
use googleapis::google::firestore::v1::{
    AggregationResult, Document, ListCollectionIdsRequest, ListCollectionIdsResponse,
    ListDocumentsResponse, RunAggregationQueryRequest, RunAggregationQueryResponse,
    RunQueryRequest, RunQueryResponse, run_aggregation_query_request, run_query_request,
    structured_aggregation_query,
};
use serde_json::json;
use tracing::{Level, Span, instrument};

use super::query_params::{DocumentQueryParams, percent_decode_path_segment};
use crate::error::{RestError, Result, parse_json_body};

const PHASE2_VERBS: &[&str] = &[
    "commit",
    "batchGet",
    "batchWrite",
    "beginTransaction",
    "rollback",
    "executePipeline",
    "partitionQuery",
];

pub(super) async fn dispatch_documents(
    project: &'static FirestoreProject,
    parsed_ref: Ref,
    verb: Option<&str>,
    method: Method,
    uri: &Uri,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let params = DocumentQueryParams::parse(uri.query())?;
    let db = project.database(parsed_ref.root()).await;

    match (method, parsed_ref, verb) {
        (Method::POST, parent @ (Ref::Root(_) | Ref::Document(_)), Some("listCollectionIds")) => {
            handle_list_collection_ids(&db, parent, headers, body).await
        }
        (Method::POST, parent @ (Ref::Root(_) | Ref::Document(_)), Some("runQuery")) => {
            handle_run_query(&db, parent, headers, body).await
        }
        (Method::POST, parent @ (Ref::Root(_) | Ref::Document(_)), Some("runAggregationQuery")) => {
            handle_run_aggregation_query(&db, parent, headers, body).await
        }
        (Method::POST, Ref::Document(_), Some(v)) if PHASE2_VERBS.contains(&v) => {
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

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err(level = Level::DEBUG))]
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

#[instrument(level = Level::DEBUG, skip_all, fields(collection = %col_ref), err(level = Level::DEBUG))]
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

#[instrument(level = Level::DEBUG, skip_all, fields(collection = %col_ref), err(level = Level::DEBUG))]
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

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err(level = Level::DEBUG))]
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

#[instrument(level = Level::DEBUG, skip_all, fields(doc = %doc_ref), err(level = Level::DEBUG))]
async fn handle_delete_document(
    db: &Arc<FirestoreDatabase>,
    doc_ref: DocumentRef,
    params: DocumentQueryParams,
) -> Result<Response> {
    let current_document = params.precondition()?;
    db.delete_doc(&doc_ref, current_document).await?;
    Ok(Json(json!({})).into_response())
}

#[instrument(level = Level::DEBUG, skip_all, fields(parent = %parent), err(level = Level::DEBUG))]
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
    let (collection_ids, next_page_token) = db
        .list_collection_ids(&parent, req.page_size, &req.page_token)
        .await?;
    Ok(Json(ListCollectionIdsResponse {
        collection_ids,
        next_page_token,
    })
    .into_response())
}

fn validate_request_parent(req_parent: &str, url_parent: &Ref) -> Result<()> {
    if !req_parent.is_empty() && req_parent != url_parent.to_string() {
        return Err(RestError::bad_request(
            "request.parent does not match target URL path",
        ));
    }
    Ok(())
}

fn ensure_supported_query_options(
    has_explain_options: bool,
    has_request_options: bool,
    has_find_nearest: bool,
) -> Result<()> {
    if has_explain_options {
        return Err(RestError::not_implemented(
            "explainOptions over REST is not implemented",
        ));
    }
    if has_request_options {
        return Err(RestError::not_implemented(
            "requestOptions over REST is not implemented",
        ));
    }
    if has_find_nearest {
        return Err(RestError::not_implemented(
            "StructuredQuery.findNearest over REST is not implemented",
        ));
    }
    Ok(())
}

#[instrument(level = Level::DEBUG, skip_all, fields(parent = %parent), err(level = Level::DEBUG))]
async fn handle_run_query(
    db: &Arc<FirestoreDatabase>,
    parent: Ref,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let req: RunQueryRequest = parse_json_body(headers, body)?;
    validate_request_parent(&req.parent, &parent)?;
    let Some(run_query_request::QueryType::StructuredQuery(structured_query)) = req.query_type
    else {
        return Err(RestError::bad_request(
            "missing structuredQuery in RunQueryRequest",
        ));
    };
    ensure_supported_query_options(
        req.explain_options.is_some(),
        req.request_options.is_some(),
        structured_query.find_nearest.is_some(),
    )?;
    let read_consistency = match req.consistency_selector {
        None => ReadConsistency::Default,
        Some(run_query_request::ConsistencySelector::ReadTime(ts)) => ReadConsistency::ReadTime(ts),
        Some(run_query_request::ConsistencySelector::NewTransaction(_)) => {
            return Err(RestError::not_implemented(
                "newTransaction over REST is not implemented",
            ));
        }
        Some(run_query_request::ConsistencySelector::Transaction(_)) => {
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

#[instrument(
    level = Level::DEBUG,
    skip_all,
    fields(parent = %parent, agg_count = tracing::field::Empty),
    err(level = Level::DEBUG)
)]
async fn handle_run_aggregation_query(
    db: &Arc<FirestoreDatabase>,
    parent: Ref,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Response> {
    let req: RunAggregationQueryRequest = parse_json_body(headers, body)?;
    validate_request_parent(&req.parent, &parent)?;
    let Some(run_aggregation_query_request::QueryType::StructuredAggregationQuery(agg_query)) =
        req.query_type
    else {
        return Err(RestError::bad_request(
            "missing structuredAggregationQuery in RunAggregationQueryRequest",
        ));
    };
    let Some(structured_aggregation_query::QueryType::StructuredQuery(structured_query)) =
        agg_query.query_type
    else {
        return Err(RestError::bad_request(
            "missing structuredQuery in StructuredAggregationQuery",
        ));
    };
    ensure_supported_query_options(
        req.explain_options.is_some(),
        req.request_options.is_some(),
        structured_query.find_nearest.is_some(),
    )?;
    let read_consistency = match req.consistency_selector {
        None => ReadConsistency::Default,
        Some(run_aggregation_query_request::ConsistencySelector::ReadTime(ts)) => {
            ReadConsistency::ReadTime(ts)
        }
        Some(run_aggregation_query_request::ConsistencySelector::NewTransaction(_)) => {
            return Err(RestError::not_implemented(
                "newTransaction over REST is not implemented",
            ));
        }
        Some(run_aggregation_query_request::ConsistencySelector::Transaction(_)) => {
            return Err(RestError::not_implemented(
                "transactional runAggregationQuery over REST is not implemented yet",
            ));
        }
    };
    Span::current().record("agg_count", agg_query.aggregations.len());
    let (read_time, aggregate_fields) = db
        .run_aggregation_query(
            parent,
            structured_query,
            agg_query.aggregations,
            read_consistency,
        )
        .await?;
    let response = vec![RunAggregationQueryResponse {
        result: Some(AggregationResult { aggregate_fields }),
        transaction: vec![],
        read_time: Some(read_time),
        explain_metrics: None,
    }];
    Ok(Json(response).into_response())
}

pub(super) async fn unmatched_projects_route(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if method == Method::POST {
        match parse_root_phase2_verb_path(path) {
            Ok(Some((project_id, database_id, verb))) => {
                if let Err(err) = super::parse_root_ref(&project_id, &database_id) {
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
