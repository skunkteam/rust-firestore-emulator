use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use emulator_database::{FirestoreProject, read_consistency::ReadConsistency, reference::Ref};
use serde_json::json;

use crate::error::{RestError, Result};

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new()
        .route("/", get(list_databases))
        .route("/{*ref}", get(get_by_ref).delete(delete_by_ref))
}

async fn list_databases(State(project): State<&FirestoreProject>) -> impl IntoResponse {
    Json(project.database_names().await)
}

async fn get_by_ref(
    State(project): State<&'static FirestoreProject>,
    Path(r): Path<Ref>,
) -> Result<Response> {
    let database = project.database(r.root()).await;
    match r {
        r @ Ref::Root(_) => {
            let collections = database.get_collection_ids(&r).await?;
            Ok((Json(collections)).into_response())
        }
        Ref::Collection(r) => Ok(Json(json!({
            "type": "collection",
            "documents": database.get_document_ids(&r).await?,
        }))
        .into_response()),
        Ref::Document(r) => Ok(Json(json!({
            "type": "document",
            "document": database.get_doc(&r, ReadConsistency::Default).await?.map(|d| d.to_document()),
            "collections": database.get_collection_ids(&Ref::Document(r)).await?,
        }))
        .into_response()),
    }
}

async fn delete_by_ref(State(project): State<&FirestoreProject>, Path(r): Path<Ref>) -> Result<()> {
    match r {
        Ref::Root(r) => {
            project.clear_database(&r).await;
            Ok(())
        }
        Ref::Collection(_) => Err(RestError::new(
            StatusCode::NOT_IMPLEMENTED,
            "Deleting a collection not implemented yet!".to_string(),
        )),
        Ref::Document(_) => Err(RestError::new(
            StatusCode::NOT_IMPLEMENTED,
            "Deleting a document not implemented yet!".to_string(),
        )),
    }
}
