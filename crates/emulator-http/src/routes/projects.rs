use std::sync::atomic;

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
};
use emulator_database::{FirestoreProject, reference::RootRef};
use serde::{Deserialize, Serialize};

use crate::error::{RestError, Result};

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new().route(
        "/{project_id}/databases/{database_id}",
        axum::routing::get(get_database).patch(patch_database),
    )
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DatabaseMetadata {
    name: String,
    uid: &'static str,
    r#type: &'static str,
    database_edition: &'static str,
    concurrency_mode: &'static str,
}

impl DatabaseMetadata {
    fn new(name: &RootRef, enterprise_edition: bool) -> Self {
        Self {
            name: name.to_string(),
            uid: "emulator-uid",
            r#type: "FIRESTORE_NATIVE",
            database_edition: if enterprise_edition {
                "ENTERPRISE"
            } else {
                "STANDARD"
            },
            concurrency_mode: if enterprise_edition {
                "OPTIMISTIC"
            } else {
                "PESSIMISTIC"
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchDatabase {
    database_edition: Option<String>,
}

#[derive(Deserialize)]
struct PatchParams {
    update_mask: Option<String>,
}

async fn get_database(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
) -> Result<Json<DatabaseMetadata>> {
    let name: RootRef = format!("projects/{}/databases/{}", project_id, database_id)
        .parse()
        .map_err(|_| {
            RestError::new(StatusCode::BAD_REQUEST, "invalid database name".to_string())
        })?;

    let db = project.database(&name).await;
    let is_enterprise = db.enterprise_edition.load(atomic::Ordering::Relaxed);

    Ok(Json(DatabaseMetadata::new(&name, is_enterprise)))
}

async fn patch_database(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
    Query(params): Query<PatchParams>,
    Json(payload): Json<PatchDatabase>,
) -> Result<Json<DatabaseMetadata>> {
    let name = RootRef::new(project_id, database_id);

    let db = project.database(&name).await;

    if let Some(edition) = payload.database_edition {
        let should_update = params
            .update_mask
            .as_deref()
            .is_none_or(|mask| mask.split(',').any(|s| s == "databaseEdition"));

        if should_update {
            let is_enterprise = match edition.as_str() {
                "ENTERPRISE" => true,
                "STANDARD" => false,
                _ => {
                    return Err(RestError::new(
                        StatusCode::BAD_REQUEST,
                        "invalid database edition".to_string(),
                    ));
                }
            };
            db.enterprise_edition
                .store(is_enterprise, atomic::Ordering::Relaxed);
        }
    }

    let is_enterprise = db.enterprise_edition.load(atomic::Ordering::Relaxed);
    Ok(Json(DatabaseMetadata::new(&name, is_enterprise)))
}
