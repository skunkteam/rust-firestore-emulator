use axum::{
    Json, Router,
    extract::{Path, State},
};
use emulator_database::{FirestoreProject, reference::RootRef};
use serde::{Deserialize, Serialize};

use crate::error::Result;

pub(crate) fn router() -> Router<&'static FirestoreProject> {
    Router::new().route(
        "/{project_id}/databases/{database_id}",
        axum::routing::get(get_database).patch(patch_database),
    )
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum DatabaseEdition {
    Standard,
    Enterprise,
}

#[derive(Serialize, Deserialize)]
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PatchDatabase {
    database_edition: Option<DatabaseEdition>,
    concurrency_mode: Option<ConcurrencyMode>,
}

async fn get_database(
    State(project): State<&'static FirestoreProject>,
    Path((project_id, database_id)): Path<(String, String)>,
) -> Result<Json<DatabaseMetadata>> {
    let name = RootRef::new(project_id, database_id);

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
    Json(payload): Json<PatchDatabase>,
) -> Result<Json<DatabaseMetadata>> {
    let name = RootRef::new(project_id, database_id);

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
