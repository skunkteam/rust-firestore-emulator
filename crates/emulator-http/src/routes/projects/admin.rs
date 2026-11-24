use axum::{Json, extract::State, http::Uri};
use emulator_database::{FirestoreProject, reference::RootRef};
use serde::{Deserialize, Serialize};

use super::{parse_raw_root_ref, query_params::percent_decode};
use crate::error::{ApiJson, RestError, Result};

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
pub(super) struct DatabaseMetadata {
    name: String,
    uid: &'static str,
    r#type: &'static str,
    database_edition: DatabaseEdition,
    concurrency_mode: ConcurrencyMode,
}

impl DatabaseMetadata {
    fn new(name: &RootRef, enterprise_edition: bool, optimistic_concurrency: bool) -> Self {
        Self {
            name: format!(
                "projects/{}/databases/{}",
                name.project_id, name.database_id
            ),
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
pub(super) struct PatchDatabase {
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

pub(super) async fn get_database(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
) -> Result<Json<DatabaseMetadata>> {
    let name = parse_raw_root_ref(uri.path())?;

    let db = project.database(&name).await;
    Ok(Json(DatabaseMetadata::new(
        &name,
        db.enterprise_edition(),
        db.optimistic_concurrency(),
    )))
}

pub(super) async fn patch_database(
    State(project): State<&'static FirestoreProject>,
    uri: Uri,
    ApiJson(payload): ApiJson<PatchDatabase>,
) -> Result<Json<DatabaseMetadata>> {
    let name = parse_raw_root_ref(uri.path())?;
    let update_mask = parse_patch_database_mask(uri.query())?;

    if update_mask.is_none() {
        for (field_name, val) in [
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
                    "Database.{field_name} is not supported yet"
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
