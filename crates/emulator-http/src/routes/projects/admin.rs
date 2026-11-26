use axum::{Json, extract::State, http::Uri};
use emulator_database::{
    DatabaseConcurrencyMode, DatabaseEdition, DatabaseSettings, FirestoreDatabase,
    FirestoreProject, reference::RootRef,
};
use serde::{Deserialize, Serialize};

use super::{parse_query_pairs, parse_raw_root_ref};
use crate::error::{ApiJson, RestError, Result};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct DatabaseMetadata {
    name: String,
    uid: &'static str,
    r#type: &'static str,
    database_edition: DatabaseEdition,
    concurrency_mode: DatabaseConcurrencyMode,
}

impl DatabaseMetadata {
    fn new(name: &RootRef, settings: DatabaseSettings) -> Self {
        Self {
            name: format!(
                "projects/{}/databases/{}",
                name.project_id, name.database_id
            ),
            uid: "emulator-uid",
            r#type: "FIRESTORE_NATIVE",
            database_edition: settings.edition,
            concurrency_mode: settings.concurrency_mode,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct PatchDatabase {
    #[serde(alias = "database_edition")]
    database_edition: Option<DatabaseEdition>,
    #[serde(alias = "concurrency_mode")]
    concurrency_mode: Option<DatabaseConcurrencyMode>,
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
    Ok(Json(DatabaseMetadata::new(&name, db.settings())))
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
    Ok(Json(apply_database_patch(
        &db,
        &name,
        &payload,
        update_mask.as_ref(),
    )?))
}

fn apply_database_patch(
    db: &FirestoreDatabase,
    name: &RootRef,
    payload: &PatchDatabase,
    update_mask: Option<&PatchDatabaseMask>,
) -> Result<DatabaseMetadata> {
    let apply_edition = update_mask.is_none_or(|m| m.database_edition);
    let apply_concurrency = update_mask.is_none_or(|m| m.concurrency_mode);

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

    let settings = db.try_update_settings(|settings| {
        if let Some(edition) = effective_edition
            && edition != settings.edition
        {
            settings.edition = edition;
            if effective_concurrency.is_none() {
                // Derive optimistic concurrency mode automatically when edition changes
                settings.concurrency_mode = DatabaseSettings::for_edition(edition).concurrency_mode;
            }
        }
        if let Some(mode) = effective_concurrency {
            settings.concurrency_mode = mode;
        }
    })?;

    Ok(DatabaseMetadata::new(name, settings))
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
    for (key, val) in parse_query_pairs(query)? {
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

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
    use emulator_database::{config::FirestoreConfig, snapshot::SnapshotError};
    use tokio_stream::StreamExt;

    use super::*;

    async fn assert_409_aborted(err: RestError) {
        let resp = err.into_response();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"]["code"], 409);
        assert_eq!(json["error"]["status"], "ABORTED");
    }

    #[tokio::test]
    async fn apply_database_patch_returns_409_aborted_when_cleared_or_replaced() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let name: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let payload = PatchDatabase {
            database_edition: Some(DatabaseEdition::Enterprise),
            concurrency_mode: None,
            firestore_data_access_mode: None,
            mongodb_compatible_data_access_mode: None,
            realtime_updates_mode: None,
        };

        // 1. Cleared database handle returns 409 Conflict (ABORTED).
        let cleared_db = project.database(&name).await;
        project.clear_database(&name).await;
        let err = apply_database_patch(&cleared_db, &name, &payload, None)
            .err()
            .expect("patching a cleared database handle must abort");
        assert_409_aborted(err).await;

        // 2. Database handle replaced by snapshot import returns 409 Conflict (ABORTED).
        let replaced_db = project.database(&name).await;
        let snap_stream = project
            .prepare_export(None)
            .await
            .unwrap()
            .into_stream()
            .map(|res| res.map_err(|err| SnapshotError::Stream(err.to_string())));
        project.import_snapshot(snap_stream).await.unwrap();
        let err = apply_database_patch(&replaced_db, &name, &payload, None)
            .err()
            .expect("patching a database handle replaced by import must abort");
        assert_409_aborted(err).await;
    }
}
