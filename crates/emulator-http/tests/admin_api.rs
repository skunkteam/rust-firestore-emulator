#![allow(unused_crate_dependencies)]

use std::time::Duration;

use axum::http::StatusCode;
use emulator_database::{FirestoreProject, config::FirestoreConfig};
use emulator_http::RouterBuilder;
use googletest::prelude::*;
use reqwest::Client;
use serde_json::json;

async fn spawn_server(
    default_enterprise: bool,
) -> Result<(&'static FirestoreProject, Client, String)> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
        default_enterprise,
        ..Default::default()
    })));
    let router = RouterBuilder::new(project).build();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let base_url = format!("http://127.0.0.1:{port}");
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    Ok((project, Client::builder().no_proxy().build()?, base_url))
}

#[gtest]
#[tokio::test]
async fn test_database_admin_api() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;
    let url = format!("{base_url}/v1/projects/my-project/databases/(default)");

    // 1. GET - Should default to STANDARD (because default_enterprise is false)
    let res = client.get(&url).send().await?;
    expect_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await?;
    expect_eq!(body["databaseEdition"], "STANDARD");
    expect_eq!(body["concurrencyMode"], "PESSIMISTIC");

    // 2. PATCH - Change to ENTERPRISE
    let res = client
        .patch(&url)
        .json(&json!({
            "databaseEdition": "ENTERPRISE"
        }))
        .send()
        .await?;
    expect_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await?;
    expect_eq!(body["databaseEdition"], "ENTERPRISE");
    expect_eq!(body["concurrencyMode"], "OPTIMISTIC");

    // 3. GET - Should now be ENTERPRISE and OPTIMISTIC
    let res = client.get(&url).send().await?;
    expect_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await?;
    expect_eq!(body["databaseEdition"], "ENTERPRISE");
    expect_eq!(body["concurrencyMode"], "OPTIMISTIC");

    // 4. PATCH - Change to PESSIMISTIC
    let res = client
        .patch(&url)
        .json(&json!({
            "concurrencyMode": "PESSIMISTIC"
        }))
        .send()
        .await?;
    expect_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await?;
    expect_eq!(body["databaseEdition"], "ENTERPRISE");
    expect_eq!(body["concurrencyMode"], "PESSIMISTIC");

    // 5. GET - Should now be ENTERPRISE and PESSIMISTIC
    let res = client.get(&url).send().await?;
    expect_eq!(res.status(), StatusCode::OK);
    let body: serde_json::Value = res.json().await?;
    expect_eq!(body["databaseEdition"], "ENTERPRISE");
    expect_eq!(body["concurrencyMode"], "PESSIMISTIC");

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_firestore_v1_rest_crud_and_types() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;
    let docs_root = format!("{base_url}/v1/projects/p/databases/(default)/documents");

    // 1. Create document with all 11 storable Firestore value types
    let create_res = client
        .post(format!("{docs_root}/metrics?documentId=m1"))
        .json(&json!({
            "fields": {
                "nullField": { "nullValue": null },
                "boolField": { "booleanValue": true },
                "maxInt": { "integerValue": "9223372036854775807" },
                "minInt": { "integerValue": "-9223372036854775808" },
                "oneInt": { "integerValue": "1" },
                "oneFloat": { "doubleValue": 1.0 },
                "nanFloat": { "doubleValue": "NaN" },
                "infFloat": { "doubleValue": "Infinity" },
                "negInfFloat": { "doubleValue": "-Infinity" },
                "tsField": { "timestampValue": "2026-11-10T15:30:45.123456789Z" },
                "strField": { "stringValue": "hello" },
                "bytesField": { "bytesValue": "AAECAw==" },
                "refField": { "referenceValue": "projects/p/databases/(default)/documents/users/alice" },
                "geoField": { "geoPointValue": { "latitude": 52.3676, "longitude": 4.9041 } },
                "arrField": {
                    "arrayValue": {
                        "values": [
                            { "integerValue": "42" },
                            { "doubleValue": 42.0 }
                        ]
                    }
                },
                "mapField": {
                    "mapValue": {
                        "fields": {
                            "nested": { "stringValue": "value" }
                        }
                    }
                }
            }
        }))
        .send()
        .await?;
    expect_eq!(create_res.status(), StatusCode::OK);
    let created: serde_json::Value = create_res.json().await?;
    expect_eq!(
        created["name"],
        "projects/p/databases/(default)/documents/metrics/m1"
    );
    expect_eq!(
        created["fields"]["maxInt"]["integerValue"],
        "9223372036854775807"
    );
    expect_eq!(
        created["fields"]["minInt"]["integerValue"],
        "-9223372036854775808"
    );
    expect_eq!(created["fields"]["oneInt"]["integerValue"], "1");
    expect_eq!(created["fields"]["oneFloat"]["doubleValue"], 1.0);
    expect_eq!(created["fields"]["nanFloat"]["doubleValue"], "NaN");
    expect_eq!(created["fields"]["infFloat"]["doubleValue"], "Infinity");
    expect_eq!(created["fields"]["negInfFloat"]["doubleValue"], "-Infinity");
    expect_eq!(created["fields"]["bytesField"]["bytesValue"], "AAECAw==");
    let initial_update_time = created["updateTime"].as_str().unwrap().to_string();

    // 2. GetDocument with mask.fieldPaths
    let get_res = client
        .get(format!(
            "{docs_root}/metrics/m1?mask.fieldPaths=maxInt&mask.fieldPaths=oneFloat"
        ))
        .send()
        .await?;
    expect_eq!(get_res.status(), StatusCode::OK);
    let masked: serde_json::Value = get_res.json().await?;
    expect_eq!(
        masked["fields"]["maxInt"]["integerValue"],
        "9223372036854775807"
    );
    expect_eq!(masked["fields"]["oneFloat"]["doubleValue"], 1.0);
    expect_that!(masked["fields"].get("strField"), none());

    // 3. UpdateDocument with updateMask.fieldPaths and URL-encoded currentDocument.updateTime
    let encoded_ts = initial_update_time.replace(':', "%3A");
    let patch_res = client
        .patch(format!(
            "{docs_root}/metrics/m1?updateMask.fieldPaths=strField&updateMask.\
             fieldPaths=nullField&currentDocument.updateTime={encoded_ts}"
        ))
        .json(&json!({
            "fields": {
                "strField": { "stringValue": "updated" }
            }
        }))
        .send()
        .await?;
    expect_eq!(patch_res.status(), StatusCode::OK);
    let patched: serde_json::Value = patch_res.json().await?;
    expect_eq!(patched["fields"]["strField"]["stringValue"], "updated");
    // nullField was in updateMask but omitted from fields, so it must be deleted
    expect_that!(patched["fields"].get("nullField"), none());
    // maxInt was NOT in updateMask, so it must be preserved intact
    expect_eq!(
        patched["fields"]["maxInt"]["integerValue"],
        "9223372036854775807"
    );

    // 4. Phantom ancestor document discovery via subcollection + ListDocuments?showMissing=true
    let sub_create = client
        .post(format!(
            "{docs_root}/rooms/room-phantom/messages?documentId=msg-1"
        ))
        .json(&json!({
            "fields": { "text": { "stringValue": "hi" } }
        }))
        .send()
        .await?;
    expect_eq!(sub_create.status(), StatusCode::OK);

    // Root :listCollectionIds should list metrics and rooms
    let root_cols_res = client
        .post(format!("{docs_root}:listCollectionIds"))
        .json(&json!({}))
        .send()
        .await?;
    expect_eq!(root_cols_res.status(), StatusCode::OK);
    let root_cols: serde_json::Value = root_cols_res.json().await?;
    expect_eq!(root_cols["collectionIds"], json!(["metrics", "rooms"]));

    // ListDocuments on rooms WITH showMissing=true
    let list_rooms_res = client
        .get(format!(
            "{docs_root}/rooms?showMissing=true&mask.fieldPaths=__name__"
        ))
        .send()
        .await?;
    expect_eq!(list_rooms_res.status(), StatusCode::OK);
    let rooms_body: serde_json::Value = list_rooms_res.json().await?;
    expect_eq!(rooms_body["documents"].as_array().unwrap().len(), 1);
    expect_eq!(
        rooms_body["documents"][0]["name"],
        "projects/p/databases/(default)/documents/rooms/room-phantom"
    );
    expect_that!(rooms_body["documents"][0].get("createTime"), none());
    expect_that!(rooms_body["documents"][0].get("updateTime"), none());

    // Subcollection :listCollectionIds on phantom document rooms/room-phantom
    let sub_cols_res = client
        .post(format!("{docs_root}/rooms/room-phantom:listCollectionIds"))
        .json(&json!({}))
        .send()
        .await?;
    expect_eq!(sub_cols_res.status(), StatusCode::OK);
    let sub_cols: serde_json::Value = sub_cols_res.json().await?;
    expect_eq!(sub_cols["collectionIds"], json!(["messages"]));

    // Delete subcollection document msg-1 via REST DeleteDocument; phantom room-phantom must
    // disappear!
    let del_res = client
        .delete(format!("{docs_root}/rooms/room-phantom/messages/msg-1"))
        .send()
        .await?;
    expect_eq!(del_res.status(), StatusCode::OK);

    let list_rooms_after = client
        .get(format!("{docs_root}/rooms?showMissing=true"))
        .send()
        .await?;
    expect_eq!(list_rooms_after.status(), StatusCode::OK);
    let rooms_after_body: serde_json::Value = list_rooms_after.json().await?;
    expect_eq!(
        rooms_after_body
            .get("documents")
            .and_then(|d| d.as_array())
            .map_or(0, Vec::len),
        0
    );

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_firestore_v1_rest_run_query() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;
    let docs_root = format!("{base_url}/v1/projects/p/databases/(default)/documents");

    for (id, age, active) in [("alice", 30, true), ("bob", 19, true), ("carol", 42, false)] {
        let res = client
            .post(format!("{docs_root}/users?documentId={id}"))
            .json(&json!({
                "fields": {
                    "age": { "integerValue": age.to_string() },
                    "active": { "booleanValue": active }
                }
            }))
            .send()
            .await?;
        expect_eq!(res.status(), StatusCode::OK);
    }

    // Query matching active == true AND age >= 21 ordered by age DESC
    let query_res = client
        .post(format!("{docs_root}:runQuery"))
        .json(&json!({
            "structuredQuery": {
                "from": [{ "collectionId": "users" }],
                "where": {
                    "compositeFilter": {
                        "op": "AND",
                        "filters": [
                            {
                                "fieldFilter": {
                                    "field": { "fieldPath": "active" },
                                    "op": "EQUAL",
                                    "value": { "booleanValue": true }
                                }
                            },
                            {
                                "fieldFilter": {
                                    "field": { "fieldPath": "age" },
                                    "op": "GREATER_THAN_OR_EQUAL",
                                    "value": { "integerValue": "21" }
                                }
                            }
                        ]
                    }
                },
                "orderBy": [
                    { "field": { "fieldPath": "age" }, "direction": "DESCENDING" }
                ],
                "limit": 10
            }
        }))
        .send()
        .await?;
    expect_eq!(query_res.status(), StatusCode::OK);
    let rows: Vec<serde_json::Value> = query_res.json().await?;
    expect_eq!(rows.len(), 1);
    expect_eq!(
        rows[0]["document"]["name"],
        "projects/p/databases/(default)/documents/users/alice"
    );
    expect_that!(rows[0].get("readTime"), some(anything()));

    // Empty result query returns single RunQueryResponse with readTime and no document
    let empty_res = client
        .post(format!("{docs_root}:runQuery"))
        .json(&json!({
            "structuredQuery": {
                "from": [{ "collectionId": "users" }],
                "where": {
                    "fieldFilter": {
                        "field": { "fieldPath": "age" },
                        "op": "GREATER_THAN",
                        "value": { "integerValue": "100" }
                    }
                }
            }
        }))
        .send()
        .await?;
    expect_eq!(empty_res.status(), StatusCode::OK);
    let empty_rows: Vec<serde_json::Value> = empty_res.json().await?;
    expect_eq!(empty_rows.len(), 1);
    expect_that!(empty_rows[0].get("document"), none());
    expect_that!(empty_rows[0].get("readTime"), some(anything()));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_error_envelopes_and_404_isolation() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;
    let docs_root = format!("{base_url}/v1/projects/p/databases/(default)/documents");

    // 1. Missing documentId on CreateDocument -> 400 INVALID_ARGUMENT
    let no_id = client
        .post(format!("{docs_root}/users"))
        .json(&json!({ "fields": {} }))
        .send()
        .await?;
    expect_eq!(no_id.status(), StatusCode::BAD_REQUEST);
    let err: serde_json::Value = no_id.json().await?;
    expect_eq!(err["error"]["code"], 400);
    expect_eq!(err["error"]["status"], "INVALID_ARGUMENT");

    // 2. Duplicate CreateDocument -> 409 ALREADY_EXISTS
    client
        .post(format!("{docs_root}/users?documentId=dup"))
        .json(&json!({ "fields": {} }))
        .send()
        .await?;
    let dup = client
        .post(format!("{docs_root}/users?documentId=dup"))
        .json(&json!({ "fields": {} }))
        .send()
        .await?;
    expect_eq!(dup.status(), StatusCode::CONFLICT);
    let dup_err: serde_json::Value = dup.json().await?;
    expect_eq!(dup_err["error"]["code"], 409);
    expect_eq!(dup_err["error"]["status"], "ALREADY_EXISTS");

    // 3. Empty {} Value object -> 400 INVALID_ARGUMENT
    let empty_val = client
        .post(format!("{docs_root}/users?documentId=bad"))
        .json(&json!({ "fields": { "broken": {} } }))
        .send()
        .await?;
    expect_eq!(empty_val.status(), StatusCode::BAD_REQUEST);
    let empty_err: serde_json::Value = empty_val.json().await?;
    expect_eq!(empty_err["error"]["status"], "INVALID_ARGUMENT");

    // 4. Preconditions on PATCH: Exists(true) on missing -> 404 NOT_FOUND; UpdateTime mismatch ->
    //    400 FAILED_PRECONDITION
    let missing_exists = client
        .patch(format!(
            "{docs_root}/users/missing-doc?currentDocument.exists=true"
        ))
        .json(&json!({ "fields": {} }))
        .send()
        .await?;
    expect_eq!(missing_exists.status(), StatusCode::NOT_FOUND);
    let missing_exists_err: serde_json::Value = missing_exists.json().await?;
    expect_eq!(missing_exists_err["error"]["code"], 404);
    expect_eq!(missing_exists_err["error"]["status"], "NOT_FOUND");

    let precond = client
        .patch(format!(
            "{docs_root}/users/missing-doc?currentDocument.updateTime=1970-01-01T00:00:01Z"
        ))
        .json(&json!({ "fields": {} }))
        .send()
        .await?;
    expect_eq!(precond.status(), StatusCode::BAD_REQUEST);
    let precond_err: serde_json::Value = precond.json().await?;
    expect_eq!(precond_err["error"]["code"], 400);
    expect_eq!(precond_err["error"]["status"], "FAILED_PRECONDITION");

    // 5. Unmatched /v1/... and /emulator/v1/... routes return 404 JSON, never 200 HTML
    let unknown_v1 = client
        .get(format!("{base_url}/v1/unknown/path"))
        .send()
        .await?;
    expect_eq!(unknown_v1.status(), StatusCode::NOT_FOUND);
    let v1_err: serde_json::Value = unknown_v1.json().await?;
    expect_eq!(v1_err["error"]["status"], "NOT_FOUND");

    let unknown_em = client
        .get(format!("{base_url}/emulator/v1/nonexistent"))
        .send()
        .await?;
    expect_eq!(unknown_em.status(), StatusCode::NOT_FOUND);

    #[cfg(feature = "ui")]
    {
        let missing_asset = client
            .get(format!("{base_url}/ui/nonexistent.js"))
            .send()
            .await?;
        expect_eq!(missing_asset.status(), StatusCode::NOT_FOUND);

        let dts_asset = client
            .get(format!("{base_url}/ui/lit-html.d.ts"))
            .send()
            .await?;
        expect_eq!(dts_asset.status(), StatusCode::NOT_FOUND);

        let md_asset = client
            .get(format!("{base_url}/ui/README.md"))
            .send()
            .await?;
        expect_eq!(md_asset.status(), StatusCode::NOT_FOUND);

        let root_html = client.get(format!("{base_url}/")).send().await?;
        expect_eq!(root_html.status(), StatusCode::OK);
        expect_eq!(
            root_html
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
        let html_body = root_html.text().await?;
        expect_that!(html_body, contains_substring("window.EMULATOR_VERSION="));
        expect_that!(html_body, contains_substring("/ui/app.js"));

        let spa_deep = client
            .get(format!(
                "{base_url}/projects/demo/databases/(default)/documents/users/alice"
            ))
            .send()
            .await?;
        expect_eq!(spa_deep.status(), StatusCode::OK);

        for js_path in [
            "/ui/app.js",
            "/ui/values.js",
            "/ui/api.js",
            "/ui/components/editor.js",
            "/ui/components/explorer.js",
            "/ui/components/query.js",
            "/ui/lit-html.js",
            "/lit-html.js",
        ] {
            let res = client.get(format!("{base_url}{js_path}")).send().await?;
            expect_eq!(res.status(), StatusCode::OK);
            expect_eq!(
                res.headers()
                    .get("content-type")
                    .and_then(|v| v.to_str().ok()),
                Some("text/javascript; charset=utf-8")
            );
        }

        let css_res = client
            .get(format!("{base_url}/ui/styles.css"))
            .send()
            .await?;
        expect_eq!(css_res.status(), StatusCode::OK);
        expect_eq!(
            css_res
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/css; charset=utf-8")
        );
    }

    #[cfg(not(feature = "ui"))]
    {
        let root_ok = client.get(format!("{base_url}/")).send().await?;
        expect_eq!(root_ok.status(), StatusCode::OK);
        expect_eq!(root_ok.text().await?, "OK");

        let meta_res = client.get(format!("{base_url}/emulator/v1")).send().await?;
        expect_eq!(meta_res.status(), StatusCode::OK);
        let dbs: Vec<String> = meta_res.json().await?;
        expect_that!(
            dbs,
            contains(eq("projects/p/databases/(default)/documents"))
        );
    }

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_sse_events_stream() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;

    let mut sse_res = client
        .get(format!("{base_url}/emulator/v1/events"))
        .send()
        .await?;
    expect_eq!(sse_res.status(), StatusCode::OK);

    // Trigger database_created + documents_changed + database_cleared
    let docs_root = format!("{base_url}/v1/projects/sse-proj/databases/(default)/documents");
    let create_res = client
        .post(format!("{docs_root}/items?documentId=item1"))
        .json(&json!({ "fields": { "n": { "integerValue": "1" } } }))
        .send()
        .await?;
    expect_eq!(create_res.status(), StatusCode::OK);

    let clear_res = client
        .delete(format!(
            "{base_url}/emulator/v1/projects/sse-proj/databases/(default)/documents"
        ))
        .send()
        .await?;
    expect_eq!(clear_res.status(), StatusCode::OK);

    let mut collected = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    while tokio::time::Instant::now() < deadline {
        if collected.contains("\"database_created\"")
            && collected.contains("\"documents_changed\"")
            && collected.contains("\"database_cleared\"")
        {
            break;
        }
        if let Ok(chunk_res) =
            tokio::time::timeout(Duration::from_millis(500), sse_res.chunk()).await
            && let Some(bytes) = chunk_res?
        {
            collected.push_str(&String::from_utf8_lossy(&bytes));
        }
    }

    expect_that!(collected, contains_substring("\"database_created\""));
    expect_that!(collected, contains_substring("\"documents_changed\""));
    expect_that!(collected, contains_substring("\"database_cleared\""));

    let created_pos = collected.find("\"database_created\"").unwrap();
    let changed_pos = collected.find("\"documents_changed\"").unwrap();
    let cleared_pos = collected.find("\"database_cleared\"").unwrap();
    expect_pred!(created_pos < changed_pos);
    expect_pred!(changed_pos < cleared_pos);

    Ok(())
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn test_sse_heartbeat_and_malformed_json_rejection() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;

    // 1. Malformed JSON payloads are rejected with 4xx client errors on both Admin and REST
    //    endpoints.
    let bad_admin = client
        .patch(format!("{base_url}/v1/projects/p/databases/(default)"))
        .header("content-type", "application/json")
        .body("{malformed-json")
        .send()
        .await?;
    expect_pred!(bad_admin.status().is_client_error());

    let bad_rest = client
        .post(format!(
            "{base_url}/v1/projects/p/databases/(default)/documents/items?documentId=i1"
        ))
        .header("content-type", "application/json")
        .body("{\"fields\": ")
        .send()
        .await?;
    expect_pred!(bad_rest.status().is_client_error());

    // 2. Under `start_paused = true`, advancing across the 15-second SSE KeepAlive interval emits a
    //    `: heartbeat` comment frame without waiting 15 seconds of wall-clock time.
    let mut sse_res = client
        .get(format!("{base_url}/emulator/v1/events"))
        .send()
        .await?;
    expect_eq!(sse_res.status(), StatusCode::OK);

    let chunk = tokio::time::timeout(Duration::from_secs(16), sse_res.chunk())
        .await
        .expect("SSE keep-alive heartbeat should fire at 15s virtual time")?
        .expect("SSE stream should remain open and yield heartbeat bytes");
    let text = String::from_utf8_lossy(&chunk);
    expect_that!(text.as_ref(), contains_substring("heartbeat"));

    Ok(())
}
