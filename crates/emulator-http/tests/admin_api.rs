#![allow(unused_crate_dependencies)]

use std::time::Duration;

use axum::{
    Router,
    body::{Body, BodyDataStream, Bytes, to_bytes},
    http::{HeaderMap, Method, Request, StatusCode, header},
};
use emulator_database::{FirestoreProject, config::FirestoreConfig};
use emulator_http::RouterBuilder;
use googletest::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::json;
use tokio_stream::StreamExt;
use tower::ServiceExt;

#[derive(Clone)]
struct Client {
    router: Router,
}

impl Client {
    fn request(&self, method: Method, uri: impl Into<String>) -> TestRequestBuilder {
        TestRequestBuilder {
            router: self.router.clone(),
            method,
            uri: uri.into(),
            content_type: None,
            body: Vec::new(),
        }
    }

    fn get(&self, uri: impl Into<String>) -> TestRequestBuilder {
        self.request(Method::GET, uri)
    }

    fn post(&self, uri: impl Into<String>) -> TestRequestBuilder {
        self.request(Method::POST, uri)
    }

    fn patch(&self, uri: impl Into<String>) -> TestRequestBuilder {
        self.request(Method::PATCH, uri)
    }

    fn delete(&self, uri: impl Into<String>) -> TestRequestBuilder {
        self.request(Method::DELETE, uri)
    }
}

struct TestRequestBuilder {
    router: Router,
    method: Method,
    uri: String,
    content_type: Option<&'static str>,
    body: Vec<u8>,
}

impl TestRequestBuilder {
    fn json<T: Serialize + ?Sized>(mut self, value: &T) -> Self {
        self.content_type = Some("application/json");
        self.body = serde_json::to_vec(value).expect("serialize JSON request body");
        self
    }

    async fn send(self) -> Result<TestResponse> {
        let mut builder = Request::builder().method(self.method).uri(self.uri);
        if let Some(ct) = self.content_type {
            builder = builder.header(header::CONTENT_TYPE, ct);
        }
        let resp = self
            .router
            .oneshot(builder.body(Body::from(self.body))?)
            .await?;
        let status = resp.status();
        let headers = resp.headers().clone();
        Ok(TestResponse {
            status,
            headers,
            body: Some(resp.into_body()),
            stream: None,
        })
    }
}

struct TestResponse {
    status:  StatusCode,
    headers: HeaderMap,
    body:    Option<Body>,
    stream:  Option<BodyDataStream>,
}

impl TestResponse {
    fn status(&self) -> StatusCode {
        self.status
    }

    #[cfg_attr(not(feature = "ui"), allow(dead_code))]
    fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    async fn bytes(mut self) -> Result<Bytes> {
        let body = self.body.take().expect("response body already consumed");
        Ok(to_bytes(body, usize::MAX).await?)
    }

    async fn text(self) -> Result<String> {
        Ok(String::from_utf8(self.bytes().await?.to_vec())?)
    }

    async fn json<T: DeserializeOwned>(self) -> Result<T> {
        Ok(serde_json::from_slice(&self.bytes().await?)?)
    }

    async fn chunk(&mut self) -> Result<Option<Bytes>> {
        if self.stream.is_none() {
            let body = self.body.take().expect("response body already consumed");
            self.stream = Some(body.into_data_stream());
        }
        Ok(self.stream.as_mut().unwrap().next().await.transpose()?)
    }
}

async fn spawn_server(
    default_enterprise: bool,
) -> Result<(&'static FirestoreProject, Client, String)> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig {
        default_enterprise,
        ..Default::default()
    })));
    let router = RouterBuilder::new(project).build();
    Ok((project, Client { router }, String::new()))
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
    expect_eq!(body["name"], "projects/my-project/databases/(default)");
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
    expect_eq!(body["name"], "projects/my-project/databases/(default)");
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
            "/ui/state.js",
            "/ui/router.js",
            "/ui/values.js",
            "/ui/api.js",
            "/ui/components/contention.js",
            "/ui/components/editor.js",
            "/ui/components/explorer.js",
            "/ui/components/modals.js",
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
    // Drive the Axum router directly in-memory (`oneshot` + `BodyDataStream`) rather than over an
    // OS loopback TCP socket so `start_paused = true` virtual-time auto-advance never races Linux
    // kernel Nagle / Delayed-ACK timers.
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Malformed JSON payloads are rejected with 4xx client errors on both Admin and REST
    //    endpoints.
    let bad_admin = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri("/v1/projects/p/databases/(default)")
                .header("content-type", "application/json")
                .body(Body::from("{malformed-json"))?,
        )
        .await?;
    expect_pred!(bad_admin.status().is_client_error());

    let bad_rest = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/projects/p/databases/(default)/documents/items?documentId=i1")
                .header("content-type", "application/json")
                .body(Body::from("{\"fields\": "))?,
        )
        .await?;
    expect_pred!(bad_rest.status().is_client_error());

    // 2. Under `start_paused = true`, advancing across the 15-second SSE KeepAlive interval emits a
    //    `: heartbeat` comment frame without waiting 15 seconds of wall-clock time.
    let sse_res = router
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/emulator/v1/events")
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(sse_res.status(), StatusCode::OK);

    let mut sse_stream = sse_res.into_body().into_data_stream();
    let before_interval = tokio::time::timeout(Duration::from_secs(14), sse_stream.next()).await;
    expect_pred!(before_interval.is_err());

    let chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
        .await
        .expect("SSE keep-alive heartbeat should fire at 15s virtual time")
        .expect("SSE stream should remain open and yield heartbeat bytes")?;
    let text = String::from_utf8_lossy(&chunk);
    expect_that!(text.as_ref(), contains_substring(": heartbeat"));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_vector_passthrough_body_limit_and_run_query_find_nearest() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();
    let docs_root = "/v1/projects/p/databases/(default)/documents";

    // 1. POST :runQuery with object `findNearest` returns 501 UNIMPLEMENTED; non-object returns
    //    400.
    let unimpl_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{docs_root}:runQuery"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "structuredQuery": {
                        "from": [{ "collectionId": "items" }],
                        "findNearest": {
                            "vectorField": { "fieldPath": "embedding" },
                            "distanceMeasure": "EUCLIDEAN",
                            "queryVector": {
                                "mapValue": {
                                    "fields": {
                                        "__type__": { "stringValue": "__vector__" },
                                        "value": {
                                            "arrayValue": {
                                                "values": [{ "doubleValue": 1.0 }]
                                            }
                                        }
                                    }
                                }
                            },
                            "limit": 1
                        }
                    }
                }))?))?,
        )
        .await?;
    expect_eq!(unimpl_res.status(), StatusCode::NOT_IMPLEMENTED);
    let unimpl_bytes = to_bytes(unimpl_res.into_body(), usize::MAX).await?;
    let unimpl_body: serde_json::Value = serde_json::from_slice(&unimpl_bytes)?;
    expect_eq!(unimpl_body["error"]["status"], "UNIMPLEMENTED");

    let bad_fn_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri(format!("{docs_root}:runQuery"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "structuredQuery": {
                        "from": [{ "collectionId": "items" }],
                        "findNearest": 123
                    }
                }))?))?,
        )
        .await?;
    expect_eq!(bad_fn_res.status(), StatusCode::BAD_REQUEST);
    let bad_fn_bytes = to_bytes(bad_fn_res.into_body(), usize::MAX).await?;
    let bad_fn_body: serde_json::Value = serde_json::from_slice(&bad_fn_bytes)?;
    expect_eq!(bad_fn_body["error"]["status"], "INVALID_ARGUMENT");

    // 2. PATCH and GET with a valid __vector__ map (including -0.0) serialize "__type__" before
    //    "value" deterministically and preserve -0.0; invalid __vector__ writes return 400.
    let vec_payload = json!({
        "fields": {
            "embedding": {
                "mapValue": {
                    "fields": {
                        "value": {
                            "arrayValue": {
                                "values": [
                                    { "doubleValue": -0.0 },
                                    { "doubleValue": 2.5 }
                                ]
                            }
                        },
                        "__type__": { "stringValue": "__vector__" }
                    }
                }
            }
        }
    });
    let patch_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{docs_root}/items/v1"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&vec_payload)?))?,
        )
        .await?;
    expect_eq!(patch_res.status(), StatusCode::OK);
    let patch_bytes = to_bytes(patch_res.into_body(), usize::MAX).await?;
    let patch_text = String::from_utf8(patch_bytes.to_vec())?;
    let type_pos = patch_text
        .find("\"__type__\"")
        .expect("PATCH response should contain __type__");
    let value_pos = patch_text
        .find("\"value\"")
        .expect("PATCH response should contain value");
    expect_pred!(type_pos < value_pos);
    expect_that!(patch_text.as_str(), contains_substring("-0.0"));

    let get_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri(format!("{docs_root}/items/v1"))
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(get_res.status(), StatusCode::OK);
    let get_bytes = to_bytes(get_res.into_body(), usize::MAX).await?;
    let get_text = String::from_utf8(get_bytes.to_vec())?;
    let get_type_pos = get_text
        .find("\"__type__\"")
        .expect("GET response should contain __type__");
    let get_value_pos = get_text
        .find("\"value\"")
        .expect("GET response should contain value");
    expect_pred!(get_type_pos < get_value_pos);
    expect_that!(get_text.as_str(), contains_substring("-0.0"));

    let invalid_vec_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{docs_root}/items/bad"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "fields": {
                        "embedding": {
                            "mapValue": {
                                "fields": {
                                    "__type__": { "stringValue": "__vector__" },
                                    "value": { "arrayValue": { "values": [] } }
                                }
                            }
                        }
                    }
                }))?))?,
        )
        .await?;
    expect_eq!(invalid_vec_res.status(), StatusCode::BAD_REQUEST);
    let invalid_vec_bytes = to_bytes(invalid_vec_res.into_body(), usize::MAX).await?;
    let invalid_vec_body: serde_json::Value = serde_json::from_slice(&invalid_vec_bytes)?;
    expect_eq!(invalid_vec_body["error"]["status"], "INVALID_ARGUMENT");

    // 3. PATCH with a valid > 2 MiB multi-vector payload succeeds (200 OK) under the 50 MiB limit.
    let vector_2048 = json!({
        "mapValue": {
            "fields": {
                "__type__": { "stringValue": "__vector__" },
                "value": {
                    "arrayValue": {
                        "values": vec![json!({ "doubleValue": 0.25 }); 2048]
                    }
                }
            }
        }
    });
    let mut big_fields = serde_json::Map::new();
    for i in 0..75 {
        big_fields.insert(format!("v{i}"), vector_2048.clone());
    }
    let big_body = serde_json::to_vec(&json!({ "fields": big_fields }))?;
    expect_pred!(big_body.len() > 2 * 1024 * 1024);

    let big_patch_res = router
        .oneshot(
            Request::builder()
                .method(Method::PATCH)
                .uri(format!("{docs_root}/items/big"))
                .header("content-type", "application/json")
                .body(Body::from(big_body))?,
        )
        .await?;
    expect_eq!(big_patch_res.status(), StatusCode::OK);

    Ok(())
}

async fn send_oneshot(
    router: &Router,
    method: Method,
    uri: &str,
    json_body: Option<serde_json::Value>,
) -> Result<(StatusCode, serde_json::Value)> {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = if let Some(val) = json_body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&val)?)
    } else {
        Body::empty()
    };
    let resp = router.clone().oneshot(builder.body(body)?).await?;
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await?;
    let parsed = if bytes.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&bytes)?
    };
    Ok((status, parsed))
}

#[gtest]
#[tokio::test]
async fn test_rest_custom_verbs_percent_encoded_colons_and_empty_prefixes() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();
    let docs_root = "/v1/projects/p/databases/(default)/documents";

    // 1. Percent-encoded %3A in collection IDs must not be treated as a custom :verb.
    let (status, created_commit_col) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/events%3Acommit?documentId=d1"),
        Some(json!({ "fields": { "k": { "stringValue": "v1" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(
        created_commit_col["name"],
        "projects/p/databases/(default)/documents/events:commit/d1"
    );

    let (status, created_query_col) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/jobs%3ArunQuery?documentId=d1"),
        Some(json!({ "fields": { "k": { "stringValue": "v2" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(
        created_query_col["name"],
        "projects/p/databases/(default)/documents/jobs:runQuery/d1"
    );

    let (status, created_leading_colon) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/%3ArunQuery?documentId=d1"),
        Some(json!({ "fields": {} })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(
        created_leading_colon["name"],
        "projects/p/databases/(default)/documents/:runQuery/d1"
    );

    // 2. Empty prefix before :verb (`/:runQuery` or `/col/:runQuery`) must not route to root or
    //    produce trailing-slash Ref errors.
    let (status, empty_prefix_err) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/:runQuery"),
        Some(json!({
            "structuredQuery": { "from": [{ "collectionId": "events:commit" }] }
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);
    expect_eq!(empty_prefix_err["error"]["status"], "INVALID_ARGUMENT");

    let (status, slash_prefix_err) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/col/:runQuery"),
        Some(json!({ "structuredQuery": {} })),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    expect_eq!(slash_prefix_err["error"]["status"], "NOT_FOUND");

    // 3. Unencoded :runQuery on a document path with %3A inside the doc ID works as parent query.
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/col/doc%3A1/sub?documentId=s1"),
        Some(json!({ "fields": { "ok": { "booleanValue": true } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    let (status, sub_rows) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/col/doc%3A1:runQuery"),
        Some(json!({
            "structuredQuery": { "from": [{ "collectionId": "sub" }] }
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(sub_rows.as_array().unwrap().len(), 1);
    expect_eq!(
        sub_rows[0]["document"]["name"],
        "projects/p/databases/(default)/documents/col/doc:1/sub/s1"
    );

    // 4. Root-only Phase-2 verbs (:commit) return 501 only on valid database/documents:commit, and
    //    404 on subpaths or bogus paths; document-capable Phase-2 verbs return 501 on doc paths.
    let (status, root_commit) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}:commit"),
        Some(json!({})),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(root_commit["error"]["status"], "UNIMPLEMENTED");

    let (status, doc_commit) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/col/doc:commit"),
        Some(json!({})),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    expect_eq!(doc_commit["error"]["status"], "NOT_FOUND");

    let (status, bogus_commit) =
        send_oneshot(&router, Method::POST, "/v1/projects/bogus:commit", None).await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    expect_eq!(bogus_commit["error"]["status"], "NOT_FOUND");

    let (status, doc_agg) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/col/doc:runAggregationQuery"),
        Some(json!({})),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(doc_agg["error"]["status"], "UNIMPLEMENTED");

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_rejects_percent_encoded_slashes_and_preserves_plus() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // Seed a real document so we verify %2F delete/read cannot target it.
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/col/doc/sub?documentId=d1",
        Some(json!({ "fields": { "x": { "integerValue": "1" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    for (method, bad_uri) in [
        (
            Method::GET,
            "/v1/projects/p/databases/(default)%2Fdocuments",
        ),
        (Method::GET, "/v1/projects/p%2Fx/databases/(default)"),
        (
            Method::GET,
            "/v1/projects/p/databases/(default)%2Fdocuments%2Fcol%2Fdoc/documents/sub",
        ),
        (
            Method::GET,
            "/v1/projects/p/databases/(default)/documents/col/doc%2Fsub",
        ),
        (
            Method::PATCH,
            "/v1/projects/p/databases/(default)/documents/col/doc%2Fsub%2Fd1",
        ),
        (
            Method::DELETE,
            "/emulator/v1/projects/p/databases/(default)/documents/col/doc%2Fsub",
        ),
        (
            Method::DELETE,
            "/emulator/v1/projects/p/databases/(default)%2Fdocuments",
        ),
    ] {
        let body = (method == Method::PATCH).then(|| json!({ "fields": {} }));
        let (status, err) = send_oneshot(&router, method, bad_uri, body).await?;
        expect_eq!(status, StatusCode::BAD_REQUEST, "uri: {bad_uri}");
        expect_eq!(err["error"]["status"], "INVALID_ARGUMENT", "uri: {bad_uri}");
    }

    // Seeded doc must remain untouched after rejected %2F DELETE attempts.
    let (status, _) = send_oneshot(
        &router,
        Method::GET,
        "/v1/projects/p/databases/(default)/documents/col/doc/sub/d1",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    // Literal '+' in URL path segments must not be decoded as a space.
    let (status, created_plus) = send_oneshot(
        &router,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/col?documentId=doc%2B1",
        Some(json!({ "fields": { "plus": { "booleanValue": true } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(
        created_plus["name"],
        "projects/p/databases/(default)/documents/col/doc+1"
    );

    let (status, got_plus) = send_oneshot(
        &router,
        Method::GET,
        "/v1/projects/p/databases/(default)/documents/col/doc+1",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(got_plus["fields"]["plus"]["booleanValue"], true);

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_patch_database_coupling_sse_events_and_update_mask() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();
    let admin_url = "/v1/projects/p/databases/(default)";

    // Subscribe to in-memory SSE events before patching.
    let sse_resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/emulator/v1/events")
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(sse_resp.status(), StatusCode::OK);
    let mut sse_stream = sse_resp.into_body().into_data_stream();

    // 1. Switching from STANDARD+PESSIMISTIC -> ENTERPRISE+PESSIMISTIC must not transiently flip
    //    optimistic_concurrency or emit duplicate database_config_changed events.
    let (status, patched) = send_oneshot(
        &router,
        Method::PATCH,
        admin_url,
        Some(json!({
            "databaseEdition": "ENTERPRISE",
            "concurrencyMode": "PESSIMISTIC"
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(patched["databaseEdition"], "ENTERPRISE");
    expect_eq!(patched["concurrencyMode"], "PESSIMISTIC");

    // Trigger one sentinel event afterward so we can deterministically read all preceding events.
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/items?documentId=sentinel",
        Some(json!({ "fields": {} })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    let mut sse_text = String::new();
    while !sse_text.contains("\"documents_changed\"") {
        let chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
            .await
            .expect("SSE chunk arrived")
            .expect("SSE stream open")?;
        sse_text.push_str(&String::from_utf8_lossy(&chunk));
    }
    expect_eq!(sse_text.matches("\"database_config_changed\"").count(), 1);

    // 2. updateMask restricts which fields in the PATCH body are applied.
    let (status, mask_concurrency) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{admin_url}?updateMask=concurrencyMode"),
        Some(json!({
            "databaseEdition": "STANDARD",
            "concurrencyMode": "OPTIMISTIC"
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(mask_concurrency["databaseEdition"], "ENTERPRISE");
    expect_eq!(mask_concurrency["concurrencyMode"], "OPTIMISTIC");

    let (status, mask_edition) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{admin_url}?updateMask=databaseEdition"),
        Some(json!({
            "databaseEdition": "STANDARD",
            "concurrencyMode": "OPTIMISTIC"
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(mask_edition["databaseEdition"], "STANDARD");
    expect_eq!(mask_edition["concurrencyMode"], "PESSIMISTIC");

    let (status, mask_snake) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{admin_url}?updateMask=database_edition,concurrency_mode"),
        Some(json!({
            "databaseEdition": "ENTERPRISE",
            "concurrencyMode": "PESSIMISTIC"
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(mask_snake["databaseEdition"], "ENTERPRISE");
    expect_eq!(mask_snake["concurrencyMode"], "PESSIMISTIC");

    for bad_mask in ["", "unknownField", "databaseEdition,"] {
        let (status, err) = send_oneshot(
            &router,
            Method::PATCH,
            &format!("{admin_url}?updateMask={bad_mask}"),
            Some(json!({ "concurrencyMode": "OPTIMISTIC" })),
        )
        .await?;
        expect_eq!(status, StatusCode::BAD_REQUEST, "mask: {bad_mask}");
        expect_eq!(err["error"]["status"], "INVALID_ARGUMENT");
    }

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_masks_read_time_consistency_and_create_document_slash_id() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();
    let docs_root = "/v1/projects/p/databases/(default)/documents";

    // 1. Empty mask.fieldPaths= on CREATE returns metadata only (no fields).
    let (status, created_empty_mask) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/items?documentId=i1&mask.fieldPaths="),
        Some(json!({ "fields": { "v": { "integerValue": "1" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_that!(created_empty_mask.get("fields"), none());
    let t1 = created_empty_mask["updateTime"]
        .as_str()
        .unwrap()
        .to_string();

    // 2. Empty mask.fieldPaths= on GET and LIST returns metadata only.
    let (status, got_empty_mask) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?mask.fieldPaths="),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_that!(got_empty_mask.get("fields"), none());

    let (status, listed_empty_mask) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items?mask.fieldPaths="),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_that!(listed_empty_mask["documents"][0].get("fields"), none());

    // 3. Mixed empty + non-empty mask/updateMask entries must fail with 400 INVALID_ARGUMENT, while
    //    sole empty updateMask.fieldPaths= preserves existing fields.
    let (status, mixed_mask_err) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?mask.fieldPaths=v&mask.fieldPaths="),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);
    expect_eq!(mixed_mask_err["error"]["status"], "INVALID_ARGUMENT");

    let (status, mixed_update_mask_err) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{docs_root}/items/i1?updateMask.fieldPaths=v&updateMask.fieldPaths="),
        Some(json!({ "fields": { "v": { "integerValue": "2" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);
    expect_eq!(mixed_update_mask_err["error"]["status"], "INVALID_ARGUMENT");

    let (status, noop_patch) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{docs_root}/items/i1?updateMask.fieldPaths="),
        Some(json!({ "fields": { "other": { "stringValue": "ignored" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(noop_patch["fields"]["v"]["integerValue"], "1");
    expect_that!(noop_patch["fields"].get("other"), none());

    // 4. Update i1 to v=2 and create i2; readTime=t1 must still observe the historical snapshot.
    let (status, _) = send_oneshot(
        &router,
        Method::PATCH,
        &format!("{docs_root}/items/i1"),
        Some(json!({ "fields": { "v": { "integerValue": "2" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/items?documentId=i2"),
        Some(json!({ "fields": { "v": { "integerValue": "10" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    let encoded_t1 = t1.replace(':', "%3A");
    let (status, get_at_t1) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?readTime={encoded_t1}"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(get_at_t1["fields"]["v"]["integerValue"], "1");

    let (status, missing_at_t1) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i2?readTime={encoded_t1}"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    expect_eq!(missing_at_t1["error"]["status"], "NOT_FOUND");

    let (status, list_at_t1) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items?readTime={encoded_t1}"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(list_at_t1["documents"].as_array().unwrap().len(), 1);
    expect_eq!(
        list_at_t1["documents"][0]["fields"]["v"]["integerValue"],
        "1"
    );

    // 5. Invalid/unsupported consistency selectors on GET and :listCollectionIds.
    let (status, bad_rt) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?readTime=not-a-timestamp"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);
    expect_eq!(bad_rt["error"]["status"], "INVALID_ARGUMENT");

    let (status, txn_get) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?transaction=AAEC"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(txn_get["error"]["status"], "UNIMPLEMENTED");

    let (status, txn_list) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items?transaction=AAEC"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(txn_list["error"]["status"], "UNIMPLEMENTED");

    let (status, conflict_sel) = send_oneshot(
        &router,
        Method::GET,
        &format!("{docs_root}/items/i1?readTime={encoded_t1}&transaction=AAEC"),
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);
    expect_eq!(conflict_sel["error"]["status"], "INVALID_ARGUMENT");

    let (status, list_cols_rt) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}:listCollectionIds"),
        Some(json!({ "readTime": t1 })),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(list_cols_rt["error"]["status"], "UNIMPLEMENTED");

    // 6. CreateDocument must reject documentId containing '/'.
    for bad_doc_id in ["alice/posts/p1", "alice/posts"] {
        let (status, err) = send_oneshot(
            &router,
            Method::POST,
            &format!("{docs_root}/users?documentId={bad_doc_id}"),
            Some(json!({ "fields": {} })),
        )
        .await?;
        expect_eq!(status, StatusCode::BAD_REQUEST, "doc_id: {bad_doc_id}");
        expect_eq!(err["error"]["status"], "INVALID_ARGUMENT");
    }

    Ok(())
}

async fn send_raw_oneshot(
    router: &Router,
    method: Method,
    uri: &str,
    json_body: Option<serde_json::Value>,
) -> Result<(StatusCode, HeaderMap, Bytes)> {
    let mut builder = Request::builder().method(method).uri(uri);
    let body = if let Some(val) = json_body {
        builder = builder.header("content-type", "application/json");
        Body::from(serde_json::to_vec(&val)?)
    } else {
        Body::empty()
    };
    let resp = router.clone().oneshot(builder.body(body)?).await?;
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), usize::MAX).await?;
    Ok((status, headers, bytes))
}

#[gtest]
#[tokio::test]
async fn test_patch_database_update_mask_preserves_unmasked_concurrency_and_single_sse_and_snake_case()
-> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Configure (default) as ENTERPRISE + PESSIMISTIC.
    let (status, _) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/(default)",
        Some(json!({ "databaseEdition": "ENTERPRISE" })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    let (status, body) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/(default)",
        Some(json!({ "concurrencyMode": "PESSIMISTIC" })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["databaseEdition"], "ENTERPRISE");
    assert_eq!(body["concurrencyMode"], "PESSIMISTIC");

    // 2. PATCH with updateMask=databaseEdition only must preserve explicit PESSIMISTIC
    //    concurrencyMode.
    let (status, masked_body) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/(default)?updateMask=databaseEdition",
        Some(json!({ "databaseEdition": "ENTERPRISE" })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        masked_body["concurrencyMode"], "PESSIMISTIC",
        "updateMask=databaseEdition on already-ENTERPRISE database must not reset PESSIMISTIC to \
         OPTIMISTIC"
    );

    // 3. Single PATCH changing edition (and implicitly concurrency) must emit at most one
    //    database_config_changed SSE event.
    let (status, _) = send_oneshot(
        &router,
        Method::GET,
        "/v1/projects/p/databases/db-multi",
        None,
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let sse_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/emulator/v1/events")
                .body(Body::empty())?,
        )
        .await?;
    assert_eq!(sse_res.status(), StatusCode::OK);
    let mut sse_stream = sse_res.into_body().into_data_stream();

    let (status, _) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/db-multi",
        Some(json!({ "databaseEdition": "ENTERPRISE" })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    // Sentinel document write so we know all prior SSE events have been flushed.
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        "/v1/projects/p/databases/db-multi/documents/items?documentId=s1",
        Some(json!({ "fields": {} })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let mut sse_text = String::new();
    while !sse_text.contains("\"documents_changed\"") {
        let chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
            .await
            .expect("timed out waiting for SSE sentinel")
            .expect("SSE stream ended unexpectedly")?;
        sse_text.push_str(&String::from_utf8_lossy(&chunk));
    }
    assert_eq!(
        sse_text.matches("\"database_config_changed\"").count(),
        1,
        "expected exactly 1 database_config_changed SSE event for single PATCH, got SSE \
         stream:\n{sse_text}"
    );

    // 4. ProtoJSON snake_case field names (`database_edition`, `concurrency_mode`) and snake_case
    //    updateMask paths must be accepted.
    let (status, snake_body) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/db-snake?updateMask=database_edition",
        Some(json!({ "database_edition": "ENTERPRISE" })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(snake_body["databaseEdition"], "ENTERPRISE");

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_invalid_percent_encoding_and_snake_read_time_return_json_errors() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Invalid UTF-8 percent-encoding (%FF) and malformed hex (%ZZ) across v1 REST, /emulator/v1,
    //    and Admin routes must return structured JSON error envelopes (`application/json`,
    //    `INVALID_ARGUMENT`), not plain-text rejections.
    for (method, uri) in [
        (
            Method::GET,
            "/v1/projects/p/databases/(default)/documents/col/%FF",
        ),
        (
            Method::DELETE,
            "/emulator/v1/projects/p/databases/(default)/documents/col/%FF",
        ),
        (Method::GET, "/v1/projects/p%ZZ/databases/(default)"),
    ] {
        let (status, headers, bytes) = send_raw_oneshot(&router, method.clone(), uri, None).await?;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri: {uri}");
        let content_type = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            content_type.starts_with("application/json"),
            "expected application/json error envelope for {method} {uri}, got \
             content-type={content_type:?} body={:?}",
            String::from_utf8_lossy(&bytes)
        );
        let parsed: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(parsed["error"]["status"], "INVALID_ARGUMENT", "uri: {uri}");
    }

    // 2. Snake_case `?read_time=` query parameter must be parsed and validated just like
    //    `?readTime=` (not silently ignored!).
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/col?documentId=d1",
        Some(json!({ "fields": { "v": { "integerValue": "1" } } })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);

    let (status, snake_rt_body) = send_oneshot(
        &router,
        Method::GET,
        "/v1/projects/p/databases/(default)/documents/col/d1?read_time=not-a-timestamp",
        None,
    )
    .await?;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "invalid ?read_time= must be rejected with 400 INVALID_ARGUMENT instead of silently \
         ignored: {snake_rt_body}"
    );
    assert_eq!(snake_rt_body["error"]["status"], "INVALID_ARGUMENT");

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_rest_unimplemented_request_options_and_new_database_admin_fields() -> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Document REST endpoints reject `requestOptions.requestTags` (camelCase and snake_case)
    //    with 501 UNIMPLEMENTED rather than silently ignoring them.
    for query in [
        "requestOptions.requestTags=tag1",
        "request_options.request_tags=tag1",
    ] {
        let (status, body) = send_oneshot(
            &router,
            Method::GET,
            &format!("/v1/projects/p/databases/(default)/documents/col/d1?{query}"),
            None,
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "query: {query}");
        assert_eq!(body["error"]["status"], "UNIMPLEMENTED");
    }

    // 2. PATCH Database rejects new admin/v1 Database fields when sent directly without updateMask
    //    or when named in updateMask, while preserving masked updates when omitted from updateMask.
    for payload in [
        json!({ "firestoreDataAccessMode": "DATA_ACCESS_MODE_ENABLED" }),
        json!({ "firestore_data_access_mode": "DATA_ACCESS_MODE_ENABLED" }),
        json!({ "mongodbCompatibleDataAccessMode": "DATA_ACCESS_MODE_DISABLED" }),
        json!({ "mongodb_compatible_data_access_mode": "DATA_ACCESS_MODE_DISABLED" }),
        json!({ "realtimeUpdatesMode": "REALTIME_UPDATES_MODE_ENABLED" }),
        json!({ "realtime_updates_mode": "REALTIME_UPDATES_MODE_ENABLED" }),
    ] {
        let (status, body) = send_oneshot(
            &router,
            Method::PATCH,
            "/v1/projects/p/databases/db-new-fields",
            Some(payload.clone()),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "payload: {payload}");
        assert_eq!(body["error"]["status"], "UNIMPLEMENTED");
    }

    for field in [
        "firestoreDataAccessMode",
        "firestore_data_access_mode",
        "mongodbCompatibleDataAccessMode",
        "mongodb_compatible_data_access_mode",
        "realtimeUpdatesMode",
        "realtime_updates_mode",
    ] {
        let (status, body) = send_oneshot(
            &router,
            Method::PATCH,
            &format!("/v1/projects/p/databases/db-new-fields?updateMask={field}"),
            Some(json!({})),
        )
        .await?;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "mask field: {field}");
        assert_eq!(body["error"]["status"], "UNIMPLEMENTED");
    }

    let (status, masked_ok) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p/databases/db-new-fields?updateMask=databaseEdition",
        Some(json!({
            "databaseEdition": "ENTERPRISE",
            "firestoreDataAccessMode": "DATA_ACCESS_MODE_ENABLED",
        })),
    )
    .await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(masked_ok["databaseEdition"], "ENTERPRISE");

    Ok(())
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn test_contention_get_delete_and_database_query_validation() -> Result<()> {
    use emulator_database::{
        read_consistency::ReadConsistency,
        reference::{DocumentRef, RootRef},
    };
    use googleapis::google::firestore::v1::{Document, TransactionOptions, Value, Write, write};

    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Empty project snapshot returns 200 `{ "databases": [] }`.
    let (status, empty_snap) =
        send_oneshot(&router, Method::GET, "/emulator/v1/contention", None).await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(empty_snap, json!({ "databases": [] }));

    // 2. Valid absent database returns empty without lazily creating the database.
    let (status, absent_snap) = send_oneshot(
        &router,
        Method::GET,
        "/emulator/v1/contention?database=projects%2Fp%2Fdatabases%2Fabsent",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(absent_snap, json!({ "databases": [] }));
    let (status, absent_del) = send_oneshot(
        &router,
        Method::DELETE,
        "/emulator/v1/contention?database=projects/p/databases/absent",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(absent_del, json!({}));
    expect_true!(project.database_names().await.is_empty());

    // 3. Malformed or unrecognized query parameters return 400 INVALID_ARGUMENT on GET and DELETE.
    for bad_uri in [
        "/emulator/v1/contention?database=",
        "/emulator/v1/contention?database",
        "/emulator/v1/contention?database=not-a-db",
        "/emulator/v1/contention?database=projects/p/databases/(default)/documents/c/d",
        "/emulator/v1/contention?database=projects/p/databases/a&database=projects/p/databases/b",
        "/emulator/v1/contention?database=projects%ZZp",
    ] {
        for method in [Method::GET, Method::DELETE] {
            let (status, err_json) = send_oneshot(&router, method, bad_uri, None).await?;
            expect_eq!(status, StatusCode::BAD_REQUEST);
            expect_eq!(err_json["error"]["status"], "INVALID_ARGUMENT");
        }
    }
    for (bad_uri, key) in [
        ("/emulator/v1/contention?db=projects/p/databases/a", "db"),
        (
            "/emulator/v1/contention?databases=projects/p/databases/a",
            "databases",
        ),
        (
            "/emulator/v1/contention?database=projects/p/databases/a&extra=1",
            "extra",
        ),
    ] {
        for method in [Method::GET, Method::DELETE] {
            let (status, err_json) = send_oneshot(&router, method, bad_uri, None).await?;
            expect_eq!(status, StatusCode::BAD_REQUEST);
            expect_eq!(err_json["error"]["status"], "INVALID_ARGUMENT");
            expect_eq!(
                err_json["error"]["message"],
                format!("Unsupported query parameter: {key}")
            );
        }
    }

    // 4. Populate active contention + timeout history across two databases (`db-a` and `db-b`).
    let root_a: RootRef = "projects/p/databases/db-a".parse()?;
    let root_b: RootRef = "projects/p/databases/db-b".parse()?;
    let db_a = project.database(&root_a).await;
    let db_b = project.database(&root_b).await;

    let doc_a: DocumentRef = "projects/p/databases/db-a/documents/c/d1".parse()?;
    let doc_b: DocumentRef = "projects/p/databases/db-b/documents/c/d1".parse()?;

    // Cause a write timeout on `db-b` so `db-b` has non-zero summary counter & event.
    let b_hold = db_b.new_txn(TransactionOptions::READ_WRITE).await?;
    let b_wait = db_b.new_txn(TransactionOptions::READ_WRITE).await?;
    let _unused = db_b
        .get_doc(&doc_b, ReadConsistency::Transaction(b_hold))
        .await?;
    let mut b_commit_task = tokio::spawn({
        let db_b = db_b.clone();
        let doc_b = doc_b.clone();
        async move {
            db_b.commit(
                vec![Write {
                    operation: Some(write::Operation::Update(Document {
                        name: doc_b.to_string(),
                        fields: std::collections::HashMap::from([(
                            "k".to_string(),
                            Value::integer(1),
                        )]),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                Some(b_wait),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let b_res = (&mut b_commit_task).await?;
    expect_pred!(b_res.is_err());
    db_b.rollback(b_hold).await?;

    // Cause a write timeout on `db-a` (history=1), plus keep an active blocker + parked waiter!
    let a_timeout_hold = db_a.new_txn(TransactionOptions::READ_WRITE).await?;
    let a_timeout_wait = db_a.new_txn(TransactionOptions::READ_WRITE).await?;
    let _unused = db_a
        .get_doc(&doc_a, ReadConsistency::Transaction(a_timeout_hold))
        .await?;
    let a_to_task = tokio::spawn({
        let db_a = db_a.clone();
        let doc_a = doc_a.clone();
        async move {
            db_a.commit(
                vec![Write {
                    operation: Some(write::Operation::Update(Document {
                        name: doc_a.to_string(),
                        fields: std::collections::HashMap::from([(
                            "k".to_string(),
                            Value::integer(1),
                        )]),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                Some(a_timeout_wait),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let _unused = a_to_task.await?;
    db_a.rollback(a_timeout_hold).await?;

    let a_hold = db_a.new_txn(TransactionOptions::READ_WRITE).await?;
    let a_wait = db_a.new_txn(TransactionOptions::READ_WRITE).await?;
    let _unused = db_a
        .get_doc(&doc_a, ReadConsistency::Transaction(a_hold))
        .await?;
    let a_wait_task = tokio::spawn({
        let db_a = db_a.clone();
        let doc_a = doc_a.clone();
        async move {
            db_a.commit(
                vec![Write {
                    operation: Some(write::Operation::Update(Document {
                        name: doc_a.to_string(),
                        fields: std::collections::HashMap::from([(
                            "k".to_string(),
                            Value::integer(2),
                        )]),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                Some(a_wait),
            )
            .await
        }
    });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_millis(150)).await;

    // 5. `GET /emulator/v1/contention` returns sorted snapshots for both databases.
    let (status, all_snap) =
        send_oneshot(&router, Method::GET, "/emulator/v1/contention", None).await?;
    expect_eq!(status, StatusCode::OK);
    let dbs = all_snap["databases"].as_array().expect("databases array");
    expect_eq!(dbs.len(), 2);
    expect_eq!(dbs[0]["database"], "projects/p/databases/db-a/documents");
    expect_eq!(dbs[1]["database"], "projects/p/databases/db-b/documents");

    expect_eq!(dbs[0]["summary"]["timeoutsTotal"], 1);
    expect_eq!(dbs[0]["totalContendedDocuments"], 1);
    expect_eq!(dbs[0]["truncated"], false);
    let doc_snap = &dbs[0]["contendedDocuments"][0];
    expect_eq!(doc_snap["document"], doc_a.to_string());
    expect_eq!(doc_snap["readerCount"], 1);
    expect_eq!(doc_snap["readers"][0]["kind"], "PESSIMISTIC_TXN");
    expect_eq!(doc_snap["readers"][0]["state"], "ACTIVE");
    expect_eq!(doc_snap["readers"][0]["retained"], true);
    expect_eq!(doc_snap["readers"][0]["txnId"], a_hold.to_string());
    expect_eq!(doc_snap["waiterCount"], 1);
    expect_eq!(doc_snap["waiters"][0]["kind"], "PESSIMISTIC_TXN");
    expect_eq!(doc_snap["waiters"][0]["mode"], "WRITE");
    expect_eq!(doc_snap["waiters"][0]["waitedMs"], 150);
    expect_eq!(doc_snap["waiters"][0]["txnId"], a_wait.to_string());

    // 6. Filtered `GET` with encoded `?database=` and `/documents` normalization.
    let (status, filtered_snap) = send_oneshot(
        &router,
        Method::GET,
        "/emulator/v1/contention?database=projects%2Fp%2Fdatabases%2Fdb-a%2Fdocuments",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    let fdbs = filtered_snap["databases"].as_array().unwrap();
    expect_eq!(fdbs.len(), 1);
    expect_eq!(fdbs[0]["database"], "projects/p/databases/db-a/documents");

    // 7. Scoped `DELETE` clears `db-a` history/counters ONLY, preserving active locks and `db-b`.
    let (status, del_a) = send_oneshot(
        &router,
        Method::DELETE,
        "/emulator/v1/contention?database=projects/p/databases/db-a",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(del_a, json!({}));

    let (status, after_del_a) =
        send_oneshot(&router, Method::GET, "/emulator/v1/contention", None).await?;
    expect_eq!(status, StatusCode::OK);
    let dbs = after_del_a["databases"].as_array().unwrap();
    expect_eq!(dbs[0]["summary"]["timeoutsTotal"], 0);
    expect_eq!(dbs[0]["recentEvents"].as_array().unwrap().len(), 0);
    expect_eq!(dbs[0]["totalContendedDocuments"], 1);
    expect_eq!(dbs[0]["contendedDocuments"][0]["waiterCount"], 1);
    expect_eq!(dbs[1]["summary"]["timeoutsTotal"], 1);

    // 8. Unscoped `DELETE` clears `db-b` too while keeping `db-a`'s active locks intact.
    let (status, del_all) =
        send_oneshot(&router, Method::DELETE, "/emulator/v1/contention", None).await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(del_all, json!({}));
    let (status, after_del_all) =
        send_oneshot(&router, Method::GET, "/emulator/v1/contention", None).await?;
    expect_eq!(status, StatusCode::OK);
    let dbs = after_del_all["databases"].as_array().unwrap();
    expect_eq!(dbs[0]["totalContendedDocuments"], 1);
    expect_eq!(dbs[1]["summary"]["timeoutsTotal"], 0);

    // Clean up pending waiter cleanly.
    db_a.rollback(a_hold).await?;
    let _unused = a_wait_task.await?;

    Ok(())
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn test_contention_sse_events_resync_heartbeat_and_drop_cleanup() -> Result<()> {
    use emulator_database::{
        read_consistency::ReadConsistency,
        reference::{DocumentRef, RootRef},
    };
    use googleapis::google::firestore::v1::{Document, TransactionOptions, Value, Write, write};

    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Open SSE stream and verify subscriber count increments, then heartbeat fires at 15s.
    expect_eq!(project.contention_subscriber_count(), 0);
    let sse_res = router
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::GET)
                .uri("/emulator/v1/contention/events")
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(sse_res.status(), StatusCode::OK);
    expect_eq!(project.contention_subscriber_count(), 1);
    let mut sse_stream = sse_res.into_body().into_data_stream();

    let before_hb = tokio::time::timeout(Duration::from_secs(14), sse_stream.next()).await;
    expect_pred!(before_hb.is_err());
    let hb_chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
        .await
        .expect("contention SSE keep-alive heartbeat should fire at 15s")
        .expect("contention SSE stream open")?;
    expect_that!(
        String::from_utf8_lossy(&hb_chunk).as_ref(),
        contains_substring(": heartbeat")
    );

    // 2. Trigger lock contention and verify `event: contention` (`contention_changed`).
    let root: RootRef = "projects/p/databases/(default)".parse()?;
    let db = project.database(&root).await;
    let doc_ref: DocumentRef = "projects/p/databases/(default)/documents/c/d1".parse()?;
    let hold_txn = db.new_txn(TransactionOptions::READ_WRITE).await?;
    let wait_txn = db.new_txn(TransactionOptions::READ_WRITE).await?;
    let _unused = db
        .get_doc(&doc_ref, ReadConsistency::Transaction(hold_txn))
        .await?;
    let wait_task = tokio::spawn({
        let db = db.clone();
        let doc_ref = doc_ref.clone();
        async move {
            db.commit(
                vec![Write {
                    operation: Some(write::Operation::Update(Document {
                        name: doc_ref.to_string(),
                        fields: std::collections::HashMap::from([(
                            "k".to_string(),
                            Value::integer(1),
                        )]),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                Some(wait_txn),
            )
            .await
        }
    });
    tokio::task::yield_now().await;

    let ev_chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
        .await
        .expect("contention_changed event")
        .expect("stream item")?;
    let ev_text = String::from_utf8_lossy(&ev_chunk);
    expect_that!(ev_text.as_ref(), contains_substring("event: contention"));
    expect_that!(
        ev_text.as_ref(),
        contains_substring("\"type\":\"contention_changed\"")
    );
    expect_that!(
        ev_text.as_ref(),
        contains_substring("\"database\":\"projects/p/databases/(default)/documents\"")
    );

    // 3. Clearing database emits `database_cleared` on contention SSE stream.
    let (status, _) = send_oneshot(
        &router,
        Method::DELETE,
        "/emulator/v1/projects/p/databases/(default)",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    let _unused = wait_task.await?;
    let clear_chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
        .await
        .expect("database_cleared event")
        .expect("stream item")?;
    let clear_text = String::from_utf8_lossy(&clear_chunk);
    expect_that!(clear_text.as_ref(), contains_substring("event: contention"));
    expect_that!(
        clear_text.as_ref(),
        contains_substring("\"type\":\"database_cleared\"")
    );

    // 4. Lagging beyond the 512-event broadcast buffer emits `event: resync`.
    for i in 0..520u32 {
        let lag_root: RootRef = format!("projects/p/databases/lag-{i}").parse()?;
        let _lag_db = project.database(&lag_root).await;
        project.clear_database(&lag_root).await;
    }
    let mut saw_resync = false;
    while let Ok(Some(Ok(chunk))) =
        tokio::time::timeout(Duration::from_millis(50), sse_stream.next()).await
    {
        let text = String::from_utf8_lossy(&chunk);
        if text.contains("event: resync") && text.contains("\"type\":\"resync\"") {
            saw_resync = true;
            break;
        }
    }
    expect_true!(saw_resync);

    // 5. Dropping the SSE stream decrements the lock-free subscriber counter back to zero.
    drop(sse_stream);
    expect_eq!(project.contention_subscriber_count(), 0);

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_ui_contention_asset_and_api_coexistence() -> Result<()> {
    let (_project, client, base_url) = spawn_server(false).await?;

    #[cfg(feature = "ui")]
    {
        let js_res = client
            .get(format!("{base_url}/ui/components/contention.js"))
            .send()
            .await?;
        expect_eq!(js_res.status(), StatusCode::OK);
        expect_eq!(
            js_res
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/javascript; charset=utf-8")
        );
        let js_body = js_res.text().await?;
        expect_that!(
            js_body,
            contains_substring("export function renderContention")
        );

        let spa_contention = client
            .get(format!(
                "{base_url}/projects/demo/databases/(default)/contention"
            ))
            .send()
            .await?;
        expect_eq!(spa_contention.status(), StatusCode::OK);
        expect_eq!(
            spa_contention
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok()),
            Some("text/html; charset=utf-8")
        );
    }

    let get_res = client
        .get(format!(
            "{base_url}/emulator/v1/contention?database=projects%2Fp%2Fdatabases%2F%28default%29%\
             2Fdocuments"
        ))
        .send()
        .await?;
    expect_eq!(get_res.status(), StatusCode::OK);
    let snap: serde_json::Value = get_res.json().await?;
    expect_eq!(snap, json!({ "databases": [] }));

    let del_res = client
        .delete(format!("{base_url}/emulator/v1/contention"))
        .send()
        .await?;
    expect_eq!(del_res.status(), StatusCode::OK);
    let del_body: serde_json::Value = del_res.json().await?;
    expect_eq!(del_body, json!({}));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_protocol_fixes_database_metadata_run_query_snake_case_and_non_creating_delete()
-> Result<()> {
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();

    // 1. Subscribe to project SSE events before deleting nonexistent collection/document paths.
    let mut sse_rx = project.subscribe_project_events();

    for nonexistent_ref in [
        "/emulator/v1/projects/absent-proj/databases/(default)/documents/items",
        "/emulator/v1/projects/absent-proj/databases/(default)/documents/items/doc1",
        "/emulator/v1/projects/absent-proj/databases/(default)",
    ] {
        let (status, body) = send_oneshot(&router, Method::DELETE, nonexistent_ref, None).await?;
        expect_eq!(status, StatusCode::OK, "uri: {nonexistent_ref}");
        expect_eq!(body, json!({}), "uri: {nonexistent_ref}");
    }
    expect_true!(project.database_names().await.is_empty());
    expect_pred!(sse_rx.try_recv().is_err());

    // 2. DatabaseMetadata.name uses Admin API v1 format (`projects/{p}/databases/{d}` without
    //    `/documents`), while `GET /emulator/v1` preserves `RootRef` (`/documents`).
    let (status, meta) =
        send_oneshot(&router, Method::GET, "/v1/projects/p1/databases/db1", None).await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(meta["name"], "projects/p1/databases/db1");

    let (status, patched_meta) = send_oneshot(
        &router,
        Method::PATCH,
        "/v1/projects/p1/databases/db1",
        Some(json!({ "databaseEdition": "ENTERPRISE" })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(patched_meta["name"], "projects/p1/databases/db1");

    let (status, emu_dbs) = send_oneshot(&router, Method::GET, "/emulator/v1", None).await?;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(emu_dbs, json!(["projects/p1/databases/db1/documents"]));

    // 3. Deleting an existing collection/document via `/emulator/v1/{*ref}` still deletes data.
    let docs_root = "/v1/projects/p1/databases/db1/documents";
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/items?documentId=d1"),
        Some(json!({ "fields": { "x": { "integerValue": "1" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    let (status, _) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}/items?documentId=d2"),
        Some(json!({ "fields": { "x": { "integerValue": "2" } } })),
    )
    .await?;
    expect_eq!(status, StatusCode::OK);

    let (status, _) = send_oneshot(
        &router,
        Method::DELETE,
        "/emulator/v1/projects/p1/databases/db1/documents/items/d1",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    let (status, _) =
        send_oneshot(&router, Method::GET, &format!("{docs_root}/items/d1"), None).await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) =
        send_oneshot(&router, Method::GET, &format!("{docs_root}/items/d2"), None).await?;
    expect_eq!(status, StatusCode::OK);

    let (status, _) = send_oneshot(
        &router,
        Method::DELETE,
        "/emulator/v1/projects/p1/databases/db1/documents/items",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::OK);
    let (status, _) =
        send_oneshot(&router, Method::GET, &format!("{docs_root}/items/d2"), None).await?;
    expect_eq!(status, StatusCode::NOT_FOUND);

    // 4. `:runQuery` enforces strict camelCase (`structuredQuery` / `findNearest`): snake_case
    //    variants return 400 INVALID_ARGUMENT, while camelCase `structuredQuery.findNearest` object
    //    returns 501 UNIMPLEMENTED.
    for snake_payload in [
        json!({
            "structured_query": {
                "from": [{ "collectionId": "items" }],
                "find_nearest": { "limit": 1 }
            }
        }),
        json!({
            "structured_query": {
                "from": [{ "collectionId": "items" }],
                "findNearest": { "limit": 1 }
            }
        }),
        json!({
            "structuredQuery": {
                "from": [{ "collectionId": "items" }],
                "find_nearest": { "limit": 1 }
            }
        }),
    ] {
        let (status, err) = send_oneshot(
            &router,
            Method::POST,
            &format!("{docs_root}:runQuery"),
            Some(snake_payload.clone()),
        )
        .await?;
        expect_eq!(status, StatusCode::BAD_REQUEST, "payload: {snake_payload}");
        expect_eq!(err["error"]["code"], 400, "payload: {snake_payload}");
        expect_eq!(
            err["error"]["status"],
            "INVALID_ARGUMENT",
            "payload: {snake_payload}"
        );
    }

    let (status, err) = send_oneshot(
        &router,
        Method::POST,
        &format!("{docs_root}:runQuery"),
        Some(json!({
            "structuredQuery": {
                "from": [{ "collectionId": "items" }],
                "findNearest": { "limit": 1 }
            }
        })),
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_IMPLEMENTED);
    expect_eq!(err["error"]["code"], 501);
    expect_eq!(err["error"]["status"], "UNIMPLEMENTED");

    Ok(())
}

#[gtest]
#[tokio::test]
async fn test_uniform_json_error_envelopes_405_head_413_logging_and_rest_debug_level() -> Result<()>
{
    use emulator_tracing::DefaultTracing;

    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router_without_logging = RouterBuilder::new(project).build();
    let tracing = DefaultTracing::start();
    let router_with_logging = RouterBuilder::new(project)
        .add_dynamic_tracing(tracing)
        .build();

    // 1. Unmatched `/emulator/v1/nonexistent` remains 404 NOT_FOUND JSON (not 405).
    let (status, headers, bytes) = send_raw_oneshot(
        &router_without_logging,
        Method::GET,
        "/emulator/v1/nonexistent",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    expect_that!(ct, starts_with("application/json"));
    let body: serde_json::Value = serde_json::from_slice(&bytes)?;
    expect_eq!(body["error"]["code"], 404);
    expect_eq!(body["error"]["status"], "NOT_FOUND");

    // 2. Fixed-path routes return uniform JSON 405 METHOD_NOT_ALLOWED with `Allow` header.
    for (router, method, uri, expected_allow) in [
        (
            &router_without_logging,
            Method::POST,
            "/emulator/v1",
            "GET, HEAD",
        ),
        (
            &router_without_logging,
            Method::POST,
            "/emulator/v1/events",
            "GET, HEAD",
        ),
        (
            &router_without_logging,
            Method::POST,
            "/emulator/v1/contention",
            "GET, HEAD, DELETE",
        ),
        (
            &router_without_logging,
            Method::POST,
            "/emulator/v1/contention/events",
            "GET, HEAD",
        ),
        (
            &router_without_logging,
            Method::POST,
            "/v1/projects/p/databases/(default)",
            "GET, HEAD, PATCH",
        ),
        (
            &router_without_logging,
            Method::DELETE,
            "/v1/projects/p/databases/(default)",
            "GET, HEAD, PATCH",
        ),
        (
            &router_without_logging,
            Method::GET,
            "/v1/projects/p/databases/(default)/documents:listCollectionIds",
            "POST",
        ),
        (
            &router_without_logging,
            Method::GET,
            "/v1/projects/p/databases/(default)/documents:runQuery",
            "POST",
        ),
        (
            &router_with_logging,
            Method::GET,
            "/emulator/v1/logging/my-handle",
            "POST, DELETE",
        ),
    ] {
        let (status, headers, bytes) = send_raw_oneshot(router, method.clone(), uri, None).await?;
        expect_eq!(status, StatusCode::METHOD_NOT_ALLOWED, "{method} {uri}");
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        expect_that!(ct, starts_with("application/json"));
        expect_eq!(
            headers.get(header::ALLOW).and_then(|v| v.to_str().ok()),
            Some(expected_allow),
            "{method} {uri}"
        );
        let err: serde_json::Value = serde_json::from_slice(&bytes)?;
        expect_eq!(err["error"]["code"], 405, "{method} {uri}");
        expect_eq!(
            err["error"]["status"],
            "METHOD_NOT_ALLOWED",
            "{method} {uri}"
        );
    }

    // 3. `HEAD` remains supported (200 OK with empty body) on `get(...)` routes with fallbacks.
    for uri in [
        "/emulator/v1",
        "/emulator/v1/events",
        "/emulator/v1/contention",
        "/emulator/v1/contention/events",
        "/v1/projects/p/databases/(default)",
    ] {
        let (status, _headers, bytes) =
            send_raw_oneshot(&router_without_logging, Method::HEAD, uri, None).await?;
        expect_eq!(status, StatusCode::OK, "HEAD {uri}");
        expect_true!(bytes.is_empty(), "HEAD {uri} must have empty body");
    }

    // 4. Unmounted `/emulator/v1/logging` returns JSON 501 UNIMPLEMENTED (never 400 Invalid
    //    reference from `emulator::router`'s `/{*ref}`), while mounted logging routes return 404
    //    JSON on unmatched logging subpaths.
    for (method, uri) in [
        (Method::GET, "/emulator/v1/logging"),
        (Method::GET, "/emulator/v1/logging/"),
        (Method::DELETE, "/emulator/v1/logging"),
        (Method::DELETE, "/emulator/v1/logging/"),
        (Method::POST, "/emulator/v1/logging/my-handle"),
        (Method::DELETE, "/emulator/v1/logging/my-handle"),
        (Method::DELETE, "/emulator/v1/logging/nested/path"),
    ] {
        let (status, headers, bytes) =
            send_raw_oneshot(&router_without_logging, method.clone(), uri, None).await?;
        expect_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "unmounted {method} {uri}"
        );
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        expect_that!(ct, starts_with("application/json"));
        let err: serde_json::Value = serde_json::from_slice(&bytes)?;
        expect_eq!(err["error"]["code"], 501, "unmounted {method} {uri}");
        expect_eq!(
            err["error"]["status"],
            "UNIMPLEMENTED",
            "unmounted {method} {uri}"
        );
    }

    for (method, uri) in [
        (Method::GET, "/emulator/v1/logging"),
        (Method::GET, "/emulator/v1/logging/"),
        (Method::DELETE, "/emulator/v1/logging"),
        (Method::DELETE, "/emulator/v1/logging/"),
        (Method::DELETE, "/emulator/v1/logging/nested/path"),
    ] {
        let (status, headers, bytes) =
            send_raw_oneshot(&router_with_logging, method.clone(), uri, None).await?;
        expect_eq!(status, StatusCode::NOT_FOUND, "mounted {method} {uri}");
        let ct = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        expect_that!(ct, starts_with("application/json"));
        let err: serde_json::Value = serde_json::from_slice(&bytes)?;
        expect_eq!(err["error"]["code"], 404, "mounted {method} {uri}");
        expect_eq!(
            err["error"]["status"],
            "NOT_FOUND",
            "mounted {method} {uri}"
        );
    }

    // 5. Oversized bodies return JSON 413 RESOURCE_EXHAUSTED across `RestBytes` (document/query and
    //    logging handlers) AND `ApiJson` (`PATCH /v1/projects/.../databases/...`, fixing the 413 ->
    //    400 coercion bug).
    let oversized_projects_body = Bytes::from(vec![b' '; 50 * 1024 * 1024 + 1]);
    for (method, uri) in [
        (Method::PATCH, "/v1/projects/p/databases/(default)"),
        (
            Method::POST,
            "/v1/projects/p/databases/(default)/documents/items?documentId=huge",
        ),
        (
            Method::POST,
            "/v1/projects/p/databases/(default)/documents:runQuery",
        ),
    ] {
        let resp = router_without_logging
            .clone()
            .oneshot(
                Request::builder()
                    .method(method.clone())
                    .uri(uri)
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(oversized_projects_body.clone()))?,
            )
            .await?;
        expect_eq!(
            resp.status(),
            StatusCode::PAYLOAD_TOO_LARGE,
            "{method} {uri}"
        );
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned();
        expect_that!(ct.as_str(), starts_with("application/json"));
        let err: serde_json::Value =
            serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await?)?;
        expect_eq!(err["error"]["code"], 413, "{method} {uri}");
        expect_eq!(
            err["error"]["status"],
            "RESOURCE_EXHAUSTED",
            "{method} {uri}"
        );
    }

    // Logging route uses default Axum 2 MiB body limit and returns JSON 413 / 400 on limit or
    // invalid UTF-8 errors.
    let oversized_logging_body = vec![b'a'; 2 * 1024 * 1024 + 1];
    let resp = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/emulator/v1/logging/oversized-handle")
                .body(Body::from(oversized_logging_body))?,
        )
        .await?;
    expect_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    let err: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await?)?;
    expect_eq!(err["error"]["code"], 413);
    expect_eq!(err["error"]["status"], "RESOURCE_EXHAUSTED");

    let bad_utf8_resp = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/emulator/v1/logging/bad-utf8-handle")
                .body(Body::from(vec![0xFF, 0xFE]))?,
        )
        .await?;
    expect_eq!(bad_utf8_resp.status(), StatusCode::BAD_REQUEST);
    let bad_utf8_err: serde_json::Value =
        serde_json::from_slice(&to_bytes(bad_utf8_resp.into_body(), usize::MAX).await?)?;
    expect_eq!(bad_utf8_err["error"]["code"], 400);
    expect_eq!(bad_utf8_err["error"]["status"], "INVALID_ARGUMENT");

    // 6. Expected REST handler errors log at DEBUG (never ERROR) via `err(level = Level::DEBUG)`.
    let start_err_capture = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/emulator/v1/logging/rest-err-level")
                .body(Body::from("emulator_http=error"))?,
        )
        .await?;
    expect_eq!(start_err_capture.status(), StatusCode::OK);

    let (status, _) = send_oneshot(
        &router_with_logging,
        Method::GET,
        "/v1/projects/p/databases/(default)/documents/missing_col/missing_doc",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_oneshot(
        &router_with_logging,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/missing_col",
        Some(json!({ "fields": {} })),
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);

    let stop_err_capture = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/emulator/v1/logging/rest-err-level")
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(stop_err_capture.status(), StatusCode::OK);
    let captured_err_logs = String::from_utf8(
        to_bytes(stop_err_capture.into_body(), usize::MAX)
            .await?
            .to_vec(),
    )?;
    expect_eq!(captured_err_logs, "");

    // Companion capture at `emulator_http=debug` confirms `err(level = Level::DEBUG)` emits
    // the expected `error=...` event at `DEBUG` (not merely suppressing logs altogether).
    let start_debug_capture = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/emulator/v1/logging/rest-debug-level")
                .body(Body::from("emulator_http=debug"))?,
        )
        .await?;
    expect_eq!(start_debug_capture.status(), StatusCode::OK);

    let (status, _) = send_oneshot(
        &router_with_logging,
        Method::GET,
        "/v1/projects/p/databases/(default)/documents/missing_col/missing_doc",
        None,
    )
    .await?;
    expect_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = send_oneshot(
        &router_with_logging,
        Method::POST,
        "/v1/projects/p/databases/(default)/documents/missing_col",
        Some(json!({ "fields": {} })),
    )
    .await?;
    expect_eq!(status, StatusCode::BAD_REQUEST);

    let stop_debug_capture = router_with_logging
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::DELETE)
                .uri("/emulator/v1/logging/rest-debug-level")
                .body(Body::empty())?,
        )
        .await?;
    expect_eq!(stop_debug_capture.status(), StatusCode::OK);
    let captured_debug_logs = String::from_utf8(
        to_bytes(stop_debug_capture.into_body(), usize::MAX)
            .await?
            .to_vec(),
    )?;
    // Strip ANSI SGR sequences emitted by `tracing_subscriber::fmt` so `error=` matches literally.
    let plain_debug_logs = {
        let mut out = String::with_capacity(captured_debug_logs.len());
        let mut chars = captured_debug_logs.chars();
        while let Some(ch) = chars.next() {
            if ch == '\x1b' {
                for next in chars.by_ref() {
                    if next == 'm' {
                        break;
                    }
                }
            } else {
                out.push(ch);
            }
        }
        out
    };
    let debug_error_lines: Vec<&str> = plain_debug_logs
        .lines()
        .filter(|line| line.contains("DEBUG") && line.contains("error="))
        .collect();
    expect_true!(
        debug_error_lines
            .iter()
            .any(|line| line.contains("handle_get_document") && line.contains("404 (NOT_FOUND)")),
        "expected DEBUG error line from handle_get_document; got:\n{plain_debug_logs}"
    );
    expect_true!(
        debug_error_lines.iter().any(|line| {
            line.contains("handle_create_document") && line.contains("400 (INVALID_ARGUMENT)")
        }),
        "expected DEBUG error line from handle_create_document; got:\n{plain_debug_logs}"
    );

    Ok(())
}

#[gtest]
#[tokio::test]
async fn rest_list_collection_ids_uses_opaque_pagination_tokens() -> Result<()> {
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let router = RouterBuilder::new(project).build();
    let docs_root = "/v1/projects/p/databases/(default)/documents";

    for col in ["c3", "c1", "c5", "c2", "c4"] {
        let (status, _) = send_oneshot(
            &router,
            Method::POST,
            &format!("{docs_root}/parents/doc1/{col}?documentId=d"),
            Some(json!({ "fields": {} })),
        )
        .await?;
        expect_eq!(status, StatusCode::OK);
    }

    let parent_url = format!("{docs_root}/parents/doc1:listCollectionIds");
    let (s1, p1) = send_oneshot(
        &router,
        Method::POST,
        &parent_url,
        Some(json!({ "pageSize": 2 })),
    )
    .await?;
    expect_eq!(s1, StatusCode::OK);
    expect_eq!(p1["collectionIds"], json!(["c1", "c2"]));
    let tok1 = p1["nextPageToken"].as_str().unwrap_or("");
    assert!(!tok1.is_empty());
    assert_ne!(tok1, "c2");

    let (s2, p2) = send_oneshot(
        &router,
        Method::POST,
        &parent_url,
        Some(json!({ "pageSize": 2, "pageToken": tok1 })),
    )
    .await?;
    expect_eq!(s2, StatusCode::OK);
    expect_eq!(p2["collectionIds"], json!(["c3", "c4"]));
    let tok2 = p2["nextPageToken"].as_str().unwrap_or("");
    assert!(!tok2.is_empty());

    let (s3, p3) = send_oneshot(
        &router,
        Method::POST,
        &parent_url,
        Some(json!({ "pageSize": 2, "pageToken": tok2 })),
    )
    .await?;
    expect_eq!(s3, StatusCode::OK);
    expect_eq!(p3["collectionIds"], json!(["c5"]));
    expect_eq!(
        p3.get("nextPageToken")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        ""
    );

    // Exact fit pageSize == total omits nextPageToken
    let (s_exact, exact) = send_oneshot(
        &router,
        Method::POST,
        &parent_url,
        Some(json!({ "pageSize": 5 })),
    )
    .await?;
    expect_eq!(s_exact, StatusCode::OK);
    expect_eq!(
        exact["collectionIds"],
        json!(["c1", "c2", "c3", "c4", "c5"])
    );
    expect_eq!(
        exact
            .get("nextPageToken")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        ""
    );

    // Raw-string / malformed pageToken and negative pageSize -> 400 INVALID_ARGUMENT
    for body in [
        json!({ "pageToken": "c2" }),
        json!({ "pageToken": "not-a-valid-token" }),
        json!({ "pageSize": -1 }),
    ] {
        let (st, err) = send_oneshot(&router, Method::POST, &parent_url, Some(body)).await?;
        expect_eq!(st, StatusCode::BAD_REQUEST);
        expect_eq!(err["error"]["status"], "INVALID_ARGUMENT");
    }

    Ok(())
}
