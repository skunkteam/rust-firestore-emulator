use std::{
    task::{Context, Poll},
    time::Duration,
};

use emulator_tracing::DefaultTracing;
use futures::StreamExt;
use googleapis::{
    fields,
    google::firestore::v1::{
        Document, GetDocumentRequest, UpdateDocumentRequest, firestore_client::FirestoreClient,
    },
};
use googletest::prelude::*;
use http::{HeaderValue, Method, StatusCode, Uri, header::CONTENT_TYPE};
use serde_json::json;
use tokio::{net::TcpListener, sync::oneshot};
use tower::Service;

use crate::harness::{
    builders::{QuerySpec, doc_path},
    env::{DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, short_ids},
};

#[gtest]
#[tokio::test]
async fn dynamic_logging_capture_poll_filter_update_and_delete_against_idempotent_tracing() {
    // Verify `DefaultTracing::start()` returns the exact same `'static` instance on repeated calls.
    let t1 = DefaultTracing::start();
    let t2 = DefaultTracing::start();
    assert!(std::ptr::eq(t1, t2));

    let mut db = TestEnv::new().build_db();
    let handle_uri = "/emulator/v1/logging/blackbox-handle";
    let poll_uri = "/emulator/v1/logging/blackbox-handle:poll";

    // 1. Start dynamic log capture for `tower_http` and `emulator_grpc` debug spans/events.
    let (status, initial_bytes) = db
        .http_raw_body(
            Method::POST,
            handle_uri,
            Some("text/plain"),
            "tower_http=debug,emulator_grpc=debug",
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(initial_bytes.as_ref(), b"");

    // 2. Drive both a gRPC write and an HTTP GET so traced spans fire.
    db.put("logs/entry1", fields!({ "msg": "hello" }))
        .await
        .unwrap();
    let (meta_status, _) = db.http_oneshot(Method::GET, "/emulator/v1", None).await;
    expect_eq!(meta_status, StatusCode::OK);

    // 3. Poll captured logs and verify DEBUG output from `tower_http` was recorded.
    let (poll_status, poll_bytes) = db.http_raw_body(Method::POST, poll_uri, None, "").await;
    expect_eq!(poll_status, StatusCode::OK);
    let captured = String::from_utf8_lossy(&poll_bytes);
    expect_that!(captured.as_ref(), contains_substring("DEBUG"));
    expect_that!(captured.as_ref(), contains_substring("tower_http"));

    // 4. Reconfigure the handle filter to `error` (draining any interim logs), run a clean read,
    //    and verify the next poll is empty.
    let (reconf_status, _) = db
        .http_raw_body(Method::POST, handle_uri, Some("text/plain"), "error")
        .await;
    expect_eq!(reconf_status, StatusCode::OK);

    let _doc = db.get("logs/entry1").await.unwrap();
    let (quiet_status, quiet_bytes) = db.http_raw_body(Method::POST, poll_uri, None, "").await;
    expect_eq!(quiet_status, StatusCode::OK);
    expect_eq!(String::from_utf8_lossy(&quiet_bytes), "");

    // 5. Invalid filter directive syntax returns `400 Bad Request`.
    let (bad_status, _) = db
        .http_raw_body(
            Method::POST,
            "/emulator/v1/logging/bad-handle",
            Some("text/plain"),
            "=invalid=filter=",
        )
        .await;
    expect_eq!(bad_status, StatusCode::BAD_REQUEST);

    // 6. Delete capture handle and confirm subsequent polls/deletes return `404 Not Found`.
    let (del_status, _) = db.http_oneshot(Method::DELETE, handle_uri, None).await;
    expect_eq!(del_status, StatusCode::OK);

    let (missing_poll, _) = db.http_raw_body(Method::POST, poll_uri, None, "").await;
    expect_eq!(missing_poll, StatusCode::NOT_FOUND);

    let (missing_del, _) = db.http_oneshot(Method::DELETE, handle_uri, None).await;
    expect_eq!(missing_del, StatusCode::NOT_FOUND);
}

#[gtest]
#[tokio::test]
async fn cross_protocol_grpc_and_http_rest_roundtrips_with_in_memory_sse_events() {
    let mut db = TestEnv::new().build_db();

    // Subscribe to in-memory SSE project events before mutating the database.
    let (sse_status, mut sse_stream) = db.http_stream("/emulator/v1/events").await;
    expect_eq!(sse_status, StatusCode::OK);

    // 1. Write via gRPC -> Read via HTTP REST (ProtoJSON) and verify wire-exact parity.
    let grpc_fields = fields!({
        "count": 42i64,
        "ratio": 3.5f64,
        "tags": ["grpc", "rest"],
        "nested": { "ok": true },
    });
    db.put("bridge/item1", grpc_fields.clone()).await.unwrap();

    let rest_item1_uri = format!("/v1/{DOCUMENTS_ROOT}/bridge/item1");
    let (get_status, get_bytes) = db.http_oneshot(Method::GET, &rest_item1_uri, None).await;
    expect_eq!(get_status, StatusCode::OK);
    let rest_doc: Document = serde_json::from_slice(&get_bytes).expect("valid ProtoJSON Document");
    assert_doc_wire_eq(&rest_doc, "bridge/item1", &grpc_fields);

    // 2. Write via HTTP REST -> Read & Query via gRPC.
    let rest_create_uri = format!("/v1/{DOCUMENTS_ROOT}/bridge?documentId=item2");
    let (post_status, _) = db
        .http_oneshot(
            Method::POST,
            &rest_create_uri,
            Some(&json!({
                "fields": {
                    "count": { "integerValue": "99" },
                    "ratio": { "doubleValue": 1.25 },
                    "tags": { "arrayValue": { "values": [{ "stringValue": "http" }] } },
                    "nested": { "mapValue": { "fields": { "ok": { "booleanValue": false } } } }
                }
            })),
        )
        .await;
    expect_eq!(post_status, StatusCode::OK);

    let from_grpc = db.get("bridge/item2").await.unwrap();
    assert_doc_wire_eq(
        &from_grpc,
        "bridge/item2",
        &fields!({
            "count": 99i64,
            "ratio": 1.25f64,
            "tags": ["http"],
            "nested": { "ok": false },
        }),
    );

    let queried = db
        .query(QuerySpec::collection("bridge").order_asc("count"))
        .await
        .unwrap();
    expect_eq!(short_ids(&queried.documents), vec!["item1", "item2"]);

    // 3. Switch edition via HTTP Admin PATCH and clear database via HTTP Emulator DELETE, then
    //    verify all lifecycle events arrived on the SSE stream in order.
    db.set_admin_mode(Some("ENTERPRISE"), None).await;
    let clear_uri = format!("/emulator/v1/{DOCUMENTS_ROOT}");
    let (clear_status, _) = db.http_oneshot(Method::DELETE, &clear_uri, None).await;
    expect_eq!(clear_status, StatusCode::OK);

    let mut sse_text = String::new();
    while !(sse_text.contains("\"database_created\"")
        && sse_text.contains("\"documents_changed\"")
        && sse_text.contains("\"database_config_changed\"")
        && sse_text.contains("\"database_cleared\""))
    {
        let chunk = tokio::time::timeout(Duration::from_secs(2), sse_stream.next())
            .await
            .expect("SSE event chunk arrived in time")
            .expect("SSE stream open")
            .expect("infallible SSE body chunk");
        sse_text.push_str(&String::from_utf8_lossy(&chunk));
    }

    let created_idx = sse_text.find("\"database_created\"").unwrap();
    let changed_idx = sse_text.find("\"documents_changed\"").unwrap();
    let config_idx = sse_text.find("\"database_config_changed\"").unwrap();
    let cleared_idx = sse_text.find("\"database_cleared\"").unwrap();
    assert!(created_idx < changed_idx);
    assert!(changed_idx < config_idx);
    assert!(config_idx < cleared_idx);
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_serves_grpc_and_http_on_same_port_and_shuts_down_cleanly() {
    let db = TestEnv::enterprise().build_db();
    let project = db.project();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral loopback port");
    let addr = listener.local_addr().expect("listener local_addr");
    let base_url = format!("http://{addr}");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        firestore_emulator::run(
            project,
            listener,
            async move {
                let _unused = shutdown_rx.await;
            },
            DefaultTracing::start(),
        )
        .await
    });

    // Connect a real HTTP/2 Tonic gRPC client AND an HTTP/1.1 reqwest client to the SAME port.
    let endpoint = tonic::transport::Endpoint::from_shared(base_url.clone())
        .expect("valid endpoint URI")
        .tcp_nodelay(true);
    let channel = endpoint
        .connect()
        .await
        .expect("connect Tonic channel to multiplexed server");
    let mut grpc_client = FirestoreClient::new(channel);
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build no-proxy reqwest client");

    // 1. Write over gRPC (HTTP/2 `application/grpc`) -> Read over HTTP/1.1 REST on the same socket
    //    port.
    grpc_client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("mux/from-grpc"),
                fields: fields!({ "origin": "grpc", "n": 7i64 }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
        })
        .await
        .expect("gRPC UpdateDocument over multiplexed TCP");

    let rest_get = http_client
        .get(format!("{base_url}/v1/{DOCUMENTS_ROOT}/mux/from-grpc"))
        .send()
        .await
        .expect("HTTP GET over multiplexed TCP");
    expect_eq!(rest_get.status(), StatusCode::OK);
    let rest_json: serde_json::Value = rest_get.json().await.unwrap();
    expect_eq!(rest_json["fields"]["origin"]["stringValue"], "grpc");
    expect_eq!(rest_json["fields"]["n"]["integerValue"], "7");

    // 2. Write over HTTP/1.1 REST -> Read over gRPC + inspect `/emulator/v1` metadata.
    let rest_post = http_client
        .post(format!(
            "{base_url}/v1/{DOCUMENTS_ROOT}/mux?documentId=from-http"
        ))
        .json(&json!({
            "fields": {
                "origin": { "stringValue": "http" },
                "n": { "integerValue": "11" }
            }
        }))
        .send()
        .await
        .expect("HTTP POST over multiplexed TCP");
    expect_eq!(rest_post.status(), StatusCode::OK);

    let grpc_got = grpc_client
        .get_document(GetDocumentRequest {
            name: doc_path("mux/from-http"),
            mask: None,
            consistency_selector: None,
        })
        .await
        .expect("gRPC GetDocument over multiplexed TCP")
        .into_inner();
    assert_doc_wire_eq(
        &grpc_got,
        "mux/from-http",
        &fields!({ "origin": "http", "n": 11i64 }),
    );

    let dbs_res = http_client
        .get(format!("{base_url}/emulator/v1"))
        .send()
        .await
        .unwrap();
    expect_eq!(dbs_res.status(), StatusCode::OK);
    let dbs: Vec<String> = dbs_res.json().await.unwrap();
    expect_eq!(dbs, vec![DOCUMENTS_ROOT.to_string()]);

    // 3. Drop active client connections, fire shutdown signal, and assert clean server exit.
    drop(grpc_client);
    drop(http_client);
    shutdown_tx.send(()).expect("send shutdown signal");

    let run_result = tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("firestore_emulator::run exited within 5s of shutdown signal")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "firestore_emulator::run returned error: {run_result:?}"
    );
}

#[derive(Clone)]
struct OverrideContentType<S> {
    inner: S,
    content_type: HeaderValue,
}

impl<S, B> Service<http::Request<B>> for OverrideContentType<S>
where
    S: Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<B>) -> Self::Future {
        req.headers_mut()
            .insert(CONTENT_TYPE, self.content_type.clone());
        self.inner.call(req)
    }
}

#[derive(Clone)]
struct InjectRawBody<S> {
    inner: S,
    raw:   Vec<u8>,
}

impl<S> Service<http::Request<tonic::body::Body>> for InjectRawBody<S>
where
    S: Service<http::Request<tonic::body::Body>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<tonic::body::Body>) -> Self::Future {
        let (parts, _body) = req.into_parts();
        let body = tonic::body::Body::new(axum::body::Body::from(self.raw.clone()));
        self.inner.call(http::Request::from_parts(parts, body))
    }
}

#[gtest]
#[tokio::test]
async fn grpc_service_accepts_mixed_case_content_type_with_params() {
    let db = TestEnv::new().build_db();
    let project = db.project();

    let svc = OverrideContentType {
        inner: emulator_grpc::service(project),
        content_type: HeaderValue::from_static("Application/Grpc+proto; charset=utf-8"),
    };
    let mut client = FirestoreClient::with_origin(svc, Uri::from_static("http://localhost"));
    let created = client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("ct/d1"),
                fields: fields!({ "ok": true }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
        })
        .await
        .expect("Tonic FirestoreServer accepts mixed-case gRPC content-type")
        .into_inner();
    assert_doc_wire_eq(&created, "ct/d1", &fields!({ "ok": true }));
}

#[gtest]
#[tokio::test]
async fn listen_surfaces_malformed_protobuf_frame_as_grpc_error() {
    let db = TestEnv::new().build_db();
    let project = db.project();

    // Valid 5-byte gRPC frame header (len = 3) followed by truncated protobuf varint `0xFF 0xFF
    // 0xFF`.
    let svc = InjectRawBody {
        inner: emulator_grpc::service(project),
        raw:   vec![0, 0, 0, 0, 3, 0xFF, 0xFF, 0xFF],
    };
    let mut client = FirestoreClient::with_origin(svc, Uri::from_static("http://localhost"));
    let mut stream = client
        .listen(futures::stream::empty())
        .await
        .expect("Listen RPC stream opens")
        .into_inner();
    let err = stream
        .next()
        .await
        .expect("malformed request frame must yield an error item, not clean EOF")
        .expect_err("malformed request frame must fail with gRPC Status");
    assert_ne!(err.code(), tonic::Code::Ok);
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_routes_case_insensitive_grpc_content_type_and_rejects_grpc_web() {
    let db = TestEnv::new().build_db();
    let project = db.project();

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral loopback port");
    let addr = listener.local_addr().expect("listener local_addr");
    let base_url = format!("http://{addr}");

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        firestore_emulator::run(
            project,
            listener,
            async move {
                let _unused = shutdown_rx.await;
            },
            DefaultTracing::start(),
        )
        .await
    });

    let channel = tonic::transport::Endpoint::from_shared(base_url.clone())
        .expect("valid endpoint URI")
        .tcp_nodelay(true)
        .connect()
        .await
        .expect("connect Tonic channel");

    let wrapped = OverrideContentType {
        inner: channel.clone(),
        content_type: HeaderValue::from_static("Application/Grpc+proto; charset=utf-8"),
    };
    let mut grpc_client = FirestoreClient::new(wrapped);
    grpc_client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("ct_mux/doc1"),
                fields: fields!({ "via": "Application/Grpc" }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
        })
        .await
        .expect("mixed-case gRPC Content-Type routes to Tonic over multiplexed TCP");

    let got = grpc_client
        .get_document(GetDocumentRequest {
            name: doc_path("ct_mux/doc1"),
            mask: None,
            consistency_selector: None,
        })
        .await
        .expect("GetDocument succeeds")
        .into_inner();
    assert_doc_wire_eq(&got, "ct_mux/doc1", &fields!({ "via": "Application/Grpc" }));

    // `application/grpc-web` must NOT match gRPC routing and instead hits Axum 404 JSON.
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("build no-proxy reqwest client");
    let non_grpc_resp = http_client
        .post(format!(
            "{base_url}/google.firestore.v1.Firestore/GetDocument"
        ))
        .header(CONTENT_TYPE, "application/grpc-web")
        .body(Vec::<u8>::new())
        .send()
        .await
        .expect("send non-gRPC request");
    expect_eq!(non_grpc_resp.status(), StatusCode::NOT_FOUND);

    // Drop clients before shutdown.
    drop(grpc_client);
    drop(channel);
    drop(http_client);
    shutdown_tx.send(()).expect("send shutdown signal");
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server exited within 5s")
        .expect("server task did not panic")
        .expect("server exited cleanly");
}
