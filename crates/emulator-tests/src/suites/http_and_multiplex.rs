use std::{
    collections::HashMap,
    net::SocketAddr,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use bytes::Bytes;
use emulator_database::FirestoreProject;
use emulator_tracing::DefaultTracing;
use googleapis::{
    fields,
    google::firestore::v1::{
        BeginTransactionRequest, Document, GetDocumentRequest, ListenRequest, ListenResponse,
        Target, TransactionOptions, UpdateDocumentRequest,
        firestore_client::FirestoreClient,
        get_document_request, listen_request,
        listen_response::ResponseType,
        target::{self, DocumentsTarget},
        target_change::TargetChangeType,
    },
};
use googletest::prelude::*;
use http::{
    HeaderValue, Method, StatusCode, Uri,
    header::{self, CONTENT_TYPE},
};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde_json::json;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tower::Service;

use crate::harness::{
    builders::{QuerySpec, doc_path},
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
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
async fn dynamic_logging_caps_active_handles_with_lru_eviction() {
    const DEFAULT_MAX_HANDLES: usize = 64;
    let db = TestEnv::new().build_db();

    for i in 0..DEFAULT_MAX_HANDLES {
        let uri = format!("/emulator/v1/logging/cap-{i}");
        let (status, body) = db
            .http_raw_body(Method::POST, &uri, Some("text/plain"), "error")
            .await;
        expect_eq!(status, StatusCode::OK);
        expect_eq!(body.as_ref(), b"");
    }

    // Refresh `cap-0` via `:poll` so it moves to MRU and `cap-1` becomes LRU.
    let (refresh_status, _) = db
        .http_raw_body(Method::POST, "/emulator/v1/logging/cap-0:poll", None, "")
        .await;
    expect_eq!(refresh_status, StatusCode::OK);

    // Opening one more handle at capacity evicts `cap-1` (LRU) while keeping `cap-0`.
    let overflow_uri = format!("/emulator/v1/logging/cap-{DEFAULT_MAX_HANDLES}");
    let (overflow_status, overflow_body) = db
        .http_raw_body(Method::POST, &overflow_uri, Some("text/plain"), "error")
        .await;
    expect_eq!(overflow_status, StatusCode::OK);
    expect_eq!(overflow_body.as_ref(), b"");

    let (evicted_poll, _) = db
        .http_raw_body(Method::POST, "/emulator/v1/logging/cap-1:poll", None, "")
        .await;
    expect_eq!(evicted_poll, StatusCode::NOT_FOUND);

    // `cap-0` and `cap-2..=cap-64` remain active and poll cleanly.
    for i in std::iter::once(0).chain(2..=DEFAULT_MAX_HANDLES) {
        let poll_uri = format!("/emulator/v1/logging/cap-{i}:poll");
        let (poll_status, _) = db.http_raw_body(Method::POST, &poll_uri, None, "").await;
        expect_eq!(poll_status, StatusCode::OK);
    }

    // Cleanly delete all remaining handles so shared-process test runs leave no dynamic layers.
    for i in std::iter::once(0).chain(2..=DEFAULT_MAX_HANDLES) {
        let uri = format!("/emulator/v1/logging/cap-{i}");
        let (del_status, _) = db.http_oneshot(Method::DELETE, &uri, None).await;
        expect_eq!(del_status, StatusCode::OK);
    }
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

async fn send_http1_raw_request(
    addr: SocketAddr,
    method: Method,
    path_and_query: &str,
    content_type: Option<&str>,
    payload: Bytes,
) -> (StatusCode, Bytes) {
    let stream = TcpStream::connect(addr)
        .await
        .expect("connect HTTP/1.1 TCP stream to multiplexed server");
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1.1 client handshake");
    let conn_task = tokio::spawn(conn);

    let mut builder = http::Request::builder()
        .method(method)
        .uri(path_and_query)
        .header(header::HOST, addr.to_string())
        .header(header::CONNECTION, "close");
    if let Some(content_type) = content_type {
        builder = builder.header(CONTENT_TYPE, content_type);
    }
    let req = builder
        .body(Full::new(payload))
        .expect("build HTTP/1.1 request");
    let resp = sender
        .send_request(req)
        .await
        .expect("send HTTP/1.1 request over multiplexed TCP");
    let status = resp.status();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("read HTTP/1.1 response body")
        .to_bytes();
    drop(sender);
    conn_task
        .await
        .expect("HTTP/1.1 connection task did not panic")
        .expect("HTTP/1.1 connection completed cleanly");
    (status, bytes)
}

async fn send_http1_request(
    addr: SocketAddr,
    method: Method,
    path_and_query: &str,
    json_body: Option<&serde_json::Value>,
) -> (StatusCode, Bytes) {
    let (content_type, payload) = if let Some(val) = json_body {
        (
            Some("application/json"),
            Bytes::from(serde_json::to_vec(val).expect("serialize JSON request body")),
        )
    } else {
        (None, Bytes::new())
    };
    send_http1_raw_request(addr, method, path_and_query, content_type, payload).await
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

    // Connect a real HTTP/2 Tonic gRPC client AND an HTTP/1.1 hyper client to the SAME port.
    let endpoint = tonic::transport::Endpoint::from_shared(base_url)
        .expect("valid endpoint URI")
        .tcp_nodelay(true);
    let channel = endpoint
        .connect()
        .await
        .expect("connect Tonic channel to multiplexed server");
    let mut grpc_client = FirestoreClient::new(channel);

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
            request_options: None,
        })
        .await
        .expect("gRPC UpdateDocument over multiplexed TCP");

    let (rest_get_status, rest_get_bytes) = send_http1_request(
        addr,
        Method::GET,
        &format!("/v1/{DOCUMENTS_ROOT}/mux/from-grpc"),
        None,
    )
    .await;
    expect_eq!(rest_get_status, StatusCode::OK);
    let rest_json: serde_json::Value = serde_json::from_slice(&rest_get_bytes).unwrap();
    expect_eq!(rest_json["fields"]["origin"]["stringValue"], "grpc");
    expect_eq!(rest_json["fields"]["n"]["integerValue"], "7");

    // 2. Write over HTTP/1.1 REST -> Read over gRPC + inspect `/emulator/v1` metadata.
    let (rest_post_status, _) = send_http1_request(
        addr,
        Method::POST,
        &format!("/v1/{DOCUMENTS_ROOT}/mux?documentId=from-http"),
        Some(&json!({
            "fields": {
                "origin": { "stringValue": "http" },
                "n": { "integerValue": "11" }
            }
        })),
    )
    .await;
    expect_eq!(rest_post_status, StatusCode::OK);

    let grpc_got = grpc_client
        .get_document(GetDocumentRequest {
            name: doc_path("mux/from-http"),
            mask: None,
            consistency_selector: None,
            request_options: None,
        })
        .await
        .expect("gRPC GetDocument over multiplexed TCP")
        .into_inner();
    assert_doc_wire_eq(
        &grpc_got,
        "mux/from-http",
        &fields!({ "origin": "http", "n": 11i64 }),
    );

    let (dbs_status, dbs_bytes) = send_http1_request(addr, Method::GET, "/emulator/v1", None).await;
    expect_eq!(dbs_status, StatusCode::OK);
    let dbs: Vec<String> = serde_json::from_slice(&dbs_bytes).unwrap();
    expect_eq!(dbs, vec![DOCUMENTS_ROOT.to_string()]);

    // 3. Drop active client connections, fire shutdown signal, and assert clean server exit.
    drop(grpc_client);
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

async fn spawn_loopback_server(
    project: &'static FirestoreProject,
    shutdown_grace: Option<Duration>,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    JoinHandle<firestore_emulator::Result<()>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral loopback port");
    let addr = listener.local_addr().expect("listener local_addr");
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let server_task = tokio::spawn(async move {
        let shutdown = async move {
            drop(shutdown_rx.await);
        };
        match shutdown_grace {
            Some(grace) => {
                firestore_emulator::run_with_shutdown_grace(
                    project,
                    listener,
                    shutdown,
                    grace,
                    DefaultTracing::start(),
                )
                .await
            }
            None => {
                firestore_emulator::run(project, listener, shutdown, DefaultTracing::start()).await
            }
        }
    });
    (addr, shutdown_tx, server_task)
}

async fn connect_grpc_channel(addr: SocketAddr) -> tonic::transport::Channel {
    tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("valid endpoint URI")
        .tcp_nodelay(true)
        .connect()
        .await
        .expect("connect Tonic channel to multiplexed server")
}

struct Http1SseClient {
    sender:    hyper::client::conn::http1::SendRequest<Full<Bytes>>,
    body:      hyper::body::Incoming,
    close_tx:  oneshot::Sender<()>,
    conn_task: JoinHandle<Option<hyper::Result<()>>>,
}

impl Http1SseClient {
    async fn connect(addr: SocketAddr) -> Self {
        Self::connect_path(addr, "/emulator/v1/events").await
    }

    async fn connect_path(addr: SocketAddr, path: &str) -> Self {
        let stream = TcpStream::connect(addr)
            .await
            .expect("connect HTTP/1.1 SSE TCP stream");
        stream.set_nodelay(true).expect("set TCP_NODELAY");
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .expect("HTTP/1.1 SSE client handshake");
        let (close_tx, close_rx) = oneshot::channel::<()>();
        let conn_task = tokio::spawn(async move {
            tokio::select! {
                res = conn => Some(res),
                _ = close_rx => None,
            }
        });

        let req = http::Request::builder()
            .method(Method::GET)
            .uri(path)
            .header(header::HOST, addr.to_string())
            .body(Full::new(Bytes::new()))
            .expect("build HTTP/1.1 SSE request");
        let resp = sender
            .send_request(req)
            .await
            .expect("send HTTP/1.1 SSE request");
        assert_eq!(resp.status(), StatusCode::OK);

        Self {
            sender,
            body: resp.into_body(),
            close_tx,
            conn_task,
        }
    }

    async fn wait_for_event(&mut self, needle: &str, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut received = String::new();
        while !received.contains(needle) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for SSE event containing {needle:?}; saw {received:?}"
            );
            let frame = tokio::time::timeout(remaining, self.body.frame())
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "timed out waiting for SSE frame containing {needle:?}; saw {received:?}"
                    )
                })
                .expect("SSE stream ended before expected event arrived")
                .expect("SSE stream yielded error before expected event arrived");
            if let Ok(data) = frame.into_data() {
                received.push_str(&String::from_utf8_lossy(&data));
            }
        }
    }

    async fn assert_terminates(mut self, timeout: Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            assert!(
                !remaining.is_zero(),
                "SSE stream hung past {timeout:?} after server shutdown"
            );
            match tokio::time::timeout(remaining, self.body.frame()).await {
                Ok(Some(Ok(_frame))) => continue,
                Ok(None) | Ok(Some(Err(_))) => break,
                Err(_) => panic!("SSE stream hung after server shutdown"),
            }
        }
        drop(self.body);
        drop(self.sender);
        let remaining = deadline
            .saturating_duration_since(tokio::time::Instant::now())
            .max(Duration::from_millis(50));
        let conn_res = tokio::time::timeout(remaining, self.conn_task)
            .await
            .expect("HTTP/1.1 SSE connection task hung after server shutdown")
            .expect("HTTP/1.1 SSE connection task did not panic");
        drop(conn_res);
        drop(self.close_tx);
    }

    async fn disconnect(self) {
        drop(self.body);
        drop(self.sender);
        let _unused = self.close_tx.send(());
        let _unused = tokio::time::timeout(Duration::from_secs(2), self.conn_task)
            .await
            .expect("SSE client connection closed cleanly")
            .expect("SSE client connection task did not panic");
    }
}

async fn open_active_listen_stream(
    client: &mut FirestoreClient<tonic::transport::Channel>,
    rel_doc: &str,
) -> (
    mpsc::Sender<ListenRequest>,
    tonic::Streaming<ListenResponse>,
) {
    let (req_tx, req_rx) = mpsc::channel(4);
    let mut listen_stream = client
        .listen(ReceiverStream::new(req_rx))
        .await
        .expect("open gRPC Listen stream")
        .into_inner();
    req_tx
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Documents(DocumentsTarget {
                    documents: vec![doc_path(rel_doc)],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .expect("send AddTarget request");

    loop {
        let frame = tokio::time::timeout(Duration::from_secs(2), listen_stream.message())
            .await
            .expect("timed out waiting for initial Listen TargetChange")
            .expect("initial Listen stream failed")
            .expect("Listen stream ended before initial TargetChange(NoChange)");
        if let Some(ResponseType::TargetChange(tc)) = frame.response_type
            && tc.target_change_type == TargetChangeType::NoChange as i32
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    (req_tx, listen_stream)
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_shuts_down_cleanly_with_active_sse_and_listen_streams() {
    let db = TestEnv::new().build_db();
    let (addr, shutdown_tx, server_task) = spawn_loopback_server(db.project(), None).await;

    // 1. Open an HTTP/1.1 SSE stream and verify a live database write produces an SSE frame.
    let mut sse_client = Http1SseClient::connect(addr).await;

    let channel = connect_grpc_channel(addr).await;
    let mut grpc_client = FirestoreClient::new(channel);
    grpc_client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("mux/sse-trigger"),
                fields: fields!({ "active": true }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
            request_options: None,
        })
        .await
        .expect("write document to trigger SSE frame");
    sse_client
        .wait_for_event("\"documents_changed\"", Duration::from_secs(2))
        .await;

    // 2. Open a gRPC Listen bidirectional stream and verify the server Listener reaches NoChange.
    let (req_tx, mut listen_stream) =
        open_active_listen_stream(&mut grpc_client, "mux/listen-watch").await;

    // 3. Fire shutdown WITHOUT dropping the SSE client, gRPC client, or Listen req_tx.
    shutdown_tx.send(()).expect("send shutdown signal");

    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("firestore_emulator::run exited within 2s with active SSE + Listen streams")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "firestore_emulator::run returned error: {run_result:?}"
    );

    // 4. Both active streams and the client-side outbound Listen channel must close promptly on
    //    server HTTP/2 + TCP teardown.
    sse_client.assert_terminates(Duration::from_secs(2)).await;

    let listen_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = listen_deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "gRPC Listen stream hung past 2s after server shutdown"
        );
        match tokio::time::timeout(remaining, listen_stream.message()).await {
            Ok(Ok(Some(_frame))) => continue,
            Ok(Ok(None)) | Ok(Err(_)) => break,
            Err(_) => panic!("gRPC Listen stream hung after server shutdown"),
        }
    }

    tokio::time::timeout(Duration::from_secs(2), req_tx.closed())
        .await
        .expect("client-side Listen outbound channel must close when server aborts HTTP/2 stream");
    drop(listen_stream);
    drop(grpc_client);
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_exits_immediately_on_shutdown_when_idle_despite_long_grace() {
    let db = TestEnv::new().build_db();
    let (addr, shutdown_tx, server_task) =
        spawn_loopback_server(db.project(), Some(Duration::from_secs(10))).await;

    // Exercise both protocols briefly, then drop all clients so no active streams remain.
    let channel = connect_grpc_channel(addr).await;
    let mut grpc_client = FirestoreClient::new(channel);
    grpc_client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("grace/idle"),
                fields: fields!({ "ok": true }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
            request_options: None,
        })
        .await
        .expect("unary gRPC write before idle shutdown");
    drop(grpc_client);

    let (status, _) = send_http1_request(addr, Method::GET, "/emulator/v1", None).await;
    expect_eq!(status, StatusCode::OK);

    let started = Instant::now();
    shutdown_tx.send(()).expect("send shutdown signal");

    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("idle server must exit immediately without waiting out 10s grace")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "run_with_shutdown_grace returned error: {run_result:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "idle shutdown took {:?}",
        started.elapsed()
    );
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_contention_sse_emits_events_and_closes_promptly_on_shutdown() {
    let mut db = TestEnv::new().build_db();
    let project = db.project();
    let (addr, shutdown_tx, server_task) =
        spawn_loopback_server(project, Some(Duration::from_millis(200))).await;

    let mut sse_client = Http1SseClient::connect_path(addr, "/emulator/v1/contention/events").await;
    assert_eq!(project.contention_subscriber_count(), 1);

    // Park a conflicting transaction commit behind an active read lock to emit
    // `contention_changed`.
    db.put("mux/contend", fields!({ "v": 0i64 })).await.unwrap();
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    let _unused = txn1.get("mux/contend").await.unwrap();

    let mut actor2 = db.clone();
    let blocked_write = tokio::spawn(async move {
        let mut txn2 = actor2.begin_rw_txn().await.unwrap();
        txn2.put("mux/contend", fields!({ "v": 1i64 }));
        txn2.commit().await
    });

    sse_client
        .wait_for_event("\"contention_changed\"", Duration::from_secs(2))
        .await;

    // Fire shutdown without dropping the SSE client or rolling back `txn1`.
    shutdown_tx.send(()).expect("send shutdown signal");
    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server exited within 2s with open contention SSE stream")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "firestore_emulator::run returned error: {run_result:?}"
    );

    sse_client.assert_terminates(Duration::from_secs(2)).await;
    drop(txn1);
    let _unused = tokio::time::timeout(Duration::from_secs(2), blocked_write).await;
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_preserves_grace_window_and_exits_promptly_on_clean_disconnect() {
    let mut db = TestEnv::new().build_db();
    let (addr, shutdown_tx, mut server_task) =
        spawn_loopback_server(db.project(), Some(Duration::from_secs(10))).await;

    let mut sse_client = Http1SseClient::connect(addr).await;
    let channel = connect_grpc_channel(addr).await;
    let mut grpc_client = FirestoreClient::new(channel.clone());
    grpc_client
        .update_document(UpdateDocumentRequest {
            document: Some(Document {
                name: doc_path("grace/active"),
                fields: fields!({ "n": 1i64 }),
                create_time: None,
                update_time: None,
            }),
            update_mask: None,
            mask: None,
            current_document: None,
            request_options: None,
        })
        .await
        .expect("write doc to verify active SSE");
    sse_client
        .wait_for_event("\"documents_changed\"", Duration::from_secs(2))
        .await;

    let (req_tx, mut listen_stream) =
        open_active_listen_stream(&mut grpc_client, "grace/active").await;

    // 1. Fire shutdown: server must stay alive in the 10s grace window while streams remain open.
    shutdown_tx.send(()).expect("send shutdown signal");
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut server_task)
            .await
            .is_err(),
        "server must preserve grace window while SSE and Listen streams stay open"
    );
    assert!(
        !req_tx.is_closed(),
        "client-side Listen outbound stream must stay open during grace window"
    );

    // Live mutations occurring inside the grace window still flow to active SSE & Listen clients.
    db.put("grace/active", fields!({ "n": 2i64 }))
        .await
        .expect("in-process write during grace window");
    sse_client
        .wait_for_event("\"documents_changed\"", Duration::from_secs(2))
        .await;
    let live_doc_msg = tokio::time::timeout(Duration::from_secs(2), listen_stream.message())
        .await
        .expect("Listen update arrived during grace window")
        .expect("Listen stream healthy during grace")
        .expect("Listen frame present");
    assert!(
        matches!(
            live_doc_msg.response_type,
            Some(ResponseType::DocumentChange(_))
        ),
        "expected DocumentChange during grace window, got {live_doc_msg:?}"
    );

    // 2. Disconnect SSE first: server must still wait while the gRPC Listen stream remains open.
    sse_client.disconnect().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut server_task)
            .await
            .is_err(),
        "server must remain in grace window while gRPC Listen stream is still open"
    );

    // 3. Cleanly disconnect the Listen stream and channel before grace expires: server exits fast.
    let disconnected_at = Instant::now();
    drop(req_tx);
    drop(listen_stream);
    drop(grpc_client);
    drop(channel);

    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server must exit promptly upon clean client disconnect inside grace window")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "run_with_shutdown_grace returned error: {run_result:?}"
    );
    assert!(
        disconnected_at.elapsed() < Duration::from_secs(2),
        "clean disconnect shutdown waited {:?}",
        disconnected_at.elapsed()
    );
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_shuts_down_cleanly_with_stalled_raw_tcp_connections() {
    let db = TestEnv::new().build_db();
    let (addr, shutdown_tx, server_task) = spawn_loopback_server(db.project(), None).await;

    // 1. Silent TCP socket that never sends protocol bytes.
    let mut silent_tcp = TcpStream::connect(addr)
        .await
        .expect("connect silent raw TCP stream");
    // 2. Partial HTTP/1.1 header socket that stalls mid-request line.
    let mut partial_tcp = TcpStream::connect(addr)
        .await
        .expect("connect partial-header raw TCP stream");
    partial_tcp
        .write_all(b"GET /emulator/v1 HTTP/1.1\r\n")
        .await
        .expect("write partial HTTP request headers");
    partial_tcp.flush().await.expect("flush partial headers");
    // 3. HTTP/1.1 POST declaring Content-Length: 1024 whose body stalls after 4 bytes.
    let mut stalled_body_tcp = TcpStream::connect(addr)
        .await
        .expect("connect stalled-body raw TCP stream");
    let stalled_post = format!(
        "POST /v1/{DOCUMENTS_ROOT}/stalled HTTP/1.1\r\nHost: {addr}\r\nContent-Length: \
         1024\r\nContent-Type: application/json\r\n\r\nabcd"
    );
    stalled_body_tcp
        .write_all(stalled_post.as_bytes())
        .await
        .expect("write incomplete HTTP POST body");
    stalled_body_tcp.flush().await.expect("flush stalled body");

    // Deterministic FIFO-accept barrier: this probe cannot succeed until the accept loop has
    // already dequeued and registered the three prior TCP sockets in `GracefulShutdown`.
    let (probe_status, _) = send_http1_request(addr, Method::GET, "/emulator/v1", None).await;
    expect_eq!(probe_status, StatusCode::OK);

    let started = Instant::now();
    shutdown_tx.send(()).expect("send shutdown signal");
    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("firestore_emulator::run exited within 2s despite stalled raw TCP connections")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "firestore_emulator::run returned error: {run_result:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "expected server to hold stalled accepted connections through grace window, exited in {:?}",
        started.elapsed()
    );

    let mut buf = [0u8; 64];
    for (label, sock) in [
        ("silent", &mut silent_tcp),
        ("partial", &mut partial_tcp),
        ("stalled-body", &mut stalled_body_tcp),
    ] {
        let eof = tokio::time::timeout(Duration::from_secs(1), sock.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("{label} TCP socket hung after shutdown"));
        assert!(
            matches!(eof, Ok(0) | Err(_)),
            "{label} TCP socket did not close cleanly: {eof:?}"
        );
    }
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_aborts_blocked_http1_handler_on_lock_contention() {
    let mut db = TestEnv::new().cloud_timeouts().build_db();
    db.put("lock/doc1", fields!({ "n": 1i64 }))
        .await
        .expect("seed lock target document");
    let (addr, shutdown_tx, server_task) = spawn_loopback_server(db.project(), None).await;

    // 1. Open a gRPC pessimistic RW transaction that holds a shared lock on `lock/doc1`.
    let channel = connect_grpc_channel(addr).await;
    let mut grpc_client = FirestoreClient::new(channel);
    let txn_id = grpc_client
        .begin_transaction(BeginTransactionRequest {
            database: DATABASE_ROOT.to_string(),
            options: Some(TransactionOptions::READ_WRITE),
            request_options: None,
        })
        .await
        .expect("begin pessimistic RW transaction")
        .into_inner()
        .transaction;

    grpc_client
        .get_document(GetDocumentRequest {
            name: doc_path("lock/doc1"),
            mask: None,
            consistency_selector: Some(get_document_request::ConsistencySelector::Transaction(
                txn_id,
            )),
            request_options: None,
        })
        .await
        .expect("acquire shared read lock in open transaction");

    // 2. Issue an HTTP/1.1 PATCH on `lock/doc1`: its handler blocks on wound-wait lock contention
    //    (up to 15s under `cloud_timeouts()`).
    let stream = TcpStream::connect(addr)
        .await
        .expect("connect HTTP/1.1 contention client");
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1.1 contention handshake");
    let conn_task = tokio::spawn(conn);
    let patch_req = http::Request::builder()
        .method(Method::PATCH)
        .uri(format!("/v1/{DOCUMENTS_ROOT}/lock/doc1"))
        .header(header::HOST, addr.to_string())
        .header(CONTENT_TYPE, "application/json")
        .header(header::CONNECTION, "close")
        .body(Full::new(Bytes::from(
            serde_json::to_vec(&json!({ "fields": { "n": { "integerValue": "2" } } })).unwrap(),
        )))
        .expect("build HTTP/1.1 PATCH request");
    let mut patch_fut = tokio::spawn(async move { sender.send_request(patch_req).await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !patch_fut.is_finished(),
        "HTTP/1.1 PATCH handler must be blocked on transaction lock before shutdown"
    );

    // 3. Shutdown must cancel the blocked handler after 250ms grace rather than stalling 15s.
    let started = Instant::now();
    shutdown_tx.send(()).expect("send shutdown signal");
    let run_result = tokio::time::timeout(Duration::from_secs(2), server_task)
        .await
        .expect("server must abort blocked HTTP/1.1 handler within 2s rather than waiting 15s")
        .expect("server task did not panic");
    assert!(
        run_result.is_ok(),
        "firestore_emulator::run returned error: {run_result:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(150),
        "expected server to wait out grace window for blocked handler, exited in {:?}",
        started.elapsed()
    );

    let patch_res = tokio::time::timeout(Duration::from_secs(2), &mut patch_fut)
        .await
        .expect("blocked HTTP/1.1 request must fail promptly once server aborts connection")
        .expect("HTTP/1.1 client task did not panic");
    assert!(
        patch_res.is_err(),
        "expected aborted connection error for cancelled HTTP/1.1 handler, got {patch_res:?}"
    );
    let _unused = tokio::time::timeout(Duration::from_secs(2), conn_task)
        .await
        .expect("HTTP/1.1 client connection task exited");
    drop(grpc_client);
}

#[gtest]
#[tokio::test]
async fn loopback_tcp_multiplexer_handles_zero_and_max_shutdown_grace() {
    // 1. `Duration::ZERO` aborts an open SSE stream without hanging.
    let db_zero = TestEnv::new().build_db();
    let (addr_zero, shutdown_zero, server_zero) =
        spawn_loopback_server(db_zero.project(), Some(Duration::ZERO)).await;
    let sse_client = Http1SseClient::connect(addr_zero).await;
    shutdown_zero
        .send(())
        .expect("send shutdown signal for zero grace");
    let zero_res = tokio::time::timeout(Duration::from_secs(2), server_zero)
        .await
        .expect("zero-grace server exited within 2s")
        .expect("zero-grace server task did not panic");
    assert!(zero_res.is_ok(), "zero-grace error: {zero_res:?}");
    sse_client.assert_terminates(Duration::from_secs(2)).await;

    // 2. `Duration::MAX` does not overflow Tokio timers or panic and exits promptly when idle.
    let db_max = TestEnv::new().build_db();
    let (addr_max, shutdown_max, server_max) =
        spawn_loopback_server(db_max.project(), Some(Duration::MAX)).await;
    let (status, _) = send_http1_request(addr_max, Method::GET, "/emulator/v1", None).await;
    expect_eq!(status, StatusCode::OK);
    shutdown_max
        .send(())
        .expect("send shutdown signal for max grace");
    let max_res = tokio::time::timeout(Duration::from_secs(2), server_max)
        .await
        .expect("idle max-grace server exited within 2s")
        .expect("max-grace server task did not panic");
    assert!(max_res.is_ok(), "max-grace error: {max_res:?}");
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
            request_options: None,
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
        .listen(tokio_stream::empty())
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
            request_options: None,
        })
        .await
        .expect("mixed-case gRPC Content-Type routes to Tonic over multiplexed TCP");

    let got = grpc_client
        .get_document(GetDocumentRequest {
            name: doc_path("ct_mux/doc1"),
            mask: None,
            consistency_selector: None,
            request_options: None,
        })
        .await
        .expect("GetDocument succeeds")
        .into_inner();
    assert_doc_wire_eq(&got, "ct_mux/doc1", &fields!({ "via": "Application/Grpc" }));

    // `application/grpc-web` must NOT match gRPC routing and instead hits Axum 404 JSON.
    let (non_grpc_status, _) = send_http1_raw_request(
        addr,
        Method::POST,
        "/google.firestore.v1.Firestore/GetDocument",
        Some("application/grpc-web"),
        Bytes::new(),
    )
    .await;
    expect_eq!(non_grpc_status, StatusCode::NOT_FOUND);

    // Drop clients before shutdown.
    drop(grpc_client);
    drop(channel);
    shutdown_tx.send(()).expect("send shutdown signal");
    tokio::time::timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server exited within 5s")
        .expect("server task did not panic")
        .expect("server exited cleanly");
}

#[gtest]
#[tokio::test]
async fn http_rest_run_query_array_filters_and_validation_across_editions() {
    let mut db = TestEnv::new().build_db();
    db.put(
        "restq/d_num",
        fields!({ "n": 42i64, "tags": [-0.0f64, "rust"] }),
    )
    .await
    .unwrap();
    db.put("restq/d_null", fields!({ "n": null, "tags": ["go"] }))
        .await
        .unwrap();
    db.put("restq/d_missing", fields!({ "tags": ["py"] }))
        .await
        .unwrap();

    let run_query_uri = format!("/v1/{DOCUMENTS_ROOT}:runQuery");
    let extract_ids = |bytes: &[u8]| -> Vec<String> {
        let rows: Vec<serde_json::Value> = serde_json::from_slice(bytes).unwrap();
        rows.into_iter()
            .filter_map(|r| {
                r.get("document")
                    .and_then(|d| d.get("name"))
                    .and_then(|n| n.as_str())
                    .map(|s| s.rsplit('/').next().unwrap().to_string())
            })
            .collect()
    };

    // 1. Standard Edition: `IN` with `integerValue` `"42"` & `doubleValue` `-0.0` (raw JSON
    //    `-0.0`), `ARRAY_CONTAINS_ANY` matching `-0.0` via `integerValue` `"0"`, and `NOT_IN`
    //    excluding missing/null.
    let (status, bytes) = db
        .http_raw_body(
            Method::POST,
            &run_query_uri,
            Some("application/json"),
            r#"{"structuredQuery":{"from":[{"collectionId":"restq"}],"where":{"fieldFilter":{"field":{"fieldPath":"tags"},"op":"ARRAY_CONTAINS_ANY","value":{"arrayValue":{"values":[{"integerValue":"0"},{"stringValue":"nope"}]}}}}}}"#,
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_num"]);

    let (status, bytes) = db
        .http_raw_body(
            Method::POST,
            &run_query_uri,
            Some("application/json"),
            r#"{"structuredQuery":{"from":[{"collectionId":"restq"}],"where":{"fieldFilter":{"field":{"fieldPath":"tags"},"op":"ARRAY_CONTAINS_ANY","value":{"arrayValue":{"values":[{"doubleValue":-0.0}]}}}}}}"#,
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_num"]);

    let (status, bytes) = db
        .http_oneshot(
            Method::POST,
            &run_query_uri,
            Some(&json!({
                "structuredQuery": {
                    "from": [{ "collectionId": "restq" }],
                    "where": {
                        "fieldFilter": {
                            "field": { "fieldPath": "n" },
                            "op": "IN",
                            "value": { "arrayValue": { "values": [{ "integerValue": "42" }] } }
                        }
                    }
                }
            })),
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_num"]);

    let (status, bytes) = db
        .http_oneshot(
            Method::POST,
            &run_query_uri,
            Some(&json!({
                "structuredQuery": {
                    "from": [{ "collectionId": "restq" }],
                    "where": {
                        "fieldFilter": {
                            "field": { "fieldPath": "n" },
                            "op": "NOT_IN",
                            "value": { "arrayValue": { "values": [{ "integerValue": "1" }] } }
                        }
                    }
                }
            })),
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_num"]);

    // 2. Empty `AND` returns 200 OK (all docs), while empty `OR`, Standard > 10 `NOT_IN`, and
    //    reserved paths return 400 Bad Request with `"status": "INVALID_ARGUMENT"`.
    let (status, bytes) = db
        .http_oneshot(
            Method::POST,
            &run_query_uri,
            Some(&json!({
                "structuredQuery": {
                    "from": [{ "collectionId": "restq" }],
                    "where": { "compositeFilter": { "op": "AND", "filters": [] } }
                }
            })),
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_missing", "d_null", "d_num"]);

    let too_many_not_in: Vec<serde_json::Value> = (1..=11)
        .map(|i| json!({ "integerValue": i.to_string() }))
        .collect();
    for bad_where in [
        json!({ "compositeFilter": { "op": "OR", "filters": [] } }),
        json!({
            "fieldFilter": {
                "field": { "fieldPath": "n" },
                "op": "NOT_IN",
                "value": { "arrayValue": { "values": too_many_not_in } }
            }
        }),
        json!({
            "fieldFilter": {
                "field": { "fieldPath": "__custom__" },
                "op": "EQUAL",
                "value": { "integerValue": "1" }
            }
        }),
    ] {
        let (status, bytes) = db
            .http_oneshot(
                Method::POST,
                &run_query_uri,
                Some(&json!({
                    "structuredQuery": {
                        "from": [{ "collectionId": "restq" }],
                        "where": bad_where
                    }
                })),
            )
            .await;
        expect_eq!(status, StatusCode::BAD_REQUEST);
        let err_json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        expect_eq!(err_json["error"]["status"], "INVALID_ARGUMENT");
    }

    // 3. Enterprise Edition via REST: `NOT_IN` includes missing `d_missing` and `null` `d_null`,
    //    and
    //    > 10 `NOT_IN` elements succeed.
    db.set_admin_mode(Some("ENTERPRISE"), None).await;
    let eleven_vals: Vec<serde_json::Value> = (1..=11)
        .map(|i| json!({ "integerValue": i.to_string() }))
        .collect();
    let (status, bytes) = db
        .http_oneshot(
            Method::POST,
            &run_query_uri,
            Some(&json!({
                "structuredQuery": {
                    "from": [{ "collectionId": "restq" }],
                    "where": {
                        "fieldFilter": {
                            "field": { "fieldPath": "n" },
                            "op": "NOT_IN",
                            "value": { "arrayValue": { "values": eleven_vals } }
                        }
                    },
                    "orderBy": [{ "field": { "fieldPath": "__name__" }, "direction": "ASCENDING" }]
                }
            })),
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    expect_eq!(extract_ids(&bytes), vec!["d_missing", "d_null", "d_num"]);
}

#[gtest]
#[tokio::test]
async fn http_rest_run_aggregation_query_cross_protocol_and_editions() {
    let mut db = TestEnv::new().build_db();
    let agg_uri = format!("/v1/{DOCUMENTS_ROOT}:runAggregationQuery");

    // 1. Seed via gRPC (large i64 beyond JS safe-integer range, standard int, and double) and via
    //    HTTP REST (document-parent subcollection), then aggregate over REST.
    db.put(
        "metrics/m1",
        fields!({ "n": 9_007_199_254_740_993_i64, "kind": "big" }),
    )
    .await
    .unwrap();
    db.put("metrics/m2", fields!({ "n": 7_i64, "kind": "big" }))
        .await
        .unwrap();
    db.put("metrics/m3", fields!({ "n": 2.5_f64, "kind": "dbl" }))
        .await
        .unwrap();

    let (sub_create_status, _) = db
        .http_oneshot(
            Method::POST,
            &format!("/v1/{DOCUMENTS_ROOT}/orgs/acme/tasks?documentId=t1"),
            Some(&json!({ "fields": { "pts": { "integerValue": "12" } } })),
        )
        .await;
    expect_eq!(sub_create_status, StatusCode::OK);

    let (status, bytes) = db
        .http_oneshot(
            Method::POST,
            &agg_uri,
            Some(&json!({
                "structuredAggregationQuery": {
                    "structuredQuery": {
                        "from": [{ "collectionId": "metrics" }],
                        "where": {
                            "fieldFilter": {
                                "field": { "fieldPath": "kind" },
                                "op": "EQUAL",
                                "value": { "stringValue": "big" }
                            }
                        }
                    },
                    "aggregations": [
                        { "count": {}, "alias": "cnt" },
                        { "count": { "upTo": "1" }, "alias": "capped" },
                        { "sum": { "field": { "fieldPath": "n" } }, "alias": "sum_i64" },
                        { "avg": { "field": { "fieldPath": "n" } }, "alias": "avg_i64" }
                    ]
                }
            })),
        )
        .await;
    expect_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    expect_eq!(body.as_array().unwrap().len(), 1);
    expect_eq!(
        body[0]["result"]["aggregateFields"],
        json!({
            "avg_i64": { "doubleValue": 4_503_599_627_370_500.0 },
            "capped": { "integerValue": "1" },
            "cnt": { "integerValue": "2" },
            "sum_i64": { "integerValue": "9007199254741000" }
        })
    );

    // Subcollection document-parent REST aggregation
    let (sub_status, sub_bytes) = db
        .http_oneshot(
            Method::POST,
            &format!("/v1/{DOCUMENTS_ROOT}/orgs/acme:runAggregationQuery"),
            Some(&json!({
                "structuredAggregationQuery": {
                    "structuredQuery": { "from": [{ "collectionId": "tasks" }] },
                    "aggregations": [
                        { "count": {} },
                        { "sum": { "field": { "fieldPath": "pts" } } }
                    ]
                }
            })),
        )
        .await;
    expect_eq!(sub_status, StatusCode::OK);
    let sub_body: serde_json::Value = serde_json::from_slice(&sub_bytes).unwrap();
    expect_eq!(
        sub_body[0]["result"]["aggregateFields"],
        json!({
            "field_1": { "integerValue": "1" },
            "field_2": { "integerValue": "12" }
        })
    );

    // 2. Standard vs Enterprise empty SUM and i64 overflow semantics over REST.
    db.put("overflow_cancel/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("overflow_cancel/d1", fields!({ "v": 1_i64 }))
        .await
        .unwrap();
    db.put("overflow_cancel/d2", fields!({ "v": -1_i64 }))
        .await
        .unwrap();
    db.put("overflow_dbl/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("overflow_dbl/d1", fields!({ "v": i64::MAX }))
        .await
        .unwrap();

    let empty_agg_req = json!({
        "structuredAggregationQuery": {
            "structuredQuery": { "from": [{ "collectionId": "no_such_col" }] },
            "aggregations": [
                { "sum": { "field": { "fieldPath": "v" } }, "alias": "s" },
                { "avg": { "field": { "fieldPath": "v" } }, "alias": "a" }
            ]
        }
    });
    let (std_empty_st, std_empty_bytes) = db
        .http_oneshot(Method::POST, &agg_uri, Some(&empty_agg_req))
        .await;
    expect_eq!(std_empty_st, StatusCode::OK);
    let std_empty: serde_json::Value = serde_json::from_slice(&std_empty_bytes).unwrap();
    expect_eq!(
        std_empty[0]["result"]["aggregateFields"],
        json!({ "s": { "integerValue": "0" }, "a": { "nullValue": null } })
    );

    // Standard overflow cancellation returns exact i64::MAX, and double-overflow promotes to
    // doubleValue.
    let (std_cancel_st, std_cancel_bytes) = db
        .http_oneshot(
            Method::POST,
            &agg_uri,
            Some(&json!({
                "structuredAggregationQuery": {
                    "structuredQuery": {
                        "from": [{ "collectionId": "overflow_cancel" }],
                        "orderBy": [{ "field": { "fieldPath": "__name__" }, "direction": "ASCENDING" }]
                    },
                    "aggregations": [{ "sum": { "field": { "fieldPath": "v" } }, "alias": "s" }]
                }
            })),
        )
        .await;
    expect_eq!(std_cancel_st, StatusCode::OK);
    let std_cancel: serde_json::Value = serde_json::from_slice(&std_cancel_bytes).unwrap();
    expect_eq!(
        std_cancel[0]["result"]["aggregateFields"]["s"]["integerValue"],
        i64::MAX.to_string()
    );

    let (std_dbl_st, std_dbl_bytes) = db
        .http_oneshot(
            Method::POST,
            &agg_uri,
            Some(&json!({
                "structuredAggregationQuery": {
                    "structuredQuery": { "from": [{ "collectionId": "overflow_dbl" }] },
                    "aggregations": [{ "sum": { "field": { "fieldPath": "v" } }, "alias": "s" }]
                }
            })),
        )
        .await;
    expect_eq!(std_dbl_st, StatusCode::OK);
    let std_dbl: serde_json::Value = serde_json::from_slice(&std_dbl_bytes).unwrap();
    expect_eq!(
        std_dbl[0]["result"]["aggregateFields"]["s"]["doubleValue"],
        18_446_744_073_709_551_616.0
    );

    // Switch to Enterprise via HTTP Admin PATCH.
    db.set_admin_mode(Some("ENTERPRISE"), None).await;

    let (ent_empty_st, ent_empty_bytes) = db
        .http_oneshot(Method::POST, &agg_uri, Some(&empty_agg_req))
        .await;
    expect_eq!(ent_empty_st, StatusCode::OK);
    let ent_empty: serde_json::Value = serde_json::from_slice(&ent_empty_bytes).unwrap();
    expect_eq!(
        ent_empty[0]["result"]["aggregateFields"],
        json!({ "s": { "nullValue": null }, "a": { "nullValue": null } })
    );

    // Enterprise sequential i64 overflow (`[i64::MAX, 1, -1]`) rejects with structured 400
    // `google.rpc.Status` JSON error (`INVALID_ARGUMENT`).
    let (ent_ovf_st, ent_ovf_bytes) = db
        .http_oneshot(
            Method::POST,
            &agg_uri,
            Some(&json!({
                "structuredAggregationQuery": {
                    "structuredQuery": {
                        "from": [{ "collectionId": "overflow_cancel" }],
                        "orderBy": [{ "field": { "fieldPath": "__name__" }, "direction": "ASCENDING" }]
                    },
                    "aggregations": [{ "sum": { "field": { "fieldPath": "v" } }, "alias": "s" }]
                }
            })),
        )
        .await;
    expect_eq!(ent_ovf_st, StatusCode::BAD_REQUEST);
    let ent_ovf_err: serde_json::Value = serde_json::from_slice(&ent_ovf_bytes).unwrap();
    expect_eq!(ent_ovf_err["error"]["code"], 400);
    expect_eq!(ent_ovf_err["error"]["status"], "INVALID_ARGUMENT");
    expect_that!(
        ent_ovf_err["error"]["message"].as_str().unwrap_or(""),
        contains_substring("Long overflow")
    );

    // 3. Structured `google.rpc.Status` JSON error objects for 400 and 501 over REST.
    for (bad_body, expected_http, expected_code, expected_status) in [
        (
            json!({
                "structuredAggregationQuery": {
                    "structuredQuery": { "from": [{ "collectionId": "metrics" }] },
                    "aggregations": [{ "count": { "upTo": "1" } }]
                }
            }),
            StatusCode::BAD_REQUEST,
            400,
            "INVALID_ARGUMENT",
        ),
        (
            json!({
                "structuredAggregationQuery": {
                    "structuredQuery": {
                        "from": [{ "collectionId": "metrics" }],
                        "findNearest": { "limit": 1 }
                    },
                    "aggregations": [{ "count": {} }]
                }
            }),
            StatusCode::NOT_IMPLEMENTED,
            501,
            "UNIMPLEMENTED",
        ),
        (
            json!({
                "newTransaction": { "readWrite": {} },
                "structuredAggregationQuery": {
                    "structuredQuery": { "from": [{ "collectionId": "metrics" }] },
                    "aggregations": [{ "count": {} }]
                }
            }),
            StatusCode::NOT_IMPLEMENTED,
            501,
            "UNIMPLEMENTED",
        ),
        (
            json!({
                "transaction": "AAEC",
                "structuredAggregationQuery": {
                    "structuredQuery": { "from": [{ "collectionId": "metrics" }] },
                    "aggregations": [{ "count": {} }]
                }
            }),
            StatusCode::NOT_IMPLEMENTED,
            501,
            "UNIMPLEMENTED",
        ),
    ] {
        let (st, err_bytes) = db
            .http_oneshot(Method::POST, &agg_uri, Some(&bad_body))
            .await;
        expect_eq!(st, expected_http);
        let err: serde_json::Value = serde_json::from_slice(&err_bytes).unwrap();
        expect_eq!(err["error"]["code"], expected_code);
        expect_eq!(err["error"]["status"], expected_status);
        expect_true!(!err["error"]["message"].as_str().unwrap_or("").is_empty());
    }

    // 4. Proto3 JSON `"NaN"`, `"Infinity"`, and `"-Infinity"` `doubleValue` serialization from
    //    `SUM` / `AVG` over `:runAggregationQuery` (seeded via gRPC).
    db.put("non_finite_nan/d1", fields!({ "v": f64::NAN }))
        .await
        .unwrap();
    db.put("non_finite_nan/d2", fields!({ "v": 5_i64 }))
        .await
        .unwrap();
    db.put("non_finite_inf/d1", fields!({ "v": f64::INFINITY }))
        .await
        .unwrap();
    db.put("non_finite_inf/d2", fields!({ "v": 1.0_f64 }))
        .await
        .unwrap();
    db.put("non_finite_neginf/d1", fields!({ "v": f64::NEG_INFINITY }))
        .await
        .unwrap();
    db.put("non_finite_neginf/d2", fields!({ "v": 2_i64 }))
        .await
        .unwrap();
    db.put("non_finite_cancel/d1", fields!({ "v": f64::INFINITY }))
        .await
        .unwrap();
    db.put("non_finite_cancel/d2", fields!({ "v": f64::NEG_INFINITY }))
        .await
        .unwrap();

    for (collection, expected_wire) in [
        ("non_finite_nan", "NaN"),
        ("non_finite_inf", "Infinity"),
        ("non_finite_neginf", "-Infinity"),
        ("non_finite_cancel", "NaN"),
    ] {
        let (nf_st, nf_bytes) = db
            .http_oneshot(
                Method::POST,
                &agg_uri,
                Some(&json!({
                    "structuredAggregationQuery": {
                        "structuredQuery": { "from": [{ "collectionId": collection }] },
                        "aggregations": [
                            { "sum": { "field": { "fieldPath": "v" } }, "alias": "s" },
                            { "avg": { "field": { "fieldPath": "v" } }, "alias": "a" }
                        ]
                    }
                })),
            )
            .await;
        expect_eq!(nf_st, StatusCode::OK, "collection={collection}");
        let nf_json: serde_json::Value = serde_json::from_slice(&nf_bytes).unwrap();
        expect_eq!(
            nf_json[0]["result"]["aggregateFields"],
            json!({
                "s": { "doubleValue": expected_wire },
                "a": { "doubleValue": expected_wire }
            }),
            "collection={collection}"
        );
    }
}
