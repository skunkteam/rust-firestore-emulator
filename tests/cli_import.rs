#![allow(unused_crate_dependencies)]

use std::{
    collections::HashMap,
    fs,
    io::{BufRead, BufReader},
    net::{SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use bytes::Bytes;
use emulator_database::{
    FirestoreConfig, FirestoreProject,
    reference::{CollectionRef, RootRef},
};
use futures_util::StreamExt;
use googleapis::google::{
    firestore::v1::{Value, value::ValueType},
    protobuf::Timestamp,
    r#type::LatLng,
};
use http::{Method, Request, StatusCode, header};
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use serde_json::Value as JsonValue;
use tokio::net::TcpStream;

static NEXT_TMP_ID: AtomicU64 = AtomicU64::new(1);

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(prefix: &str) -> Self {
        let base = Path::new(env!("CARGO_TARGET_TMPDIR"));
        let id = NEXT_TMP_ID.fetch_add(1, Ordering::Relaxed);
        let dir = base.join(format!("cli-import-{prefix}-{}-{id}", std::process::id()));
        fs::create_dir_all(&dir).expect("create unique test directory in CARGO_TARGET_TMPDIR");
        Self(dir)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.0));
    }
}

struct ChildGuard {
    child:  Option<Child>,
    stderr: Option<JoinHandle<()>>,
}

impl ChildGuard {
    fn new(child: Child, stderr: JoinHandle<()>) -> Self {
        Self {
            child:  Some(child),
            stderr: Some(stderr),
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            drop(child.kill());
            drop(child.wait());
        }
        if let Some(handle) = self.stderr.take() {
            drop(handle.join());
        }
    }
}

fn base_command() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_firestore-emulator"));
    cmd.env("RUST_LOG", "ERROR")
        .env_remove("FIRESTORE_EMULATOR_HOST")
        .env_remove("FIRESTORE_EMULATOR_IMPORT")
        .env_remove("FIRESTORE_EMULATOR_DEFAULT_ENTERPRISE")
        .env_remove("FIRESTORE_EMULATOR_TRANSACTION_IDLE_TIMEOUT_MS")
        .env_remove("FIRESTORE_EMULATOR_TRANSACTION_MAX_LIFETIME_MS");
    cmd
}

async fn export_project_bytes(project: &'static FirestoreProject) -> Vec<u8> {
    let plan = project
        .prepare_export(None)
        .await
        .expect("prepare export plan");
    let mut stream = plan.into_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.expect("export stream chunk"));
    }
    bytes
}

struct StartedEmulator {
    addr:   SocketAddr,
    _guard: ChildGuard,
}

fn spawn_and_wait_listening(mut cmd: Command, expected_loaded_line: &str) -> StartedEmulator {
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn firestore-emulator binary");

    let stderr = child.stderr.take().expect("take child stderr pipe");
    let (tx, rx) = mpsc::channel();
    let stderr_reader = thread::spawn(move || {
        for line in BufReader::new(stderr).lines() {
            match line {
                Ok(line) => {
                    // Keep draining stderr to EOF even after `rx` is dropped so the child never
                    // blocks on a full pipe.
                    drop(tx.send(line));
                }
                Err(_) => break,
            }
        }
    });
    let guard = ChildGuard::new(child, stderr_reader);

    let mut lines = Vec::new();
    let mut loaded_idx = None;
    let mut listening_idx = None;
    let mut bound_addr = None;
    let mut saw_eof = false;

    let startup_deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < startup_deadline {
        let remaining = startup_deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(remaining) {
            Ok(trimmed) => {
                if trimmed == expected_loaded_line && loaded_idx.is_none() {
                    loaded_idx = Some(lines.len());
                }
                if let Some(addr_str) = trimmed.strip_prefix("Firestore emulator listening on ") {
                    bound_addr = Some(
                        addr_str
                            .trim()
                            .parse::<SocketAddr>()
                            .expect("parse bound SocketAddr"),
                    );
                    listening_idx = Some(lines.len());
                    lines.push(trimmed);
                    break;
                }
                lines.push(trimmed);
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                saw_eof = true;
                break;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => break,
        }
    }

    let Some(addr) = bound_addr else {
        if saw_eof {
            panic!(
                "emulator exited before listening (premature stderr EOF), stderr lines:\n{}",
                lines.join("\n")
            );
        }
        panic!(
            "emulator did not print listening banner within 10s, stderr lines:\n{}",
            lines.join("\n")
        );
    };

    let loaded_idx = loaded_idx.unwrap_or_else(|| {
        panic!(
            "expected startup summary `{expected_loaded_line}` in stderr, got:\n{}",
            lines.join("\n")
        )
    });
    let listening_idx = listening_idx.expect("listening line recorded");
    assert!(
        loaded_idx < listening_idx,
        "snapshot load message must appear before listening message:\n{}",
        lines.join("\n")
    );

    StartedEmulator {
        addr,
        _guard: guard,
    }
}

async fn http_get_json(addr: SocketAddr, path: &str) -> (StatusCode, JsonValue) {
    let stream = TcpStream::connect(addr)
        .await
        .expect("connect to emulator REST port");
    stream.set_nodelay(true).expect("set TCP_NODELAY");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("HTTP/1.1 handshake");
    let conn_task = tokio::spawn(conn);

    let req = Request::builder()
        .method(Method::GET)
        .uri(path)
        .header(header::HOST, addr.to_string())
        .header(header::CONNECTION, "close")
        .body(Empty::<Bytes>::new())
        .expect("build GET request");

    let resp = sender.send_request(req).await.expect("send GET request");
    let status = resp.status();
    let body = resp
        .into_body()
        .collect()
        .await
        .expect("collect REST response body")
        .to_bytes();
    drop(sender);
    conn_task
        .await
        .expect("HTTP/1.1 connection task did not panic")
        .expect("HTTP/1.1 connection completed cleanly");

    let json = serde_json::from_slice(&body).expect("parse REST JSON body");
    (status, json)
}

#[tokio::test]
async fn cli_import_flag_preloads_databases_settings_and_documents_before_listening() {
    let dir = TempDirGuard::new("flag-preload");
    let snapshot_path = dir.path().join("checkpoint.fsemu");

    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));

    let default_root: RootRef = "projects/demo/databases/(default)".parse().unwrap();
    let analytics_root: RootRef = "projects/demo/databases/analytics".parse().unwrap();
    let audit_root: RootRef = "projects/demo/databases/audit".parse().unwrap();

    // 1. Enterprise/Optimistic database with rich typed document, zero-field doc, and
    //    phantom-parent subcollection doc.
    let db_default = project.database(&default_root).await;
    db_default.set_config(true, true);

    let users_col: CollectionRef = format!("{default_root}/users").parse().unwrap();
    let alice_fields = HashMap::from([
        ("null_val".to_string(), Value::null()),
        ("bool_val".to_string(), Value::boolean(true)),
        ("int_val".to_string(), Value::integer(i64::MAX)),
        ("double_val".to_string(), Value::double(12.5)),
        (
            "ts_val".to_string(),
            Value::timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos:   123_456_789,
            }),
        ),
        ("str_val".to_string(), Value::string("alice")),
        (
            "bytes_val".to_string(),
            Value::bytes(Bytes::from_static(b"\x01\x02\x03")),
        ),
        (
            "ref_val".to_string(),
            Value::reference(format!("{default_root}/flags/ready")),
        ),
        (
            "geo_val".to_string(),
            Value {
                value_type: Some(ValueType::GeoPointValue(LatLng {
                    latitude:  52.3676,
                    longitude: 4.9041,
                })),
            },
        ),
        (
            "arr_val".to_string(),
            Value::array(vec![Value::integer(1), Value::string("two")]),
        ),
        (
            "map_val".to_string(),
            Value::map(HashMap::from([(
                "nested".to_string(),
                Value::boolean(false),
            )])),
        ),
        ("vec_val".to_string(), Value::vector([1.0, 2.0, 3.0])),
    ]);
    db_default
        .create_doc(&users_col, "alice", alice_fields, None)
        .await
        .unwrap();

    let flags_col: CollectionRef = format!("{default_root}/flags").parse().unwrap();
    db_default
        .create_doc(&flags_col, "ready", HashMap::new(), None)
        .await
        .unwrap();

    let msg_col: CollectionRef = format!("{default_root}/rooms/r1/messages").parse().unwrap();
    db_default
        .create_doc(
            &msg_col,
            "m1",
            HashMap::from([("body".to_string(), Value::string("hello"))]),
            None,
        )
        .await
        .unwrap();

    // 2. Standard/Pessimistic database with one document.
    let db_analytics = project.database(&analytics_root).await;
    db_analytics.set_config(false, false);
    let metrics_col: CollectionRef = format!("{analytics_root}/metrics").parse().unwrap();
    db_analytics
        .create_doc(
            &metrics_col,
            "m1",
            HashMap::from([("count".to_string(), Value::integer(7))]),
            None,
        )
        .await
        .unwrap();

    // 3. Empty Enterprise/Pessimistic database (0 documents).
    let db_audit = project.database(&audit_root).await;
    db_audit.set_config(true, false);

    let snapshot_bytes = export_project_bytes(project).await;
    fs::write(&snapshot_path, &snapshot_bytes).expect("write snapshot fixture");

    let mut cmd = base_command();
    cmd.arg("--host-port")
        .arg("127.0.0.1:0")
        .arg("--import")
        .arg(&snapshot_path);
    let expected_line = format!(
        "Loaded snapshot from {}: 3 database(s), 4 document(s)",
        snapshot_path.display()
    );
    let emu = spawn_and_wait_listening(cmd, &expected_line);

    // Verify imported settings and default Standard edition for post-startup databases.
    let (status, meta_default) =
        http_get_json(emu.addr, "/v1/projects/demo/databases/(default)").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_default["databaseEdition"], "ENTERPRISE");
    assert_eq!(meta_default["concurrencyMode"], "OPTIMISTIC");

    let (status, meta_analytics) =
        http_get_json(emu.addr, "/v1/projects/demo/databases/analytics").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_analytics["databaseEdition"], "STANDARD");
    assert_eq!(meta_analytics["concurrencyMode"], "PESSIMISTIC");

    let (status, meta_audit) = http_get_json(emu.addr, "/v1/projects/demo/databases/audit").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_audit["databaseEdition"], "ENTERPRISE");
    assert_eq!(meta_audit["concurrencyMode"], "PESSIMISTIC");

    let (status, meta_fresh) = http_get_json(emu.addr, "/v1/projects/demo/databases/fresh").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_fresh["databaseEdition"], "STANDARD");
    assert_eq!(meta_fresh["concurrencyMode"], "PESSIMISTIC");

    // Verify documents over REST /v1/projects/...
    let (status, alice_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/(default)/documents/users/alice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        alice_doc["name"],
        "projects/demo/databases/(default)/documents/users/alice"
    );
    assert!(alice_doc["fields"]["null_val"]["nullValue"].is_null());
    assert_eq!(alice_doc["fields"]["bool_val"]["booleanValue"], true);
    assert_eq!(
        alice_doc["fields"]["int_val"]["integerValue"],
        i64::MAX.to_string()
    );
    assert_eq!(alice_doc["fields"]["double_val"]["doubleValue"], 12.5);
    assert_eq!(
        alice_doc["fields"]["ts_val"]["timestampValue"],
        "2023-11-14T22:13:20.123456789Z"
    );
    assert_eq!(alice_doc["fields"]["str_val"]["stringValue"], "alice");
    assert_eq!(alice_doc["fields"]["bytes_val"]["bytesValue"], "AQID");
    assert_eq!(
        alice_doc["fields"]["ref_val"]["referenceValue"],
        "projects/demo/databases/(default)/documents/flags/ready"
    );
    assert_eq!(
        alice_doc["fields"]["geo_val"]["geoPointValue"]["latitude"],
        52.3676
    );
    assert_eq!(
        alice_doc["fields"]["arr_val"]["arrayValue"]["values"][1]["stringValue"],
        "two"
    );
    assert_eq!(
        alice_doc["fields"]["map_val"]["mapValue"]["fields"]["nested"]["booleanValue"],
        false
    );
    assert_eq!(
        alice_doc["fields"]["vec_val"]["mapValue"]["fields"]["__type__"]["stringValue"],
        "__vector__"
    );

    // Zero-field document exists with createTime/updateTime and omitted empty fields.
    let (status, ready_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/(default)/documents/flags/ready",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        ready_doc["name"],
        "projects/demo/databases/(default)/documents/flags/ready"
    );
    assert!(ready_doc.get("fields").is_none());
    assert!(ready_doc["createTime"].as_str().is_some());
    assert!(ready_doc["updateTime"].as_str().is_some());

    // Subcollection document exists while its phantom parent does not.
    let (status, sub_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/(default)/documents/rooms/r1/messages/m1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(sub_doc["fields"]["body"]["stringValue"], "hello");

    let (status, _) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/(default)/documents/rooms/r1",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let (status, metric_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/analytics/documents/metrics/m1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(metric_doc["fields"]["count"]["integerValue"], "7");
}

#[tokio::test]
async fn cli_import_env_var_streams_multi_chunk_snapshot_and_preserves_settings_with_default_enterprise()
 {
    let dir = TempDirGuard::new("env-default-enterprise");
    let snapshot_path = dir.path().join("multi-chunk.fsemu");

    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));

    let default_root: RootRef = "projects/demo/databases/(default)".parse().unwrap();
    let legacy_root: RootRef = "projects/demo/databases/legacy".parse().unwrap();

    // Snapshot explicitly has Standard/Pessimistic on `(default)` and a >150 KiB payload
    // exercising multiple 64 KiB file read chunks.
    let db_default = project.database(&default_root).await;
    db_default.set_config(false, false);
    let blobs_col: CollectionRef = format!("{default_root}/blobs").parse().unwrap();
    let large_payload = vec![0x5Au8; 150 * 1024];
    db_default
        .create_doc(
            &blobs_col,
            "large",
            HashMap::from([("payload".to_string(), Value::bytes(large_payload))]),
            None,
        )
        .await
        .unwrap();

    let db_legacy = project.database(&legacy_root).await;
    db_legacy.set_config(true, false);
    let items_col: CollectionRef = format!("{legacy_root}/items").parse().unwrap();
    db_legacy
        .create_doc(
            &items_col,
            "i1",
            HashMap::from([("name".to_string(), Value::string("kept"))]),
            None,
        )
        .await
        .unwrap();

    let snapshot_bytes = export_project_bytes(project).await;
    assert!(snapshot_bytes.len() > 2 * 64 * 1024);
    fs::write(&snapshot_path, &snapshot_bytes).expect("write multi-chunk snapshot");

    let mut cmd = base_command();
    cmd.arg("--host-port")
        .arg("127.0.0.1:0")
        .arg("--default-enterprise")
        .env("FIRESTORE_EMULATOR_IMPORT", &snapshot_path);
    let expected_line = format!(
        "Loaded snapshot from {}: 2 database(s), 2 document(s)",
        snapshot_path.display()
    );
    let emu = spawn_and_wait_listening(cmd, &expected_line);

    // Imported databases keep snapshot settings instead of inheriting `--default-enterprise`.
    let (status, meta_default) =
        http_get_json(emu.addr, "/v1/projects/demo/databases/(default)").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_default["databaseEdition"], "STANDARD");
    assert_eq!(meta_default["concurrencyMode"], "PESSIMISTIC");

    let (status, meta_legacy) = http_get_json(emu.addr, "/v1/projects/demo/databases/legacy").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_legacy["databaseEdition"], "ENTERPRISE");
    assert_eq!(meta_legacy["concurrencyMode"], "PESSIMISTIC");

    // Newly created post-startup database inherits `--default-enterprise`.
    let (status, meta_post_startup) =
        http_get_json(emu.addr, "/v1/projects/demo/databases/post-startup").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(meta_post_startup["databaseEdition"], "ENTERPRISE");
    assert_eq!(meta_post_startup["concurrencyMode"], "OPTIMISTIC");

    // Multi-chunk document intact.
    let (status, large_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/(default)/documents/blobs/large",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        large_doc["fields"]["payload"]["bytesValue"]
            .as_str()
            .unwrap()
            .len(),
        (150 * 1024 / 3) * 4
    );

    let (status, legacy_doc) = http_get_json(
        emu.addr,
        "/v1/projects/demo/databases/legacy/documents/items/i1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(legacy_doc["fields"]["name"]["stringValue"], "kept");
}

#[tokio::test]
async fn cli_import_fails_fast_before_binding_on_invalid_inputs() {
    let dir = TempDirGuard::new("fail-fast");

    // Keep an occupied port open: if the binary ever reached `TcpListener::bind` before failing
    // `--import`, bind would fail with "Could not start listener on provided address" instead of
    // "Could not import snapshot from".
    let occupied_listener = TcpListener::bind("127.0.0.1:0").expect("bind occupied test port");
    let occupied_addr = occupied_listener.local_addr().unwrap();

    // Seed a valid snapshot to derive truncated / corrupt / future-version fixtures.
    let project: &'static FirestoreProject =
        Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    let root: RootRef = "projects/demo/databases/(default)".parse().unwrap();
    let col: CollectionRef = format!("{root}/c").parse().unwrap();
    project
        .database(&root)
        .await
        .create_doc(
            &col,
            "d",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();
    let valid_bytes = export_project_bytes(project).await;

    let run_failure = |import_arg: &Path, use_env: bool| {
        let mut cmd = base_command();
        cmd.arg("--host-port").arg(occupied_addr.to_string());
        if use_env {
            cmd.env("FIRESTORE_EMULATOR_IMPORT", import_arg);
        } else {
            cmd.arg("--import").arg(import_arg);
        }
        let output = cmd.output().expect("execute failing CLI binary");
        assert!(
            !output.status.success(),
            "expected non-zero exit for `{}`",
            import_arg.display()
        );
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
        assert!(
            !stderr.contains("Firestore emulator listening on "),
            "must fail before listening: {stderr}"
        );
        assert!(
            !stderr.contains("Could not start listener on provided address"),
            "must fail before TcpListener::bind: {stderr}"
        );
        assert!(
            stderr.contains(&format!(
                "Could not import snapshot from '{}'",
                import_arg.display()
            )),
            "missing CliError context for `{}`: {stderr}",
            import_arg.display()
        );
        assert!(
            stderr.contains("Suggestion:"),
            "missing Suggestion for `{}`: {stderr}",
            import_arg.display()
        );
        stderr
    };

    // 1. Directory path (e.g., Firebase Java emulator export folder).
    let subdir = dir.path().join("firebase-export-dir");
    fs::create_dir_all(&subdir).unwrap();
    let dir_stderr = run_failure(&subdir, false);
    assert!(dir_stderr.contains("directory"), "{dir_stderr}");
    assert!(dir_stderr.contains(".fsemu"), "{dir_stderr}");

    // 2. Missing file (both `--import` and `FIRESTORE_EMULATOR_IMPORT`).
    let missing_path = dir.path().join("missing.fsemu");
    let missing_stderr = run_failure(&missing_path, false);
    assert!(
        missing_stderr.contains("exists and is readable"),
        "{missing_stderr}"
    );
    let missing_env_stderr = run_failure(&missing_path, true);
    assert!(
        missing_env_stderr.contains("exists and is readable"),
        "{missing_env_stderr}"
    );

    // 3. Truncated file.
    let truncated_path = dir.path().join("truncated.fsemu");
    fs::write(&truncated_path, &valid_bytes[..valid_bytes.len() / 2]).unwrap();
    let trunc_stderr = run_failure(&truncated_path, false);
    assert!(
        trunc_stderr.contains("Truncated snapshot"),
        "{trunc_stderr}"
    );
    assert!(
        trunc_stderr.contains("valid, uncorrupted .fsemu snapshot"),
        "{trunc_stderr}"
    );

    // 4. Bad CRC-32 checksum in trailer.
    let bad_crc_path = dir.path().join("bad-crc.fsemu");
    let mut bad_crc = valid_bytes.clone();
    let crc_offset = bad_crc.len() - 12;
    bad_crc[crc_offset] ^= 0x01;
    fs::write(&bad_crc_path, &bad_crc).unwrap();
    let crc_stderr = run_failure(&bad_crc_path, false);
    assert!(crc_stderr.contains("checksum mismatch"), "{crc_stderr}");
    assert!(
        crc_stderr.contains("valid, uncorrupted .fsemu snapshot"),
        "{crc_stderr}"
    );

    // 5. Unsupported major version.
    let future_ver_path = dir.path().join("future-v2.fsemu");
    let mut future_ver = valid_bytes;
    future_ver[8..10].copy_from_slice(&2u16.to_le_bytes());
    fs::write(&future_ver_path, &future_ver).unwrap();
    let ver_stderr = run_failure(&future_ver_path, false);
    assert!(
        ver_stderr.contains("Unsupported snapshot version: found major 2, supported major 1"),
        "{ver_stderr}"
    );
    assert!(ver_stderr.contains("compatible version"), "{ver_stderr}");

    drop(occupied_listener);
}
