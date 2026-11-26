use std::{collections::HashMap, time::Duration};

use axum::body::Body;
use bytes::Bytes;
use googleapis::{
    fields,
    google::{
        firestore::v1::{
            Value, structured_query::field_filter::Operator, target, value::ValueType,
        },
        protobuf::Timestamp,
        r#type::LatLng,
    },
};
use googletest::prelude::*;
use http::{HeaderMap, HeaderValue, Method, StatusCode, header};
use serde_json::{Value as JsonValue, json};
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::Code;

use crate::harness::{
    builders::{QuerySpec, WriteBatchSpec, doc_path, field_filter},
    client::TestDb,
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

fn octet_stream_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    headers
}

async fn export_http_snapshot(db: &TestDb, uri: &str) -> (HeaderMap, Vec<u8>) {
    let (status, headers, bytes) = db
        .http_raw_request(Method::GET, uri, HeaderMap::new(), Body::empty())
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "GET {uri} failed: {}",
        String::from_utf8_lossy(&bytes)
    );
    (headers, bytes.to_vec())
}

async fn import_http_snapshot_chunked(
    db: &TestDb,
    bytes: &[u8],
    chunk_size: usize,
) -> (StatusCode, JsonValue) {
    let owned_chunks: Vec<Result<Bytes, std::io::Error>> = bytes
        .chunks(chunk_size.max(1))
        .map(|slice| Ok(Bytes::copy_from_slice(slice)))
        .collect();
    let stream = tokio_stream::iter(owned_chunks);
    let (status, _headers, resp_bytes) = db
        .http_raw_request(
            Method::POST,
            "/emulator/v1/import",
            octet_stream_headers(),
            Body::from_stream(stream),
        )
        .await;
    let json: JsonValue =
        serde_json::from_slice(&resp_bytes).expect("parse /emulator/v1/import JSON response");
    (status, json)
}

fn all_eleven_value_types_fields() -> HashMap<String, Value> {
    HashMap::from([
        ("null_val".to_string(), Value::null()),
        ("bool_true".to_string(), Value::boolean(true)),
        ("int_min".to_string(), Value::integer(i64::MIN)),
        ("int_max".to_string(), Value::integer(i64::MAX)),
        ("neg_zero".to_string(), Value::double(-0.0)),
        ("nan_val".to_string(), Value::double(f64::NAN)),
        ("pos_inf".to_string(), Value::double(f64::INFINITY)),
        ("neg_inf".to_string(), Value::double(f64::NEG_INFINITY)),
        (
            "ts_val".to_string(),
            Value::timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos:   123_456_789,
            }),
        ),
        ("str_val".to_string(), Value::string("héllo 🌍")),
        (
            "bytes_val".to_string(),
            Value::bytes(Bytes::from_static(b"\x00\x01\xfe\xffbin")),
        ),
        (
            "ref_val".to_string(),
            Value::reference(doc_path("flags/ready")),
        ),
        (
            "geo_val".to_string(),
            Value {
                value_type: Some(ValueType::GeoPointValue(LatLng {
                    latitude:  -37.8136,
                    longitude: 144.9631,
                })),
            },
        ),
        (
            "arr_val".to_string(),
            Value::array(vec![
                Value::null(),
                Value::integer(1),
                Value::double(1.0),
                Value::vector([0.5, -1.5, 2.5]),
            ]),
        ),
        (
            "map_val".to_string(),
            Value::map(HashMap::from([(
                "nested".to_string(),
                Value::vector([1.0, 2.0, 3.0]),
            )])),
        ),
        ("vec_val".to_string(), Value::vector([4.0, 5.0, 6.0])),
    ])
}

#[gtest]
#[tokio::test]
async fn streaming_http_export_and_import_preserves_all_types_empty_dbs_and_settings() {
    let mut src = TestEnv::new().build_db();

    // 1. Seed `(default)` with Enterprise/Optimistic, all 11 types, zero-field doc, and
    //    subcollection under non-existent parent.
    src.set_admin_mode(Some("ENTERPRISE"), Some("OPTIMISTIC"))
        .await;
    let rich_fields = all_eleven_value_types_fields();
    src.put("all/doc1", rich_fields.clone()).await.unwrap();
    src.put("flags/ready", HashMap::new()).await.unwrap();
    src.put("rooms/r1/messages/m1", fields!({ "text": "hello" }))
        .await
        .unwrap();

    // 2. Seed a second empty database with Enterprise/Pessimistic settings via REST Admin PATCH.
    let (patch_status, _) = src
        .http_oneshot(
            Method::PATCH,
            "/v1/projects/test-project/databases/empty-ent",
            Some(&json!({
                "databaseEdition": "ENTERPRISE",
                "concurrencyMode": "PESSIMISTIC",
            })),
        )
        .await;
    expect_eq!(patch_status, StatusCode::OK);

    // 3. Verify GET /emulator/v1/export headers for multi-DB all-export and single-DB export.
    let (all_headers, snapshot_bytes) = export_http_snapshot(&src, "/emulator/v1/export").await;
    assert_eq!(
        all_headers[header::CONTENT_TYPE],
        "application/octet-stream"
    );
    let all_disp = all_headers[header::CONTENT_DISPOSITION].to_str().unwrap();
    assert!(
        all_disp.starts_with("attachment; filename=\"firestore-emulator-snapshot-")
            && all_disp.ends_with(".fsemu\""),
        "unexpected all-export Content-Disposition: {all_disp}"
    );

    let (single_headers, _single_bytes) = export_http_snapshot(
        &src,
        "/emulator/v1/export?database=projects/test-project/databases/empty-ent",
    )
    .await;
    let single_disp = single_headers[header::CONTENT_DISPOSITION]
        .to_str()
        .unwrap();
    assert!(
        single_disp.starts_with("attachment; filename=\"test-project-empty-ent-")
            && single_disp.ends_with(".fsemu\""),
        "unexpected single-export Content-Disposition: {single_disp}"
    );

    // 4. Import into a fresh `TestDb` using chunked streaming `Body::from_stream` via
    //    `http_raw_request`.
    let mut dst = TestEnv::new().build_db();
    let (imp_status, summary) = import_http_snapshot_chunked(&dst, &snapshot_bytes, 512).await;
    expect_eq!(imp_status, StatusCode::OK);
    expect_eq!(summary["totalDatabases"], 2);
    expect_eq!(summary["totalDocuments"], 3);

    // Verify all 11 value types (including `-0.0` bit pattern and `NaN`), zero-field doc, phantom
    // parent, empty database, and settings over both gRPC and REST.
    let got_rich = dst.get("all/doc1").await.unwrap();
    assert_doc_wire_eq(&got_rich, "all/doc1", &rich_fields);
    assert_eq!(
        got_rich.fields["neg_zero"].as_double().unwrap().to_bits(),
        (-0.0_f64).to_bits()
    );

    let got_zero = dst.get("flags/ready").await.unwrap();
    assert!(got_zero.fields.is_empty());
    assert_doc_wire_eq(&got_zero, "flags/ready", &HashMap::new());

    let got_sub = dst.get("rooms/r1/messages/m1").await.unwrap();
    assert_doc_wire_eq(
        &got_sub,
        "rooms/r1/messages/m1",
        &fields!({ "text": "hello" }),
    );
    assert_status_code(dst.get("rooms/r1").await, Code::NotFound);

    let (st, db_meta_bytes) = dst
        .http_oneshot(Method::GET, &format!("/v1/{DATABASE_ROOT}"), None)
        .await;
    expect_eq!(st, StatusCode::OK);
    let db_meta: JsonValue = serde_json::from_slice(&db_meta_bytes).unwrap();
    expect_eq!(db_meta["databaseEdition"], "ENTERPRISE");
    expect_eq!(db_meta["concurrencyMode"], "OPTIMISTIC");

    let (st, empty_meta_bytes) = dst
        .http_oneshot(
            Method::GET,
            "/v1/projects/test-project/databases/empty-ent",
            None,
        )
        .await;
    expect_eq!(st, StatusCode::OK);
    let empty_meta: JsonValue = serde_json::from_slice(&empty_meta_bytes).unwrap();
    expect_eq!(empty_meta["databaseEdition"], "ENTERPRISE");
    expect_eq!(empty_meta["concurrencyMode"], "PESSIMISTIC");

    // 5. Selective overwrite restores mutated `(default)` and resets `empty-ent` back to empty
    //    while leaving an unrelated `scratch` database intact.
    dst.put("all/doc1", fields!({ "mutated": true }))
        .await
        .unwrap();
    let (st, _) = dst
        .http_oneshot(
            Method::PATCH,
            "/v1/projects/test-project/databases/empty-ent/documents/leaked/d1",
            Some(&json!({ "fields": { "x": { "integerValue": "1" } } })),
        )
        .await;
    expect_eq!(st, StatusCode::OK);
    let (st, _) = dst
        .http_oneshot(
            Method::PATCH,
            "/v1/projects/test-project/databases/scratch/documents/notes/n1",
            Some(&json!({ "fields": { "keep": { "stringValue": "yes" } } })),
        )
        .await;
    expect_eq!(st, StatusCode::OK);

    let (re_status, re_summary) = import_http_snapshot_chunked(&dst, &snapshot_bytes, 1024).await;
    expect_eq!(re_status, StatusCode::OK);
    expect_eq!(re_summary["totalDatabases"], 2);
    expect_eq!(re_summary["totalDocuments"], 3);
    assert!(
        re_summary["databases"]
            .as_array()
            .unwrap()
            .iter()
            .all(|d| d["replaced"] == true)
    );

    assert_doc_wire_eq(
        &dst.get("all/doc1").await.unwrap(),
        "all/doc1",
        &rich_fields,
    );

    let (st, leaked_check) = dst
        .http_oneshot(
            Method::GET,
            "/v1/projects/test-project/databases/empty-ent/documents/leaked/d1",
            None,
        )
        .await;
    expect_eq!(
        st,
        StatusCode::NOT_FOUND,
        "{}",
        String::from_utf8_lossy(&leaked_check)
    );

    let (st, scratch_bytes) = dst
        .http_oneshot(
            Method::GET,
            "/v1/projects/test-project/databases/scratch/documents/notes/n1",
            None,
        )
        .await;
    expect_eq!(st, StatusCode::OK);
    let scratch_doc: JsonValue = serde_json::from_slice(&scratch_bytes).unwrap();
    expect_eq!(scratch_doc["fields"]["keep"]["stringValue"], "yes");
}

#[gtest]
#[tokio::test]
async fn point_in_time_mvcc_export_is_queue_free_and_consistent_across_concurrent_transactions() {
    let mut db = TestEnv::new().build_db();
    db.put("acct/a", fields!({ "bal": 100i64 })).await.unwrap();
    db.put("acct/b", fields!({ "bal": 100i64 })).await.unwrap();
    db.put("acct/keep", fields!({ "ok": true })).await.unwrap();

    // 1. An uncommitted RW transaction holding a read lock must neither block `GET
    //    /emulator/v1/export` nor be wounded by it, and buffered uncommitted writes must not leak.
    let mut held_txn = db.begin_rw_txn().await.unwrap();
    let _read_a = held_txn.get("acct/a").await.unwrap();
    held_txn
        .put("acct/a", fields!({ "bal": 90i64 }))
        .put("acct/b", fields!({ "bal": 110i64 }));

    let (_headers, pre_commit_bytes) = tokio::time::timeout(
        Duration::from_secs(2),
        export_http_snapshot(&db, "/emulator/v1/export"),
    )
    .await
    .expect("MVCC export must not block on active transaction locks");
    held_txn
        .commit()
        .await
        .expect("unwounded txn must still commit");

    let mut check_pre = TestEnv::new().build_db();
    let (st, _) = import_http_snapshot_chunked(&check_pre, &pre_commit_bytes, 4096).await;
    expect_eq!(st, StatusCode::OK);
    expect_eq!(
        check_pre.get("acct/a").await.unwrap().fields["bal"].as_integer(),
        Some(100)
    );
    expect_eq!(
        check_pre.get("acct/b").await.unwrap().fields["bal"].as_integer(),
        Some(100)
    );

    // 2. Pin `snapshot_time` via `http_stream` (`prepare_export`), then commit mutations before
    //    draining the stream; the exported snapshot must stay frozen at `snapshot_time`.
    let (stream_status, mut body_stream) = db.http_stream("/emulator/v1/export").await;
    expect_eq!(stream_status, StatusCode::OK);

    let mut post_pin_txn = db.begin_rw_txn().await.unwrap();
    post_pin_txn
        .put("acct/a", fields!({ "bal": 80i64 }))
        .put("acct/b", fields!({ "bal": 120i64 }))
        .delete("acct/keep", None)
        .put("acct/later", fields!({ "new": true }));
    post_pin_txn.commit().await.unwrap();

    let mut pinned_bytes = Vec::new();
    while let Some(chunk) = body_stream.next().await {
        pinned_bytes.extend_from_slice(&chunk.expect("export stream chunk"));
    }

    let mut check_pinned = TestEnv::new().build_db();
    let (st, _) = import_http_snapshot_chunked(&check_pinned, &pinned_bytes, 4096).await;
    expect_eq!(st, StatusCode::OK);
    expect_eq!(
        check_pinned.get("acct/a").await.unwrap().fields["bal"].as_integer(),
        Some(90)
    );
    expect_eq!(
        check_pinned.get("acct/b").await.unwrap().fields["bal"].as_integer(),
        Some(110)
    );
    assert_doc_wire_eq(
        &check_pinned.get("acct/keep").await.unwrap(),
        "acct/keep",
        &fields!({ "ok": true }),
    );
    assert_status_code(check_pinned.get("acct/later").await, Code::NotFound);

    // 3. Concurrent multi-document gRPC transactions racing `GET /emulator/v1/export` always yield
    //    tear-free snapshots preserving `bal(a) + bal(b) == 200`.
    let mut writer_db = db.clone();
    let writer = tokio::spawn(async move {
        for step in 1..=25i64 {
            let mut txn = writer_db.begin_rw_txn().await.unwrap();
            let a = txn.get("acct/a").await.unwrap().fields["bal"]
                .as_integer()
                .unwrap();
            let b = txn.get("acct/b").await.unwrap().fields["bal"]
                .as_integer()
                .unwrap();
            assert_eq!(a + b, 200);
            txn.put("acct/a", fields!({ "bal": a - 1 }))
                .put("acct/b", fields!({ "bal": b + 1 }));
            txn.commit().await.unwrap();
            if step % 5 == 0 {
                writer_db
                    .commit_writes(
                        WriteBatchSpec::new()
                            .put("acct/a", fields!({ "bal": a - 2 }))
                            .put("acct/b", fields!({ "bal": b + 2 })),
                    )
                    .await
                    .unwrap();
            }
        }
    });

    for _ in 0..8 {
        let (_h, snap) = export_http_snapshot(&db, "/emulator/v1/export").await;
        let mut verifier_db = TestEnv::new().build_db();
        let (st, _) = import_http_snapshot_chunked(&verifier_db, &snap, 4096).await;
        expect_eq!(st, StatusCode::OK);
        let a = verifier_db.get("acct/a").await.unwrap().fields["bal"]
            .as_integer()
            .unwrap();
        let b = verifier_db.get("acct/b").await.unwrap().fields["bal"]
            .as_integer()
            .unwrap();
        assert_eq!(
            a + b,
            200,
            "exported snapshot tore transaction: a={a}, b={b}"
        );
    }

    writer.await.unwrap();
}

#[gtest]
#[tokio::test]
async fn active_grpc_listeners_and_resume_token_converge_across_streaming_import() {
    let mut db = TestEnv::new().build_db();
    db.put("watch/d1", fields!({ "v": 1i64 })).await.unwrap();
    db.put("tasks/t1", fields!({ "active": true, "v": 1i64 }))
        .await
        .unwrap();
    db.put("tasks/t2", fields!({ "active": true, "v": 2i64 }))
        .await
        .unwrap();

    let (_headers, snap_v1) = export_http_snapshot(&db, "/emulator/v1/export").await;

    // 1. Open active `DocumentsTarget` and compatible `QueryTarget` (`STANDARD` -> `STANDARD`).
    let (mut doc_handle, doc_init) = db.listen_doc("watch/d1", None).await.unwrap();
    assert_doc_wire_eq(
        &doc_init.changed_docs[0],
        "watch/d1",
        &fields!({ "v": 1i64 }),
    );
    let pre_import_token = doc_init.resume_token.clone();
    assert!(!pre_import_token.is_empty());

    let (mut query_handle, query_init) = db
        .listen_query(QuerySpec::collection("tasks").where_filter(field_filter(
            "active",
            Operator::Equal,
            true,
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&query_init.changed_docs), vec!["t1", "t2"]);

    // 2. Start a streaming HTTP import and synchronize deterministically via a 1-slot channel so we
    //    know `import_lock` is held and `SnapshotDecoder` is mid-stream before issuing live writes.
    let (chunk_tx, chunk_rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(1);
    let split_1 = 10.min(snap_v1.len() / 3);
    let split_2 = (snap_v1.len() * 2) / 3;
    chunk_tx
        .send(Ok(Bytes::copy_from_slice(&snap_v1[..split_1])))
        .await
        .unwrap();

    let import_db = db.clone();
    let import_task = tokio::spawn(async move {
        import_db
            .http_raw_request(
                Method::POST,
                "/emulator/v1/import",
                octet_stream_headers(),
                Body::from_stream(ReceiverStream::new(chunk_rx)),
            )
            .await
    });

    // Sending chunk 2 and then chunk 3 across a capacity-1 channel only completes once the import
    // handler has acquired `import_lock` and consumed chunk 1 and chunk 2 inside `import_snapshot`.
    chunk_tx
        .send(Ok(Bytes::copy_from_slice(&snap_v1[split_1..split_2])))
        .await
        .unwrap();
    let tail_slice = Bytes::copy_from_slice(&snap_v1[split_2..]);
    let split_tail = tail_slice.len() / 2;
    chunk_tx
        .send(Ok(tail_slice.slice(..split_tail)))
        .await
        .unwrap();

    // Overlapping import fails immediately with 409 Conflict (ABORTED).
    let (overlap_status, _overlap_body) = import_http_snapshot_chunked(&db, &snap_v1, 1024).await;
    expect_eq!(overlap_status, StatusCode::CONFLICT);

    // 3. Perform concurrent live writes while the import stream is still open mid-decode.
    db.put("watch/d1", fields!({ "v": 99i64 })).await.unwrap();
    let live_doc_batch = doc_handle.next_batch().await.unwrap();
    assert_doc_wire_eq(
        &live_doc_batch.changed_docs[0],
        "watch/d1",
        &fields!({ "v": 99i64 }),
    );
    let mid_write_token = live_doc_batch.resume_token.clone();

    db.put("tasks/t1", fields!({ "active": true, "v": 99i64 }))
        .await
        .unwrap();
    drop(query_handle.next_batch().await.unwrap());
    db.delete("tasks/t2", None).await.unwrap();
    drop(query_handle.next_batch().await.unwrap());
    db.put("tasks/t3", fields!({ "active": true, "v": 3i64 }))
        .await
        .unwrap();
    let mid_query_batch = query_handle.next_batch().await.unwrap();
    expect_eq!(short_ids(&mid_query_batch.changed_docs), vec!["t1", "t3"]);

    // 4. Finish streaming the snapshot import.
    chunk_tx
        .send(Ok(tail_slice.slice(split_tail..)))
        .await
        .unwrap();
    drop(chunk_tx);
    let (imp_status, _imp_hdrs, imp_body) = import_task.await.unwrap();
    expect_eq!(
        imp_status,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&imp_body)
    );

    // 5. Active `DocumentsTarget` converges via single reset to `v = 1` without emitting a deletion
    //    for an empty intermediate state.
    let doc_reset = doc_handle.next_batch().await.unwrap();
    expect_eq!(doc_reset.was_reset, true);
    expect_eq!(doc_reset.saw_current, true);
    assert!(doc_reset.deleted_docs.is_empty());
    assert_doc_wire_eq(
        &doc_reset.changed_docs[0],
        "watch/d1",
        &fields!({ "v": 1i64 }),
    );

    // 6. Active `QueryTarget` converges in a single reset directly to `["t1", "t2"]`.
    let query_reset = query_handle.next_batch().await.unwrap();
    expect_eq!(query_reset.was_reset, true);
    expect_eq!(query_reset.saw_current, true);
    expect_eq!(short_ids(&query_reset.changed_docs), vec!["t1", "t2"]);
    expect_eq!(
        query_reset.changed_docs[0].fields["v"].as_integer(),
        Some(1)
    );

    // 7. Reconnecting `DocumentsTarget` with either `pre_import_token` or `mid_write_token`
    //    delivers the restored `v = 1` document (because `import_time` is stamped after
    //    `old_db.mark_cleared()`).
    for token in [pre_import_token, mid_write_token] {
        let (reconnected, rec_init) = db
            .listen_doc("watch/d1", Some(target::ResumeType::ResumeToken(token)))
            .await
            .unwrap();
        expect_eq!(rec_init.changed_docs.len(), 1);
        assert_doc_wire_eq(
            &rec_init.changed_docs[0],
            "watch/d1",
            &fields!({ "v": 1i64 }),
        );
        reconnected.close().await;
    }

    // 8. Incremental post-import writes continue cleanly without secondary resets.
    db.put("watch/d1", fields!({ "v": 5i64 })).await.unwrap();
    let doc_after = doc_handle.next_batch().await.unwrap();
    assert!(!doc_after.was_reset);
    assert_doc_wire_eq(
        &doc_after.changed_docs[0],
        "watch/d1",
        &fields!({ "v": 5i64 }),
    );

    db.put("tasks/t1", fields!({ "active": true, "v": 5i64 }))
        .await
        .unwrap();
    let query_after = query_handle.next_batch().await.unwrap();
    assert!(!query_after.was_reset);
    expect_eq!(
        query_after.changed_docs[0].fields["v"].as_integer(),
        Some(5)
    );

    doc_handle.close().await;
    query_handle.close().await;
}

#[gtest]
#[tokio::test]
async fn query_listener_terminates_with_failed_precondition_and_error_log_on_edition_change() {
    let mut db = TestEnv::new().build_db();
    db.put("items/a", fields!({ "v": 1i64 })).await.unwrap();

    // Build an Enterprise snapshot to replace the currently Standard `(default)` database.
    let mut ent_src = TestEnv::enterprise().build_db();
    ent_src
        .put("items/a", fields!({ "v": 2i64 }))
        .await
        .unwrap();
    let (_hdrs, ent_snapshot) = export_http_snapshot(&ent_src, "/emulator/v1/export").await;

    // 1. Start dynamic ERROR log capture via `/emulator/v1/logging/{name}`.
    let log_handle = "/emulator/v1/logging/edition-change-check";
    let log_poll = "/emulator/v1/logging/edition-change-check:poll";
    let (log_st, _) = db
        .http_raw_body(
            Method::POST,
            log_handle,
            Some("text/plain"),
            "emulator_database=error",
        )
        .await;
    expect_eq!(log_st, StatusCode::OK);

    // 2. Open active `DocumentsTarget` and `QueryTarget` listeners on the Standard database.
    let (mut doc_handle, doc_init) = db.listen_doc("items/a", None).await.unwrap();
    assert_doc_wire_eq(
        &doc_init.changed_docs[0],
        "items/a",
        &fields!({ "v": 1i64 }),
    );

    let (mut query_handle, query_init) = db
        .listen_query(QuerySpec::collection("items"))
        .await
        .unwrap();
    expect_eq!(short_ids(&query_init.changed_docs), vec!["a"]);

    // 3. Import the Enterprise snapshot via HTTP control plane.
    let (imp_st, imp_json) = import_http_snapshot_chunked(&db, &ent_snapshot, 1024).await;
    expect_eq!(imp_st, StatusCode::OK);
    expect_eq!(imp_json["databases"][0]["databaseEdition"], "ENTERPRISE");

    // 4. `QueryTarget` terminates immediately with `FAILED_PRECONDITION`, whereas `DocumentsTarget`
    //    converges across the edition transition.
    let query_err = assert_status_code(query_handle.next_batch().await, Code::FailedPrecondition);
    assert!(
        query_err.message().contains("STANDARD") && query_err.message().contains("ENTERPRISE"),
        "unexpected FailedPrecondition message: {}",
        query_err.message()
    );

    let doc_reset = doc_handle.next_batch().await.unwrap();
    expect_eq!(doc_reset.was_reset, true);
    assert_doc_wire_eq(
        &doc_reset.changed_docs[0],
        "items/a",
        &fields!({ "v": 2i64 }),
    );

    // 5. Poll and release `/emulator/v1/logging` handle, verifying the ERROR-level log entry.
    let (poll_st, poll_bytes) = db.http_raw_body(Method::POST, log_poll, None, "").await;
    let (del_st, _) = db.http_oneshot(Method::DELETE, log_handle, None).await;
    expect_eq!(poll_st, StatusCode::OK);
    expect_eq!(del_st, StatusCode::OK);

    let captured = String::from_utf8_lossy(&poll_bytes);
    expect_that!(
        captured.as_ref(),
        all!(
            contains_substring("ERROR"),
            contains_substring(DATABASE_ROOT),
            contains_substring("STANDARD"),
            contains_substring("ENTERPRISE"),
        )
    );

    doc_handle.close().await;
}

async fn wait_for_sse_substring(
    stream: &mut axum::body::BodyDataStream,
    buf: &mut String,
    needle: &str,
    timeout: Duration,
) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !buf.contains(needle) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        assert!(
            !remaining.is_zero(),
            "timed out waiting for SSE event containing {needle:?}; saw {buf:?}"
        );
        let chunk = tokio::time::timeout(remaining, stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!("timed out waiting for SSE chunk containing {needle:?}; saw {buf:?}")
            })
            .expect("SSE stream ended before expected event arrived")
            .expect("infallible SSE body chunk");
        buf.push_str(&String::from_utf8_lossy(&chunk));
    }
}

#[gtest]
#[tokio::test]
async fn import_aborts_blocked_lock_waiters_invalidates_stale_txns_and_rejected_import_preserves_state()
 {
    let mut db = TestEnv::new().cloud_timeouts().build_db();
    db.put("lock/d1", fields!({ "v": 1i64 })).await.unwrap();
    db.put("watch/d1", fields!({ "v": 1i64 })).await.unwrap();
    let (_hdrs, snap_v1) = export_http_snapshot(&db, "/emulator/v1/export").await;
    db.put("lock/d1", fields!({ "v": 2i64 })).await.unwrap();

    // (a) Subscribe to `/emulator/v1/contention/events`, hold a pessimistic read lock in Txn A,
    //     and block Txn B on Wound-Wait write-lock acquisition before triggering replacement
    // import.
    let (sse_status, mut contention_sse) = db.http_stream("/emulator/v1/contention/events").await;
    expect_eq!(sse_status, StatusCode::OK);

    let mut txn_a = db.begin_rw_txn().await.unwrap();
    let read_a = txn_a.get("lock/d1").await.unwrap();
    expect_eq!(read_a.fields["v"].as_integer(), Some(2));

    // (b) Also open a pre-import idle transaction to verify post-cutover rejection.
    let mut txn_idle = db.begin_rw_txn().await.unwrap();

    let mut actor_b = db.clone();
    let mut txn_b_task = tokio::spawn(async move {
        let mut txn_b = actor_b.begin_rw_txn().await.unwrap();
        txn_b.put("lock/d1", fields!({ "v": 99i64 }));
        txn_b.commit().await
    });

    let mut sse_buf = String::new();
    wait_for_sse_substring(
        &mut contention_sse,
        &mut sse_buf,
        "\"contention_changed\"",
        Duration::from_secs(2),
    )
    .await;
    assert!(
        !txn_b_task.is_finished(),
        "txn B should remain blocked on txn A's lock before snapshot import"
    );

    let (imp_status, imp_json) = import_http_snapshot_chunked(&db, &snap_v1, 1024).await;
    expect_eq!(imp_status, StatusCode::OK);
    expect_eq!(imp_json["databases"][0]["replaced"], true);

    // Txn B was in-flight on the cleared DB and unblocks promptly (well under 15s CLOUD timeout)
    // with `Code::Aborted`.
    let txn_b_res = tokio::time::timeout(Duration::from_secs(2), &mut txn_b_task)
        .await
        .expect("blocked txn B must unblock promptly on import replacement")
        .expect("txn B task did not panic");
    assert_status_code(txn_b_res, Code::Aborted);

    // `/emulator/v1/contention/events` emits `ContentionSseEvent::DatabaseCleared`.
    wait_for_sse_substring(
        &mut contention_sse,
        &mut sse_buf,
        "\"database_cleared\"",
        Duration::from_secs(2),
    )
    .await;
    let cleared_payload: JsonValue = sse_buf
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .filter_map(|data| serde_json::from_str::<JsonValue>(data.trim()).ok())
        .find(|json| json["type"] == "database_cleared")
        .expect("parsed database_cleared SSE payload");
    expect_eq!(cleared_payload["database"], DOCUMENTS_ROOT);
    drop(contention_sse);

    // Post-cutover gRPC calls using pre-import Txn A or idle Txn IDs resolve against the
    // replacement DB and fail with `Code::InvalidArgument` without mutating imported state.
    txn_a.put("lock/d1", fields!({ "v": 77i64 }));
    assert_status_code(txn_a.commit().await, Code::InvalidArgument);
    assert_status_code(txn_idle.get("lock/d1").await, Code::InvalidArgument);
    assert_status_code(txn_idle.commit().await, Code::InvalidArgument);
    assert_doc_wire_eq(
        &db.get("lock/d1").await.unwrap(),
        "lock/d1",
        &fields!({ "v": 1i64 }),
    );

    // (c) A rejected corrupt/truncated import (`400 Bad Request`) leaves an active
    //     `DocumentsTarget` listener and an active read-write transaction undisturbed.
    let (mut doc_handle, doc_init) = db.listen_doc("watch/d1", None).await.unwrap();
    assert_doc_wire_eq(
        &doc_init.changed_docs[0],
        "watch/d1",
        &fields!({ "v": 1i64 }),
    );

    let mut active_txn = db.begin_rw_txn().await.unwrap();
    assert_doc_wire_eq(
        &active_txn.get("watch/d1").await.unwrap(),
        "watch/d1",
        &fields!({ "v": 1i64 }),
    );
    active_txn.put("watch/d1", fields!({ "v": 3i64 }));

    let truncated = &snap_v1[..snap_v1.len() / 2];
    let (trunc_status, trunc_err) = import_http_snapshot_chunked(&db, truncated, 64).await;
    expect_eq!(trunc_status, StatusCode::BAD_REQUEST);
    expect_eq!(trunc_err["error"]["status"], "INVALID_ARGUMENT");

    let mut corrupt = snap_v1.clone();
    let crc_offset = corrupt.len() - 12;
    corrupt[crc_offset] ^= 0x01;
    let (corrupt_status, corrupt_err) = import_http_snapshot_chunked(&db, &corrupt, 1024).await;
    expect_eq!(corrupt_status, StatusCode::BAD_REQUEST);
    expect_eq!(corrupt_err["error"]["status"], "INVALID_ARGUMENT");

    active_txn
        .commit()
        .await
        .expect("active RW transaction survives rejected imports");
    let batch = tokio::time::timeout(Duration::from_secs(2), doc_handle.next_batch())
        .await
        .expect("listener receives post-rejection commit promptly")
        .unwrap();
    assert!(!batch.was_reset);
    assert_doc_wire_eq(&batch.changed_docs[0], "watch/d1", &fields!({ "v": 3i64 }));
    doc_handle.close().await;
}
