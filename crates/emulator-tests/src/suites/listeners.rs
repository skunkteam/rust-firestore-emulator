use std::{collections::HashMap, time::Duration};

use googleapis::{
    fields,
    google::firestore::v1::{
        ListenRequest, StructuredQuery, Target, listen_request,
        listen_response::ResponseType,
        structured_query::{CollectionSelector, field_filter::Operator},
        target::{self, DocumentsTarget, QueryTarget, query_target},
        target_change::TargetChangeType,
    },
};
use googletest::prelude::*;
use rstest::rstest;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Code;

use crate::harness::{
    builders::{QuerySpec, doc_path, field_filter},
    env::{DATABASE_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

#[gtest]
#[tokio::test]
async fn listen_document_target_tracks_add_modify_delete_resume_token_and_database_clear() {
    let mut db = TestEnv::new().build_db();

    // 1. Initial listen on missing document emits Add + DocumentDelete + Current/NoChange
    let (mut handle, init) = db.listen_doc("watch/doc1", None).await.unwrap();
    expect_eq!(init.added_targets, vec![1]);
    expect_eq!(init.saw_current, true);
    expect_eq!(init.deleted_docs, vec![doc_path("watch/doc1")]);
    assert!(!init.resume_token.is_empty());

    // 2. Create -> DocumentChange
    db.put("watch/doc1", fields!({ "n": 1i64 })).await.unwrap();
    let b1 = handle.next_batch().await.unwrap();
    expect_eq!(b1.changed_docs.len(), 1);
    assert_doc_wire_eq(&b1.changed_docs[0], "watch/doc1", &fields!({ "n": 1i64 }));

    // 3. Modify -> DocumentChange; capture resume_token
    db.put("watch/doc1", fields!({ "n": 2i64 })).await.unwrap();
    let b2 = handle.next_batch().await.unwrap();
    expect_eq!(b2.changed_docs.len(), 1);
    assert_doc_wire_eq(&b2.changed_docs[0], "watch/doc1", &fields!({ "n": 2i64 }));
    let token_at_2 = b2.resume_token.clone();

    // Resuming a new listener at `token_at_2` skips re-sending unchanged `n = 2`!
    let (resumed_handle, resumed_init) = db
        .listen_doc(
            "watch/doc1",
            Some(target::ResumeType::ResumeToken(token_at_2)),
        )
        .await
        .unwrap();
    expect_that!(resumed_init.changed_docs.is_empty(), eq(true));
    expect_that!(resumed_init.deleted_docs.is_empty(), eq(true));
    resumed_handle.close().await;

    // 4. clear_database resets active listener (emits DocumentDelete) and reconnects seamlessly!
    db.clear_default_db().await;
    let reset_batch = handle.next_batch().await.unwrap();
    expect_eq!(reset_batch.deleted_docs, vec![doc_path("watch/doc1")]);

    // Writing to the recreated `(default)` database still reaches the same listener!
    db.put("watch/doc1", fields!({ "n": 3i64 })).await.unwrap();
    let after_clear = handle.next_batch().await.unwrap();
    expect_eq!(after_clear.changed_docs.len(), 1);
    assert_doc_wire_eq(
        &after_clear.changed_docs[0],
        "watch/doc1",
        &fields!({ "n": 3i64 }),
    );

    // 5. Explicit DeleteDocument -> DocumentDelete
    db.delete("watch/doc1", None).await.unwrap();
    let del_batch = handle.next_batch().await.unwrap();
    expect_eq!(del_batch.deleted_docs, vec![doc_path("watch/doc1")]);

    handle.close().await;
}

#[gtest]
#[tokio::test]
async fn listen_query_target_handles_membership_resets_in_place_updates_and_32_rapid_writes() {
    let mut db = TestEnv::new().build_db();
    db.put("tasks/t1", fields!({ "done": false, "seq": 0i64 }))
        .await
        .unwrap();

    let (mut handle, init) = db
        .listen_query(QuerySpec::collection("tasks").where_filter(field_filter(
            "done",
            Operator::Equal,
            false,
        )))
        .await
        .unwrap();
    expect_eq!(init.was_reset, true);
    expect_eq!(short_ids(&init.changed_docs), vec!["t1"]);

    // 1. Over-16 rapid sequential writes without reading in between must NOT deadlock on the
    //    server's bounded `mpsc::channel(16)` thanks to `ListenHandle`'s background collector!
    for seq in 1..=32i64 {
        db.put("tasks/t1", fields!({ "done": false, "seq": seq }))
            .await
            .unwrap();
    }
    let mut last_seq = 0i64;
    for _ in 1..=32 {
        let batch = handle.next_batch().await.unwrap();
        expect_eq!(batch.was_reset, false); // In-place update (order_by is __name__ only & membership unchanged)
        expect_eq!(batch.changed_docs.len(), 1);
        last_seq = batch.changed_docs[0].fields["seq"].as_integer().unwrap();
    }
    expect_eq!(last_seq, 32);

    // 2. Document leaving query (`done = true`) triggers TargetChangeType::Reset with empty
    //    snapshot
    db.put("tasks/t1", fields!({ "done": true, "seq": 33i64 }))
        .await
        .unwrap();
    let left_batch = handle.next_batch().await.unwrap();
    expect_eq!(left_batch.was_reset, true);
    expect_that!(left_batch.changed_docs.is_empty(), eq(true));

    // 3. New matching document (`tasks/t2`) triggers Reset with `["t2"]`, and `clear_database`
    //    resets to `[]`
    db.put("tasks/t2", fields!({ "done": false, "seq": 1i64 }))
        .await
        .unwrap();
    let entered = handle.next_batch().await.unwrap();
    expect_eq!(entered.was_reset, true);
    expect_eq!(short_ids(&entered.changed_docs), vec!["t2"]);

    db.clear_default_db().await;
    let cleared = handle.next_batch().await.unwrap();
    expect_eq!(cleared.was_reset, true);
    expect_that!(cleared.changed_docs.is_empty(), eq(true));

    handle.close().await;
}

#[rstest]
#[case::remove_target(
    ListenRequest {
        database: DATABASE_ROOT.to_string(),
        labels: HashMap::new(),
        target_change: Some(listen_request::TargetChange::RemoveTarget(1)),
        request_options: None,
    },
    Code::Unimplemented
)]
#[case::wrong_target_id(
    ListenRequest {
        database: DATABASE_ROOT.to_string(),
        labels: HashMap::new(),
        target_change: Some(listen_request::TargetChange::AddTarget(Target {
            target_id: 2,
            once: false,
            expected_count: None,
            target_type: Some(target::TargetType::Documents(DocumentsTarget {
                documents: vec![doc_path("watch/d1")],
            })),
            resume_type: None,
        })),
        request_options: None,
    },
    Code::Unimplemented
)]
#[case::multi_doc_target(
    ListenRequest {
        database: DATABASE_ROOT.to_string(),
        labels: HashMap::new(),
        target_change: Some(listen_request::TargetChange::AddTarget(Target {
            target_id: 1,
            once: false,
            expected_count: None,
            target_type: Some(target::TargetType::Documents(DocumentsTarget {
                documents: vec![doc_path("watch/d1"), doc_path("watch/d2")],
            })),
            resume_type: None,
        })),
        request_options: None,
    },
    Code::Unimplemented
)]
#[case::missing_target_change(
    ListenRequest {
        database: DATABASE_ROOT.to_string(),
        labels: HashMap::new(),
        target_change: None,
        request_options: None,
    },
    Code::InvalidArgument
)]
#[case::invalid_database_root(
    ListenRequest {
        database: "not-a-database".to_string(),
        labels: HashMap::new(),
        target_change: Some(listen_request::TargetChange::AddTarget(Target {
            target_id: 1,
            once: false,
            expected_count: None,
            target_type: Some(target::TargetType::Documents(DocumentsTarget {
                documents: vec![doc_path("watch/d1")],
            })),
            resume_type: None,
        })),
        request_options: None,
    },
    Code::InvalidArgument
)]
#[tokio::test]
async fn listen_rejects_invalid_or_unimplemented_requests(
    #[case] req: ListenRequest,
    #[case] expected_code: Code,
) {
    let mut db = TestEnv::new().build_db();
    let mut handle = db.open_raw_listen().await.unwrap();
    handle.send(req).await;
    let first = handle
        .next_raw()
        .await
        .expect("listener emits error frame before closing");
    assert_status_code(first, expected_code);
}

#[gtest]
#[tokio::test]
async fn listen_rejects_second_target_on_same_stream_with_unimplemented() {
    let mut db = TestEnv::new().build_db();
    let (mut handle, _init) = db.listen_doc("watch/d1", None).await.unwrap();
    handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Documents(DocumentsTarget {
                    documents: vec![doc_path("watch/d2")],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await;
    let err_frame = handle.next_raw().await.expect("error frame");
    assert_status_code(err_frame, Code::Unimplemented);
}

#[gtest]
#[tokio::test]
async fn listen_rejects_mismatched_database_root_in_target_with_invalid_argument() {
    let mut db = TestEnv::new().build_db();
    let other_docs = "projects/other-project/databases/(default)/documents";

    // 1. DocumentsTarget referencing a different database root -> InvalidArgument (no panic)
    let mut doc_handle = db.open_raw_listen().await.unwrap();
    doc_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Documents(DocumentsTarget {
                    documents: vec![format!("{other_docs}/watch/d1")],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await;
    let err = doc_handle.next_raw().await.expect("error frame");
    assert_status_code(err, Code::InvalidArgument);

    // 2. QueryTarget whose parent is rooted in another database -> InvalidArgument (no panic)
    let mut query_handle = db.open_raw_listen().await.unwrap();
    query_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(QueryTarget {
                    parent:     other_docs.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(StructuredQuery {
                        from: vec![CollectionSelector {
                            collection_id:   "items".to_string(),
                            all_descendants: false,
                        }],
                        ..Default::default()
                    })),
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await;
    let err = query_handle.next_raw().await.expect("error frame");
    assert_status_code(err, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn listen_dropping_response_stream_terminates_idle_grpc_listener() {
    let mut db = TestEnv::new().build_db();
    let (req_tx, req_rx) = mpsc::channel(4);

    let mut resp_stream = db
        .raw_grpc()
        .listen(ReceiverStream::new(req_rx))
        .await
        .unwrap()
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
                    documents: vec![doc_path("watch/idle")],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .unwrap();

    while let Some(frame) = resp_stream.message().await.unwrap() {
        if let Some(ResponseType::TargetChange(tc)) = frame.response_type
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    drop(resp_stream);
    tokio::time::timeout(Duration::from_secs(2), req_tx.closed())
        .await
        .expect("gRPC Listen task must terminate when client drops response stream");
}

#[gtest]
#[tokio::test]
async fn listen_document_target_lag_reset_preserves_existing_document_instead_of_deleting() {
    let mut db = TestEnv::new().build_db();
    db.put("watch/doc1", fields!({ "n": 0i64 })).await.unwrap();

    // Use `raw_grpc().listen(...)` directly rather than `ListenHandle` (which spawns an eager
    // background reader) so HTTP/2 + server mpsc backpressure genuinely pauses `Listener::go`.
    let (req_tx, req_rx) = mpsc::channel(4);
    let mut raw_stream = db
        .raw_grpc()
        .listen(ReceiverStream::new(req_rx))
        .await
        .unwrap()
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
                    documents: vec![doc_path("watch/doc1")],
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .unwrap();
    while let Some(frame) = raw_stream.message().await.unwrap() {
        if let Some(ResponseType::TargetChange(tc)) = frame.response_type
            && tc.target_change_type == TargetChangeType::NoChange as i32
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    // Fill server mpsc(16) + HTTP/2 stream window (>64 KiB) with padded updates so `Listener::go`
    // stalls before the >1024 unrelated writes overflow the broadcast channel
    // (`RecvError::Lagged`).
    let pad = "x".repeat(2048);
    for i in 1..=100i64 {
        db.put("watch/doc1", fields!({ "n": i, "pad": pad.as_str() }))
            .await
            .unwrap();
    }
    for i in 0..1150i64 {
        db.put(&format!("noise/d{i:04}"), fields!({ "x": i }))
            .await
            .unwrap();
    }
    db.put("watch/doc1", fields!({ "n": 999i64 }))
        .await
        .unwrap();

    let mut saw_reset = false;
    let mut saw_999 = false;
    loop {
        let frame = raw_stream
            .message()
            .await
            .unwrap()
            .expect("listen stream ended unexpectedly");
        match frame.response_type {
            Some(ResponseType::DocumentDelete(dd)) => {
                panic!("existing document must never be reported deleted during lag reset: {dd:?}");
            }
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::Reset as i32 =>
            {
                saw_reset = true;
            }
            Some(ResponseType::DocumentChange(dc)) => {
                if let Some(doc) = dc.document
                    && let Some(val) = doc.fields.get("n")
                    && matches!(
                        val.value_type,
                        Some(
                            googleapis::google::firestore::v1::value::ValueType::IntegerValue(999)
                        )
                    )
                {
                    saw_999 = true;
                }
            }
            Some(ResponseType::TargetChange(tc))
                if saw_reset
                    && saw_999
                    && tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty() =>
            {
                break;
            }
            _ => {}
        }
    }
    assert!(saw_reset);

    // Incremental updates continue cleanly after recovery.
    db.put("watch/doc1", fields!({ "n": 1000i64 }))
        .await
        .unwrap();
    let mut final_doc = None;
    while let Some(frame) = raw_stream.message().await.unwrap() {
        match frame.response_type {
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::Reset as i32 =>
            {
                panic!("unexpected reset on post-recovery update");
            }
            Some(ResponseType::DocumentChange(dc)) => {
                final_doc = dc.document;
            }
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty() =>
            {
                break;
            }
            _ => {}
        }
    }
    let final_doc = final_doc.expect("missing post-recovery DocumentChange");
    assert_doc_wire_eq(&final_doc, "watch/doc1", &fields!({ "n": 1000i64 }));
    drop(req_tx);
}

#[gtest]
#[tokio::test]
async fn listen_query_target_lag_reset_recovers_cleanly_without_stale_deletions() {
    let mut db = TestEnv::new().build_db();
    db.put("lagq/fill", fields!({ "v": 0i64 })).await.unwrap();
    db.put("lagq/keep", fields!({ "v": 0i64 })).await.unwrap();

    let (parent, sq) = QuerySpec::collection("lagq").into_parts();
    let (req_tx, req_rx) = mpsc::channel(4);
    let mut raw_stream = db
        .raw_grpc()
        .listen(ReceiverStream::new(req_rx))
        .await
        .unwrap()
        .into_inner();
    req_tx
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(QueryTarget {
                    parent,
                    query_type: Some(query_target::QueryType::StructuredQuery(sq)),
                })),
                resume_type: None,
            })),
            request_options: None,
        })
        .await
        .unwrap();
    while let Some(frame) = raw_stream.message().await.unwrap() {
        if let Some(ResponseType::TargetChange(tc)) = frame.response_type
            && tc.target_change_type == TargetChangeType::NoChange as i32
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    // Park listener on HTTP/2 + mpsc backpressure, then create & delete a transient doc amidst
    // >1024 unrelated writes so BroadcastStream lags.
    let pad = "x".repeat(2048);
    for i in 1..=100i64 {
        db.put("lagq/fill", fields!({ "v": i, "pad": pad.as_str() }))
            .await
            .unwrap();
    }
    db.put("lagq/transient", fields!({ "v": 1i64 }))
        .await
        .unwrap();
    for i in 0..1150i64 {
        db.put(&format!("noise/n{i:04}"), fields!({ "x": i }))
            .await
            .unwrap();
    }
    db.delete("lagq/transient", None).await.unwrap();
    db.put("lagq/keep", fields!({ "v": 42i64 })).await.unwrap();

    let mut saw_reset = false;
    let mut keep_v: Option<i64> = None;
    let mut reset_ids = Vec::new();
    let mut in_reset = false;
    loop {
        let frame = raw_stream
            .message()
            .await
            .unwrap()
            .expect("listen stream ended unexpectedly");
        match frame.response_type {
            Some(ResponseType::TargetChange(tc)) => {
                if tc.target_change_type == TargetChangeType::Reset as i32 {
                    saw_reset = true;
                    in_reset = true;
                    reset_ids.clear();
                } else if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty()
                {
                    in_reset = false;
                    if saw_reset && keep_v == Some(42) {
                        break;
                    }
                }
            }
            Some(ResponseType::DocumentChange(dc)) => {
                if let Some(doc) = dc.document {
                    if in_reset {
                        reset_ids.push(doc.name.rsplit('/').next().unwrap_or_default().to_string());
                    }
                    if doc.name.ends_with("/lagq/keep") {
                        keep_v = doc.fields.get("v").and_then(|v| match v.value_type {
                            Some(
                                googleapis::google::firestore::v1::value::ValueType::IntegerValue(
                                    n,
                                ),
                            ) => Some(n),
                            _ => None,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    assert!(saw_reset);
    assert_eq!(reset_ids, vec!["fill", "keep"]);

    // Subsequent incremental update on existing `lagq/keep` must not trigger a secondary reset.
    db.put("lagq/keep", fields!({ "v": 100i64 })).await.unwrap();
    let mut final_doc = None;
    while let Some(frame) = raw_stream.message().await.unwrap() {
        match frame.response_type {
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::Reset as i32 =>
            {
                panic!("unexpected secondary reset after recovery");
            }
            Some(ResponseType::DocumentChange(dc)) => {
                final_doc = dc.document;
            }
            Some(ResponseType::TargetChange(tc))
                if tc.target_change_type == TargetChangeType::NoChange as i32
                    && tc.target_ids.is_empty() =>
            {
                break;
            }
            _ => {}
        }
    }
    let final_doc = final_doc.expect("missing post-recovery DocumentChange");
    assert_doc_wire_eq(&final_doc, "lagq/keep", &fields!({ "v": 100i64 }));
    drop(req_tx);
}

#[gtest]
#[tokio::test]
async fn listen_query_target_projects_incremental_updates_when_select_present() {
    let mut db = TestEnv::new().build_db();
    db.put("proj/d1", fields!({ "title": "v1", "secret": "s1" }))
        .await
        .unwrap();

    let (mut handle, init) = db
        .listen_query(QuerySpec::collection("proj").select(["title"]))
        .await
        .unwrap();
    expect_eq!(init.changed_docs.len(), 1);
    assert_doc_wire_eq(
        &init.changed_docs[0],
        "proj/d1",
        &fields!({ "title": "v1" }),
    );

    // Incremental update to existing document (unselected `secret` must not leak).
    db.put("proj/d1", fields!({ "title": "v2", "secret": "s2" }))
        .await
        .unwrap();
    let b1 = handle.next_batch().await.unwrap();
    assert!(!b1.was_reset);
    expect_eq!(b1.changed_docs.len(), 1);
    assert_doc_wire_eq(&b1.changed_docs[0], "proj/d1", &fields!({ "title": "v2" }));

    // Inserting a new matching document triggers a target reset whose documents must remain
    // projected.
    db.put("proj/d2", fields!({ "title": "new", "secret": "hidden" }))
        .await
        .unwrap();
    let b2 = handle.next_batch().await.unwrap();
    assert!(b2.was_reset);
    expect_eq!(b2.changed_docs.len(), 2);
    assert_doc_wire_eq(&b2.changed_docs[0], "proj/d1", &fields!({ "title": "v2" }));
    assert_doc_wire_eq(&b2.changed_docs[1], "proj/d2", &fields!({ "title": "new" }));
}

#[gtest]
#[tokio::test]
async fn listen_query_target_with_offset_resets_when_skipped_doc_mutates() {
    let mut db = TestEnv::new().build_db();
    db.put("off/a", fields!({ "v": 1i64 })).await.unwrap();
    db.put("off/b", fields!({ "v": 2i64 })).await.unwrap();
    db.put("off/c", fields!({ "v": 3i64 })).await.unwrap();

    // Unordered query with offset(1): `reset_on_update` is false, and initial snapshot skips
    // `off/a`.
    let (mut handle, init) = db
        .listen_query(QuerySpec::collection("off").offset(1))
        .await
        .unwrap();
    expect_eq!(short_ids(&init.changed_docs), vec!["b", "c"]);

    // Deleting the offset-skipped doc `off/a` must reset the window so `off/b` becomes skipped
    // and only `off/c` remains.
    db.delete("off/a", None).await.unwrap();
    let b1 = handle.next_batch().await.unwrap();
    assert!(
        b1.was_reset,
        "deleting offset-skipped doc must emit TargetChange::Reset"
    );
    expect_eq!(short_ids(&b1.changed_docs), vec!["c"]);

    // Filtered query with offset(1): toggling filter eligibility of the skipped doc also resets.
    db.put("off_flt/d1", fields!({ "active": true }))
        .await
        .unwrap();
    db.put("off_flt/d2", fields!({ "active": true }))
        .await
        .unwrap();
    db.put("off_flt/d3", fields!({ "active": true }))
        .await
        .unwrap();

    let (mut fhandle, finit) = db
        .listen_query(
            QuerySpec::collection("off_flt")
                .where_filter(field_filter("active", Operator::Equal, true))
                .offset(1),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&finit.changed_docs), vec!["d2", "d3"]);

    db.put("off_flt/d1", fields!({ "active": false }))
        .await
        .unwrap();
    let fb1 = fhandle.next_batch().await.unwrap();
    assert!(fb1.was_reset);
    expect_eq!(short_ids(&fb1.changed_docs), vec!["d3"]);
}
