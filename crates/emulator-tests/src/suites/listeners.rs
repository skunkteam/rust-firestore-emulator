use std::collections::HashMap;

use googleapis::{
    fields,
    google::firestore::v1::{
        ListenRequest, Target, listen_request,
        structured_query::field_filter::Operator,
        target::{self, DocumentsTarget},
    },
};
use googletest::prelude::*;
use rstest::rstest;
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
    },
    Code::Unimplemented
)]
#[case::missing_target_change(
    ListenRequest {
        database: DATABASE_ROOT.to_string(),
        labels: HashMap::new(),
        target_change: None,
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
        })
        .await;
    let err_frame = handle.next_raw().await.expect("error frame");
    assert_status_code(err_frame, Code::Unimplemented);
}
