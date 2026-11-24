use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use emulator_database::{query::QueryBuilder, read_consistency::ReadConsistency, reference::Ref};
use googleapis::{
    fields,
    google::{
        firestore::v1::{
            BatchGetDocumentsRequest, BeginTransactionRequest, Precondition, TransactionOptions,
            batch_get_documents_request, document_transform, list_documents_request, precondition,
            structured_query::{CollectionSelector, field_filter::Operator},
            transaction_options::{self, ReadOnly, ReadWrite, read_only},
        },
        protobuf::Timestamp,
    },
    val,
};
use googletest::prelude::*;
use http::{Method, StatusCode};
use serde_json::json;
use tokio::sync::Barrier;
use tonic::{Code, Status};

use crate::harness::{
    builders::{
        AggregationSpec, PipelineSpec, QuerySpec, WriteBatchSpec, doc_path, field_filter,
        pre_exists,
    },
    contention::ContentionProbe,
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

#[gtest]
#[tokio::test]
async fn pessimistic_begin_commit_rollback_retry_and_invalid_txn_ids() {
    let mut db = TestEnv::new().build_db(); // STANDARD -> PESSIMISTIC
    db.put("accounts/a1", fields!({ "balance": 100i64 }))
        .await
        .unwrap();

    // 1. Read-write transaction commit updates atomically
    let mut txn = db.begin_rw_txn().await.expect("BeginTransaction");
    let snap = txn.get("accounts/a1").await.unwrap();
    let bg_snap = txn.batch_get(["accounts/a1"]).await.unwrap();
    expect_eq!(bg_snap.found_docs().len(), 1);
    let bal = snap.fields["balance"].as_integer().unwrap();
    txn.put("accounts/a1", fields!({ "balance": bal - 30 }))
        .stage_writes(WriteBatchSpec::new().put("accounts/a2", fields!({ "balance": 30i64 })))
        .delete("accounts/stale", None);
    let (commit_ts, results) = txn.commit().await.expect("Commit");
    expect_that!(commit_ts, some(anything()));
    expect_eq!(results.len(), 3);

    assert_doc_wire_eq(
        &db.get("accounts/a1").await.unwrap(),
        "accounts/a1",
        &fields!({ "balance": 70i64 }),
    );
    assert_doc_wire_eq(
        &db.get("accounts/a2").await.unwrap(),
        "accounts/a2",
        &fields!({ "balance": 30i64 }),
    );

    // 2. Retry transaction: reusing an active txn_id fails with FailedPrecondition, but after
    //    Rollback the same txn_id can be started again cleanly.
    let active = db.begin_rw_txn().await.unwrap();
    let saved_id = active.id().to_vec();
    assert_status_code(
        db.begin_retry_rw_txn(&saved_id).await,
        Code::FailedPrecondition,
    );
    active.rollback().await.expect("Rollback");

    // Using a rolled-back txn_id for reads/commits fails with InvalidArgument
    assert_status_code(
        db.get_in_txn("accounts/a1", &saved_id).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.commit_raw(&saved_id, vec![]).await,
        Code::InvalidArgument,
    );

    // Retrying with the released txn_id now succeeds and rotates the 16-byte token
    let retried = db
        .begin_retry_rw_txn(&saved_id)
        .await
        .expect("retry after rollback");
    expect_ne!(retried.id(), saved_id.as_slice());
    expect_eq!(retried.id().len(), 16);
    retried.rollback().await.unwrap();

    // Malformed transaction bytes -> InvalidArgument
    assert_status_code(
        db.get_in_txn("accounts/a1", b"bad-id").await,
        Code::InvalidArgument,
    );
    assert_status_code(db.rollback_raw(b"bad-id").await, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn optimistic_concurrency_aborts_on_document_or_query_write_skew() {
    // Verified in `test-suite/tests/8-optimistic-transaction.test.ts`: Enterprise defaults to
    // OPTIMISTIC.
    let mut db = TestEnv::enterprise().build_db();
    db.put("stock/sku1", fields!({ "qty": 10i64 }))
        .await
        .unwrap();

    // 1. Concurrent modification to an observed document aborts a writing commit with Code::Aborted
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    let seen = txn1.get("stock/sku1").await.unwrap();
    expect_eq!(seen.fields["qty"].as_integer(), Some(10));

    // Concurrent external write advances `stock/sku1` without blocking (optimistic mode holds no
    // read locks!)
    db.put("stock/sku1", fields!({ "qty": 9i64 }))
        .await
        .expect("external write does not block on optimistic reader");

    txn1.put("stock/sku1", fields!({ "qty": 8i64 }));
    assert_status_code(txn1.commit().await, Code::Aborted);

    // 2. Concurrent insertion into a queried range also aborts optimistic commit
    let mut txn2 = db.begin_rw_txn().await.unwrap();
    let q = txn2
        .query(QuerySpec::collection("stock").where_filter(field_filter(
            "qty",
            Operator::GreaterThan,
            5i64,
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&q.documents), vec!["sku1"]);

    db.put("stock/sku2", fields!({ "qty": 20i64 }))
        .await
        .unwrap();
    txn2.put("audit/log1", fields!({ "checked": true }));
    assert_status_code(txn2.commit().await, Code::Aborted);

    // 3. Optimistic commit with zero writes succeeds even if observed doc changed
    let mut read_only_rw = db.begin_rw_txn().await.unwrap();
    let _unused = read_only_rw.get("stock/sku1").await.unwrap();
    db.put("stock/sku1", fields!({ "qty": 7i64 }))
        .await
        .unwrap();
    read_only_rw
        .commit()
        .await
        .expect("empty writes commit has no write skew");
}

#[gtest]
#[tokio::test]
async fn read_only_transactions_mvcc_snapshots_and_new_transaction_stream_options() {
    let mut db = TestEnv::enterprise().build_db();

    let v1 = db
        .put("docs/d1", fields!({ "ver": 1i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    let v2 = db
        .put("docs/d1", fields!({ "ver": 2i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    db.put("docs/d2", fields!({ "ver": 20i64 })).await.unwrap();
    db.delete("docs/d1", None).await.unwrap();

    // Direct historical `ReadTime` reads see exact MVCC versions across updates and deletes
    assert_doc_wire_eq(
        &db.get_at_time("docs/d1", v1).await.unwrap(),
        "docs/d1",
        &fields!({ "ver": 1i64 }),
    );
    assert_doc_wire_eq(
        &db.get_at_time("docs/d1", v2).await.unwrap(),
        "docs/d1",
        &fields!({ "ver": 2i64 }),
    );
    assert_status_code(db.get("docs/d1").await, Code::NotFound);

    // ReadOnly transaction pinned to `v2` sees `d1 = 2` via both GetDocument and ExecutePipeline,
    // and rejects writes on Commit
    let mut ro = db.begin_ro_txn(Some(v2)).await.unwrap();
    assert_doc_wire_eq(
        &ro.get("docs/d1").await.unwrap(),
        "docs/d1",
        &fields!({ "ver": 2i64 }),
    );
    let ro_pipe = ro.pipeline(PipelineSpec::collection("docs")).await.unwrap();
    expect_eq!(short_ids(&ro_pipe.results), vec!["d1"]);
    expect_that!(ro_pipe.transaction.is_empty(), eq(true));
    ro.put("docs/d1", fields!({ "ver": 999i64 }));
    assert_status_code(ro.commit().await, Code::InvalidArgument);

    // `NewTransaction` on `BatchGetDocuments`, `RunQuery`, and `ExecutePipeline`
    let bg = db
        .batch_get_new_txn(
            ["docs/d2"],
            TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(ReadWrite::default())),
            },
        )
        .await
        .unwrap();
    assert!(!bg.transaction.is_empty());
    db.commit_raw(&bg.transaction, vec![])
        .await
        .expect("commit NewTransaction from BatchGetDocuments");

    let rq = db
        .query_new_txn(
            QuerySpec::collection("docs"),
            TransactionOptions {
                mode: Some(transaction_options::Mode::ReadOnly(ReadOnly {
                    consistency_selector: Some(read_only::ConsistencySelector::ReadTime(v2)),
                })),
            },
        )
        .await
        .unwrap();
    assert!(!rq.transaction.is_empty());
    expect_eq!(short_ids(&rq.documents), vec!["d1"]);
    db.rollback_raw(&rq.transaction).await.unwrap();

    let ep = db
        .pipeline_new_txn(
            PipelineSpec::collection("docs"),
            TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(ReadWrite::default())),
            },
        )
        .await
        .unwrap();
    assert!(!ep.transaction.is_empty());
    expect_eq!(short_ids(&ep.results), vec!["d2"]);
    db.rollback_raw(&ep.transaction).await.unwrap();
}

#[gtest]
#[tokio::test]
async fn read_only_transactions_pin_snapshot_without_explicit_read_time() {
    let mut db = TestEnv::enterprise().build_db();

    db.put("docs/a", fields!({ "val": 1i64 })).await.unwrap();
    db.put("docs/b", fields!({ "val": 1i64 })).await.unwrap();

    // Pin read-only transaction before concurrent mutations occur.
    let mut ro = db.begin_ro_txn(None).await.unwrap();

    // Subsequent writes and inserts occur after `ro` began.
    db.put("docs/a", fields!({ "val": 2i64 })).await.unwrap();
    db.put("docs/b", fields!({ "val": 2i64 })).await.unwrap();
    db.put("docs/c", fields!({ "val": 2i64 })).await.unwrap();

    assert_doc_wire_eq(
        &ro.get("docs/a").await.unwrap(),
        "docs/a",
        &fields!({ "val": 1i64 }),
    );

    let bg = ro.batch_get(["docs/a", "docs/b", "docs/c"]).await.unwrap();
    expect_eq!(
        short_ids(&bg.found_docs().into_iter().cloned().collect::<Vec<_>>()),
        vec!["a", "b"]
    );
    expect_eq!(bg.missing_names(), vec![doc_path("docs/c")]);
    for doc in bg.found_docs() {
        expect_eq!(doc.fields["val"].as_integer(), Some(1));
    }

    let q = ro
        .query(QuerySpec::collection("docs").order_asc("__name__"))
        .await
        .unwrap();
    expect_eq!(short_ids(&q.documents), vec!["a", "b"]);
    for doc in &q.documents {
        expect_eq!(doc.fields["val"].as_integer(), Some(1));
    }

    let agg = ro
        .aggregate(
            QuerySpec::collection("docs"),
            AggregationSpec::new().count("n").sum("total", "val"),
        )
        .await
        .unwrap();
    expect_eq!(agg.fields["n"].as_integer(), Some(2));
    expect_eq!(agg.fields["total"].as_integer(), Some(2));

    let pipe = ro.pipeline(PipelineSpec::collection("docs")).await.unwrap();
    let mut pipe_ids = short_ids(&pipe.results);
    pipe_ids.sort();
    expect_eq!(pipe_ids, vec!["a", "b"]);
    for doc in &pipe.results {
        expect_eq!(doc.fields["val"].as_integer(), Some(1));
    }

    expect_that!(q.read_time, some(anything()));
    expect_eq!(bg.read_time, q.read_time);
    expect_eq!(agg.read_time, q.read_time);
    expect_eq!(pipe.execution_time, q.read_time);
    ro.rollback().await.unwrap();

    // Inline `NewTransaction` with `mode: None` (defaults to ReadOnly) also pins snapshot time.
    let bg_new = db
        .batch_get_new_txn(["docs/a"], TransactionOptions { mode: None })
        .await
        .unwrap();
    assert!(!bg_new.transaction.is_empty());
    db.put("docs/b", fields!({ "val": 3i64 })).await.unwrap();
    assert_doc_wire_eq(
        &db.get_in_txn("docs/b", &bg_new.transaction).await.unwrap(),
        "docs/b",
        &fields!({ "val": 2i64 }),
    );
    db.rollback_raw(&bg_new.transaction).await.unwrap();
}

#[gtest]
#[tokio::test]
async fn read_time_rejects_invalid_and_future_timestamps_and_allows_epoch() {
    let mut db = TestEnv::new().build_db();
    db.put("docs/a", fields!({ "val": 1i64 })).await.unwrap();

    let now = Timestamp::now();
    let invalid_times = [
        Timestamp {
            seconds: now.seconds + 60,
            nanos:   0,
        },
        Timestamp {
            seconds: -1,
            nanos:   0,
        },
        Timestamp {
            seconds: 1,
            nanos:   -1,
        },
        Timestamp {
            seconds: 1,
            nanos:   1_000_000_000,
        },
    ];

    for ts in invalid_times {
        assert_status_code(db.begin_ro_txn(Some(ts)).await, Code::InvalidArgument);
        assert_status_code(db.get_at_time("docs/a", ts).await, Code::InvalidArgument);
        assert_status_code(
            db.query_at_time(QuerySpec::collection("docs"), ts).await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.aggregate_at_time(
                QuerySpec::collection("docs"),
                AggregationSpec::new().count("n"),
                ts,
            )
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.batch_get_new_txn(
                ["docs/a"],
                TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadOnly(ReadOnly {
                        consistency_selector: Some(read_only::ConsistencySelector::ReadTime(ts)),
                    })),
                },
            )
            .await,
            Code::InvalidArgument,
        );
        let mut stream = db
            .raw_grpc()
            .batch_get_documents(BatchGetDocumentsRequest {
                database: DATABASE_ROOT.to_string(),
                documents: vec![doc_path("docs/a")],
                mask: None,
                consistency_selector: Some(
                    batch_get_documents_request::ConsistencySelector::ReadTime(ts),
                ),
                request_options: None,
            })
            .await
            .unwrap()
            .into_inner();
        assert_status_code(stream.message().await, Code::InvalidArgument);
    }

    // REST `:runQuery` with future `readTime` must also be rejected with HTTP 400.
    let (status, _) = db
        .http_oneshot(
            Method::POST,
            &format!("/v1/{DOCUMENTS_ROOT}:runQuery"),
            Some(&json!({
                "readTime": "2099-01-01T00:00:00Z",
                "structuredQuery": { "from": [{ "collectionId": "docs" }] },
            })),
        )
        .await;
    expect_eq!(status, StatusCode::BAD_REQUEST);

    // Unix epoch (1970-01-01T00:00:00Z) is valid across all read paths.
    let epoch = Timestamp {
        seconds: 0,
        nanos:   0,
    };
    let ro = db.begin_ro_txn(Some(epoch)).await.unwrap();
    ro.rollback().await.unwrap();
    assert_status_code(db.get_at_time("docs/a", epoch).await, Code::NotFound);
    let q = db
        .query_at_time(QuerySpec::collection("docs"), epoch)
        .await
        .unwrap();
    expect_that!(q.documents, is_empty());
    let agg = db
        .aggregate_at_time(
            QuerySpec::collection("docs"),
            AggregationSpec::new().count("n"),
            epoch,
        )
        .await
        .unwrap();
    expect_eq!(agg.fields["n"].as_integer(), Some(0));
}

#[gtest]
#[tokio::test]
async fn pessimistic_commit_cleanup_releases_locks_and_serializes_concurrent_commit_or_rollback() {
    let mut db = TestEnv::new().build_db();
    db.put("docs/a", fields!({ "v": 1i64 })).await.unwrap();

    // 1. A failed commit drops read guards and frees the transaction ID for immediate retry.
    let mut txn = db.begin_rw_txn().await.unwrap();
    let saved_id = txn.id().to_vec();
    let _unused = txn.get("docs/a").await.unwrap();
    txn.stage_writes(WriteBatchSpec::new().update_with(
        "docs/a",
        fields!({ "v": 99i64 }),
        None,
        vec![],
        Some(pre_exists(false)),
    ));
    assert_status_code(txn.commit().await, Code::AlreadyExists);

    // Held read guard must already be dropped (write must succeed immediately without deadlock).
    db.put("docs/a", fields!({ "v": 2i64 }))
        .await
        .expect("write after failed commit must not be blocked by leaked read guard");

    // Failed transaction is closed (`GetDocument` rejects), Rollback is idempotent `Ok(())`, and
    // `retry_transaction` succeeds immediately.
    assert_status_code(
        db.get_in_txn("docs/a", &saved_id).await,
        Code::InvalidArgument,
    );
    db.rollback_raw(&saved_id)
        .await
        .expect("rollback after failed commit must be idempotent Ok");
    db.rollback_raw(&saved_id)
        .await
        .expect("second rollback after failed commit must remain idempotent Ok");
    let retried = db
        .begin_retry_rw_txn(&saved_id)
        .await
        .expect("retry_transaction succeeds immediately after failed commit");
    retried.rollback().await.unwrap();

    // 2. Concurrent commit/commit on same txn ID produces one winner and one InvalidArgument.
    let txn_cc = db.begin_rw_txn().await.unwrap();
    let id_cc = txn_cc.id().to_vec();
    let mut c1 = db.clone();
    let mut c2 = db.clone();
    let w1 = WriteBatchSpec::new()
        .put("docs/cc", fields!({ "winner": 1i64 }))
        .into_writes();
    let w2 = WriteBatchSpec::new()
        .put("docs/cc", fields!({ "winner": 2i64 }))
        .into_writes();
    let (r1, r2) = tokio::join!(c1.commit_raw(&id_cc, w1), c2.commit_raw(&id_cc, w2));
    match (r1, r2) {
        (Ok(_), Err(e)) | (Err(e), Ok(_)) => expect_eq!(e.code(), Code::InvalidArgument),
        other => panic!("expected exactly one winner and one InvalidArgument, got {other:?}"),
    }

    // 3. Concurrent commit/rollback on same txn ID:
    //    - Rollback wins before commit starts -> Commit fails with InvalidArgument and doc is
    //      unwritten.
    //    - Rollback runs while commit is Committing -> Rollback returns Ok and Commit still writes.
    //    - Commit finishes before rollback runs -> Commit succeeds and Rollback fails with Aborted.
    let txn_cr = db.begin_rw_txn().await.unwrap();
    let id_cr = txn_cr.id().to_vec();
    let mut cr1 = db.clone();
    let mut cr2 = db.clone();
    let w_cr = WriteBatchSpec::new()
        .put("docs/cr", fields!({ "written": true }))
        .into_writes();
    let (rc, rr) = tokio::join!(cr1.commit_raw(&id_cr, w_cr), cr2.rollback_raw(&id_cr));
    match (rc, rr) {
        (Err(e), Ok(())) => {
            expect_eq!(e.code(), Code::InvalidArgument);
            assert_status_code(db.get("docs/cr").await, Code::NotFound);
        }
        (Ok(_), Ok(())) => {
            assert_doc_wire_eq(
                &db.get("docs/cr").await.unwrap(),
                "docs/cr",
                &fields!({ "written": true }),
            );
        }
        (Ok(_), Err(e)) => {
            expect_eq!(e.code(), Code::Aborted);
            assert_doc_wire_eq(
                &db.get("docs/cr").await.unwrap(),
                "docs/cr",
                &fields!({ "written": true }),
            );
        }
        other => {
            panic!(
                "expected rollback-before-commit (InvalidArgument, Ok), rollback-during-commit \
                 (Ok, Ok), or rollback-after-commit (Ok, Aborted), got {other:?}"
            );
        }
    }

    // 4. Concurrent/repeated reads of the same document within one pessimistic transaction never
    //    leave an extra Arc reference that breaks guard upgrade during commit.
    let mut txn_dup = db.begin_rw_txn().await.unwrap();
    let id_dup = txn_dup.id().to_vec();
    let mut d1 = db.clone();
    let mut d2 = db.clone();
    let (g1, g2) = tokio::join!(
        d1.get_in_txn("docs/a", &id_dup),
        d2.get_in_txn("docs/a", &id_dup),
    );
    expect_eq!(g1.unwrap().fields["v"].as_integer(), Some(2));
    expect_eq!(g2.unwrap().fields["v"].as_integer(), Some(2));
    txn_dup.put("docs/a", fields!({ "v": 3i64 }));
    txn_dup
        .commit()
        .await
        .expect("commit after duplicate same-doc reads");
    // Rollback after a succeeded commit fails with Aborted (even on repeat).
    assert_status_code(db.rollback_raw(&id_dup).await, Code::Aborted);
    assert_status_code(db.rollback_raw(&id_dup).await, Code::Aborted);

    // 5. Invalid-length and zero TransactionId wire bytes are never valid.
    assert_status_code(
        db.begin_retry_rw_txn(&0usize.to_ne_bytes()).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.rollback_raw(&0usize.to_ne_bytes()).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.begin_retry_rw_txn(&[0u8; 16]).await,
        Code::InvalidArgument,
    );
    assert_status_code(db.rollback_raw(&[0u8; 16]).await, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn rollback_idempotency_validation_preservation_and_read_only_commit_lifecycle() {
    let mut db = TestEnv::new().build_db();
    db.put("docs/a", fields!({ "v": 1i64 })).await.unwrap();

    // 1. Commit request validation errors (reserved field `__bad__`, unspecified/unknown
    //    `SetToServerValue`, and invalid `Precondition::UpdateTime`) keep RW transaction Active.
    let txn_val = db.begin_rw_txn().await.unwrap();
    let id_val = txn_val.id().to_vec();
    let bad_write = WriteBatchSpec::new()
        .put("docs/a", fields!({ "__bad__": 1i64 }))
        .into_writes();
    let bad_sv_unspecified = WriteBatchSpec::new()
        .update_with(
            "docs/a",
            fields!({ "v": 9i64 }),
            None,
            vec![document_transform::FieldTransform {
                field_path:     "ts".to_string(),
                transform_type: Some(
                    document_transform::field_transform::TransformType::SetToServerValue(0),
                ),
            }],
            None,
        )
        .into_writes();
    let bad_sv_unknown = WriteBatchSpec::new()
        .update_with(
            "docs/a",
            fields!({ "v": 9i64 }),
            None,
            vec![document_transform::FieldTransform {
                field_path:     "ts".to_string(),
                transform_type: Some(
                    document_transform::field_transform::TransformType::SetToServerValue(999),
                ),
            }],
            None,
        )
        .into_writes();
    let bad_precond_ts = WriteBatchSpec::new()
        .update_with(
            "docs/a",
            fields!({ "v": 9i64 }),
            None,
            vec![],
            Some(Precondition {
                condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                    seconds: -1,
                    nanos:   0,
                })),
            }),
        )
        .into_writes();
    for invalid_writes in [
        &bad_write,
        &bad_sv_unspecified,
        &bad_sv_unknown,
        &bad_precond_ts,
    ] {
        assert_status_code(
            db.commit_raw(&id_val, invalid_writes.clone()).await,
            Code::InvalidArgument,
        );
        assert_doc_wire_eq(
            &db.get_in_txn("docs/a", &id_val).await.unwrap(),
            "docs/a",
            &fields!({ "v": 1i64 }),
        );
    }
    let good_write = WriteBatchSpec::new()
        .put("docs/a", fields!({ "v": 2i64 }))
        .into_writes();
    db.commit_raw(&id_val, good_write)
        .await
        .expect("valid commit after validation error must succeed");
    assert_doc_wire_eq(
        &db.get("docs/a").await.unwrap(),
        "docs/a",
        &fields!({ "v": 2i64 }),
    );
    assert_status_code(db.rollback_raw(&id_val).await, Code::Aborted);

    // 2. Validation error keeps RO transaction Active, while a valid write closes RO and allows
    //    idempotent Rollback while rejecting retry_transaction.
    let ro_val = db.begin_ro_txn(None).await.unwrap();
    let ro_val_id = ro_val.id().to_vec();
    assert_status_code(
        db.commit_raw(&ro_val_id, bad_write).await,
        Code::InvalidArgument,
    );
    assert_doc_wire_eq(
        &db.get_in_txn("docs/a", &ro_val_id).await.unwrap(),
        "docs/a",
        &fields!({ "v": 2i64 }),
    );
    db.rollback_raw(&ro_val_id).await.unwrap();
    db.rollback_raw(&ro_val_id).await.unwrap();

    let ro_write = db.begin_ro_txn(None).await.unwrap();
    let ro_write_id = ro_write.id().to_vec();
    assert_status_code(
        db.begin_retry_rw_txn(&ro_write_id).await,
        Code::InvalidArgument,
    );
    let valid_ro_write = WriteBatchSpec::new()
        .put("docs/a", fields!({ "v": 3i64 }))
        .into_writes();
    assert_status_code(
        db.commit_raw(&ro_write_id, valid_ro_write).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.get_in_txn("docs/a", &ro_write_id).await,
        Code::InvalidArgument,
    );
    db.rollback_raw(&ro_write_id).await.unwrap();
    assert_status_code(
        db.begin_retry_rw_txn(&ro_write_id).await,
        Code::InvalidArgument,
    );

    // 3. Empty RO commit succeeds and subsequent Rollback returns Aborted.
    let ro_empty = db.begin_ro_txn(None).await.unwrap();
    let ro_empty_id = ro_empty.id().to_vec();
    db.commit_raw(&ro_empty_id, vec![]).await.unwrap();
    assert_status_code(db.rollback_raw(&ro_empty_id).await, Code::Aborted);

    // 4. Concurrent Rollback while Commit is blocked waiting on locks returns Ok without aborting
    //    the Commit; after blocker releases, Commit succeeds and later Rollback returns Aborted.
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("docs/a").await.unwrap();

    let committer = db.begin_rw_txn().await.unwrap();
    let committer_id = committer.id().to_vec();
    let mut commit_client = db.clone();
    let commit_writes = WriteBatchSpec::new()
        .put("docs/a", fields!({ "v": 42i64 }))
        .into_writes();
    let mut commit_probe = ContentionProbe::spawn_op(async move {
        commit_client.commit_raw(&committer_id, commit_writes).await
    });
    commit_probe.assert_pending().await;

    db.rollback_raw(committer.id())
        .await
        .expect("rollback during blocked commit must return Ok");
    commit_probe.assert_pending().await;

    blocker.rollback().await.unwrap();
    commit_probe
        .expect_ready()
        .await
        .expect("blocked commit must still succeed after concurrent rollback");
    assert_doc_wire_eq(
        &db.get("docs/a").await.unwrap(),
        "docs/a",
        &fields!({ "v": 42i64 }),
    );
    assert_status_code(db.rollback_raw(committer.id()).await, Code::Aborted);

    // 5. Failed commit followed by Rollback emits no `emulator_grpc` error logs for invalid txn ID.
    let log_handle = "/emulator/v1/logging/rollback-idempotent-check";
    let log_poll = "/emulator/v1/logging/rollback-idempotent-check:poll";
    let (st, _) = db
        .http_raw_body(
            Method::POST,
            log_handle,
            Some("text/plain"),
            "emulator_grpc=error",
        )
        .await;
    expect_eq!(st, StatusCode::OK);

    let mut failed_txn = db.begin_rw_txn().await.unwrap();
    let failed_id = failed_txn.id().to_vec();
    let failed_lineage = u64::from_le_bytes(failed_id[..8].try_into().unwrap());
    let failed_attempt = u64::from_le_bytes(failed_id[8..16].try_into().unwrap());
    failed_txn.stage_writes(WriteBatchSpec::new().update_with(
        "docs/a",
        fields!({ "v": 100i64 }),
        None,
        vec![],
        Some(pre_exists(false)),
    ));
    assert_status_code(failed_txn.commit().await, Code::AlreadyExists);
    db.rollback_raw(&failed_id).await.unwrap();

    let (poll_st, poll_bytes) = db.http_raw_body(Method::POST, log_poll, None, "").await;
    let (del_st, _) = db.http_oneshot(Method::DELETE, log_handle, None).await;
    expect_eq!(poll_st, StatusCode::OK);
    expect_eq!(del_st, StatusCode::OK);
    let captured = String::from_utf8_lossy(&poll_bytes);
    let expected_err_fragment =
        format!("invalid transaction ID: {failed_lineage}:{failed_attempt}");
    assert!(
        !captured.contains(&expected_err_fragment) && !captured.contains("invalid transaction ID"),
        "failed commit + follow-up rollback must not log `{expected_err_fragment}`, \
         got:\n{captured}"
    );
}

#[gtest]
#[tokio::test]
async fn optimistic_offset_query_aborts_on_skipped_doc_deletion_or_shift() {
    let mut db = TestEnv::enterprise().build_db();
    db.put("items/d1", fields!({ "k": 1i64 })).await.unwrap();
    db.put("items/d2", fields!({ "k": 2i64 })).await.unwrap();
    db.put("items/d3", fields!({ "k": 3i64 })).await.unwrap();
    db.put("out/target", fields!({ "n": 0i64 })).await.unwrap();

    // Case 1: Deleting the skipped doc shifts `d3` into the offset(1).limit(1) window.
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    let q1 = txn1
        .query(
            QuerySpec::collection("items")
                .order_asc("k")
                .offset(1)
                .limit(1),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&q1.documents), vec!["d2"]);
    db.delete("items/d1", None).await.unwrap();
    txn1.put("out/target", fields!({ "n": 1i64 }));
    assert_status_code(txn1.commit().await, Code::Aborted);

    // Case 2: Deleting the skipped doc shrinks offset(1).limit(1) to empty.
    db.delete("items/d3", None).await.unwrap();
    db.put("items/d1", fields!({ "k": 1i64 })).await.unwrap();
    let mut txn2 = db.begin_rw_txn().await.unwrap();
    let q2 = txn2
        .query(
            QuerySpec::collection("items")
                .order_asc("k")
                .offset(1)
                .limit(1),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&q2.documents), vec!["d2"]);
    db.delete("items/d1", None).await.unwrap();
    txn2.put("out/target", fields!({ "n": 2i64 }));
    assert_status_code(txn2.commit().await, Code::Aborted);

    assert_doc_wire_eq(
        &db.get("out/target").await.unwrap(),
        "out/target",
        &fields!({ "n": 0i64 }),
    );
}

#[gtest]
#[tokio::test]
async fn optimistic_commit_prevents_lost_updates_and_write_skew() {
    let mut db = TestEnv::enterprise().build_db();
    db.put("c/a", fields!({ "val": 1i64 })).await.unwrap();
    db.put("c/b", fields!({ "val": 1i64 })).await.unwrap();

    // 1. Same-doc lost update prevention: pause T1 acquiring write guard on `c/0_lock` (ordered
    //    before `c/a`), mutate `c/a` concurrently, then release `c/0_lock`.
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;
    let mut blocker = db.begin_rw_txn().await.unwrap();
    assert_status_code(blocker.get("c/0_lock").await, Code::NotFound);
    db.set_admin_mode(None, Some("OPTIMISTIC")).await;

    let mut t1 = db.begin_rw_txn().await.unwrap();
    let _unused = t1.get("c/a").await.unwrap();
    t1.put("c/0_lock", fields!({ "held": true }))
        .put("c/a", fields!({ "val": 2i64 }));

    let mut op1 = ContentionProbe::spawn_op(t1.commit());
    op1.assert_pending().await;

    db.put("c/a", fields!({ "val": 99i64 })).await.unwrap();
    blocker.rollback().await.unwrap();

    op1.expect_aborted().await;
    assert_doc_wire_eq(
        &db.get("c/a").await.unwrap(),
        "c/a",
        &fields!({ "val": 99i64 }),
    );

    // 2. Cross-doc write-skew prevention: T1 reads {a, b} writes {0_lock, a}; T2 reads {a, b}
    //    writes {b}. Only T1 may commit; T2 must abort cleanly.
    db.put("c/a", fields!({ "val": 1i64 })).await.unwrap();
    db.put("c/b", fields!({ "val": 1i64 })).await.unwrap();
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;
    let mut blocker2 = db.begin_rw_txn().await.unwrap();
    let _unused = blocker2.get("c/0_lock").await;
    db.set_admin_mode(None, Some("OPTIMISTIC")).await;

    let mut ws1 = db.begin_rw_txn().await.unwrap();
    let _unused = ws1.get("c/a").await.unwrap();
    let _unused = ws1.get("c/b").await.unwrap();
    ws1.put("c/0_lock", fields!({ "held": true }))
        .put("c/a", fields!({ "val": 10i64 }));

    let mut ws2 = db.begin_rw_txn().await.unwrap();
    let _unused = ws2.get("c/a").await.unwrap();
    let _unused = ws2.get("c/b").await.unwrap();
    ws2.put("c/b", fields!({ "val": 20i64 }));

    let mut op_ws1 = ContentionProbe::spawn_op(ws1.commit());
    op_ws1.assert_pending().await;
    let mut op_ws2 = ContentionProbe::spawn_op(ws2.commit());
    op_ws2.assert_pending().await;

    blocker2.rollback().await.unwrap();
    op_ws1
        .expect_ready()
        .await
        .expect("first optimistic commit should succeed");
    op_ws2.expect_aborted().await;

    assert_doc_wire_eq(
        &db.get("c/a").await.unwrap(),
        "c/a",
        &fields!({ "val": 10i64 }),
    );
    assert_doc_wire_eq(
        &db.get("c/b").await.unwrap(),
        "c/b",
        &fields!({ "val": 1i64 }),
    );

    // 3. Updating a document returned by an optimistic query inside the same txn succeeds cleanly.
    let mut self_write = db.begin_rw_txn().await.unwrap();
    let queried = self_write.query(QuerySpec::collection("c")).await.unwrap();
    assert!(!queried.documents.is_empty());
    self_write.put("c/a", fields!({ "val": 11i64 }));
    self_write.commit().await.unwrap();
    assert_doc_wire_eq(
        &db.get("c/a").await.unwrap(),
        "c/a",
        &fields!({ "val": 11i64 }),
    );
}

#[gtest]
#[tokio::test]
async fn remote_wound_aborts_follow_ups_inherits_root_seq_with_rotated_tokens_and_preserves_tombstones()
 {
    let mut db = TestEnv::new().build_db();
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;
    for doc in ["txl/seed_oldest", "txl/seed_mid", "txl/a", "txl/b"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    let mut oldest = db.begin_rw_txn().await.unwrap();
    let _unused = oldest.get("txl/seed_oldest").await.unwrap();

    let mut target1 = db.begin_rw_txn().await.unwrap();
    let target1_id = target1.id().to_vec();
    expect_eq!(target1_id.len(), 16);
    let _unused = target1.get("txl/a").await.unwrap();

    let mut mid = db.begin_rw_txn().await.unwrap();
    let mid_id = mid.id().to_vec();
    let _unused = mid.get("txl/seed_mid").await.unwrap();

    // `oldest` commits a write to `txl/a`, remotely wounding `target1`.
    oldest.put("txl/a", fields!({ "v": 1i64 }));
    oldest.commit().await.unwrap();

    // Every follow-up operation on wounded `target1` returns `Code::Aborted`.
    assert_status_code(db.get_in_txn("txl/b", &target1_id).await, Code::Aborted);
    assert_status_code(
        db.query_in_txn(QuerySpec::collection("txl"), &target1_id)
            .await,
        Code::Aborted,
    );
    assert_status_code(
        db.batch_get_in_txn(["txl/b"], &target1_id).await,
        Code::Aborted,
    );
    assert_status_code(
        db.pipeline_in_txn(PipelineSpec::collection("txl"), &target1_id)
            .await,
        Code::Aborted,
    );
    assert_status_code(
        db.commit_raw(
            &target1_id,
            WriteBatchSpec::new()
                .put("txl/b", fields!({ "v": 9i64 }))
                .into_writes(),
        )
        .await,
        Code::Aborted,
    );

    // Best-effort `Rollback(target1)` is idempotent `Ok(())` and preserves the Wounded tombstone +
    // `root_seq` for the subsequent retry.
    db.rollback_raw(&target1_id)
        .await
        .expect("rollback after remote wound must return Ok");

    let mut target2 = db
        .begin_retry_rw_txn(&target1_id)
        .await
        .expect("retry after wound + rollback succeeds");
    let target2_id = target2.id().to_vec();
    expect_eq!(target2_id.len(), 16);
    expect_ne!(target2_id, target1_id);

    // Delayed duplicate `Rollback(attempt_1)` while `attempt_2` is Active is an idempotent no-op.
    db.rollback_raw(&target1_id).await.unwrap();

    // `mid` reads `txl/b`; `target2` inherited `target1`'s original `root_seq` (< `mid`), so
    // `target2` wounds `mid` and commits cleanly!
    let _unused = mid.get("txl/b").await.unwrap();
    target2.put("txl/b", fields!({ "v": 2i64 }));
    target2
        .commit()
        .await
        .expect("retried transaction inherits original root_seq and wounds younger mid");
    assert_status_code(db.get_in_txn("txl/seed_mid", &mid_id).await, Code::Aborted);
    let _unused = mid.rollback().await;

    // Delayed `Rollback(attempt_1)` after `attempt_2` is Committed STILL returns `Ok(())`, whereas
    // `Rollback(attempt_2)` returns `Code::Aborted`.
    db.rollback_raw(&target1_id).await.unwrap();
    assert_status_code(db.rollback_raw(&target2_id).await, Code::Aborted);
    assert_doc_wire_eq(
        &db.get("txl/b").await.unwrap(),
        "txl/b",
        &fields!({ "v": 2i64 }),
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn commit_retry_aborted_lineage_drop_cancel_clear_and_eviction_lifecycle() {
    let mut db = TestEnv::new().build_db();
    for doc in ["lc/a", "lc/seed_mid", "lc/u", "lc/v", "lc/drop"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    // 1. `BeginTransaction(retry_transaction)` while `Committing` fails with `FailedPrecondition`;
    //    after commit finishes, retry on `Closed { read_only: false, Committed }` rotates token.
    let mut blocker = db.begin_rw_txn().await.unwrap();
    let _unused = blocker.get("lc/a").await.unwrap();

    let committer = db.begin_rw_txn().await.unwrap();
    let committer_id = committer.id().to_vec();
    let mut commit_client = db.clone();
    let mut commit_op = ContentionProbe::spawn_op(async move {
        commit_client
            .commit_raw(
                &committer_id,
                WriteBatchSpec::new()
                    .put("lc/a", fields!({ "v": 1i64 }))
                    .into_writes(),
            )
            .await
    });
    commit_op.assert_pending().await;

    assert_status_code(
        db.begin_retry_rw_txn(committer.id()).await,
        Code::FailedPrecondition,
    );
    blocker.rollback().await.unwrap();
    commit_op.expect_ready().await.unwrap();
    assert_status_code(db.rollback_raw(committer.id()).await, Code::Aborted);

    let after_committed = db
        .begin_retry_rw_txn(committer.id())
        .await
        .expect("retry after Committed RW slot succeeds with rotated bytes");
    expect_ne!(after_committed.id(), committer.id());
    after_committed.rollback().await.unwrap();

    // 2. Contention abort enters `AbortedLineage`: follow-up ops return `InvalidArgument`,
    //    `Rollback` returns `Ok(())` without clearing `root_seq`, and retry still wounds `mid`.
    let mut t1 = db.begin_rw_txn().await.unwrap();
    let _unused = t1.get("lc/u").await.unwrap();
    let mut t2 = db.begin_rw_txn().await.unwrap();
    let t2_id = t2.id().to_vec();
    let _unused = t2.get("lc/u").await.unwrap();
    let mut mid = db.begin_rw_txn().await.unwrap();
    let mid_id = mid.id().to_vec();
    let _unused = mid.get("lc/seed_mid").await.unwrap();

    let mut t2_commit = ContentionProbe::spawn_op(async move {
        t2.put("lc/u", fields!({ "v": 2i64 }));
        t2.commit().await
    });
    t2_commit.assert_pending().await;
    t1.put("lc/u", fields!({ "v": 1i64 }));
    t1.commit().await.unwrap();
    t2_commit.expect_aborted().await;

    assert_status_code(db.get_in_txn("lc/u", &t2_id).await, Code::InvalidArgument);
    assert_status_code(
        db.query_in_txn(QuerySpec::collection("lc"), &t2_id).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.commit_raw(
            &t2_id,
            WriteBatchSpec::new()
                .put("lc/u", fields!({ "v": 3i64 }))
                .into_writes(),
        )
        .await,
        Code::InvalidArgument,
    );
    db.rollback_raw(&t2_id).await.unwrap();

    let mut t2_retry = db.begin_retry_rw_txn(&t2_id).await.unwrap();
    expect_ne!(t2_retry.id(), t2_id.as_slice());
    let _unused = mid.get("lc/v").await.unwrap();
    t2_retry.put("lc/v", fields!({ "v": 5i64 }));
    t2_retry
        .commit()
        .await
        .expect("retry after AbortedLineage preserves root_seq across Rollback");
    assert_status_code(db.get_in_txn("lc/seed_mid", &mid_id).await, Code::Aborted);
    let _unused = mid.rollback().await;

    // 3. Dropping an in-flight blocked `Commit` future cleans up via `CommitSlotGuard::drop`: held
    //    locks release, `GetDocument` returns `InvalidArgument`, and `Rollback` returns `Ok`.
    let mut drop_blocker = db.begin_rw_txn().await.unwrap();
    let _unused = drop_blocker.get("lc/drop").await.unwrap();
    let dropped_txn = db.begin_rw_txn().await.unwrap();
    let dropped_id = dropped_txn.id().to_vec();
    let mut drop_client = db.clone();
    let mut dropped_op = ContentionProbe::spawn_op(async move {
        drop_client
            .commit_raw(
                &dropped_id,
                WriteBatchSpec::new()
                    .put("lc/drop", fields!({ "v": 9i64 }))
                    .into_writes(),
            )
            .await
    });
    dropped_op.assert_pending().await;
    drop(dropped_op);

    assert_status_code(
        db.get_in_txn("lc/drop", dropped_txn.id()).await,
        Code::InvalidArgument,
    );
    db.rollback_raw(dropped_txn.id()).await.unwrap();
    drop_blocker.rollback().await.unwrap();
    db.put("lc/drop", fields!({ "v": 10i64 }))
        .await
        .expect("cancelled commit future did not leak lock");

    // 4. Evicted `Vacant` valid issued ID can still be retried with rotated generation, and
    //    `clear_database` deletes active/closed slots immediately without retaining tombstones.
    let old_closed = db.begin_rw_txn().await.unwrap();
    let old_closed_id = old_closed.id().to_vec();
    old_closed.rollback().await.unwrap();
    for _ in 0..4_100 {
        let churn = db.begin_rw_txn().await.unwrap();
        churn.rollback().await.unwrap();
    }
    assert_status_code(db.rollback_raw(&old_closed_id).await, Code::InvalidArgument);
    let revived = db
        .begin_retry_rw_txn(&old_closed_id)
        .await
        .expect("retry on evicted valid issued ID succeeds");
    expect_ne!(revived.id(), old_closed_id.as_slice());

    let revived_id = revived.id().to_vec();
    let closed_before_clear = db.begin_rw_txn().await.unwrap();
    let closed_before_clear_id = closed_before_clear.id().to_vec();
    closed_before_clear.rollback().await.unwrap();
    let cleared_db = db.project().database(&TestEnv::root_ref()).await;
    db.clear_default_db().await;

    let revived_txn_id = revived_id.clone().try_into().unwrap();
    let closed_txn_id = closed_before_clear_id.clone().try_into().unwrap();
    assert!(matches!(
        cleared_db.commit(vec![], Some(revived_txn_id)).await,
        Err(emulator_database::GenericDatabaseError::Aborted(_))
    ));
    assert!(matches!(
        cleared_db.rollback(closed_txn_id).await,
        Err(emulator_database::GenericDatabaseError::Aborted(_))
    ));
    // Over gRPC, the old database was dropped so recreated database rejects stale handles cleanly.
    assert_status_code(
        db.get_in_txn("lc/a", &revived_id).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.rollback_raw(&closed_before_clear_id).await,
        Code::InvalidArgument,
    );
}

#[gtest]
#[tokio::test]
async fn optimistic_missing_doc_unary_get_detects_write_skew() {
    let mut db = TestEnv::enterprise().build_db();

    // 1. Unary GetDocument on a missing document in an absent collection must still pin `read_time`
    //    so concurrent creation aborts commit with `Code::Aborted`.
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    assert_status_code(txn1.get("opt_missing/a").await, Code::NotFound);
    db.put("opt_missing/a", fields!({ "created": true }))
        .await
        .unwrap();
    txn1.put("opt_missing_out/w1", fields!({ "n": 1i64 }));
    assert_status_code(txn1.commit().await, Code::Aborted);

    // 2. Unary GetDocument on a missing document in an existing collection must also detect
    //    concurrent creation.
    db.put("opt_existing/seed", fields!({ "v": 0i64 }))
        .await
        .unwrap();
    let mut txn2 = db.begin_rw_txn().await.unwrap();
    assert_status_code(txn2.get("opt_existing/absent").await, Code::NotFound);
    db.put("opt_existing/absent", fields!({ "created": true }))
        .await
        .unwrap();
    txn2.put("opt_missing_out/w2", fields!({ "n": 2i64 }));
    assert_status_code(txn2.commit().await, Code::Aborted);
}

#[gtest]
#[tokio::test]
async fn optimistic_phantom_ancestor_detects_write_skew() {
    let mut db = TestEnv::enterprise().build_db();

    // 1. `list_docs(show_missing = true)` on a phantom ancestor must abort if materialized.
    db.put("phantoms/p1/sub/child", fields!({ "v": 1i64 }))
        .await
        .unwrap();
    let mut txn1 = db.begin_rw_txn().await.unwrap();
    let listed = db
        .list_docs(
            "phantoms",
            0,
            "",
            "",
            None,
            true,
            Some(list_documents_request::ConsistencySelector::Transaction(
                txn1.id().to_vec(),
            )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&listed.documents), vec!["p1"]);
    db.put("phantoms/p1", fields!({ "materialized": true }))
        .await
        .unwrap();
    txn1.put("opt_missing_out/w3", fields!({ "n": 3i64 }));
    assert_status_code(txn1.commit().await, Code::Aborted);

    // 2. Standalone `Query::missing_docs` on a fresh optimistic transaction also reads phantom
    //    ancestors via `get_doc` (`peek_doc_meta == None`) and must pin `read_time`.
    db.put("phantoms2/p1/sub/child", fields!({ "v": 1i64 }))
        .await
        .unwrap();
    let raw_db = db.project().database(&TestEnv::root_ref()).await;
    let mut txn2 = db.begin_rw_txn().await.unwrap();
    let txn2_id = txn2.id().to_vec().try_into().unwrap();
    let missing_query = QueryBuilder::from(
        Ref::Root(TestEnv::root_ref()),
        vec![CollectionSelector {
            collection_id:   "phantoms2".to_string(),
            all_descendants: false,
        }],
    )
    .enterprise_edition(true)
    .consistency(ReadConsistency::Transaction(txn2_id))
    .build()
    .unwrap();
    let missing = missing_query.missing_docs(&raw_db).await.unwrap();
    expect_eq!(missing, vec![doc_path("phantoms2/p1")]);
    db.put("phantoms2/p1", fields!({ "materialized": true }))
        .await
        .unwrap();
    txn2.put("opt_missing_out/w4", fields!({ "n": 4i64 }));
    assert_status_code(txn2.commit().await, Code::Aborted);
}

fn assert_no_internal_or_cleared<T: std::fmt::Debug>(
    label: &str,
    res: Result<T, Status>,
    allow_aborted: bool,
) {
    if let Err(status) = res {
        assert_ne!(
            status.code(),
            Code::Internal,
            "{label} must never return Code::Internal: {status:?}"
        );
        assert!(
            !status.message().contains("database was cleared"),
            "{label} must never return \"database was cleared\": {status:?}"
        );
        if allow_aborted {
            assert!(
                matches!(status.code(), Code::InvalidArgument | Code::Aborted),
                "{label} returned unexpected error status: {status:?}"
            );
        } else {
            assert_eq!(
                status.code(),
                Code::InvalidArgument,
                "{label} expected InvalidArgument, got {status:?}"
            );
        }
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn concurrent_get_promoted_waiter_racing_rollback_never_returns_cleared() {
    let mut db = TestEnv::new().build_db();
    for doc in ["race/seed", "race/a", "race/b"] {
        db.put(doc, fields!({ "v": 1i64 })).await.unwrap();
    }

    // Promoted GetDocument waiter interrupted by Rollback before `consume_wait_grant` runs must
    // fail with `InvalidArgument` (never `"database was cleared"`).
    let mut hold_b = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b.get("race/b").await.unwrap();
    let mut writer = db.begin_rw_txn().await.unwrap();
    let _unused = writer.get("race/seed").await.unwrap();
    writer
        .put("race/a", fields!({ "v": 2i64 }))
        .put("race/b", fields!({ "v": 2i64 }));
    let mut writer_commit = ContentionProbe::spawn_op(writer.commit());
    writer_commit.assert_pending().await;

    let tx = db.begin_rw_txn().await.unwrap();
    let tx_id = tx.id().to_vec();
    let mut reader_db = db.clone();
    let reader_id = tx_id.clone();
    let mut tx_read =
        ContentionProbe::spawn_op(async move { reader_db.get_in_txn("race/a", &reader_id).await });
    tx_read.assert_pending().await;

    hold_b.rollback().await.unwrap();
    writer_commit.expect_ready().await.unwrap();

    // Roll back before polling `tx_read` so `consume_wait_grant` sees a closed session.
    db.rollback_raw(&tx_id).await.unwrap();
    let err = tx_read
        .expect_ready()
        .await
        .expect_err("rolled-back promoted read must fail");
    assert_no_internal_or_cleared("promoted read after rollback", Err::<(), _>(err), false);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn concurrent_get_parked_waiter_racing_same_txn_commit_never_returns_cleared() {
    let mut db = TestEnv::new().build_db();
    for doc in ["race2/seed", "race2/a", "race2/b"] {
        db.put(doc, fields!({ "v": 1i64 })).await.unwrap();
    }

    // Parked GetDocument waiter on `race2/a` when the same transaction commits `race2/x` (sealing
    // & closing its session) must fail cleanly without `"database was cleared"`.
    let mut hold_b2 = db.begin_rw_txn().await.unwrap();
    let _unused = hold_b2.get("race2/b").await.unwrap();
    let mut writer2 = db.begin_rw_txn().await.unwrap();
    let _unused = writer2.get("race2/seed").await.unwrap();
    writer2
        .put("race2/a", fields!({ "v": 3i64 }))
        .put("race2/b", fields!({ "v": 3i64 }));
    let mut writer2_commit = ContentionProbe::spawn_op(writer2.commit());
    writer2_commit.assert_pending().await;

    let tx2 = db.begin_rw_txn().await.unwrap();
    let tx2_id = tx2.id().to_vec();
    let mut reader2_db = db.clone();
    let reader2_id = tx2_id.clone();
    let mut tx2_read =
        ContentionProbe::spawn_op(
            async move { reader2_db.get_in_txn("race2/a", &reader2_id).await },
        );
    tx2_read.assert_pending().await;

    db.commit_raw(
        &tx2_id,
        WriteBatchSpec::new()
            .put("race2/x", fields!({ "v": 10i64 }))
            .into_writes(),
    )
    .await
    .unwrap();
    hold_b2.rollback().await.unwrap();
    writer2_commit.expect_ready().await.unwrap();

    let err2 = tx2_read
        .expect_ready()
        .await
        .expect_err("parked read during same-txn commit must fail");
    assert_no_internal_or_cleared(
        "parked read during same-txn commit",
        Err::<(), _>(err2),
        true,
    );
}

#[gtest]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_get_and_query_racing_commit_or_rollback_mt_never_return_internal_or_cleared() {
    let mut db = TestEnv::new().build_db();
    db.put("race_mt/d1", fields!({ "v": 1i64 })).await.unwrap();
    db.put("race_mt/d2", fields!({ "v": 2i64 })).await.unwrap();

    for iter in 0..40i64 {
        // Race GetDocument + RunQuery against Commit on the same TransactionId and documents.
        {
            let tx = db.begin_rw_txn().await.unwrap();
            let id = tx.id().to_vec();
            let barrier = Arc::new(Barrier::new(3));

            let mut gdb = db.clone();
            let gid = id.clone();
            let gb = Arc::clone(&barrier);
            let get_task = tokio::spawn(async move {
                gb.wait().await;
                gdb.get_in_txn("race_mt/d1", &gid).await
            });

            let mut qdb = db.clone();
            let qid = id.clone();
            let qb = Arc::clone(&barrier);
            let query_task = tokio::spawn(async move {
                qb.wait().await;
                qdb.query_in_txn(QuerySpec::collection("race_mt"), &qid)
                    .await
            });

            let mut cdb = db.clone();
            let cid = id;
            let cb = Arc::clone(&barrier);
            let commit_task = tokio::spawn(async move {
                cb.wait().await;
                cdb.commit_raw(
                    &cid,
                    WriteBatchSpec::new()
                        .put("race_mt/d1", fields!({ "v": iter + 10 }))
                        .put("race_mt/d2", fields!({ "v": iter + 20 }))
                        .into_writes(),
                )
                .await
            });

            let (gr, qr, cr) = tokio::join!(get_task, query_task, commit_task);
            assert_no_internal_or_cleared("mt commit race get", gr.unwrap(), true);
            assert_no_internal_or_cleared("mt commit race query", qr.unwrap(), true);
            assert_no_internal_or_cleared("mt commit race commit", cr.unwrap(), true);
        }

        // Race GetDocument + RunQuery against Rollback on the same TransactionId.
        {
            let tx = db.begin_rw_txn().await.unwrap();
            let id = tx.id().to_vec();
            let barrier = Arc::new(Barrier::new(3));

            let mut gdb = db.clone();
            let gid = id.clone();
            let gb = Arc::clone(&barrier);
            let get_task = tokio::spawn(async move {
                gb.wait().await;
                gdb.get_in_txn("race_mt/d1", &gid).await
            });

            let mut qdb = db.clone();
            let qid = id.clone();
            let qb = Arc::clone(&barrier);
            let query_task = tokio::spawn(async move {
                qb.wait().await;
                qdb.query_in_txn(QuerySpec::collection("race_mt"), &qid)
                    .await
            });

            let mut rdb = db.clone();
            let rid = id;
            let rb = Arc::clone(&barrier);
            let rollback_task = tokio::spawn(async move {
                rb.wait().await;
                rdb.rollback_raw(&rid).await
            });

            let (gr, qr, rr) = tokio::join!(get_task, query_task, rollback_task);
            assert_no_internal_or_cleared("mt rollback race get", gr.unwrap(), false);
            assert_no_internal_or_cleared("mt rollback race query", qr.unwrap(), false);
            assert_no_internal_or_cleared("mt rollback race rollback", rr.unwrap(), false);
        }
    }
}

fn with_attempt_gen(token: &[u8], attempt_gen: u64) -> Vec<u8> {
    let mut out = token.to_vec();
    out[8..16].copy_from_slice(&attempt_gen.to_le_bytes());
    out
}

#[gtest]
#[tokio::test]
async fn transaction_id_u64_max_attempt_gen_rejects_without_overflow_while_preserving_evicted_retry()
 {
    let mut db = TestEnv::new().build_db();

    // `attempt_gen == u64::MAX` must return `InvalidArgument` (without overflowing/panicking)
    // whether the lineage slot is active, closed, or evicted from the 4,096-entry ring.
    let active_txn = db.begin_rw_txn().await.unwrap();
    let active_max_gen = with_attempt_gen(active_txn.id(), u64::MAX);
    assert_status_code(
        db.begin_retry_rw_txn(&active_max_gen).await,
        Code::InvalidArgument,
    );
    active_txn.rollback().await.unwrap();
    assert_status_code(
        db.begin_retry_rw_txn(&active_max_gen).await,
        Code::InvalidArgument,
    );

    let evictable_rw = db.begin_rw_txn().await.unwrap();
    let evictable_rw_id = evictable_rw.id().to_vec();
    evictable_rw.rollback().await.unwrap();

    for _ in 0..4_100 {
        let churn = db.begin_rw_txn().await.unwrap();
        churn.rollback().await.unwrap();
    }

    // Evicted `u64::MAX` must return `InvalidArgument` cleanly (no integer overflow panic!).
    assert_status_code(
        db.begin_retry_rw_txn(&active_max_gen).await,
        Code::InvalidArgument,
    );

    // Normal same-DB evicted RW retry token must continue succeeding.
    let revived = db
        .begin_retry_rw_txn(&evictable_rw_id)
        .await
        .expect("normal same-DB evicted RW retry must still succeed");
    expect_ne!(revived.id(), evictable_rw_id.as_slice());
    revived.rollback().await.unwrap();
}

#[gtest]
#[tokio::test]
async fn transaction_id_rejects_cross_db_and_post_clear_retry_tokens() {
    let mut db = TestEnv::new().build_db();

    // 1. Cross-DB retry tokens (another database in the same `FirestoreProject` AND a separate
    //    `TestEnv`) must be rejected with `InvalidArgument` for both RW and RO tokens.
    let db_a_active = db.begin_rw_txn().await.unwrap();
    let db_a_active_id = db_a_active.id().to_vec();
    let db_a_closed = db.begin_rw_txn().await.unwrap();
    let db_a_closed_id = db_a_closed.id().to_vec();
    db_a_closed.rollback().await.unwrap();
    let db_a_ro = db.begin_ro_txn(None).await.unwrap();
    let db_a_ro_id = db_a_ro.id().to_vec();
    db_a_ro.rollback().await.unwrap();

    let other_db_root = "projects/test-project/databases/other-db";
    for cross_id in [&db_a_active_id, &db_a_closed_id, &db_a_ro_id] {
        let res = db
            .raw_grpc()
            .begin_transaction(BeginTransactionRequest {
                database: other_db_root.to_string(),
                options: Some(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite {
                        retry_transaction: cross_id.clone(),
                        ..Default::default()
                    })),
                }),
                request_options: None,
            })
            .await;
        assert_status_code(res, Code::InvalidArgument);
    }

    let mut isolated_env_db = TestEnv::new().build_db();
    assert_status_code(
        isolated_env_db.begin_retry_rw_txn(&db_a_closed_id).await,
        Code::InvalidArgument,
    );
    db_a_active.rollback().await.unwrap();

    // 2. Post-`clear_default_db()` retry tokens on the recreated database must return
    //    `InvalidArgument`.
    let pre_clear_active = db.begin_rw_txn().await.unwrap();
    let pre_clear_active_id = pre_clear_active.id().to_vec();
    let pre_clear_closed = db.begin_rw_txn().await.unwrap();
    let pre_clear_closed_id = pre_clear_closed.id().to_vec();
    pre_clear_closed.rollback().await.unwrap();

    db.clear_default_db().await;

    assert_status_code(
        db.begin_retry_rw_txn(&pre_clear_active_id).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.begin_retry_rw_txn(&pre_clear_closed_id).await,
        Code::InvalidArgument,
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn cancel_mid_commit_cleans_retained_reads_partial_leases_and_optimistic_commit_lock() {
    // Part A: Pessimistic mid-commit cancellation holding retained reads (`r1`, `w2`) and a
    // partial `WriteLease` (`w1`) while blocked on `w2`.
    {
        let mut db = TestEnv::new().build_db();
        for doc in ["canc/r1", "canc/w1", "canc/w2"] {
            db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
        }

        let mut blocker = db.begin_rw_txn().await.unwrap();
        let _unused = blocker.get("canc/w2").await.unwrap();

        let mut tx = db.begin_rw_txn().await.unwrap();
        let tx_id = tx.id().to_vec();
        let _unused = tx.get("canc/r1").await.unwrap();
        let _unused = tx.get("canc/w2").await.unwrap();
        tx.put("canc/w1", fields!({ "v": 99i64 }))
            .put("canc/w2", fields!({ "v": 99i64 }));

        let mut commit_op = ContentionProbe::spawn_op(tx.commit());
        commit_op.assert_pending().await;
        drop(commit_op);

        assert_status_code(
            db.get_in_txn("canc/r1", &tx_id).await,
            Code::InvalidArgument,
        );
        db.rollback_raw(&tx_id).await.unwrap();

        // Both the retained read lock on `r1` and partial write lease on `w1` are released.
        db.put("canc/r1", fields!({ "v": 1i64 }))
            .await
            .expect("retained read lock must be released on commit cancel");
        db.put("canc/w1", fields!({ "v": 2i64 }))
            .await
            .expect("partial write lease must be released on commit cancel");

        blocker.rollback().await.unwrap();
        assert_doc_wire_eq(
            &db.get("canc/w2").await.unwrap(),
            "canc/w2",
            &fields!({ "v": 0i64 }),
        );
        let retried = db.begin_retry_rw_txn(&tx_id).await.unwrap();
        retried.rollback().await.unwrap();
    }

    // Part B: Enterprise Optimistic mid-commit cancellation while parked on
    // `optimistic_commit_lock`.
    {
        let mut db = TestEnv::enterprise().build_db();
        db.put("opt_c/x", fields!({ "v": 0i64 })).await.unwrap();
        db.put("opt_c/y", fields!({ "v": 0i64 })).await.unwrap();

        db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
            .await;
        let mut blocker = db.begin_rw_txn().await.unwrap();
        assert_status_code(blocker.get("opt_c/lock").await, Code::NotFound);
        db.set_admin_mode(None, Some("OPTIMISTIC")).await;

        let mut holder = db.begin_rw_txn().await.unwrap();
        let _unused = holder.get("opt_c/x").await.unwrap();
        holder
            .put("opt_c/lock", fields!({ "held": true }))
            .put("opt_c/x", fields!({ "v": 1i64 }));
        let mut holder_commit = ContentionProbe::spawn_op(holder.commit());
        holder_commit.assert_pending().await;

        let mut victim = db.begin_rw_txn().await.unwrap();
        let victim_id = victim.id().to_vec();
        let _unused = victim.get("opt_c/y").await.unwrap();
        victim.put("opt_c/y", fields!({ "v": 77i64 }));
        let mut victim_commit = ContentionProbe::spawn_op(victim.commit());
        victim_commit.assert_pending().await;
        drop(victim_commit);

        assert_status_code(
            db.get_in_txn("opt_c/y", &victim_id).await,
            Code::InvalidArgument,
        );
        db.rollback_raw(&victim_id).await.unwrap();

        blocker.rollback().await.unwrap();
        holder_commit.expect_ready().await.unwrap();

        let mut followup = db.begin_rw_txn().await.unwrap();
        followup.put("opt_c/y", fields!({ "v": 5i64 }));
        followup.commit().await.unwrap();
        assert_doc_wire_eq(
            &db.get("opt_c/y").await.unwrap(),
            "opt_c/y",
            &fields!({ "v": 5i64 }),
        );
    }
}

#[gtest]
#[tokio::test]
async fn stale_attempt_gen_rpc_matrix_across_active_wounded_and_closed_attempts() {
    let mut db = TestEnv::enterprise().build_db();
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;
    for doc in ["stale/seed", "stale/d"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("stale/seed").await.unwrap();

    let mut t1 = db.begin_rw_txn().await.unwrap();
    let attempt_1 = t1.id().to_vec();
    let _unused = t1.get("stale/d").await.unwrap();

    // Wound `attempt_1` -> retry into `attempt_2` (Active).
    older.put("stale/d", fields!({ "v": 1i64 }));
    older.commit().await.unwrap();

    let mut t2 = db.begin_retry_rw_txn(&attempt_1).await.unwrap();
    let attempt_2 = t2.id().to_vec();

    let assert_stale_matrix = |mut client: crate::harness::client::TestDb, stale_id: Vec<u8>| async move {
        assert_status_code(
            client.get_in_txn("stale/d", &stale_id).await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client
                .query_in_txn(QuerySpec::collection("stale"), &stale_id)
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client
                .commit_raw(
                    &stale_id,
                    WriteBatchSpec::new()
                        .put("stale/d", fields!({ "v": 99i64 }))
                        .into_writes(),
                )
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client.commit_raw(&stale_id, vec![]).await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client.batch_get_in_txn(["stale/d"], &stale_id).await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client
                .list_docs(
                    "stale",
                    0,
                    "",
                    "",
                    None,
                    false,
                    Some(list_documents_request::ConsistencySelector::Transaction(
                        stale_id.clone(),
                    )),
                )
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client
                .aggregate_in_txn(
                    QuerySpec::collection("stale"),
                    AggregationSpec::new().count("n"),
                    &stale_id,
                )
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            client
                .pipeline_in_txn(PipelineSpec::collection("stale"), &stale_id)
                .await,
            Code::InvalidArgument,
        );
        // Stale rollback remains an idempotent no-op without affecting newer attempts.
        client.rollback_raw(&stale_id).await.unwrap();
    };

    // 1. `attempt_2` is Active: stale `attempt_1` rejected across all read/commit RPCs and returns
    //    `FailedPrecondition` on `BeginTransaction(retry_transaction)`.
    assert_stale_matrix(db.clone(), attempt_1.clone()).await;
    assert_status_code(
        db.begin_retry_rw_txn(&attempt_1).await,
        Code::FailedPrecondition,
    );
    let _unused = t2.get("stale/d").await.unwrap();

    // 2. `attempt_2` is Closed(Committed): stale `attempt_1` still rejected across RPC matrix while
    //    `Rollback(attempt_1)` stays `Ok(())` and `Rollback(attempt_2)` returns `Aborted`.
    t2.put("stale/d", fields!({ "v": 2i64 }));
    t2.commit().await.unwrap();
    assert_stale_matrix(db.clone(), attempt_1).await;
    assert_status_code(db.rollback_raw(&attempt_2).await, Code::Aborted);

    // 3. `attempt_2` is Wounded: stale `attempt_1` still rejected across RPC matrix, while
    //    `attempt_2` returns `Aborted`.
    let mut w_older = db.begin_rw_txn().await.unwrap();
    let _unused = w_older.get("stale/seed").await.unwrap();
    let w1 = db.begin_rw_txn().await.unwrap();
    let w1_id = w1.id().to_vec();
    w1.rollback().await.unwrap();
    let mut w2 = db.begin_retry_rw_txn(&w1_id).await.unwrap();
    let w2_id = w2.id().to_vec();
    let _unused = w2.get("stale/d").await.unwrap();

    w_older.put("stale/d", fields!({ "v": 3i64 }));
    w_older.commit().await.unwrap();

    assert_status_code(db.get_in_txn("stale/d", &w2_id).await, Code::Aborted);
    assert_stale_matrix(db.clone(), w1_id).await;
    assert_status_code(db.get_in_txn("stale/d", &w2_id).await, Code::Aborted);
    db.rollback_raw(&w2_id).await.unwrap();
}

const EXPIRED_TXN_MESSAGE: &str = "The referenced transaction has expired or is no longer valid.";

fn assert_expired<T: std::fmt::Debug>(res: Result<T, Status>) {
    let status = assert_status_code(res, Code::Aborted);
    assert_eq!(status.message(), EXPIRED_TXN_MESSAGE);
}

async fn assert_expired_rpc_matrix(
    db: &mut crate::harness::client::TestDb,
    txn_id: &[u8],
    doc_rel: &str,
    col_rel: &str,
    include_pipeline: bool,
) {
    assert_expired(db.get_in_txn(doc_rel, txn_id).await);
    assert_expired(db.get_in_txn(doc_rel, txn_id).await);
    assert_expired(db.batch_get_in_txn([doc_rel], txn_id).await);
    assert_expired(db.batch_get_in_txn(["ttl/missing"], txn_id).await);
    assert_expired(
        db.query_in_txn(QuerySpec::collection(col_rel), txn_id)
            .await,
    );
    assert_expired(
        db.aggregate_in_txn(
            QuerySpec::collection(col_rel),
            AggregationSpec::new().count("n"),
            txn_id,
        )
        .await,
    );
    if include_pipeline {
        assert_expired(
            db.pipeline_in_txn(PipelineSpec::collection(col_rel), txn_id)
                .await,
        );
    }
    assert_expired(db.commit_raw(txn_id, vec![]).await);
    assert_expired(
        db.commit_raw(
            txn_id,
            WriteBatchSpec::new()
                .put(doc_rel, fields!({ "v": 999i64 }))
                .into_writes(),
        )
        .await,
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn unused_and_used_transaction_ttl_lifecycle_and_error_matrix() {
    for (edition, mode, include_pipeline) in [
        ("STANDARD", "PESSIMISTIC", false),
        ("ENTERPRISE", "OPTIMISTIC", true),
        ("ENTERPRISE", "PESSIMISTIC", true),
    ] {
        let mut db = TestEnv::new().build_db();
        db.set_admin_mode(Some(edition), Some(mode)).await;
        db.put("ttl/d1", fields!({ "v": 1i64 })).await.unwrap();

        // 1. Unused RW & RO stay alive at 59s and expire at 60s.
        let rw_unused_alive = db.begin_rw_txn().await.unwrap();
        let ro_unused_alive = db.begin_ro_txn(None).await.unwrap();
        let rw_unused_exp = db.begin_rw_txn().await.unwrap();
        let rw_unused_exp_id = rw_unused_exp.id().to_vec();
        let rw_unused_rb = db.begin_rw_txn().await.unwrap();
        let rw_unused_rb_id = rw_unused_rb.id().to_vec();
        let ro_unused_exp = db.begin_ro_txn(None).await.unwrap();
        let ro_unused_exp_id = ro_unused_exp.id().to_vec();
        let ro_unused_rb = db.begin_ro_txn(None).await.unwrap();
        let ro_unused_rb_id = ro_unused_rb.id().to_vec();

        // Used RW (read at t=0): check alive at 119s vs expired at 120s.
        let mut rw_used_alive = db.begin_rw_txn().await.unwrap();
        let _unused = rw_used_alive.get("ttl/d1").await.unwrap();
        let mut rw_used_exp = db.begin_rw_txn().await.unwrap();
        let rw_used_exp_id = rw_used_exp.id().to_vec();
        let _unused = rw_used_exp.get("ttl/d1").await.unwrap();

        tokio::time::advance(Duration::from_secs(59)).await;
        tokio::task::yield_now().await;

        let _unused = db.get_in_txn("ttl/d1", rw_unused_alive.id()).await.unwrap();
        let _unused = db.get_in_txn("ttl/d1", ro_unused_alive.id()).await.unwrap();
        rw_unused_alive.rollback().await.unwrap();
        ro_unused_alive.rollback().await.unwrap();

        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        // Unused RW & RO are expired at t=60s across every read/query/agg/pipeline/commit RPC.
        assert_expired_rpc_matrix(
            &mut db,
            &rw_unused_exp_id,
            "ttl/d1",
            "ttl",
            include_pipeline,
        )
        .await;
        assert_expired_rpc_matrix(
            &mut db,
            &ro_unused_exp_id,
            "ttl/d1",
            "ttl",
            include_pipeline,
        )
        .await;

        // Idle-expired Rollback (<270s) is idempotent Ok(()), leaving follow-up reads/commits
        // expired.
        db.rollback_raw(&rw_unused_rb_id).await.unwrap();
        db.rollback_raw(&rw_unused_rb_id).await.unwrap();
        assert_expired(db.get_in_txn("ttl/d1", &rw_unused_rb_id).await);
        assert_expired(db.commit_raw(&rw_unused_rb_id, vec![]).await);

        db.rollback_raw(&ro_unused_rb_id).await.unwrap();
        db.rollback_raw(&ro_unused_rb_id).await.unwrap();
        assert_expired(db.get_in_txn("ttl/d1", &ro_unused_rb_id).await);

        // Retry on expired RW succeeds (both direct and post-rollback) with rotated token bytes;
        // retry on expired RO fails with InvalidArgument ("Cannot retry a read-only transaction").
        for retry_target in [&rw_unused_exp_id, &rw_unused_rb_id] {
            let mut retried = db.begin_retry_rw_txn(retry_target).await.unwrap();
            expect_ne!(retried.id(), retry_target.as_slice());
            let snap = retried.get("ttl/d1").await.unwrap();
            expect_eq!(snap.fields["v"].as_integer(), Some(1));
            retried.commit().await.unwrap();
        }
        for ro_target in [&ro_unused_exp_id, &ro_unused_rb_id] {
            let err = assert_status_code(
                db.begin_retry_rw_txn(ro_target).await,
                Code::InvalidArgument,
            );
            assert!(
                err.message()
                    .contains("Cannot retry a read-only transaction"),
                "unexpected RO retry error: {}",
                err.message()
            );
        }

        // 2. Used RW remains alive at 119s and expires at 120s.
        tokio::time::advance(Duration::from_secs(59)).await; // total = 119s
        tokio::task::yield_now().await;
        let _unused = rw_used_alive.get("ttl/d1").await.unwrap();
        rw_used_alive.rollback().await.unwrap();

        tokio::time::advance(Duration::from_secs(1)).await; // total = 120s
        tokio::task::yield_now().await;
        assert_expired_rpc_matrix(&mut db, &rw_used_exp_id, "ttl/d1", "ttl", include_pipeline)
            .await;
        db.rollback_raw(&rw_used_exp_id).await.unwrap();
        let mut retried_used = db.begin_retry_rw_txn(&rw_used_exp_id).await.unwrap();
        expect_ne!(retried_used.id(), rw_used_exp_id.as_slice());
        retried_used.put("ttl/d1", fields!({ "v": 2i64 }));
        retried_used.commit().await.unwrap();
        assert_doc_wire_eq(
            &db.get("ttl/d1").await.unwrap(),
            "ttl/d1",
            &fields!({ "v": 2i64 }),
        );
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn active_transaction_activity_refresh_extends_past_120s_and_caps_at_270s_max_lifetime() {
    let mut db = TestEnv::new().build_db();
    db.put("ttl_max/d1", fields!({ "v": 1i64 })).await.unwrap();

    let mut tx_periodic = db.begin_rw_txn().await.unwrap();
    let tx_periodic_id = tx_periodic.id().to_vec();
    let _unused = tx_periodic.get("ttl_max/d1").await.unwrap();

    let tx_unused_270 = db.begin_rw_txn().await.unwrap();
    let tx_unused_270_id = tx_unused_270.id().to_vec();
    let ro_unused_270 = db.begin_ro_txn(None).await.unwrap();
    let ro_unused_270_id = ro_unused_270.id().to_vec();
    let mut ro_used_270 = db.begin_ro_txn(None).await.unwrap();
    let ro_used_270_id = ro_used_270.id().to_vec();
    let _unused = ro_used_270.get("ttl_max/d1").await.unwrap();

    // Periodic reads every 45s keep RW alive past 120s up to 265s (<270s max lifetime).
    for step_secs in [45u64, 45, 45, 45, 45, 40] {
        tokio::time::advance(Duration::from_secs(step_secs)).await;
        tokio::task::yield_now().await;
        let _unused = tx_periodic.get("ttl_max/d1").await.unwrap();
    }

    // Advance 5s -> t=270s: hard max lifetime expires even right after a read at t=265s.
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    assert_expired_rpc_matrix(&mut db, &tx_periodic_id, "ttl_max/d1", "ttl_max", false).await;

    // Past 270s max lifetime, Rollback returns Code::Aborted ("...expired...") on RW & RO.
    for expired_id in [
        &tx_periodic_id,
        &tx_unused_270_id,
        &ro_unused_270_id,
        &ro_used_270_id,
    ] {
        assert_expired(db.rollback_raw(expired_id).await);
    }

    // Past 270s max lifetime, retryTransaction still succeeds on RW and rejects RO.
    for rw_id in [&tx_periodic_id, &tx_unused_270_id] {
        let mut retried = db.begin_retry_rw_txn(rw_id).await.unwrap();
        expect_ne!(retried.id(), rw_id.as_slice());
        retried.put("ttl_max/d1", fields!({ "v": 270i64 }));
        retried.commit().await.unwrap();
    }
    assert_status_code(
        db.begin_retry_rw_txn(&ro_used_270_id).await,
        Code::InvalidArgument,
    );
    assert_doc_wire_eq(
        &db.get("ttl_max/d1").await.unwrap(),
        "ttl_max/d1",
        &fields!({ "v": 270i64 }),
    );
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn expired_rw_retry_inherits_original_lineage_priority_and_wounds_younger_active_txn() {
    for (edition, mode) in [("STANDARD", "PESSIMISTIC"), ("ENTERPRISE", "PESSIMISTIC")] {
        let mut db = TestEnv::new().build_db();
        db.set_admin_mode(Some(edition), Some(mode)).await;
        for doc in ["ttl_prio/seed", "ttl_prio/contend"] {
            db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
        }

        // 1. Expired at 120s: retry inherits `t=0` `root_seq` and wounds `t=100s` active txn.
        let mut old_120 = db.begin_rw_txn().await.unwrap();
        let old_120_id = old_120.id().to_vec();
        let _unused = old_120.get("ttl_prio/seed").await.unwrap();

        let mut old_270 = db.begin_rw_txn().await.unwrap();
        let old_270_id = old_270.id().to_vec();
        let _unused = old_270.get("ttl_prio/seed").await.unwrap();

        tokio::time::advance(Duration::from_secs(100)).await;
        tokio::task::yield_now().await;
        let mut mid = db.begin_rw_txn().await.unwrap();
        let _unused = mid.get("ttl_prio/contend").await.unwrap();

        tokio::time::advance(Duration::from_secs(20)).await; // total = 120s
        tokio::task::yield_now().await;
        assert_expired(db.get_in_txn("ttl_prio/contend", &old_120_id).await);

        let mut retried_120 = db.begin_retry_rw_txn(&old_120_id).await.unwrap();
        let _unused = retried_120.get("ttl_prio/contend").await.unwrap();
        retried_120.put("ttl_prio/contend", fields!({ "v": 1i64 }));
        retried_120.commit().await.unwrap();
        assert_status_code(mid.commit().await, Code::Aborted);

        // 2. Expired past 270s max lifetime: retry STILL inherits `t=0` `root_seq`.
        tokio::time::advance(Duration::from_secs(130)).await; // total = 250s
        tokio::task::yield_now().await;
        let mut young_250 = db.begin_rw_txn().await.unwrap();
        let _unused = young_250.get("ttl_prio/contend").await.unwrap();

        tokio::time::advance(Duration::from_secs(20)).await; // total = 270s
        tokio::task::yield_now().await;
        assert_expired(db.rollback_raw(&old_270_id).await);

        let mut retried_270 = db.begin_retry_rw_txn(&old_270_id).await.unwrap();
        let _unused = retried_270.get("ttl_prio/contend").await.unwrap();
        retried_270.put("ttl_prio/contend", fields!({ "v": 2i64 }));
        retried_270.commit().await.unwrap();
        assert_status_code(young_250.commit().await, Code::Aborted);
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn high_volume_abandoned_ro_transactions_gc_without_evicting_rw_tombstones() {
    let mut db = TestEnv::new().build_db();
    for doc in ["gc/seed", "gc/w", "gc/d"] {
        db.put(doc, fields!({ "v": 0i64 })).await.unwrap();
    }

    // Populate retry ring (wounded RW), closed ring (committed RW), and expired RW ring.
    let mut older = db.begin_rw_txn().await.unwrap();
    let _unused = older.get("gc/seed").await.unwrap();
    let mut wounded_rw = db.begin_rw_txn().await.unwrap();
    let wounded_rw_id = wounded_rw.id().to_vec();
    let _unused = wounded_rw.get("gc/w").await.unwrap();
    older.put("gc/w", fields!({ "v": 1i64 }));
    older.commit().await.unwrap();
    assert_status_code(db.get_in_txn("gc/w", &wounded_rw_id).await, Code::Aborted);

    let mut committed_rw = db.begin_rw_txn().await.unwrap();
    let committed_rw_id = committed_rw.id().to_vec();
    committed_rw.put("gc/d", fields!({ "v": 1i64 }));
    committed_rw.commit().await.unwrap();

    let mut expired_rw = db.begin_rw_txn().await.unwrap();
    let expired_rw_id = expired_rw.id().to_vec();
    let _unused = expired_rw.get("gc/d").await.unwrap();

    // Open 5,000 ReadOnly transactions (never committed or rolled back, simulating SDK RO leak).
    let mut ro_ids = Vec::with_capacity(5_000);
    for _ in 0..5_000 {
        let mut ro = db.begin_ro_txn(None).await.unwrap();
        let _unused = ro.get("gc/d").await.unwrap();
        ro_ids.push(ro.id().to_vec());
    }

    tokio::time::advance(Duration::from_secs(120)).await;
    tokio::task::yield_now().await;

    // Trigger reaping and bounded RO ring eviction via normal non-tx reads/txns.
    assert_doc_wire_eq(
        &db.get("gc/d").await.unwrap(),
        "gc/d",
        &fields!({ "v": 1i64 }),
    );
    // Oldest entries beyond `MAX_EXPIRED_TXNS` (4,096) were evicted to keep memory bounded.
    assert_status_code(
        db.get_in_txn("gc/d", ro_ids.first().unwrap()).await,
        Code::InvalidArgument,
    );
    // Most recent 4,096 expired RO entries retain exact Cloud-expired status.
    for ro_id in [&ro_ids[2_500], ro_ids.last().unwrap()] {
        assert_expired(db.get_in_txn("gc/d", ro_id).await);
    }

    // Isolated rings preserved RW retry tombstone, committed closed tombstone, and expired RW ring.
    assert_status_code(db.rollback_raw(&committed_rw_id).await, Code::Aborted);

    let mut mid = db.begin_rw_txn().await.unwrap();
    let _unused = mid.get("gc/w").await.unwrap();
    let mut retried_wounded = db.begin_retry_rw_txn(&wounded_rw_id).await.unwrap();
    let _unused = retried_wounded.get("gc/w").await.unwrap();
    retried_wounded.put("gc/w", fields!({ "v": 2i64 }));
    retried_wounded.commit().await.unwrap();
    assert_status_code(mid.commit().await, Code::Aborted);

    assert_expired(db.get_in_txn("gc/d", &expired_rw_id).await);
    let retried_expired = db.begin_retry_rw_txn(&expired_rw_id).await.unwrap();
    expect_ne!(retried_expired.id(), expired_rw_id.as_slice());
    retried_expired.rollback().await.unwrap();
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_execution_time_matches_snapshot_for_read_time_txns_and_empty_results() {
    let mut db = TestEnv::enterprise().build_db();

    let v1 = db
        .put("docs/d1", fields!({ "ver": 1i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    let v2 = db
        .put("docs/d1", fields!({ "ver": 2i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    let v3 = db
        .put("docs/d2", fields!({ "ver": 20i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();

    // 1. ReadTime(t): historical snapshots, epoch empty, filtered-to-empty, and invalid future time
    let pipe_v1 = db
        .pipeline_at_time(PipelineSpec::collection("docs").sort_asc("__name__"), v1)
        .await
        .unwrap();
    expect_eq!(pipe_v1.execution_time, Some(v1));
    expect_eq!(short_ids(&pipe_v1.results), vec!["d1"]);
    expect_eq!(pipe_v1.results[0].fields["ver"].as_integer(), Some(1));

    let pipe_v2 = db
        .pipeline_at_time(PipelineSpec::collection("docs").sort_asc("__name__"), v2)
        .await
        .unwrap();
    expect_eq!(pipe_v2.execution_time, Some(v2));
    expect_eq!(short_ids(&pipe_v2.results), vec!["d1"]);
    expect_eq!(pipe_v2.results[0].fields["ver"].as_integer(), Some(2));

    let epoch = Timestamp {
        seconds: 0,
        nanos:   0,
    };
    let pipe_epoch = db
        .pipeline_at_time(PipelineSpec::collection("docs"), epoch)
        .await
        .unwrap();
    expect_eq!(pipe_epoch.execution_time, Some(epoch));
    expect_that!(pipe_epoch.results, is_empty());

    let pipe_v3_empty = db
        .pipeline_at_time(PipelineSpec::collection("docs").where_expr(val!(false)), v3)
        .await
        .unwrap();
    expect_eq!(pipe_v3_empty.execution_time, Some(v3));
    expect_that!(pipe_v3_empty.results, is_empty());

    let future = Timestamp {
        seconds: Timestamp::now().seconds + 60,
        nanos:   0,
    };
    assert_status_code(
        db.pipeline_at_time(PipelineSpec::collection("docs"), future)
            .await,
        Code::InvalidArgument,
    );

    // 2. Explicit ReadOnly transaction preserves pinned execution_time across writes & empty
    //    results
    let mut ro = db.begin_ro_txn(Some(v2)).await.unwrap();
    let ro_pipe = ro
        .pipeline(PipelineSpec::collection("docs").sort_asc("__name__"))
        .await
        .unwrap();
    expect_eq!(ro_pipe.execution_time, Some(v2));
    expect_eq!(short_ids(&ro_pipe.results), vec!["d1"]);
    let ro_empty = ro
        .pipeline(PipelineSpec::collection("docs").where_expr(val!(false)))
        .await
        .unwrap();
    expect_eq!(ro_empty.execution_time, Some(v2));
    expect_that!(ro_empty.results, is_empty());
    ro.rollback().await.unwrap();

    // 3. Optimistic RW transaction pins snapshot execution_time despite concurrent writes
    let mut opt = db.begin_rw_txn().await.unwrap();
    let opt_first = opt
        .pipeline(PipelineSpec::collection("docs").sort_asc("__name__"))
        .await
        .unwrap();
    let opt_ts = opt_first.execution_time.expect("execution_time");
    expect_eq!(short_ids(&opt_first.results), vec!["d1", "d2"]);

    let concurrent_write_ts = db
        .put("docs/d1", fields!({ "ver": 99i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    expect_lt!(opt_ts, concurrent_write_ts);

    let opt_second = opt
        .pipeline(PipelineSpec::collection("docs").sort_asc("__name__"))
        .await
        .unwrap();
    expect_eq!(opt_second.execution_time, Some(opt_ts));
    expect_eq!(opt_second.results[0].fields["ver"].as_integer(), Some(2));

    let opt_empty = opt
        .pipeline(PipelineSpec::collection("docs").where_expr(val!(false)))
        .await
        .unwrap();
    expect_eq!(opt_empty.execution_time, Some(opt_ts));
    expect_that!(opt_empty.results, is_empty());
    opt.rollback().await.unwrap();

    // 4. Empty collection & filtered-to-empty under ReadConsistency::Default emit valid snapshot
    //    timestamps strictly ordered before subsequent writes
    let empty_col = db
        .pipeline(PipelineSpec::collection("never_created"))
        .await
        .unwrap();
    expect_that!(empty_col.results, is_empty());
    let empty_ts = empty_col.execution_time.expect("execution_time");
    expect_gt!(empty_ts, concurrent_write_ts);

    let filtered_empty = db
        .pipeline(PipelineSpec::collection("docs").where_expr(val!(false)))
        .await
        .unwrap();
    expect_that!(filtered_empty.results, is_empty());
    let filtered_ts = filtered_empty.execution_time.expect("execution_time");
    expect_gt!(filtered_ts, empty_ts);

    let after_ts = db
        .put("docs/d3", fields!({ "ver": 100i64 }))
        .await
        .unwrap()
        .update_time
        .unwrap();
    expect_lt!(filtered_ts, after_ts);
}

#[gtest]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execute_pipeline_default_consistency_execution_time_matches_mvcc_snapshot_during_concurrent_writes()
 {
    let mut db = TestEnv::enterprise().build_db();
    db.put("race/d1", fields!({ "v": 0i64 })).await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writer_stop = Arc::clone(&stop);
    let mut writer_db = db.clone();
    let writer = tokio::spawn(async move {
        let mut seq = 1i64;
        while !writer_stop.load(Ordering::Relaxed) && seq <= 10_000 {
            writer_db
                .put("race/d1", fields!({ "v": seq }))
                .await
                .unwrap();
            seq += 1;
            if seq % 8 == 0 {
                tokio::task::yield_now().await;
            }
        }
    });

    for idx in 0..40 {
        let pipe = db.pipeline(PipelineSpec::collection("race")).await.unwrap();
        let exec_ts = pipe.execution_time.expect("execution_time must be present");
        expect_eq!(pipe.results.len(), 1);
        let pipe_doc = &pipe.results[0];
        expect_le!(pipe_doc.update_time.unwrap(), exec_ts);

        let snap = db
            .query_at_time(QuerySpec::collection("race"), exec_ts)
            .await
            .unwrap();
        expect_eq!(snap.documents.len(), 1);
        expect_eq!(snap.documents[0].fields, pipe_doc.fields);
        expect_eq!(snap.documents[0].update_time, pipe_doc.update_time);

        if idx % 4 == 0 {
            let empty_pipe = db
                .pipeline(PipelineSpec::collection("race").where_expr(val!(false)))
                .await
                .unwrap();
            expect_that!(empty_pipe.results, is_empty());
            expect_that!(empty_pipe.execution_time, some(anything()));
        }
    }

    stop.store(true, Ordering::Relaxed);
    writer.await.unwrap();
}
