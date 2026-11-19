use googleapis::{
    fields,
    google::{
        firestore::v1::{
            BatchGetDocumentsRequest, Precondition, TransactionOptions,
            batch_get_documents_request, document_transform, precondition,
            structured_query::field_filter::Operator,
            transaction_options::{self, ReadOnly, ReadWrite, read_only},
        },
        protobuf::Timestamp,
    },
};
use googletest::prelude::*;
use http::{Method, StatusCode};
use serde_json::json;
use tonic::Code;

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

    // Retrying with the released txn_id now succeeds
    let retried = db
        .begin_retry_rw_txn(&saved_id)
        .await
        .expect("retry after rollback");
    expect_eq!(retried.id(), saved_id.as_slice());
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

    // 5. TransactionId(0) wire bytes are never valid.
    assert_status_code(
        db.begin_retry_rw_txn(&0usize.to_ne_bytes()).await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.rollback_raw(&0usize.to_ne_bytes()).await,
        Code::InvalidArgument,
    );
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
    let failed_numeric = usize::from_ne_bytes(failed_id.clone().try_into().unwrap());
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
    let expected_err_fragment = format!("invalid transaction ID: {failed_numeric}");
    assert!(
        !captured.contains(&expected_err_fragment),
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
