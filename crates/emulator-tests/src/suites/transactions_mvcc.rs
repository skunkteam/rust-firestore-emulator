use googleapis::{
    fields,
    google::firestore::v1::{
        TransactionOptions,
        structured_query::field_filter::Operator,
        transaction_options::{self, ReadOnly, ReadWrite, read_only},
    },
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{PipelineSpec, QuerySpec, WriteBatchSpec, field_filter},
    env::TestEnv,
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
