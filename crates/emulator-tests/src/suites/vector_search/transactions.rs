use std::time::Duration;

use googleapis::{
    fields,
    google::firestore::v1::{
        BatchGetDocumentsRequest, CommitRequest, Document, Precondition,
        RunAggregationQueryRequest, RunQueryRequest, StructuredAggregationQuery,
        TransactionOptions, Write, batch_get_documents_request, run_aggregation_query_request,
        run_query_request,
        structured_aggregation_query::{Aggregation, aggregation},
        structured_query::{field_filter::Operator, find_nearest},
        transaction_options::{self, ReadWrite},
        write,
    },
};
use googletest::prelude::*;
use tonic::Code;

use super::make_find_nearest;
use crate::harness::{
    builders::{PipelineSpec, QuerySpec, doc_path, field_filter, vec_val},
    contention::ContentionProbe,
    env::{DATABASE_ROOT, TestEnv},
    matchers::{assert_status_code, short_ids},
    stream::{collect_aggregation, collect_batch_get, collect_run_query},
};

#[gtest]
#[tokio::test(start_paused = true)]
async fn transaction_concurrency_edition_lock_scope_and_optimistic_aborts() {
    // 1. Standard Edition (PESSIMISTIC): locks ONLY returned top-K documents.
    {
        let mut db = TestEnv::new().build_db();
        db.put(
            "std/d1",
            fields!({ "emb": vec_val([1.0, 0.0]), "tag": "keep" }),
        )
        .await
        .unwrap();
        db.put(
            "std/d2",
            fields!({ "emb": vec_val([5.0, 0.0]), "tag": "keep" }),
        )
        .await
        .unwrap();

        let probe = ContentionProbe::new(&db);
        let mut txn = probe.actor_a.clone().begin_rw_txn().await.unwrap();
        let res = txn
            .query(
                QuerySpec::collection("std")
                    .where_filter(field_filter("tag", Operator::Equal, "keep"))
                    .find_nearest(make_find_nearest(
                        "emb",
                        vec_val([1.0, 0.0]),
                        find_nearest::DistanceMeasure::Euclidean,
                        1,
                        None,
                        None,
                    )),
            )
            .await
            .unwrap();
        expect_eq!(short_ids(&res.documents), vec!["d1"]);

        // Pre-filtered non-top-K `d2` is NOT locked in Standard Edition: write succeeds
        // immediately.
        probe
            .actor_b
            .clone()
            .put(
                "std/d2",
                fields!({ "emb": vec_val([9.0, 0.0]), "tag": "keep" }),
            )
            .await
            .expect("Standard Edition must not lock non-top-K candidate d2");

        // Top-K `d1` IS locked: Actor B blocks until `txn.rollback()`.
        let mut b_d1 = probe.actor_b.clone();
        let mut blocked = ContentionProbe::spawn_op(async move {
            b_d1.put(
                "std/d1",
                fields!({ "emb": vec_val([2.0, 0.0]), "tag": "keep" }),
            )
            .await
        });
        blocked.assert_pending().await;

        txn.rollback().await.unwrap();
        blocked.expect_ready().await.unwrap();
    }

    // 2. Enterprise Edition (PESSIMISTIC): locks ALL scanned docs in matching collections
    //    (including candidates AND pre-filtered-out docs) while leaving other collections unlocked;
    //    mid-scan COSINE NaN drops buffered guards cleanly.
    {
        let mut db = TestEnv::enterprise().build_db();
        db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
            .await;

        db.put(
            "ent/d1",
            fields!({ "emb": vec_val([1.0, 0.0]), "tag": "keep" }),
        )
        .await
        .unwrap();
        db.put(
            "ent/d2",
            fields!({ "emb": vec_val([5.0, 0.0]), "tag": "keep" }),
        )
        .await
        .unwrap();
        db.put(
            "ent/d3",
            fields!({ "emb": vec_val([0.0, 0.0]), "tag": "skip" }),
        )
        .await
        .unwrap();
        db.put("other/o1", fields!({ "v": 1i64 })).await.unwrap();

        let probe = ContentionProbe::new(&db);
        let mut txn = probe.actor_a.clone().begin_rw_txn().await.unwrap();
        let res = txn
            .query(
                QuerySpec::collection("ent")
                    .where_filter(field_filter("tag", Operator::Equal, "keep"))
                    .find_nearest(make_find_nearest(
                        "emb",
                        vec_val([1.0, 0.0]),
                        find_nearest::DistanceMeasure::Euclidean,
                        1,
                        None,
                        None,
                    )),
            )
            .await
            .unwrap();
        expect_eq!(short_ids(&res.documents), vec!["d1"]);

        // Document outside scanned collection `"ent"` is unlocked: write succeeds immediately.
        probe
            .actor_b
            .clone()
            .put("other/o1", fields!({ "v": 2i64 }))
            .await
            .expect("document outside scanned collection must remain unlocked");

        // BOTH non-top-K candidate `ent/d2` AND pre-filtered-out `ent/d3` in `"ent"` ARE locked
        // in Enterprise Pessimistic!
        let mut b_d2 = probe.actor_b.clone();
        let mut blocked_d2 = ContentionProbe::spawn_op(async move {
            b_d2.put(
                "ent/d2",
                fields!({ "emb": vec_val([8.0, 0.0]), "tag": "keep" }),
            )
            .await
        });
        let mut b_d3 = probe.actor_b.clone();
        let mut blocked_d3 = ContentionProbe::spawn_op(async move {
            b_d3.put(
                "ent/d3",
                fields!({ "emb": vec_val([0.0, 0.0]), "tag": "skip" }),
            )
            .await
        });
        blocked_d2.assert_pending().await;
        blocked_d3.assert_pending().await;

        txn.rollback().await.unwrap();
        blocked_d2.expect_ready().await.unwrap();
        blocked_d3.expect_ready().await.unwrap();

        // Mid-scan COSINE NaN (`ent/d3` has `[0.0, 0.0]` and is no longer pre-filtered out):
        // query errors with InvalidArgument and drops buffered read guards on `d1`/`d2` cleanly!
        let mut failing_txn = probe.actor_a.clone().begin_rw_txn().await.unwrap();
        assert_status_code(
            failing_txn
                .query(QuerySpec::collection("ent").find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Cosine,
                    2,
                    None,
                    None,
                )))
                .await,
            Code::InvalidArgument,
        );

        let mut b_d1 = probe.actor_b.clone();
        let write_d1 = ContentionProbe::spawn_op(async move {
            b_d1.put(
                "ent/d1",
                fields!({ "emb": vec_val([1.0, 0.1]), "tag": "keep" }),
            )
            .await
        });
        write_d1
            .expect_ready()
            .await
            .expect("mid-scan NaN error must drop buffered read guards cleanly");
        failing_txn.rollback().await.unwrap();
    }

    // 3. Enterprise Edition (OPTIMISTIC): write-skew entering top-K and concurrent `[0.0, 0.0]`
    //    COSINE insertion both abort commit with Code::Aborted.
    {
        let mut db = TestEnv::enterprise().build_db();
        db.put("opt/d1", fields!({ "emb": vec_val([2.0, 0.0]) }))
            .await
            .unwrap();
        db.put("opt/d2", fields!({ "emb": vec_val([5.0, 0.0]) }))
            .await
            .unwrap();

        // Write-skew: `d2` moves strictly closer (`dist = 0.0 < 1.0`) to query vector than `d1`,
        // altering top-1 result set.
        let mut txn1 = db.begin_rw_txn().await.unwrap();
        let top1 = txn1
            .query(QuerySpec::collection("opt").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                1,
                None,
                None,
            )))
            .await
            .unwrap();
        expect_eq!(short_ids(&top1.documents), vec!["d1"]);

        db.put("opt/d2", fields!({ "emb": vec_val([1.0, 0.0]) }))
            .await
            .unwrap();
        txn1.put("opt/audit", fields!({ "checked": true }));
        assert_status_code(txn1.commit().await, Code::Aborted);

        // Concurrent `[0.0, 0.0]` COSINE insertion causes re-validation failure -> Code::Aborted.
        let mut txn2 = db.begin_rw_txn().await.unwrap();
        let cos_res = txn2
            .query(QuerySpec::collection("opt").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                2,
                None,
                None,
            )))
            .await
            .unwrap();
        expect_eq!(cos_res.documents.len(), 2);

        db.put("opt/d_zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
            .await
            .unwrap();
        txn2.put("opt/audit2", fields!({ "checked": true }));
        assert_status_code(txn2.commit().await, Code::Aborted);
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn new_transaction_errors_rollback_cleanly_across_all_five_rpcs() {
    let rw_opts = || TransactionOptions {
        mode: Some(transaction_options::Mode::ReadWrite(ReadWrite::default())),
    };

    // 1-3 & 5 on Standard Edition (PESSIMISTIC): run_query, run_aggregation_query, single-shot
    // commit, and batch_get_documents.
    {
        let mut db = TestEnv::new().build_db();
        db.put("locks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
            .await
            .unwrap();
        db.put("locks/d2", fields!({ "emb": vec_val([0.0, 0.0]) }))
            .await
            .unwrap();
        for h in ["h1", "h2", "h3", "h4"] {
            db.put(&format!("locks/{h}"), fields!({ "v": 1i64 }))
                .await
                .unwrap();
        }

        // (a) run_query with NewTransaction: locks d1 during scan, then fails on d2 COSINE NaN.
        let (parent, sq) = QuerySpec::collection("locks")
            .find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                2,
                None,
                None,
            ))
            .into_parts();
        let rq_res = collect_run_query(
            db.raw_grpc()
                .run_query(RunQueryRequest {
                    parent: parent.clone(),
                    query_type: Some(run_query_request::QueryType::StructuredQuery(sq.clone())),
                    consistency_selector: Some(
                        run_query_request::ConsistencySelector::NewTransaction(rw_opts()),
                    ),
                    explain_options: None,
                    request_options: None,
                })
                .await
                .unwrap()
                .into_inner(),
        )
        .await;
        assert_status_code(rq_res, Code::FailedPrecondition);
        let mut probe_b = db.clone();
        ContentionProbe::spawn_op(async move {
            probe_b
                .put("locks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
                .await
        })
        .expect_ready()
        .await
        .expect("run_query NewTransaction error must not leak d1 lock");

        // (b) run_aggregation_query with NewTransaction: fails on d2 COSINE [0, 0].
        let agg_res = collect_aggregation(
            db.raw_grpc()
                .run_aggregation_query(RunAggregationQueryRequest {
                    parent,
                    query_type: Some(
                        run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                            StructuredAggregationQuery {
                                aggregations: vec![Aggregation {
                                    alias:    "n".to_string(),
                                    operator: Some(aggregation::Operator::Count(
                                        aggregation::Count { up_to: None },
                                    )),
                                }],
                                query_type: Some(
                                    googleapis::google::firestore::v1::structured_aggregation_query::QueryType::StructuredQuery(
                                        sq,
                                    ),
                                ),
                            },
                        ),
                    ),
                    consistency_selector: Some(
                        run_aggregation_query_request::ConsistencySelector::NewTransaction(
                            rw_opts(),
                        ),
                    ),
                    explain_options: None,
                    request_options: None,
                })
                .await
                .unwrap()
                .into_inner(),
        )
        .await;
        assert_status_code(agg_res, Code::FailedPrecondition);
        let mut probe_b = db.clone();
        ContentionProbe::spawn_op(async move {
            probe_b
                .put("locks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
                .await
        })
        .expect_ready()
        .await
        .expect("run_aggregation_query NewTransaction error must not leak d1 lock");

        // (c) single-shot Commit (`transaction = []`) locks `locks/d1` then fails precondition on
        //     `locks/missing`.
        let commit_err = db
            .raw_grpc()
            .commit(CommitRequest {
                database: DATABASE_ROOT.to_string(),
                writes: vec![
                    Write {
                        update_mask: None,
                        update_transforms: vec![],
                        current_document: None,
                        operation: Some(write::Operation::Update(Document {
                            name: doc_path("locks/d1"),
                            fields: fields!({ "emb": vec_val([2.0, 0.0]) }),
                            create_time: None,
                            update_time: None,
                        })),
                    },
                    Write {
                        update_mask: None,
                        update_transforms: vec![],
                        current_document: Some(Precondition {
                            condition_type: Some(
                                googleapis::google::firestore::v1::precondition::ConditionType::Exists(
                                    true,
                                ),
                            ),
                        }),
                        operation: Some(write::Operation::Update(Document {
                            name: doc_path("locks/missing"),
                            fields: fields!({ "x": 1i64 }),
                            create_time: None,
                            update_time: None,
                        })),
                    },
                ],
                transaction: Vec::new(),
                request_options: None,
            })
            .await;
        assert_status_code(commit_err, Code::NotFound);
        let mut probe_b = db.clone();
        ContentionProbe::spawn_op(async move {
            probe_b
                .put("locks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
                .await
        })
        .expect_ready()
        .await
        .expect("single-shot Commit error must not leak d1 lock");

        // (d) batch_get_documents with NewTransaction failing inside the spawned worker:
        //     Hold read locks on h1..h4; `commit_blocker` takes write lock on d2 and stalls 1.5s
        //     per h_i (total 6s without hitting 2s write timeout); `batch_get_new_txn` locks d1
        //     then blocks on d2 past its 5s read timeout -> Aborted + NewTransaction rolled back!
        let mut t1 = db.begin_rw_txn().await.unwrap();
        let _unused = t1.get("locks/h1").await.unwrap();
        let mut t2 = db.begin_rw_txn().await.unwrap();
        let _unused = t2.get("locks/h2").await.unwrap();
        let mut t3 = db.begin_rw_txn().await.unwrap();
        let _unused = t3.get("locks/h3").await.unwrap();
        let mut t4 = db.begin_rw_txn().await.unwrap();
        let _unused = t4.get("locks/h4").await.unwrap();

        let mut blocker_db = db.clone();
        let mut commit_blocker = ContentionProbe::spawn_op(async move {
            let mut tx = blocker_db.begin_rw_txn().await?;
            tx.put("locks/d2", fields!({ "emb": vec_val([0.0, 0.0]) }))
                .put("locks/h1", fields!({ "v": 2i64 }))
                .put("locks/h2", fields!({ "v": 2i64 }))
                .put("locks/h3", fields!({ "v": 2i64 }))
                .put("locks/h4", fields!({ "v": 2i64 }));
            tx.commit().await
        });
        commit_blocker.drive_settled().await;

        let mut bg_db = db.clone();
        let mut batch_get_op = ContentionProbe::spawn_op(async move {
            let stream = bg_db
                .raw_grpc()
                .batch_get_documents(BatchGetDocumentsRequest {
                    database: DATABASE_ROOT.to_string(),
                    documents: vec![doc_path("locks/d1"), doc_path("locks/d2")],
                    mask: None,
                    consistency_selector: Some(
                        batch_get_documents_request::ConsistencySelector::NewTransaction(rw_opts()),
                    ),
                    request_options: None,
                })
                .await?
                .into_inner();
            collect_batch_get(stream).await
        });
        batch_get_op.assert_pending().await;

        ContentionProbe::advance_and_poll(
            Duration::from_millis(1500),
            &mut commit_blocker,
            Some(&mut batch_get_op),
        )
        .await;
        t1.rollback().await.unwrap();

        ContentionProbe::advance_and_poll(
            Duration::from_millis(1500),
            &mut commit_blocker,
            Some(&mut batch_get_op),
        )
        .await;
        t2.rollback().await.unwrap();

        ContentionProbe::advance_and_poll(
            Duration::from_millis(1500),
            &mut commit_blocker,
            Some(&mut batch_get_op),
        )
        .await;
        t3.rollback().await.unwrap();

        ContentionProbe::advance_and_poll(
            Duration::from_millis(1000),
            &mut commit_blocker,
            Some(&mut batch_get_op),
        )
        .await;
        batch_get_op.expect_aborted().await;

        t4.rollback().await.unwrap();
        commit_blocker.expect_ready().await.unwrap();

        let mut probe_b = db.clone();
        ContentionProbe::spawn_op(async move {
            probe_b
                .put("locks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
                .await
        })
        .expect_ready()
        .await
        .expect("batch_get_documents NewTransaction error must not leak d1 lock");
    }

    // 4. execute_pipeline with NewTransaction on Enterprise (PESSIMISTIC): collection scan locks d1
    //    and d2, then `find_nearest` fails on d2 COSINE NaN -> rolls back NewTransaction cleanly.
    {
        let mut db = TestEnv::enterprise().build_db();
        db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
            .await;
        db.put("plocks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
            .await
            .unwrap();
        db.put("plocks/d2", fields!({ "emb": vec_val([0.0, 0.0]) }))
            .await
            .unwrap();

        let res = db
            .pipeline_new_txn(
                PipelineSpec::collection("plocks").find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    "cosine",
                    Some(2),
                    None,
                ),
                rw_opts(),
            )
            .await;
        assert_status_code(res, Code::InvalidArgument);

        let mut probe_b = db.clone();
        ContentionProbe::spawn_op(async move {
            probe_b
                .put("plocks/d1", fields!({ "emb": vec_val([1.0, 0.0]) }))
                .await
        })
        .expect_ready()
        .await
        .expect("execute_pipeline NewTransaction error must not leak d1 lock");
    }
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn standard_find_nearest_drops_unmatched_leases_and_preserves_prior_retained_reads() {
    let mut db = TestEnv::new().build_db(); // STANDARD -> PESSIMISTIC

    // `d_top`: matches prefilter, within threshold, top-1 winner -> LOCKED.
    db.put(
        "std_drop/d_top",
        fields!({ "emb": vec_val([1.0, 0.0]), "tag": "keep" }),
    )
    .await
    .unwrap();
    // `d_prefilter_miss`: filtered out before distance calc -> UNLOCKED.
    db.put(
        "std_drop/d_prefilter_miss",
        fields!({ "emb": vec_val([1.0, 0.0]), "tag": "skip" }),
    )
    .await
    .unwrap();
    // `d_missing_vec`: matches prefilter, lacks vector field -> UNLOCKED.
    db.put("std_drop/d_missing_vec", fields!({ "tag": "keep" }))
        .await
        .unwrap();
    // `d_over_threshold`: matches prefilter, Euclidean distance 5.0 > threshold 2.5 -> UNLOCKED.
    db.put(
        "std_drop/d_over_threshold",
        fields!({ "emb": vec_val([6.0, 0.0]), "tag": "keep" }),
    )
    .await
    .unwrap();
    // `d_over_limit`: matches prefilter, within threshold (distance 1.0), beaten by `d_top` (0.0)
    // under `limit(1)` -> UNLOCKED.
    db.put(
        "std_drop/d_over_limit",
        fields!({ "emb": vec_val([2.0, 0.0]), "tag": "keep" }),
    )
    .await
    .unwrap();
    // `d_prior_get`: explicitly read via `tx.get` before `findNearest`, then filtered out by
    // `findNearest` -> must STAY LOCKED (`hold.retained = true`).
    db.put(
        "std_drop/d_prior_get",
        fields!({ "emb": vec_val([1.0, 0.0]), "tag": "prior" }),
    )
    .await
    .unwrap();

    let probe = ContentionProbe::new(&db);
    let mut tx = probe.actor_a.clone().begin_rw_txn().await.unwrap();
    let _unused = tx.get("std_drop/d_prior_get").await.unwrap();

    let res = tx
        .query(
            QuerySpec::collection("std_drop")
                .where_filter(field_filter("tag", Operator::Equal, "keep"))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    1,
                    None,
                    Some(2.5),
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&res.documents), vec!["d_top"]);

    // All unmatched preliminary `ReadLease`s must be dropped immediately.
    let mut free = probe.actor_b.clone();
    for doc in [
        "std_drop/d_prefilter_miss",
        "std_drop/d_missing_vec",
        "std_drop/d_over_threshold",
        "std_drop/d_over_limit",
    ] {
        free.put(doc, fields!({ "tag": "mutated" }))
            .await
            .unwrap_or_else(|e| panic!("unmatched doc {doc} must be unlocked: {e:?}"));
    }

    // Both top-K winner `d_top` and prior-retained `d_prior_get` remain read-locked until
    // `tx.rollback()`.
    let mut b_top = probe.actor_b.clone();
    let mut blocked_top = ContentionProbe::spawn_op(async move {
        b_top
            .put("std_drop/d_top", fields!({ "tag": "mutated" }))
            .await
    });
    let mut b_prior = probe.actor_b.clone();
    let mut blocked_prior = ContentionProbe::spawn_op(async move {
        b_prior
            .put("std_drop/d_prior_get", fields!({ "tag": "mutated" }))
            .await
    });
    blocked_top.assert_pending().await;
    blocked_prior.assert_pending().await;

    tx.rollback().await.unwrap();
    blocked_top.expect_ready().await.unwrap();
    blocked_prior.expect_ready().await.unwrap();
}
