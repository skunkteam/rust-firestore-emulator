use std::collections::HashMap;

use googleapis::{
    fields,
    google::{
        firestore::v1::{
            RunAggregationQueryRequest, StructuredAggregationQuery, StructuredQuery, Value,
            run_aggregation_query_request,
            structured_aggregation_query::{
                self, Aggregation,
                aggregation::{Avg, Count, Operator, Sum},
            },
            structured_query::{
                CollectionSelector, FieldReference, Projection, field_filter::Operator as FilterOp,
            },
        },
        protobuf::Int64Value,
    },
};
use googletest::prelude::*;
use tonic::{Code, Status};

use crate::harness::{
    builders::{AggregationSpec, PipelineSpec, QuerySpec, field_filter, field_ref_val, fn_val},
    client::TestDb,
    env::{DOCUMENTS_ROOT, TestEnv},
    matchers::assert_status_code,
    stream::{AggregationOutput, collect_aggregation},
};

#[gtest]
#[tokio::test]
async fn aggregation_count_sum_avg_across_empty_mixed_numeric_nan_and_mvcc_snapshots() {
    let mut db = TestEnv::new().build_db();

    // 1. Empty collection: Count = 0, Sum = 0 (integer), Avg = Null
    let empty_agg = db
        .aggregate(
            QuerySpec::collection("ledger"),
            AggregationSpec::new()
                .count("total")
                .sum("sum_amt", "amount")
                .avg("avg_amt", "amount"),
        )
        .await
        .expect("empty RunAggregationQuery");
    expect_eq!(empty_agg.fields.get("total"), Some(&Value::integer(0)));
    expect_eq!(empty_agg.fields.get("sum_amt"), Some(&Value::integer(0)));
    expect_eq!(empty_agg.fields.get("avg_amt"), Some(&Value::null()));

    // 2. Mixed integer + double + non-numeric + missing fields
    db.put("ledger/e1", fields!({ "amount": 10i64, "kind": "credit" }))
        .await
        .unwrap();
    db.put("ledger/e2", fields!({ "amount": 20i64, "kind": "credit" }))
        .await
        .unwrap();
    let e3 = db
        .put(
            "ledger/e3",
            fields!({ "amount": "not-a-number", "kind": "note" }),
        )
        .await
        .unwrap();
    let t_integers_only = e3.update_time.unwrap();

    db.put(
        "ledger/e4",
        fields!({ "amount": 15.0f64, "kind": "credit" }),
    )
    .await
    .unwrap();

    // At `t_integers_only` (ReadTime MVCC): 2 numeric ints (10 + 20 = 30 IntegerValue) + 1 string
    // skipped -> Count = 3, Avg = 15.0
    let snap_agg = db
        .aggregate_at_time(
            QuerySpec::collection("ledger"),
            AggregationSpec::new()
                .count("n")
                .count_up_to("capped", 2)
                .sum("s", "amount")
                .avg("a", "amount"),
            t_integers_only,
        )
        .await
        .unwrap();
    expect_eq!(snap_agg.fields.get("n"), Some(&Value::integer(3)));
    expect_eq!(snap_agg.fields.get("capped"), Some(&Value::integer(2)));
    expect_that!(snap_agg.read_time, some(eq(t_integers_only)));
    assert!(
        snap_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(30))
    );
    assert!(
        snap_agg
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(15.0))
    );

    // Current state includes `15.0f64`, promoting `Sum` to `DoubleValue(45.0)`
    let mixed_agg = db
        .aggregate(
            QuerySpec::collection("ledger").where_filter(field_filter(
                "kind",
                FilterOp::Equal,
                "credit",
            )),
            AggregationSpec::new().sum("s", "amount").avg("a", "amount"),
        )
        .await
        .unwrap();
    assert!(
        mixed_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(45.0))
    );
    assert!(
        mixed_agg
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(15.0))
    );

    // 3. Transactional aggregation + NaN propagation
    let mut txn = db.begin_rw_txn().await.unwrap();
    let in_txn = txn
        .aggregate(
            QuerySpec::collection("ledger"),
            AggregationSpec::new().count("cnt"),
        )
        .await
        .unwrap();
    expect_eq!(in_txn.fields.get("cnt"), Some(&Value::integer(4)));
    expect_that!(in_txn.transaction.is_empty(), eq(true));
    txn.rollback().await.unwrap();

    db.put("ledger/e5", fields!({ "amount": f64::NAN }))
        .await
        .unwrap();
    let nan_agg = db
        .aggregate(
            QuerySpec::collection("ledger"),
            AggregationSpec::new().sum("s", "amount").avg("a", "amount"),
        )
        .await
        .unwrap();
    assert!(nan_agg.fields.get("s").unwrap().is_nan());
    assert!(nan_agg.fields.get("a").unwrap().is_nan());

    // 4. Integer overflow promotion & intermediate-overflow cancellation in Standard Sum / Avg
    db.put("big/b1", fields!({ "v": i64::MAX })).await.unwrap();
    db.put("big/b2", fields!({ "v": i64::MAX })).await.unwrap();
    let overflow_agg = db
        .aggregate(
            QuerySpec::collection("big"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        overflow_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0))
    );

    db.put("cancel_pos/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("cancel_pos/d1", fields!({ "v": 1_i64 }))
        .await
        .unwrap();
    db.put("cancel_pos/d2", fields!({ "v": -1_i64 }))
        .await
        .unwrap();
    let cancel_pos_agg = db
        .aggregate(
            QuerySpec::collection("cancel_pos"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        cancel_pos_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(i64::MAX))
    );

    db.put("cancel_neg/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("cancel_neg/d1", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("cancel_neg/d2", fields!({ "v": i64::MIN }))
        .await
        .unwrap();
    db.put("cancel_neg/d3", fields!({ "v": i64::MIN }))
        .await
        .unwrap();
    let cancel_neg_agg = db
        .aggregate(
            QuerySpec::collection("cancel_neg"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        cancel_neg_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(-2))
    );

    db.put("single_round/d0", fields!({ "v": i64::MAX - 512 }))
        .await
        .unwrap();
    db.put("single_round/d1", fields!({ "v": 1535_i64 }))
        .await
        .unwrap();
    let single_round_agg = db
        .aggregate(
            QuerySpec::collection("single_round"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        single_round_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(9_223_372_036_854_775_808.0))
    );

    db.put("avg_cancel/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("avg_cancel/d1", fields!({ "v": 1_i64 }))
        .await
        .unwrap();
    db.put("avg_cancel/d2", fields!({ "v": -i64::MAX }))
        .await
        .unwrap();
    db.put("avg_cancel/d3", fields!({ "v": -1_i64 }))
        .await
        .unwrap();
    db.put("avg_cancel/d4", fields!({ "v": 3_i64 }))
        .await
        .unwrap();
    let avg_cancel_agg = db
        .aggregate(
            QuerySpec::collection("avg_cancel"),
            AggregationSpec::new().sum("s", "v").avg("a", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        avg_cancel_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(3))
    );
    expect_true!(
        avg_cancel_agg
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(0.6))
    );

    // 5. Invalid field path in Sum -> error_in_stream InvalidArgument
    assert_status_code(
        db.aggregate(
            QuerySpec::collection("ledger"),
            AggregationSpec::new().sum("bad", "unclosed.`field"),
        )
        .await,
        Code::InvalidArgument,
    );
}

#[gtest]
#[tokio::test]
async fn enterprise_aggregation_and_pipeline_reject_sequential_i64_long_overflow() {
    let mut db = TestEnv::enterprise().build_db();

    // Order [i64::MAX, -1, 1] fits in i64 at every step -> succeeds in both RPCs.
    db.put("neg_pos/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("neg_pos/d1", fields!({ "v": -1_i64 }))
        .await
        .unwrap();
    db.put("neg_pos/d2", fields!({ "v": 1_i64 })).await.unwrap();

    // Order [i64::MAX, 1, -1] overflows on step 2 -> InvalidArgument("Long overflow").
    db.put("pos_neg/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("pos_neg/d1", fields!({ "v": 1_i64 })).await.unwrap();
    db.put("pos_neg/d2", fields!({ "v": -1_i64 }))
        .await
        .unwrap();

    // Leading double promotes accumulator before two i64::MAX values -> succeeds.
    db.put("dbl_first/d0", fields!({ "v": 1.5_f64 }))
        .await
        .unwrap();
    db.put("dbl_first/d1", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("dbl_first/d2", fields!({ "v": i64::MAX }))
        .await
        .unwrap();

    // Trailing double hits i64::MAX + i64::MAX before seeing double -> InvalidArgument.
    db.put("dbl_last/d0", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("dbl_last/d1", fields!({ "v": i64::MAX }))
        .await
        .unwrap();
    db.put("dbl_last/d2", fields!({ "v": 1.5_f64 }))
        .await
        .unwrap();

    // RunAggregationQuery (including unordered default __name__ scan order)
    let ok_agg = db
        .aggregate(
            QuerySpec::collection("neg_pos"),
            AggregationSpec::new().sum("s", "v").avg("a", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        ok_agg
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(i64::MAX))
    );
    expect_true!(
        ok_agg
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(i64::MAX as f64 / 3.0))
    );

    let ok_dbl = db
        .aggregate(
            QuerySpec::collection("dbl_first").order_asc("__name__"),
            AggregationSpec::new().sum("s", "v").avg("a", "v"),
        )
        .await
        .unwrap();
    expect_true!(
        ok_dbl
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0))
    );
    expect_true!(
        ok_dbl
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0 / 3.0))
    );

    for col in ["pos_neg", "dbl_last"] {
        assert_status_code(
            db.aggregate(
                QuerySpec::collection(col).order_asc("__name__"),
                AggregationSpec::new().sum("s", "v"),
            )
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.aggregate(
                QuerySpec::collection(col).order_asc("__name__"),
                AggregationSpec::new().avg("a", "v"),
            )
            .await,
            Code::InvalidArgument,
        );
    }

    // Unordered `QuerySpec` scan normalization on an overflowing collection (`pos_neg`) and
    // `Count`-only aggregation skipping the scan-order sort.
    assert_status_code(
        db.aggregate(
            QuerySpec::collection("pos_neg"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.aggregate(
            QuerySpec::collection("pos_neg"),
            AggregationSpec::new().avg("a", "v"),
        )
        .await,
        Code::InvalidArgument,
    );
    let count_only = db
        .aggregate(
            QuerySpec::collection("pos_neg"),
            AggregationSpec::new().count("n"),
        )
        .await
        .unwrap();
    expect_eq!(count_only.fields.get("n"), Some(&Value::integer(3)));

    // ExecutePipeline aggregate(sum/average) matches RunAggregationQuery
    let pipe_aggs = HashMap::from([
        ("s".to_string(), fn_val("sum", vec![field_ref_val("v")])),
        ("a".to_string(), fn_val("average", vec![field_ref_val("v")])),
    ]);
    let ok_pipe = db
        .pipeline(
            PipelineSpec::collection("neg_pos")
                .sort_asc("__name__")
                .aggregate(pipe_aggs.clone(), Some(HashMap::new())),
        )
        .await
        .unwrap();
    expect_eq!(ok_pipe.results.len(), 1);
    expect_true!(
        ok_pipe.results[0]
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::integer(i64::MAX))
    );
    expect_true!(
        ok_pipe.results[0]
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(i64::MAX as f64 / 3.0))
    );

    let ok_pipe_dbl = db
        .pipeline(
            PipelineSpec::collection("dbl_first")
                .sort_asc("__name__")
                .aggregate(pipe_aggs, Some(HashMap::new())),
        )
        .await
        .unwrap();
    expect_eq!(ok_pipe_dbl.results.len(), 1);
    expect_true!(
        ok_pipe_dbl.results[0]
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0))
    );
    expect_true!(
        ok_pipe_dbl.results[0]
            .fields
            .get("a")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0 / 3.0))
    );

    for col in ["pos_neg", "dbl_last"] {
        for (alias, op) in [("s", "sum"), ("a", "average")] {
            assert_status_code(
                db.pipeline(
                    PipelineSpec::collection(col)
                        .sort_asc("__name__")
                        .aggregate(
                            HashMap::from([(
                                alias.to_string(),
                                fn_val(op, vec![field_ref_val("v")]),
                            )]),
                            Some(HashMap::new()),
                        ),
                )
                .await,
                Code::InvalidArgument,
            );
        }
    }
}

async fn raw_aggregate(
    db: &mut TestDb,
    parent: &str,
    structured_query: StructuredQuery,
    aggregations: Vec<Aggregation>,
    consistency_selector: Option<run_aggregation_query_request::ConsistencySelector>,
) -> Result<AggregationOutput, Status> {
    let resp = db
        .raw_grpc()
        .run_aggregation_query(RunAggregationQueryRequest {
            parent: parent.to_string(),
            query_type: Some(
                run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                    StructuredAggregationQuery {
                        aggregations,
                        query_type: Some(structured_aggregation_query::QueryType::StructuredQuery(
                            structured_query,
                        )),
                    },
                ),
            ),
            consistency_selector,
            explain_options: None,
            request_options: None,
        })
        .await?;
    collect_aggregation(resp.into_inner()).await
}

fn agg_count(alias: &str, up_to: Option<i64>) -> Aggregation {
    Aggregation {
        alias:    alias.to_string(),
        operator: Some(Operator::Count(Count {
            up_to: up_to.map(|value| Int64Value { value }),
        })),
    }
}

fn agg_sum(alias: &str, field_path: Option<&str>) -> Aggregation {
    Aggregation {
        alias:    alias.to_string(),
        operator: Some(Operator::Sum(Sum {
            field: field_path.map(|fp| FieldReference {
                field_path: fp.to_string(),
            }),
        })),
    }
}

fn agg_avg(alias: &str, field_path: Option<&str>) -> Aggregation {
    Aggregation {
        alias:    alias.to_string(),
        operator: Some(Operator::Avg(Avg {
            field: field_path.map(|fp| FieldReference {
                field_path: fp.to_string(),
            }),
        })),
    }
}

fn col_query(collection_id: &str) -> StructuredQuery {
    StructuredQuery {
        from: vec![CollectionSelector {
            collection_id:   collection_id.to_string(),
            all_descendants: false,
        }],
        ..Default::default()
    }
}

#[gtest]
#[tokio::test]
async fn grpc_aggregation_edition_validation_aliases_select_and_pre_execution_order() {
    for enterprise in [false, true] {
        let mut db = if enterprise {
            TestEnv::enterprise().build_db()
        } else {
            TestEnv::new().build_db()
        };
        for (doc, val) in [
            ("items/d1", 10i64),
            ("items/d2", 20i64),
            ("items/d3", 30i64),
        ] {
            db.put(doc, fields!({ "v": val })).await.unwrap();
        }

        // 1. Sequential omitted aliases (`field_1`, `field_2`) + mixed explicit/omitted lists.
        let auto_out = raw_aggregate(
            &mut db,
            DOCUMENTS_ROOT,
            col_query("items"),
            vec![
                agg_count("total", None),
                agg_count("", None),
                agg_sum("sum_v", Some("v")),
                agg_avg("", Some("v")),
            ],
            None,
        )
        .await
        .unwrap();
        expect_eq!(auto_out.fields.get("total"), Some(&Value::integer(3)));
        expect_eq!(auto_out.fields.get("field_1"), Some(&Value::integer(3)));
        expect_eq!(auto_out.fields.get("sum_v"), Some(&Value::integer(60)));
        assert!(
            auto_out
                .fields
                .get("field_2")
                .unwrap()
                .storage_eq(&Value::double(20.0))
        );

        // 2. Duplicate aliases, explicit `field_1` collision, > 1,500-byte aliases, empty
        //    aggregations, missing operator, and malformed / non-__name__ reserved SUM/AVG fields.
        let max_ascii = "a".repeat(1_500);
        let max_utf8 = "é".repeat(750);
        for ok_alias in [max_ascii.as_str(), max_utf8.as_str(), "____"] {
            let out = raw_aggregate(
                &mut db,
                DOCUMENTS_ROOT,
                col_query("items"),
                vec![agg_count(ok_alias, None)],
                None,
            )
            .await
            .unwrap();
            expect_eq!(out.fields.get(ok_alias), Some(&Value::integer(3)));
        }

        let over_ascii = "a".repeat(1_501);
        let over_utf8 = "é".repeat(751);
        for bad_aggs in [
            vec![],
            vec![Aggregation {
                alias:    "missing_op".to_string(),
                operator: None,
            }],
            vec![Aggregation::default()],
            vec![agg_count("dup", None), agg_sum("dup", Some("v"))],
            vec![agg_count("field_1", None), agg_count("", None)],
            vec![agg_count("", None), agg_count("field_1", None)],
            vec![agg_count(&over_ascii, None)],
            vec![agg_count(&over_utf8, None)],
            vec![agg_sum("s", None)],
            vec![agg_avg("a", None)],
            vec![agg_sum("s", Some(""))],
            vec![agg_avg("a", Some("a..b"))],
            vec![agg_sum("s", Some("__custom__"))],
            vec![agg_avg("a", Some("nested.__custom__"))],
        ] {
            assert_status_code(
                raw_aggregate(&mut db, DOCUMENTS_ROOT, col_query("items"), bad_aggs, None).await,
                Code::InvalidArgument,
            );
        }

        // 3. Edition-specific `Count.upTo`, aggregation count caps (5 vs 64), reserved aliases, and
        //    `__name__` SUM/AVG fields.
        if !enterprise {
            let zero_cap = raw_aggregate(
                &mut db,
                DOCUMENTS_ROOT,
                col_query("items"),
                vec![agg_count("z", Some(0)), agg_count("c2", Some(2))],
                None,
            )
            .await
            .unwrap();
            expect_eq!(zero_cap.fields.get("z"), Some(&Value::integer(0)));
            expect_eq!(zero_cap.fields.get("c2"), Some(&Value::integer(2)));

            let five_ok = (0..5)
                .map(|i| agg_count(&format!("c{i}"), None))
                .collect::<Vec<_>>();
            expect_eq!(
                raw_aggregate(&mut db, DOCUMENTS_ROOT, col_query("items"), five_ok, None)
                    .await
                    .unwrap()
                    .fields
                    .len(),
                5
            );

            let six_err = (0..6)
                .map(|i| agg_count(&format!("c{i}"), None))
                .collect::<Vec<_>>();
            for bad_std in [
                six_err,
                vec![agg_count("neg", Some(-1))],
                vec![agg_count("__name__", None)],
                vec![agg_count("__custom__", None)],
                vec![agg_sum("s", Some("__name__"))],
                vec![agg_avg("a", Some("`__name__`"))],
                vec![agg_sum("s", Some("a.__name__"))],
            ] {
                assert_status_code(
                    raw_aggregate(&mut db, DOCUMENTS_ROOT, col_query("items"), bad_std, None).await,
                    Code::InvalidArgument,
                );
            }
        } else {
            let sixty_four_ok = (0..64)
                .map(|i| agg_count(&format!("c{i}"), None))
                .collect::<Vec<_>>();
            expect_eq!(
                raw_aggregate(
                    &mut db,
                    DOCUMENTS_ROOT,
                    col_query("items"),
                    sixty_four_ok,
                    None,
                )
                .await
                .unwrap()
                .fields
                .len(),
                64
            );
            let sixty_five_err = (0..65)
                .map(|i| agg_count(&format!("c{i}"), None))
                .collect::<Vec<_>>();
            assert_status_code(
                raw_aggregate(
                    &mut db,
                    DOCUMENTS_ROOT,
                    col_query("items"),
                    sixty_five_err,
                    None,
                )
                .await,
                Code::InvalidArgument,
            );
            for up_to in [-1, 0, 1, 2] {
                assert_status_code(
                    raw_aggregate(
                        &mut db,
                        DOCUMENTS_ROOT,
                        col_query("items"),
                        vec![agg_count("c", Some(up_to))],
                        None,
                    )
                    .await,
                    Code::InvalidArgument,
                );
            }
            let ent_reserved = raw_aggregate(
                &mut db,
                DOCUMENTS_ROOT,
                col_query("items"),
                vec![
                    agg_count("__name__", None),
                    agg_count("__custom__", None),
                    agg_sum("sum_doc", Some("__name__")),
                    agg_avg("avg_doc", Some("`__name__`")),
                    agg_sum("sum_nested", Some("a.__name__")),
                ],
                None,
            )
            .await
            .unwrap();
            expect_eq!(
                ent_reserved.fields.get("__name__"),
                Some(&Value::integer(3))
            );
            expect_eq!(
                ent_reserved.fields.get("__custom__"),
                Some(&Value::integer(3))
            );
            expect_eq!(ent_reserved.fields.get("sum_doc"), Some(&Value::null()));
            expect_eq!(ent_reserved.fields.get("avg_doc"), Some(&Value::null()));
            expect_eq!(ent_reserved.fields.get("sum_nested"), Some(&Value::null()));
        }

        // 4. `StructuredQuery.select`: valid projections (empty and populated) are ignored during
        //    aggregation, while invalid projection paths fail with `InvalidArgument`.
        for select_paths in [vec![], vec!["other"], vec!["__name__"]] {
            let mut sq = col_query("items");
            sq.select = Some(Projection {
                fields: select_paths
                    .into_iter()
                    .map(|p| FieldReference {
                        field_path: p.to_string(),
                    })
                    .collect(),
            });
            let sel_ok = raw_aggregate(
                &mut db,
                DOCUMENTS_ROOT,
                sq,
                vec![agg_count("n", None), agg_sum("s", Some("v"))],
                None,
            )
            .await
            .unwrap();
            expect_eq!(sel_ok.fields.get("n"), Some(&Value::integer(3)));
            expect_eq!(sel_ok.fields.get("s"), Some(&Value::integer(60)));
        }
        for bad_sel in ["__custom__", "a..b"] {
            let mut sq = col_query("items");
            sq.select = Some(Projection {
                fields: vec![FieldReference {
                    field_path: bad_sel.to_string(),
                }],
            });
            assert_status_code(
                raw_aggregate(
                    &mut db,
                    DOCUMENTS_ROOT,
                    sq,
                    vec![agg_count("n", None)],
                    None,
                )
                .await,
                Code::InvalidArgument,
            );
        }

        // 5. Pre-execution validation order: (a) `Query::from_structured` (`offset < 0` / bad
        //    `select`) precedes `compile_aggregations` (empty aggregations)
        let mut bad_offset_sq = col_query("items");
        bad_offset_sq.offset = -1;
        let offset_err = assert_status_code(
            raw_aggregate(&mut db, DOCUMENTS_ROOT, bad_offset_sq, vec![], None).await,
            Code::InvalidArgument,
        );
        expect_that!(offset_err.message(), contains_substring("offset"));

        let mut bad_select_sq = col_query("items");
        bad_select_sq.select = Some(Projection {
            fields: vec![FieldReference {
                field_path: "__custom__".to_string(),
            }],
        });
        let select_err = assert_status_code(
            raw_aggregate(&mut db, DOCUMENTS_ROOT, bad_select_sq, vec![], None).await,
            Code::InvalidArgument,
        );
        expect_that!(select_err.message(), contains_substring("__custom__"));

        //    (b) `compile_aggregations` precedes query execution: a rolled-back transaction
        //        (`Transaction(stale_id)`) would fail query execution with `Aborted`, yet invalid
        //        aggregations fail earlier with `InvalidArgument` from `compile_aggregations`.
        let stale_txn = db.begin_rw_txn().await.unwrap();
        let stale_id = stale_txn.id().to_vec();
        stale_txn.rollback().await.unwrap();
        let agg_before_exec_err = assert_status_code(
            raw_aggregate(
                &mut db,
                DOCUMENTS_ROOT,
                col_query("items"),
                vec![agg_sum("bad", Some("__custom__"))],
                Some(run_aggregation_query_request::ConsistencySelector::Transaction(stale_id)),
            )
            .await,
            Code::InvalidArgument,
        );
        expect_that!(
            agg_before_exec_err.message(),
            contains_substring("__custom__")
        );
    }
}

#[gtest]
#[tokio::test]
async fn standard_sum_avg_implicit_field_existence_filter_across_mixed_nested_limit_and_multi() {
    for enterprise in [false, true] {
        let mut db = if enterprise {
            TestEnv::enterprise().build_db()
        } else {
            TestEnv::new().build_db()
        };

        // 1. Combined [count(), sum('a'), avg('a')] vs standalone [count()] on mixed {a:'text'},
        //    {a:null}, {other:42}.
        db.put("non_num/n1", fields!({ "a": "text" }))
            .await
            .unwrap();
        db.put("non_num/n2", fields!({ "a": null })).await.unwrap();
        db.put("non_num/n3", fields!({ "other": 42i64 }))
            .await
            .unwrap();

        let combined = db
            .aggregate(
                QuerySpec::collection("non_num"),
                AggregationSpec::new()
                    .count("c")
                    .sum("s", "a")
                    .avg("avg", "a"),
            )
            .await
            .unwrap();
        if enterprise {
            expect_eq!(combined.fields.get("c"), Some(&Value::integer(3)));
            expect_eq!(combined.fields.get("s"), Some(&Value::null()));
            expect_eq!(combined.fields.get("avg"), Some(&Value::null()));
        } else {
            expect_eq!(combined.fields.get("c"), Some(&Value::integer(2)));
            expect_eq!(combined.fields.get("s"), Some(&Value::integer(0)));
            expect_eq!(combined.fields.get("avg"), Some(&Value::null()));
        }

        let count_only = db
            .aggregate(
                QuerySpec::collection("non_num"),
                AggregationSpec::new().count("c"),
            )
            .await
            .unwrap();
        expect_eq!(count_only.fields.get("c"), Some(&Value::integer(3)));

        // 2. Nested `sum('nested.x')`.
        db.put("nested/d1", fields!({ "nested": { "x": 10i64 } }))
            .await
            .unwrap();
        db.put("nested/d2", fields!({ "nested": { "x": null } }))
            .await
            .unwrap();
        db.put("nested/d3", fields!({ "nested": { "y": 1i64 } }))
            .await
            .unwrap();
        db.put("nested/d4", fields!({ "nested": 42i64 }))
            .await
            .unwrap();

        let nested_res = db
            .aggregate(
                QuerySpec::collection("nested"),
                AggregationSpec::new().count("c").sum("s", "nested.x"),
            )
            .await
            .unwrap();
        expect_eq!(
            nested_res.fields.get("c"),
            Some(&Value::integer(if enterprise { 4 } else { 2 }))
        );
        expect_eq!(nested_res.fields.get("s"), Some(&Value::integer(10)));

        // 3. `limit` interaction: docs missing 'a' do NOT consume `limit` slots on Standard.
        db.put("lim/d1", fields!({ "other": 1i64 })).await.unwrap();
        db.put("lim/d2", fields!({ "a": 10i64 })).await.unwrap();
        db.put("lim/d3", fields!({ "a": 20i64 })).await.unwrap();

        let lim_query = if enterprise {
            QuerySpec::collection("lim").order_asc("__name__").limit(2)
        } else {
            QuerySpec::collection("lim").limit(2)
        };
        let lim_res = db
            .aggregate(lim_query, AggregationSpec::new().count("c").sum("s", "a"))
            .await
            .unwrap();
        expect_eq!(lim_res.fields.get("c"), Some(&Value::integer(2)));
        expect_eq!(
            lim_res.fields.get("s"),
            Some(&Value::integer(if enterprise { 10 } else { 30 }))
        );

        // 4. Multi-field `[count(), sum('a'), avg('b')]` requires both 'a' and 'b' on Standard.
        db.put("multi/m1", fields!({ "a": 10i64, "b": 20i64 }))
            .await
            .unwrap();
        db.put("multi/m2", fields!({ "a": 5i64 })).await.unwrap();
        db.put("multi/m3", fields!({ "b": 7i64 })).await.unwrap();
        db.put("multi/m4", fields!({ "a": null, "b": null }))
            .await
            .unwrap();

        let multi_res = db
            .aggregate(
                QuerySpec::collection("multi"),
                AggregationSpec::new()
                    .count("c")
                    .sum("s", "a")
                    .avg("avg", "b"),
            )
            .await
            .unwrap();
        if enterprise {
            expect_eq!(multi_res.fields.get("c"), Some(&Value::integer(4)));
            expect_eq!(multi_res.fields.get("s"), Some(&Value::integer(15)));
            expect_eq!(multi_res.fields.get("avg"), Some(&Value::double(13.5)));
        } else {
            expect_eq!(multi_res.fields.get("c"), Some(&Value::integer(2)));
            expect_eq!(multi_res.fields.get("s"), Some(&Value::integer(10)));
            expect_eq!(multi_res.fields.get("avg"), Some(&Value::double(20.0)));
        }
    }
}
