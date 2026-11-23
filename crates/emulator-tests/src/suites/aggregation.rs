use std::collections::HashMap;

use googleapis::{
    fields,
    google::firestore::v1::{Value, structured_query::field_filter::Operator},
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{AggregationSpec, PipelineSpec, QuerySpec, field_filter, field_ref_val, fn_val},
    env::TestEnv,
    matchers::assert_status_code,
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
                Operator::Equal,
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
    expect_true!(
        ok_pipe_dbl.results[0]
            .fields
            .get("s")
            .unwrap()
            .storage_eq(&Value::double(18_446_744_073_709_551_616.0))
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
