use googleapis::{
    fields,
    google::firestore::v1::{Value, structured_query::field_filter::Operator},
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{AggregationSpec, QuerySpec, field_filter},
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

    // 4. Integer overflow promotion in Sum (`i64::MAX + i64::MAX` -> `DoubleValue`)
    db.put("big/b1", fields!({ "v": i64::MAX })).await.unwrap();
    db.put("big/b2", fields!({ "v": i64::MAX })).await.unwrap();
    let overflow_agg = db
        .aggregate(
            QuerySpec::collection("big"),
            AggregationSpec::new().sum("s", "v"),
        )
        .await
        .unwrap();
    expect_that!(
        overflow_agg.fields.get("s").and_then(Value::as_double),
        some(gt(1.0e19))
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
