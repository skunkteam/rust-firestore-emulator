use googleapis::{
    fields,
    google::{firestore::v1::Value, protobuf::Timestamp},
    val,
};
use googletest::prelude::*;
use rstest::rstest;
use tonic::Code;

use crate::harness::{
    builders::{
        WriteBatchSpec, doc_mask, pre_exists, pre_update_time, tf_append_missing, tf_increment,
        tf_maximum, tf_minimum, tf_remove_all, tf_server_timestamp,
    },
    env::TestEnv,
    matchers::{assert_doc_wire_eq, assert_status_code},
};

#[gtest]
#[tokio::test]
async fn preconditions_enforce_exists_and_update_time_guards() {
    let mut db = TestEnv::new().build_db();

    // Exists(false) creates missing document, then rejects duplicate with AlreadyExists
    // (verified in `test-suite/tests/1-get-set.test.ts`: `can only create once` -> `6
    // ALREADY_EXISTS`).
    let created = db
        .update(
            "guards/doc1",
            fields!({ "v": 1i64 }),
            None,
            Some(pre_exists(false)),
            None,
        )
        .await
        .expect("Exists(false) on missing doc succeeds");
    let t1 = created.update_time.expect("update_time set");

    assert_status_code(
        db.update(
            "guards/doc1",
            fields!({ "v": 2i64 }),
            None,
            Some(pre_exists(false)),
            None,
        )
        .await,
        Code::AlreadyExists,
    );

    // Exists(true) succeeds on existing document.
    let updated = db
        .update(
            "guards/doc1",
            fields!({ "v": 2i64 }),
            None,
            Some(pre_exists(true)),
            None,
        )
        .await
        .expect("Exists(true) on existing doc succeeds");
    let t2 = updated.update_time.expect("updated timestamp");

    // Exists(true) on missing document rejects with NotFound and leaves the document absent
    // (verified in `test-suite/tests/10-rust-oracle-edge-cases.test.ts`).
    assert_status_code(
        db.update(
            "guards/never_created",
            fields!({ "v": 1i64 }),
            None,
            Some(pre_exists(true)),
            None,
        )
        .await,
        Code::NotFound,
    );
    assert_status_code(db.get("guards/never_created").await, Code::NotFound);

    // UpdateTime mismatch -> FailedPrecondition; matching UpdateTime -> Ok.
    assert_status_code(
        db.update(
            "guards/doc1",
            fields!({ "v": 3i64 }),
            None,
            Some(pre_update_time(t1)),
            None,
        )
        .await,
        Code::FailedPrecondition,
    );
    assert_status_code(
        db.update(
            "guards/never_created",
            fields!({ "v": 3i64 }),
            None,
            Some(pre_update_time(Timestamp {
                seconds: 100,
                nanos:   0,
            })),
            None,
        )
        .await,
        Code::FailedPrecondition,
    );

    let updated_exact = db
        .update(
            "guards/doc1",
            fields!({ "v": 3i64 }),
            None,
            Some(pre_update_time(t2)),
            None,
        )
        .await
        .expect("matching UpdateTime precondition succeeds");
    assert_doc_wire_eq(&updated_exact, "guards/doc1", &fields!({ "v": 3i64 }));

    // Delete with stale UpdateTime fails with FailedPrecondition; matching UpdateTime deletes.
    assert_status_code(
        db.delete("guards/doc1", Some(pre_update_time(t2))).await,
        Code::FailedPrecondition,
    );
    db.delete(
        "guards/doc1",
        Some(pre_update_time(updated_exact.update_time.unwrap())),
    )
    .await
    .expect("DeleteDocument with matching UpdateTime succeeds");
    assert_status_code(db.get("guards/doc1").await, Code::NotFound);
}

#[gtest]
#[tokio::test]
async fn precondition_exists_true_on_missing_document_returns_not_found() {
    // Ground-truth Cloud Firestore behavior verified in
    // `test-suite/tests/10-rust-oracle-edge-cases.test.ts`:
    // `Exists(true)` on a missing document rejects with `5 NOT_FOUND` across UpdateDocument,
    // DeleteDocument, and Commit (both Update and Delete operations).
    let mut db = TestEnv::new().build_db();
    assert_status_code(
        db.update(
            "guards/missing",
            fields!({ "v": 1i64 }),
            None,
            Some(pre_exists(true)),
            None,
        )
        .await,
        Code::NotFound,
    );
    assert_status_code(
        db.delete("guards/missing", Some(pre_exists(true))).await,
        Code::NotFound,
    );
    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().update_with(
            "guards/missing",
            fields!({ "v": 1i64 }),
            Some(vec!["v"]),
            vec![],
            Some(pre_exists(true)),
        ))
        .await,
        Code::NotFound,
    );
    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().delete("guards/missing", Some(pre_exists(true))))
            .await,
        Code::NotFound,
    );
}

#[gtest]
#[tokio::test]
async fn server_timestamp_and_increment_transforms_follow_firestore_wire_rules() {
    // Verified in `test-suite/tests/1-get-set.test.ts` (`using serverTimestamp`, `increment`).
    let mut db = TestEnv::new().build_db();

    let (commit_time, results) = db
        .commit_writes(WriteBatchSpec::new().update_with(
            "stats/s1",
            fields!({ "label": "hits" }),
            Some(vec!["label"]),
            vec![
                tf_server_timestamp("touched_at"),
                tf_server_timestamp("nested.touched_at"),
                tf_increment("count", 5i64),
            ],
            None,
        ))
        .await
        .expect("Commit with ServerTimestamp + Increment");

    let commit_ts = commit_time.expect("commit_time returned");
    expect_eq!(results.len(), 1);
    expect_eq!(results[0].transform_results.len(), 3);
    expect_eq!(results[0].transform_results[0], Value::timestamp(commit_ts));
    expect_eq!(results[0].transform_results[1], Value::timestamp(commit_ts));
    expect_eq!(results[0].transform_results[2], val!(5i64));

    // Increment integer + integer, then integer + double -> promotes to double, plus i64::MAX
    // saturation
    db.commit_writes(WriteBatchSpec::new().update_with(
        "stats/s1",
        fields!({}),
        Some(vec![]),
        vec![
            tf_increment("count", 11i64),
            tf_increment("overflow_int", i64::MAX),
        ],
        None,
    ))
    .await
    .unwrap();

    db.commit_writes(WriteBatchSpec::new().update_with(
        "stats/s1",
        fields!({}),
        Some(vec![]),
        vec![
            tf_increment("count", -32.25f64),
            tf_increment("overflow_int", 100i64),
        ],
        None,
    ))
    .await
    .unwrap();

    let doc = db.get("stats/s1").await.unwrap();
    assert_doc_wire_eq(
        &doc,
        "stats/s1",
        &fields!({
            "label": "hits",
            "touched_at": commit_ts,
            "nested": { "touched_at": commit_ts },
            "count": -16.25f64,
            "overflow_int": i64::MAX,
        }),
    );
}

#[gtest]
#[tokio::test]
async fn array_union_and_remove_handle_signed_zero_nan_and_cross_type_numbers() {
    // Verified in `test-suite/tests/1-get-set.test.ts` lines 184-207 (`arrayUnion`, `arrayRemove`).
    let mut db = TestEnv::new().build_db();

    db.put(
        "arrays/a1",
        fields!({
            "items": [0.0f64, f64::NAN, { "nested": -0.0f64 }],
        }),
    )
    .await
    .unwrap();

    // AppendMissingElements treats -0.0 == 0.0, NaN == NaN, and 0i64 == 0.0 as already present,
    // preserving existing representations while appending genuinely new elements.
    db.commit_writes(WriteBatchSpec::new().update_with(
        "arrays/a1",
        fields!({}),
        Some(vec![]),
        vec![tf_append_missing(
            "items",
            vec![
                val!(-0.0f64),
                val!(0i64),
                val!(f64::NAN),
                val!({ "nested": 0.0f64 }),
                val!(42i64),
            ],
        )],
        None,
    ))
    .await
    .unwrap();

    let after_union = db.get("arrays/a1").await.unwrap();
    assert_doc_wire_eq(
        &after_union,
        "arrays/a1",
        &fields!({
            "items": [0.0f64, f64::NAN, { "nested": -0.0f64 }, 42i64],
        }),
    );

    // RemoveAllFromArray removes matching -0.0/0.0, NaN, and nested map equivalents, leaving [42].
    db.commit_writes(WriteBatchSpec::new().update_with(
        "arrays/a1",
        fields!({}),
        Some(vec![]),
        vec![tf_remove_all(
            "items",
            vec![val!(-0.0f64), val!(f64::NAN), val!({ "nested": 0i64 })],
        )],
        None,
    ))
    .await
    .unwrap();

    let after_remove = db.get("arrays/a1").await.unwrap();
    assert_doc_wire_eq(&after_remove, "arrays/a1", &fields!({ "items": [42i64] }));
}

#[rstest]
#[case::maximum(WriteBatchSpec::new().update_with("u/d1", fields!({}), Some(vec![]), vec![tf_maximum("x", 10i64)], None))]
#[case::minimum(WriteBatchSpec::new().update_with("u/d1", fields!({}), Some(vec![]), vec![tf_minimum("x", 10i64)], None))]
#[case::standalone_transform(WriteBatchSpec::new().standalone_transform("u/d1", vec![tf_increment("x", 1i64)]))]
#[tokio::test]
async fn unimplemented_transform_variants_return_unimplemented(#[case] spec: WriteBatchSpec) {
    let mut db = TestEnv::new().build_db();
    assert_status_code(db.commit_writes(spec).await, Code::Unimplemented);
    let _unused = doc_mask(["x"]);
}
