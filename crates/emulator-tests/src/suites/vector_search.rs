use std::{collections::HashMap, time::Duration};

use googleapis::{
    VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY, fields,
    google::{
        firestore::v1::{
            BatchGetDocumentsRequest, CommitRequest, Document, ExecutePipelineRequest,
            ListenRequest, Precondition, RunAggregationQueryRequest, RunQueryRequest,
            StructuredAggregationQuery, Target, TransactionOptions, Value, Write,
            batch_get_documents_request, execute_pipeline_request, listen_request,
            run_aggregation_query_request, run_query_request,
            structured_aggregation_query::{Aggregation, aggregation},
            structured_query::{FieldReference, FindNearest, field_filter::Operator, find_nearest},
            target::{self, query_target},
            transaction_options::{self, ReadWrite},
            value::ValueType,
            write,
        },
        protobuf::{DoubleValue, Int32Value},
    },
    val,
};
use googletest::prelude::*;
use prost::Message;
use tonic::Code;

use crate::harness::{
    builders::{
        AggregationSpec, PipelineSpec, QuerySpec, WriteBatchSpec, and_filters, doc_mask, doc_path,
        field_filter, field_ref_val, fn_val, tf_append_missing, tf_increment, tf_remove_all,
        tf_server_timestamp, vec_val,
    },
    contention::ContentionProbe,
    env::{DATABASE_ROOT, DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
    stream::{collect_aggregation, collect_batch_get, collect_run_query},
};

fn make_find_nearest(
    vector_field: &str,
    query_vector: Value,
    distance_measure: find_nearest::DistanceMeasure,
    limit: i32,
    distance_result_field: Option<&str>,
    distance_threshold: Option<f64>,
) -> FindNearest {
    FindNearest {
        vector_field: Some(FieldReference {
            field_path: vector_field.to_string(),
        }),
        query_vector: Some(query_vector),
        distance_measure: distance_measure as i32,
        limit: Some(Int32Value { value: limit }),
        distance_result_field: distance_result_field.unwrap_or_default().to_string(),
        distance_threshold: distance_threshold.map(|value| DoubleValue { value }),
    }
}

fn raw_map(fields: HashMap<String, Value>) -> Value {
    Value {
        value_type: Some(ValueType::MapValue(
            googleapis::google::firestore::v1::MapValue { fields },
        )),
    }
}

#[gtest]
#[tokio::test]
async fn write_mask_and_transform_validation_across_create_update_batch_write_and_commit() {
    let mut db = TestEnv::new().build_db();

    // 1. Valid vectors: 1D, 2048D, -0.0 bit preservation, +/-Infinity, VectorValue inside
    //    ArrayValue, and 4-underscore `"____"` field name.
    let neg_zero_and_inf = vec_val([-0.0, f64::NEG_INFINITY, f64::INFINITY]);
    let max_dim_vec = vec_val((0..2048).map(|i| i as f64));
    let nested_in_array = Value::array(vec![vec_val([1.0, 2.0]), vec_val([3.0, 4.0])]);

    let created = db
        .create(
            "valid/v1",
            fields!({
                "____": "allowed",
                "one_d": vec_val([42.0]),
                "max_d": max_dim_vec.clone(),
                "special": neg_zero_and_inf.clone(),
                "vec_arr": nested_in_array.clone(),
            }),
        )
        .await
        .expect("valid vector document must create cleanly");

    assert_doc_wire_eq(
        &created,
        "valid/v1",
        &fields!({
            "____": "allowed",
            "one_d": vec_val([42.0]),
            "max_d": max_dim_vec,
            "special": neg_zero_and_inf,
            "vec_arr": nested_in_array,
        }),
    );

    let fetched = db.get("valid/v1").await.unwrap();
    let special_coords: Vec<f64> = fetched.fields["special"]
        .iter_vector_f64()
        .expect("valid vector")
        .collect();
    expect_eq!(special_coords[0].to_bits(), (-0.0_f64).to_bits());
    expect_eq!(special_coords[1], f64::NEG_INFINITY);
    expect_eq!(special_coords[2], f64::INFINITY);

    // 2. Atomic subpath overwrite (`embedding.nested = "replaced"`) replaces the vector atomically
    //    rather than mutating `__type__` or `value`.
    db.put("valid/subpath", fields!({ "emb": vec_val([1.0, 2.0]) }))
        .await
        .unwrap();
    let updated = db
        .update(
            "valid/subpath",
            fields!({ "emb": { "nested": "replaced" } }),
            Some(doc_mask(["emb.nested"])),
            None,
            None,
        )
        .await
        .unwrap();
    assert_doc_wire_eq(
        &updated,
        "valid/subpath",
        &fields!({ "emb": { "nested": "replaced" } }),
    );

    // 3. Invalid vector payloads rejected with InvalidArgument across CreateDocument &
    //    UpdateDocument.
    let invalid_vectors: Vec<(&str, Value)> = vec![
        (
            "0D",
            raw_map(HashMap::from([
                (
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                ),
                (VECTOR_MAP_VALUE_KEY.to_string(), Value::array(vec![])),
            ])),
        ),
        (
            "2049D",
            raw_map(HashMap::from([
                (
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                ),
                (
                    VECTOR_MAP_VALUE_KEY.to_string(),
                    Value::array((0..2049).map(|i| Value::double(i as f64)).collect()),
                ),
            ])),
        ),
        ("NaN", vec_val([1.0, f64::NAN])),
        (
            "non_double",
            raw_map(HashMap::from([
                (
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                ),
                (
                    VECTOR_MAP_VALUE_KEY.to_string(),
                    Value::array(vec![Value::integer(1)]),
                ),
            ])),
        ),
        (
            "missing_value",
            raw_map(HashMap::from([(
                VECTOR_MAP_TYPE_KEY.to_string(),
                Value::string(VECTOR_MAP_TYPE_SENTINEL),
            )])),
        ),
        (
            "non_array_value",
            raw_map(HashMap::from([
                (
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                ),
                (VECTOR_MAP_VALUE_KEY.to_string(), Value::string("bad")),
            ])),
        ),
        (
            "extraneous_key",
            raw_map(HashMap::from([
                (
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                ),
                (
                    VECTOR_MAP_VALUE_KEY.to_string(),
                    Value::array(vec![Value::double(1.0)]),
                ),
                ("extra".to_string(), Value::integer(1)),
            ])),
        ),
    ];

    for (label, bad_vec) in &invalid_vectors {
        let create_res = db
            .create(
                &format!("bad/{label}_create"),
                HashMap::from([("v".to_string(), bad_vec.clone())]),
            )
            .await;
        assert_status_code(create_res, Code::InvalidArgument);

        let put_res = db
            .put(
                &format!("bad/{label}_put"),
                HashMap::from([("v".to_string(), bad_vec.clone())]),
            )
            .await;
        assert_status_code(put_res, Code::InvalidArgument);
    }

    // Top-level `__type__: "__vector__"` directly on Document.fields is rejected (reserved key).
    let top_level_sentinel = HashMap::from([
        (
            VECTOR_MAP_TYPE_KEY.to_string(),
            Value::string(VECTOR_MAP_TYPE_SENTINEL),
        ),
        (
            VECTOR_MAP_VALUE_KEY.to_string(),
            Value::array(vec![Value::double(1.0)]),
        ),
    ]);
    assert_status_code(
        db.create("bad/top_sentinel", top_level_sentinel.clone())
            .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.put("bad/top_sentinel", top_level_sentinel).await,
        Code::InvalidArgument,
    );

    // 4. Empty and reserved keys, masks, and transforms rejected with InvalidArgument.
    for bad_key in ["", "__custom__", "__name__"] {
        let fields = HashMap::from([(bad_key.to_string(), val!(1i64))]);
        assert_status_code(
            db.create("bad/key_create", fields.clone()).await,
            Code::InvalidArgument,
        );
        assert_status_code(db.put("bad/key_put", fields).await, Code::InvalidArgument);
    }

    for bad_path in ["", "__name__", "`__name__`", "emb.__type__", "__custom__"] {
        assert_status_code(
            db.update(
                "bad/mask",
                fields!({ "ok": 1i64 }),
                Some(doc_mask([bad_path])),
                None,
                None,
            )
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.commit_writes(WriteBatchSpec::new().update_with(
                "bad/tf",
                fields!({}),
                Some(vec![]),
                vec![tf_server_timestamp(bad_path)],
                None,
            ))
            .await,
            Code::InvalidArgument,
        );
    }

    // Transform operands: invalid vector and nested array rejected on AppendMissing / RemoveAll /
    // Increment.
    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().update_with(
            "bad/tf_op1",
            fields!({}),
            Some(vec![]),
            vec![tf_append_missing("arr", vec![vec_val([f64::NAN])])],
            None,
        ))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().update_with(
            "bad/tf_op2",
            fields!({}),
            Some(vec![]),
            vec![tf_remove_all("arr", vec![val!([1i64])])],
            None,
        ))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().update_with(
            "bad/tf_op3",
            fields!({}),
            Some(vec![]),
            vec![tf_increment("n", vec_val([f64::NAN]))],
            None,
        ))
        .await,
        Code::InvalidArgument,
    );

    // 5. Atomic Commit: a bad second write aborts the whole commit without persisting write 1.
    assert_status_code(
        db.commit_writes(
            WriteBatchSpec::new()
                .put("atomic/first", fields!({ "ok": true }))
                .update_with(
                    "atomic/second",
                    fields!({ "ok": true }),
                    Some(vec!["`__name__`"]),
                    vec![],
                    None,
                ),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(db.get("atomic/first").await, Code::NotFound);

    // 6. BatchWrite per-write statuses & Write { operation: None } -> Unimplemented in BatchWrite
    //    and Commit.
    let batch_resp = db
        .batch_write(
            WriteBatchSpec::new()
                .put("batch/ok", fields!({ "v": vec_val([1.0, 2.0]) }))
                .put("batch/bad_vec", fields!({ "v": vec_val([f64::NAN]) }))
                .update_with(
                    "batch/bad_mask",
                    fields!({ "x": 1i64 }),
                    Some(vec!["emb.__type__"]),
                    vec![],
                    None,
                )
                .raw(Write {
                    update_mask: None,
                    update_transforms: vec![],
                    current_document: None,
                    operation: None,
                }),
        )
        .await
        .unwrap();
    expect_eq!(batch_resp.status.len(), 4);
    expect_eq!(batch_resp.status[0].code, Code::Ok as i32);
    expect_eq!(batch_resp.status[1].code, Code::InvalidArgument as i32);
    expect_eq!(batch_resp.status[2].code, Code::InvalidArgument as i32);
    expect_eq!(batch_resp.status[3].code, Code::Unimplemented as i32);

    assert_status_code(
        db.commit_writes(WriteBatchSpec::new().raw(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document: None,
            operation: None,
        }))
        .await,
        Code::Unimplemented,
    );
}

#[gtest]
#[tokio::test]
async fn structured_query_order_by_and_find_nearest_measures_thresholds_and_tie_breaking() {
    let mut db = TestEnv::new().build_db();

    // 1. Cross-type orderBy("val"): Array < Vector < Map, and intra-vector (len, elements).
    db.put("order/map", fields!({ "val": { "a": 1i64 } }))
        .await
        .unwrap();
    db.put("order/vec3", fields!({ "val": vec_val([1.0, 2.0, 0.0]) }))
        .await
        .unwrap();
    db.put("order/vec2_b", fields!({ "val": vec_val([1.0, 3.0]) }))
        .await
        .unwrap();
    db.put("order/vec2_a", fields!({ "val": vec_val([1.0, 2.0]) }))
        .await
        .unwrap();
    db.put("order/arr", fields!({ "val": [1.0f64, 2.0f64] }))
        .await
        .unwrap();

    let ordered = db
        .query(QuerySpec::collection("order").order_asc("val"))
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ordered.documents),
        vec!["arr", "vec2_a", "vec2_b", "vec3", "map"]
    );

    // 2. EUCLIDEAN, COSINE, DOT_PRODUCT + distanceThreshold inclusivity + pre-filters.
    db.put(
        "vecs/d1",
        fields!({ "emb": vec_val([1.0, 0.0]), "cat": "a", "price": 10i64 }),
    )
    .await
    .unwrap();
    db.put(
        "vecs/d2",
        fields!({ "emb": vec_val([1.0, 1.0]), "cat": "a", "price": 5i64 }),
    )
    .await
    .unwrap();
    db.put(
        "vecs/d3",
        fields!({ "emb": vec_val([0.0, 1.0]), "cat": "a", "price": 20i64 }),
    )
    .await
    .unwrap();
    // Ignored by findNearest: missing field, plain array, string, 3D mismatched vector
    db.put("vecs/missing", fields!({ "cat": "a", "price": 1i64 }))
        .await
        .unwrap();
    db.put(
        "vecs/arr",
        fields!({ "emb": [1.0f64, 0.0f64], "cat": "a", "price": 2i64 }),
    )
    .await
    .unwrap();
    db.put(
        "vecs/str",
        fields!({ "emb": "nope", "cat": "a", "price": 3i64 }),
    )
    .await
    .unwrap();
    db.put(
        "vecs/dim3",
        fields!({ "emb": vec_val([1.0, 0.0, 0.0]), "cat": "a", "price": 4i64 }),
    )
    .await
    .unwrap();
    // Zero vector in `cat: "zero"`: skipped when `cat == "a"` pre-filter is active, errors when
    // unfiltered!
    db.put(
        "vecs/zero",
        fields!({ "emb": vec_val([0.0, 0.0]), "cat": "zero", "price": 50i64 }),
    )
    .await
    .unwrap();

    // EUCLIDEAN with inclusive threshold `1.0` -> d1 (dist 0.0) and d2 (dist 1.0), excludes d3
    // (sqrt(2) ~= 1.414)
    let euc = db
        .query(
            QuerySpec::collection("vecs")
                .where_filter(field_filter("cat", Operator::Equal, "a"))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    10,
                    Some("dist"),
                    Some(1.0),
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&euc.documents), vec!["d1", "d2"]);
    expect_eq!(euc.documents[0].fields["dist"].as_double(), Some(0.0));
    expect_eq!(euc.documents[1].fields["dist"].as_double(), Some(1.0));

    // Explicit `order_by("price")` pre-filter omits docs without `"price"` and acts as secondary
    // tie-breaker when distances tie.
    db.put(
        "vecs/no_price",
        fields!({ "emb": vec_val([1.0, 0.0]), "cat": "a" }),
    )
    .await
    .unwrap();
    let prefiltered_order = db
        .query(
            QuerySpec::collection("vecs")
                .where_filter(field_filter("cat", Operator::Equal, "a"))
                .order_asc("price")
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    10,
                    None,
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&prefiltered_order.documents),
        vec!["d1", "d2", "d3"]
    );
    db.delete("vecs/no_price", None).await.unwrap();

    // Inequality + explicit `order_by` secondary tie-breaking when distances tie (`price ASC`,
    // `__name__ ASC` on both Standard `ASC` and Enterprise).
    for (id, price, emb) in [
        ("a", 50i64, [1.0, 0.0]),
        ("b", 10i64, [10.0, 0.0]),
        ("c", 20i64, [1.0, 0.0]),
        ("d", 20i64, [1.0, 0.0]),
    ] {
        db.put(
            &format!("tie_ob/{id}"),
            fields!({ "price": price, "emb": vec_val(emb) }),
        )
        .await
        .unwrap();
    }
    let std_tie = db
        .query(
            QuerySpec::collection("tie_ob")
                .where_filter(field_filter("price", Operator::LessThan, 100i64))
                .order_asc("price")
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([0.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    10,
                    None,
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&std_tie.documents), vec!["c", "d", "a", "b"]);

    // COSINE with `cat == "a"` skips `vecs/zero` safely; threshold 0.5 includes d1 (0.0) and d2
    // (1 - 1/sqrt(2) ~= 0.2929), excludes d3 (1.0).
    let cos = db
        .query(
            QuerySpec::collection("vecs")
                .where_filter(field_filter("cat", Operator::Equal, "a"))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Cosine,
                    10,
                    Some("dist"),
                    Some(0.5),
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&cos.documents), vec!["d1", "d2"]);
    expect_eq!(cos.documents[0].fields["dist"].as_double(), Some(0.0));

    // Unfiltered COSINE scans `vecs/zero` -> FailedPrecondition on Standard Edition!
    assert_status_code(
        db.query(
            QuerySpec::collection("vecs").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                10,
                None,
                None,
            )),
        )
        .await,
        Code::FailedPrecondition,
    );

    // DOT_PRODUCT descending similarity with inclusive threshold `2.0` -> [2.0, 1.0] (dist 3.0)
    // and [1.0, 1.0] (dist 2.0), excludes [1.0, 0.0] (dist 1.0).
    db.put("dots/a", fields!({ "emb": vec_val([2.0, 1.0]) }))
        .await
        .unwrap();
    db.put("dots/b", fields!({ "emb": vec_val([1.0, 1.0]) }))
        .await
        .unwrap();
    db.put("dots/c", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    let dot = db
        .query(
            QuerySpec::collection("dots").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 1.0]),
                find_nearest::DistanceMeasure::DotProduct,
                10,
                Some("dist"),
                Some(2.0),
            )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&dot.documents), vec!["a", "b"]);
    expect_eq!(dot.documents[0].fields["dist"].as_double(), Some(3.0));
    expect_eq!(dot.documents[1].fields["dist"].as_double(), Some(2.0));

    // 3. Unscaled IEEE 754 Euclidean math & Standard non-finite skipping vs Enterprise +/-Infinity.
    db.put("scale/small", fields!({ "emb": vec_val([1e-200, 0.0]) }))
        .await
        .unwrap();
    db.put("scale/mid", fields!({ "emb": vec_val([1e100, 0.0]) }))
        .await
        .unwrap();
    db.put(
        "scale/inf",
        fields!({ "emb": vec_val([f64::INFINITY, 0.0]) }),
    )
    .await
    .unwrap();
    let std_euc = db
        .query(
            QuerySpec::collection("scale").find_nearest(make_find_nearest(
                "emb",
                vec_val([0.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                10,
                Some("dist"),
                None,
            )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&std_euc.documents), vec!["small", "mid"]);
    expect_eq!(std_euc.documents[0].fields["dist"].as_double(), Some(0.0));
    expect_eq!(std_euc.documents[1].fields["dist"].as_double(), Some(1e100));

    db.put("inf_q/finite", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    let std_inf_q = db
        .query(
            QuerySpec::collection("inf_q").find_nearest(make_find_nearest(
                "emb",
                vec_val([f64::INFINITY, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                10,
                Some("dist"),
                None,
            )),
        )
        .await
        .unwrap();
    assert!(std_inf_q.documents.is_empty());

    // Enterprise Edition: evaluates 1e200 (+Infinity) and +Infinity candidates, returns
    // InvalidArgument on unfiltered COSINE [0, 0], and tie-breaks `orderBy(price)` by inequality
    // filter `price ASC` before `__name__`.
    let mut ent_db = TestEnv::enterprise().build_db();
    for (id, vec) in [
        ("small", [1e-200, 0.0]),
        ("mid", [1e100, 0.0]),
        ("large", [1e200, 0.0]),
        ("inf", [f64::INFINITY, 0.0]),
    ] {
        ent_db
            .put(&format!("scale/{id}"), fields!({ "emb": vec_val(vec) }))
            .await
            .unwrap();
    }
    let ent_euc = ent_db
        .query(
            QuerySpec::collection("scale").find_nearest(make_find_nearest(
                "emb",
                vec_val([0.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                10,
                Some("dist"),
                None,
            )),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ent_euc.documents),
        vec!["small", "mid", "inf", "large"]
    );
    expect_eq!(ent_euc.documents[0].fields["dist"].as_double(), Some(0.0));
    expect_eq!(ent_euc.documents[1].fields["dist"].as_double(), Some(1e100));
    expect_eq!(
        ent_euc.documents[2].fields["dist"].as_double(),
        Some(f64::INFINITY)
    );
    expect_eq!(
        ent_euc.documents[3].fields["dist"].as_double(),
        Some(f64::INFINITY)
    );

    ent_db
        .put("inf_q/finite", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    let ent_inf_q = ent_db
        .query(
            QuerySpec::collection("inf_q").find_nearest(make_find_nearest(
                "emb",
                vec_val([f64::INFINITY, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                10,
                Some("dist"),
                None,
            )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_inf_q.documents), vec!["finite"]);
    expect_eq!(
        ent_inf_q.documents[0].fields["dist"].as_double(),
        Some(f64::INFINITY)
    );

    ent_db
        .put("ent_zero/z", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();
    assert_status_code(
        ent_db
            .query(
                QuerySpec::collection("ent_zero").find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Cosine,
                    10,
                    None,
                    None,
                )),
            )
            .await,
        Code::InvalidArgument,
    );

    for (id, price, emb) in [
        ("a", 50i64, [1.0, 0.0]),
        ("b", 10i64, [10.0, 0.0]),
        ("c", 20i64, [1.0, 0.0]),
        ("d", 20i64, [1.0, 0.0]),
    ] {
        ent_db
            .put(
                &format!("tie_ob/{id}"),
                fields!({ "price": price, "emb": vec_val(emb) }),
            )
            .await
            .unwrap();
    }
    let ent_tie = ent_db
        .query(
            QuerySpec::collection("tie_ob")
                .where_filter(field_filter("price", Operator::LessThan, 100i64))
                .order_desc("price")
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([0.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    10,
                    None,
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_tie.documents), vec!["c", "d", "a", "b"]);

    // 4. Collection-group `all_descendants` segment-wise `__name__` tie-breaking:
    //    `parents/doc/cg/x` < `parents/doc-1/cg/x` and `cg/a/cg/x` < `cg/z`
    let tie_paths = [
        "cg/z",
        "parents/doc-1/cg/x",
        "parents/doc/cg/x",
        "cg/a/cg/x",
    ];
    for p in tie_paths {
        db.put(p, fields!({ "tie": vec_val([1.0, 0.0]) }))
            .await
            .unwrap();
    }
    let cg_res = db
        .query(
            QuerySpec::collection_group("cg").find_nearest(make_find_nearest(
                "tie",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                10,
                None,
                None,
            )),
        )
        .await
        .unwrap();
    let cg_names: Vec<&str> = cg_res.documents.iter().map(|d| d.name.as_str()).collect();
    expect_eq!(
        cg_names,
        vec![
            doc_path("cg/a/cg/x"),
            doc_path("cg/z"),
            doc_path("parents/doc/cg/x"),
            doc_path("parents/doc-1/cg/x"),
        ]
    );
}

#[gtest]
#[tokio::test]
async fn structured_query_find_nearest_projection_shadowing_aggregations_and_validation() {
    let mut db = TestEnv::new().build_db();

    db.put(
        "proj/p1",
        fields!({
            "title": "one",
            "emb": vec_val([1.0, 0.0]),
            "meta": { "author": "alice", "rank": 5i64 },
        }),
    )
    .await
    .unwrap();
    db.put(
        "proj/p2",
        fields!({
            "title": "two",
            "emb": vec_val([1.0, 1.0]),
            "meta": { "author": "bob", "rank": 7i64 },
        }),
    )
    .await
    .unwrap();

    // 1. select("title", "meta") when `distance_result_field = "meta"` returns computed distance;
    //    stored document remains untouched on subsequent `get`.
    let sel_dist = db
        .query(
            QuerySpec::collection("proj")
                .select(["title", "meta"])
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    2,
                    Some("meta"),
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(sel_dist.documents.len(), 2);
    assert_doc_wire_eq(
        &sel_dist.documents[0],
        "proj/p1",
        &fields!({ "title": "one", "meta": 0.0f64 }),
    );
    assert_doc_wire_eq(
        &sel_dist.documents[1],
        "proj/p2",
        &fields!({ "title": "two", "meta": 1.0f64 }),
    );

    // Stored `proj/p1` was not mutated by `distance_result_field`.
    assert_doc_wire_eq(
        &db.get("proj/p1").await.unwrap(),
        "proj/p1",
        &fields!({
            "title": "one",
            "emb": vec_val([1.0, 0.0]),
            "meta": { "author": "alice", "rank": 5i64 },
        }),
    );

    // 2. select("meta.author") when `distance_result_field = "meta"` shadows stored `meta.author`
    //    -> empty fields!
    let shadowed = db
        .query(
            QuerySpec::collection("proj")
                .select(["meta.author"])
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    1,
                    Some("meta"),
                    None,
                )),
        )
        .await
        .unwrap();
    assert_doc_wire_eq(&shadowed.documents[0], "proj/p1", &fields!({}));

    // 3. select("title") masks out `distance_result_field`; select("__name__") returns empty
    //    fields.
    let masked = db
        .query(
            QuerySpec::collection("proj")
                .select(["title"])
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    1,
                    Some("meta"),
                    None,
                )),
        )
        .await
        .unwrap();
    assert_doc_wire_eq(
        &masked.documents[0],
        "proj/p1",
        &fields!({ "title": "one" }),
    );

    let name_only = db
        .query(
            QuerySpec::collection("proj")
                .select(["__name__"])
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    1,
                    Some("meta"),
                    None,
                )),
        )
        .await
        .unwrap();
    assert_doc_wire_eq(&name_only.documents[0], "proj/p1", &fields!({}));

    // 4. RunAggregationQuery (COUNT, SUM, AVG) with `distance_result_field = "meta"` shadowing
    //    stored `meta.rank` (SUM returns 0, AVG returns NULL) and aggregating `meta` directly
    //    (distances 0.0 + 1.0 -> SUM 1.0, AVG 0.5).
    let agg = db
        .aggregate(
            QuerySpec::collection("proj").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                2,
                Some("meta"),
                None,
            )),
            AggregationSpec::new()
                .count("n")
                .sum("sum_dist", "meta")
                .avg("avg_dist", "meta")
                .sum("sum_shadowed", "meta.rank")
                .avg("avg_shadowed", "meta.rank"),
        )
        .await
        .unwrap();
    expect_eq!(agg.fields.get("n"), Some(&Value::integer(2)));
    expect_eq!(agg.fields.get("sum_dist"), Some(&Value::double(1.0)));
    expect_eq!(agg.fields.get("avg_dist"), Some(&Value::double(0.5)));
    assert!(agg.fields["sum_shadowed"].storage_eq(&Value::integer(0)));
    expect_eq!(agg.fields.get("avg_shadowed"), Some(&Value::null()));

    // Enterprise Edition: empty numeric SUM returns NULL_VALUE rather than integer 0.
    let mut ent_db = TestEnv::enterprise().build_db();
    ent_db
        .put(
            "proj/p1",
            fields!({
                "title": "one",
                "emb": vec_val([1.0, 0.0]),
                "meta": { "author": "alice", "rank": 5i64 },
            }),
        )
        .await
        .unwrap();
    ent_db
        .put(
            "proj/p2",
            fields!({
                "title": "two",
                "emb": vec_val([1.0, 1.0]),
                "meta": { "author": "bob", "rank": 7i64 },
            }),
        )
        .await
        .unwrap();
    let ent_agg = ent_db
        .aggregate(
            QuerySpec::collection("proj").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                2,
                Some("meta"),
                None,
            )),
            AggregationSpec::new()
                .count("n")
                .sum("sum_dist", "meta")
                .avg("avg_dist", "meta")
                .sum("sum_shadowed", "meta.rank")
                .avg("avg_shadowed", "meta.rank"),
        )
        .await
        .unwrap();
    expect_eq!(ent_agg.fields.get("n"), Some(&Value::integer(2)));
    expect_eq!(ent_agg.fields.get("sum_dist"), Some(&Value::double(1.0)));
    expect_eq!(ent_agg.fields.get("avg_dist"), Some(&Value::double(0.5)));
    expect_eq!(ent_agg.fields.get("sum_shadowed"), Some(&Value::null()));
    expect_eq!(ent_agg.fields.get("avg_shadowed"), Some(&Value::null()));

    // 5. Query validation errors -> Code::InvalidArgument.
    let valid_fn = make_find_nearest(
        "emb",
        vec_val([1.0, 0.0]),
        find_nearest::DistanceMeasure::Euclidean,
        5,
        None,
        None,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("proj")
                .limit(1)
                .find_nearest(valid_fn.clone()),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("proj")
                .offset(1)
                .find_nearest(valid_fn.clone()),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("proj")
                .start_at(vec![val!(1i64)], true)
                .find_nearest(valid_fn.clone()),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("proj")
                .end_at(vec![val!(1i64)], false)
                .find_nearest(valid_fn),
        )
        .await,
        Code::InvalidArgument,
    );

    for bad_limit in [0, 1001] {
        assert_status_code(
            db.query(
                QuerySpec::collection("proj").find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    bad_limit,
                    None,
                    None,
                )),
            )
            .await,
            Code::InvalidArgument,
        );
    }

    // Missing limit
    let mut missing_limit = make_find_nearest(
        "emb",
        vec_val([1.0, 0.0]),
        find_nearest::DistanceMeasure::Euclidean,
        1,
        None,
        None,
    );
    missing_limit.limit = None;
    assert_status_code(
        db.query(QuerySpec::collection("proj").find_nearest(missing_limit))
            .await,
        Code::InvalidArgument,
    );

    // NaN threshold
    assert_status_code(
        db.query(
            QuerySpec::collection("proj").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Euclidean,
                5,
                None,
                Some(f64::NAN),
            )),
        )
        .await,
        Code::InvalidArgument,
    );

    // Zero-norm COSINE query vector: FailedPrecondition on Standard, InvalidArgument on Enterprise
    assert_status_code(
        db.query(
            QuerySpec::collection("proj").find_nearest(make_find_nearest(
                "emb",
                vec_val([0.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                5,
                None,
                None,
            )),
        )
        .await,
        Code::FailedPrecondition,
    );
    assert_status_code(
        ent_db
            .query(
                QuerySpec::collection("proj").find_nearest(make_find_nearest(
                    "emb",
                    vec_val([0.0, 0.0]),
                    find_nearest::DistanceMeasure::Cosine,
                    5,
                    None,
                    None,
                )),
            )
            .await,
        Code::InvalidArgument,
    );

    // Reserved vector_field and distance_result_field
    for reserved in ["__name__", "`__name__`", "emb.__type__"] {
        assert_status_code(
            db.query(
                QuerySpec::collection("proj").find_nearest(make_find_nearest(
                    reserved,
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    5,
                    None,
                    None,
                )),
            )
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(
                QuerySpec::collection("proj").find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    5,
                    Some(reserved),
                    None,
                )),
            )
            .await,
            Code::InvalidArgument,
        );
    }

    // 6. Non-vector oversized cursor rejection: "Cursor has too many values."
    let oversized_cursor = db
        .query(
            QuerySpec::collection("proj")
                .order_asc("title")
                .start_at(vec![val!("a"), val!("b"), val!("c")], true),
        )
        .await;
    let status = assert_status_code(oversized_cursor, Code::InvalidArgument);
    assert!(
        status.message().contains("Cursor has too many values."),
        "unexpected error message: {}",
        status.message()
    );
}

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
        let mut write_d1 = ContentionProbe::spawn_op(async move {
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
#[tokio::test]
async fn listen_find_nearest_updates_threshold_suppression_and_dynamic_nan_termination() {
    // (a) Initial catch-up evaluates `findNearest` (threshold/limit/distance injection), while
    //     subsequent writes stream incrementally as base-query updates matching Cloud Firestore.
    let mut db = TestEnv::new().build_db();
    db.put(
        "live/d1",
        fields!({ "emb": vec_val([1.0, 0.5]), "label": "one" }),
    )
    .await
    .unwrap();
    db.put(
        "live/d_init_far",
        fields!({ "emb": vec_val([0.0, 1.0]), "label": "init_far" }),
    )
    .await
    .unwrap();

    let (mut handle, init) = db
        .listen_query(
            QuerySpec::collection("live").find_nearest(make_find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                find_nearest::DistanceMeasure::Cosine,
                2,
                Some("dist"),
                Some(0.2),
            )),
        )
        .await
        .unwrap();
    expect_eq!(init.was_reset, true);
    expect_eq!(short_ids(&init.changed_docs), vec!["d1"]);
    assert!(init.changed_docs[0].fields["dist"].as_double().is_some());

    for (id, vec, label) in [
        ("d_far", [0.0, 1.0], "far"),
        ("d2", [1.0, 0.0], "two"),
        ("d_zero", [0.0, 0.0], "zero"),
    ] {
        db.put(
            &format!("live/{id}"),
            fields!({ "emb": vec_val(vec), "label": label }),
        )
        .await
        .unwrap();
        let batch = handle.next_batch().await.unwrap();
        expect_eq!(batch.was_reset, false);
        expect_eq!(short_ids(&batch.changed_docs), vec![id]);
        assert!(!batch.changed_docs[0].fields.contains_key("dist"));
    }

    // Updating an already-tracked document emits an incremental delta without `dist`.
    db.put(
        "live/d1",
        fields!({ "emb": vec_val([1.0, 0.5]), "label": "one-updated" }),
    )
    .await
    .unwrap();
    let delta = handle.next_batch().await.unwrap();
    expect_eq!(delta.was_reset, false);
    expect_eq!(short_ids(&delta.changed_docs), vec!["d1"]);
    assert!(!delta.changed_docs[0].fields.contains_key("dist"));

    // Deleting a tracked document emits an incremental DocumentDelete delta.
    db.delete("live/d2", None).await.unwrap();
    let del_batch = handle.next_batch().await.unwrap();
    expect_eq!(del_batch.was_reset, false);
    expect_eq!(del_batch.deleted_docs, vec![doc_path("live/d2")]);
    handle.close().await;

    // Prefilter exit on a findNearest listener emits DocumentRemove without a collection Reset.
    let (mut pf_handle, pf_init) = db
        .listen_query(
            QuerySpec::collection("live_pf")
                .where_filter(field_filter("cat", Operator::Equal, "keep"))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    2,
                    Some("dist"),
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(pf_init.was_reset, true);
    assert!(pf_init.changed_docs.is_empty());

    db.put(
        "live_pf/p1",
        fields!({ "cat": "keep", "emb": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    let pf_add = pf_handle.next_batch().await.unwrap();
    expect_eq!(pf_add.was_reset, false);
    expect_eq!(short_ids(&pf_add.changed_docs), vec!["p1"]);

    db.put(
        "live_pf/p1",
        fields!({ "cat": "skip", "emb": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    let pf_rm = pf_handle.next_batch().await.unwrap();
    expect_eq!(pf_rm.was_reset, false);
    expect_eq!(pf_rm.removed_docs, vec![doc_path("live_pf/p1")]);
    pf_handle.close().await;

    // BroadcastStreamRecvError::Lagged resets the findNearest target with k-NN filtering + dist
    // projection intact rather than falling back to an unfiltered collection scan.
    db.put("lag/d1", fields!({ "emb": vec_val([1.0, 0.5]) }))
        .await
        .unwrap();
    db.put("lag/d_init_far", fields!({ "emb": vec_val([0.0, 1.0]) }))
        .await
        .unwrap();
    let (_, lag_sq) = QuerySpec::collection("lag")
        .find_nearest(make_find_nearest(
            "emb",
            vec_val([1.0, 0.0]),
            find_nearest::DistanceMeasure::Cosine,
            2,
            Some("dist"),
            Some(0.2),
        ))
        .into_parts();
    let (lag_tx, lag_rx) = tokio::sync::mpsc::channel(16);
    let mut raw_lag_stream = db
        .raw_grpc()
        .listen(tokio_stream::wrappers::ReceiverStream::new(lag_rx))
        .await
        .unwrap()
        .into_inner();
    lag_tx
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(lag_sq)),
                })),
                resume_type: None,
            })),
        })
        .await
        .unwrap();
    while let Some(frame) = raw_lag_stream.message().await.unwrap() {
        if let Some(googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(
            tc,
        )) = frame.response_type
            && tc.target_change_type
                == googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange
                    as i32
            && tc.target_ids.is_empty()
        {
            break;
        }
    }

    let pad = "x".repeat(2048);
    for i in 0..1150 {
        let fields = if i < 100 {
            fields!({ "emb": vec_val([0.0, 1.0]), "pad": pad.as_str() })
        } else {
            fields!({ "emb": vec_val([0.0, 1.0]) })
        };
        db.put(&format!("lag/far_{i:04}"), fields).await.unwrap();
    }
    db.put("lag/d_close", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();

    let mut saw_reset = false;
    let mut reset_docs = Vec::new();
    while let Some(frame) = raw_lag_stream.message().await.unwrap() {
        match frame.response_type {
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(tc),
            ) => {
                let kind =
                    googleapis::google::firestore::v1::target_change::TargetChangeType::try_from(
                        tc.target_change_type,
                    );
                if kind
                    == Ok(
                        googleapis::google::firestore::v1::target_change::TargetChangeType::Reset,
                    )
                {
                    saw_reset = true;
                    reset_docs.clear();
                } else if saw_reset
                    && kind
                        == Ok(
                            googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange,
                        )
                    && tc.target_ids.is_empty()
                {
                    break;
                }
            }
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::DocumentChange(
                    dc,
                ),
            ) if saw_reset => {
                if let Some(doc) = dc.document {
                    reset_docs.push(doc);
                }
            }
            _ => {}
        }
    }
    assert!(
        saw_reset,
        "expected Lagged listener to emit TargetChange::Reset"
    );
    expect_eq!(short_ids(&reset_docs), vec!["d_close", "d1"]);
    for doc in &reset_docs {
        assert!(doc.fields.contains_key("dist"));
    }

    // Confirm no stale pre-reset `far_*` DocumentChange frames leak after post-lag Reset, and a
    // follow-up write delivers only `lag/after_reset` without any spurious `far_*` events.
    db.put("lag/after_reset", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    let mut followup_docs = Vec::new();
    while let Some(frame) = tokio::time::timeout(Duration::from_secs(2), raw_lag_stream.message())
        .await
        .expect("timed out waiting for post-lag follow-up listen batch")
        .unwrap()
    {
        match frame.response_type {
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::TargetChange(tc),
            ) => {
                let kind =
                    googleapis::google::firestore::v1::target_change::TargetChangeType::try_from(
                        tc.target_change_type,
                    );
                if kind == Ok(
                    googleapis::google::firestore::v1::target_change::TargetChangeType::NoChange,
                ) && tc.target_ids.is_empty()
                {
                    break;
                }
            }
            Some(
                googleapis::google::firestore::v1::listen_response::ResponseType::DocumentChange(
                    dc,
                ),
            ) => {
                if let Some(doc) = dc.document {
                    assert!(
                        !doc.name.contains("/lag/far_"),
                        "stale far_* document leaked after lag recovery: {}",
                        doc.name
                    );
                    followup_docs.push(doc);
                }
            }
            _ => {}
        }
    }
    expect_eq!(short_ids(&followup_docs), vec!["after_reset"]);
    drop(lag_tx);

    // (b) Opening a Listen query on a collection that already fails COSINE `[0, 0]` delivers the
    //     edition-specific error via `send_err` and terminates the stream cleanly.
    db.put("err_listen/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();
    let (_, sq) = QuerySpec::collection("err_listen")
        .find_nearest(make_find_nearest(
            "emb",
            vec_val([1.0, 0.0]),
            find_nearest::DistanceMeasure::Cosine,
            2,
            None,
            None,
        ))
        .into_parts();
    let mut err_handle = db.open_raw_listen().await.unwrap();
    err_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(sq.clone())),
                })),
                resume_type: None,
            })),
        })
        .await;
    let err = err_handle
        .next_batch()
        .await
        .expect_err("initial COSINE [0, 0] must surface on Standard Listen stream");
    expect_eq!(err.code(), Code::FailedPrecondition);

    db.put("err_listen/after", fields!({ "emb": vec_val([1.0, 0.0]) }))
        .await
        .unwrap();
    expect_that!(err_handle.next_raw().await, none());
    err_handle.close().await;

    // Enterprise Edition Listen delivers InvalidArgument on initial COSINE `[0, 0]`.
    let mut ent_db = TestEnv::enterprise().build_db();
    ent_db
        .put("err_listen/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();
    let mut ent_err_handle = ent_db.open_raw_listen().await.unwrap();
    ent_err_handle
        .send(ListenRequest {
            database: DATABASE_ROOT.to_string(),
            labels: HashMap::new(),
            target_change: Some(listen_request::TargetChange::AddTarget(Target {
                target_id: 1,
                once: false,
                expected_count: None,
                target_type: Some(target::TargetType::Query(target::QueryTarget {
                    parent:     DOCUMENTS_ROOT.to_string(),
                    query_type: Some(query_target::QueryType::StructuredQuery(sq)),
                })),
                resume_type: None,
            })),
        })
        .await;
    let ent_err = ent_err_handle
        .next_batch()
        .await
        .expect_err("initial COSINE [0, 0] must surface on Enterprise Listen stream");
    expect_eq!(ent_err.code(), Code::InvalidArgument);
    ent_err_handle.close().await;
}

#[gtest]
#[tokio::test]
async fn enterprise_pipeline_vector_functions_find_nearest_and_chunked_streaming() {
    let mut db = TestEnv::enterprise().build_db();

    db.put(
        "pvec/v1",
        fields!({ "emb": vec_val([1.0, 0.0]), "alt": vec_val([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put(
        "pvec/v2",
        fields!({ "emb": vec_val([0.0, 1.0]), "alt": vec_val([1.0, 0.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put("pvec/null_doc", fields!({ "emb": null, "alt": null }))
        .await
        .unwrap();
    db.put("pvec/bad_type", fields!({ "emb": "not_a_vec" }))
        .await
        .unwrap();
    db.put("pvec/zero", fields!({ "emb": vec_val([0.0, 0.0]) }))
        .await
        .unwrap();

    // 1a. Direct vector expressions + null propagation (excluding `bad_type` and `zero`, which
    //     raise InvalidArgument unless wrapped in `is_error`).
    let expr_out = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "and",
                    vec![
                        fn_val(
                            "not_equal",
                            vec![
                                field_ref_val("__name__"),
                                Value::reference(doc_path("pvec/bad_type")),
                            ],
                        ),
                        fn_val(
                            "not_equal",
                            vec![
                                field_ref_val("__name__"),
                                Value::reference(doc_path("pvec/zero")),
                            ],
                        ),
                    ],
                ))
                .add_fields(HashMap::from([
                    (
                        "cos".to_string(),
                        fn_val(
                            "cosine_distance",
                            vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                        ),
                    ),
                    (
                        "euc".to_string(),
                        fn_val(
                            "euclidean_distance",
                            vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                        ),
                    ),
                    (
                        "dot".to_string(),
                        fn_val(
                            "dot_product",
                            vec![field_ref_val("emb"), vec_val([2.0, 3.0])],
                        ),
                    ),
                    (
                        "vlen".to_string(),
                        fn_val("vector_length", vec![field_ref_val("emb")]),
                    ),
                ]))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();

    let by_id: HashMap<&str, &Document> = expr_out
        .results
        .iter()
        .map(|d| (d.name.rsplit('/').next().unwrap(), d))
        .collect();

    // `v1`: matching 2D vectors -> cos=0, euc=0, dot=2, vlen=2
    expect_eq!(by_id["v1"].fields["cos"].as_double(), Some(0.0));
    expect_eq!(by_id["v1"].fields["euc"].as_double(), Some(0.0));
    expect_eq!(by_id["v1"].fields["dot"].as_double(), Some(2.0));
    expect_eq!(by_id["v1"].fields["vlen"].as_integer(), Some(2));

    // `v2`: [0.0, 1.0] vs [1.0, 0.0] -> cos=1.0, vlen=2
    expect_eq!(by_id["v2"].fields["cos"].as_double(), Some(1.0));
    expect_eq!(by_id["v2"].fields["vlen"].as_integer(), Some(2));

    // `null_doc`: missing/null -> NullValue without error
    expect_eq!(by_id["null_doc"].fields["cos"], Value::null());
    expect_eq!(by_id["null_doc"].fields["euc"], Value::null());
    expect_eq!(by_id["null_doc"].fields["dot"], Value::null());
    expect_eq!(by_id["null_doc"].fields["vlen"], Value::null());

    // 1b. `is_error(...)` catching InvalidArgument (dimension mismatch, non-vector type, [0, 0]
    //     cosine NaN) while propagating `null` safely as `false`.
    let err_out = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .add_fields(HashMap::from([
                    (
                        "alt_cos_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val(
                                "cosine_distance",
                                vec![field_ref_val("emb"), field_ref_val("alt")],
                            )],
                        ),
                    ),
                    (
                        "self_cos_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val(
                                "cosine_distance",
                                vec![field_ref_val("emb"), vec_val([1.0, 0.0])],
                            )],
                        ),
                    ),
                    (
                        "vlen_err".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val("vector_length", vec![field_ref_val("emb")])],
                        ),
                    ),
                ]))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();

    let err_by_id: HashMap<&str, &Document> = err_out
        .results
        .iter()
        .map(|d| (d.name.rsplit('/').next().unwrap(), d))
        .collect();

    expect_eq!(
        err_by_id["v1"].fields["alt_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(
        err_by_id["v1"].fields["self_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(err_by_id["v1"].fields["vlen_err"].as_boolean(), Some(false));
    // `v2`: `alt` is 3D -> dimension mismatch caught by `is_error`
    expect_eq!(
        err_by_id["v2"].fields["alt_cos_err"].as_boolean(),
        Some(true)
    );
    expect_eq!(err_by_id["v2"].fields["vlen_err"].as_boolean(), Some(false));
    // `null_doc`: `null` returns NullValue, not error
    expect_eq!(
        err_by_id["null_doc"].fields["self_cos_err"].as_boolean(),
        Some(false)
    );
    expect_eq!(
        err_by_id["null_doc"].fields["vlen_err"].as_boolean(),
        Some(false)
    );
    // `bad_type`: non-vector `emb` caught by `is_error`
    expect_eq!(
        err_by_id["bad_type"].fields["self_cos_err"].as_boolean(),
        Some(true)
    );
    expect_eq!(
        err_by_id["bad_type"].fields["vlen_err"].as_boolean(),
        Some(true)
    );
    // `zero`: `[0, 0]` cosine_distance -> `NaN` InvalidArgument caught by `is_error`
    expect_eq!(
        err_by_id["zero"].fields["self_cos_err"].as_boolean(),
        Some(true)
    );

    // Unknown function inside `is_error(...)` must STILL return Code::Unimplemented!
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").add_fields(HashMap::from([(
            "x".to_string(),
            fn_val(
                "is_error",
                vec![fn_val("totally_unknown_fn", vec![val!(1i64)])],
            ),
        )])))
        .await,
        Code::Unimplemented,
    );

    // 2. Pipeline `find_nearest` stage: MapValue and ArrayValue query vectors, all 3 measures,
    //    limit & distance_field, and invalid options/limits.
    let stage_euc = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "not_equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("pvec/zero")),
                    ],
                ))
                .find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    "euclidean",
                    Some(2),
                    Some("dist"),
                ),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_euc.results), vec!["v1", "v2"]);
    expect_eq!(stage_euc.results[0].fields["dist"].as_double(), Some(0.0));
    expect_eq!(
        stage_euc.results[1].fields["dist"].as_double(),
        Some(2.0_f64.sqrt())
    );

    // Valid VectorValue query vectors with dot_product and cosine
    let stage_dot = db
        .pipeline(PipelineSpec::collection("pvec").find_nearest(
            "emb",
            vec_val([3.0, 1.0]),
            "dot_product",
            Some(2),
            Some("score"),
        ))
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_dot.results), vec!["v1", "v2"]);
    expect_eq!(stage_dot.results[0].fields["score"].as_double(), Some(3.0));
    expect_eq!(stage_dot.results[1].fields["score"].as_double(), Some(1.0));

    let stage_cos = db
        .pipeline(
            PipelineSpec::collection("pvec")
                .where_expr(fn_val(
                    "not_equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("pvec/zero")),
                    ],
                ))
                .find_nearest("emb", vec_val([1.0, 0.0]), "cosine", Some(1), Some("cdist")),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&stage_cos.results), vec!["v1"]);
    expect_eq!(stage_cos.results[0].fields["cdist"].as_double(), Some(0.0));

    // Raw ArrayValue query vectors, StringValue field, and StringValue distance_field ->
    // InvalidArgument.
    for bad_vec in [val!([1.0f64, 0.0f64]), val!([1i64, 0i64])] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").find_nearest(
                "emb",
                bad_vec,
                "euclidean",
                Some(2),
                Some("dist"),
            ))
            .await,
            Code::InvalidArgument,
        );
    }
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").raw_stage(
            "find_nearest",
            vec![val!("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
        ))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.pipeline(PipelineSpec::collection("pvec").raw_stage_with_options(
            "find_nearest",
            vec![field_ref_val("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
            HashMap::from([("distance_field".to_string(), val!("dist"))]),
        ))
        .await,
        Code::InvalidArgument,
    );

    // Unknown options (`threshold` / `distance_threshold`) and invalid limit -> InvalidArgument.
    for opt_name in ["threshold", "distance_threshold"] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").raw_stage_with_options(
                "find_nearest",
                vec![field_ref_val("emb"), vec_val([1.0, 0.0]), val!("euclidean")],
                HashMap::from([(opt_name.to_string(), val!(0.5f64))]),
            ))
            .await,
            Code::InvalidArgument,
        );
    }
    for bad_limit in [0i64, 1001i64] {
        assert_status_code(
            db.pipeline(PipelineSpec::collection("pvec").find_nearest(
                "emb",
                vec_val([1.0, 0.0]),
                "euclidean",
                Some(bad_limit),
                None,
            ))
            .await,
            Code::InvalidArgument,
        );
    }

    // 3. Raw Tonic `execute_pipeline` chunking: (a) 200 x 2048D docs with NewTransaction -> every
    //    frame <= 1 MiB, transaction on frame 0 only, multi-frame stream, total 200 docs.
    let big_vec = vec_val((0..2048).map(|i| i as f64));
    for chunk_start in (0..200).step_by(50) {
        let mut batch = WriteBatchSpec::new();
        for idx in chunk_start..(chunk_start + 50) {
            batch = batch.put(
                &format!("chunk200/d{idx:03}"),
                HashMap::from([("emb".to_string(), big_vec.clone())]),
            );
        }
        db.batch_write(batch).await.unwrap();
    }

    let mut raw_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("chunk200").into_structured(),
            )),
            consistency_selector: Some(
                execute_pipeline_request::ConsistencySelector::NewTransaction(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(ReadWrite::default())),
                }),
            ),
        })
        .await
        .unwrap()
        .into_inner();

    let mut frame_idx = 0usize;
    let mut total_docs = 0usize;
    let mut first_txn = Vec::new();
    while let Some(frame) = raw_stream.message().await.unwrap() {
        let encoded_len = frame.encoded_len();
        assert!(
            encoded_len <= 1_048_576,
            "frame {frame_idx} exceeded 1 MiB budget: {encoded_len} bytes"
        );
        if frame_idx == 0 {
            assert!(
                !frame.transaction.is_empty(),
                "frame 0 must carry NewTransaction ID"
            );
            first_txn = frame.transaction;
        } else {
            assert!(
                frame.transaction.is_empty(),
                "frame {frame_idx} must NOT repeat transaction ID"
            );
        }
        total_docs += frame.results.len();
        frame_idx += 1;
    }
    assert!(
        frame_idx > 1,
        "200 x 2048D documents must span multiple <= 1 MiB frames, got {frame_idx}"
    );
    expect_eq!(total_docs, 200);
    db.rollback_raw(&first_txn).await.unwrap();

    // (b) Single oversized document (> 1 MiB) still makes progress in 1 frame.
    let mut oversized_fields = HashMap::new();
    for k in 0..60 {
        oversized_fields.insert(format!("v{k:02}"), big_vec.clone());
    }
    db.put("oversize/one", oversized_fields).await.unwrap();
    let mut over_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("oversize").into_structured(),
            )),
            consistency_selector: None,
        })
        .await
        .unwrap()
        .into_inner();
    let mut over_frames = 0usize;
    let mut over_docs = 0usize;
    while let Some(frame) = over_stream.message().await.unwrap() {
        over_frames += 1;
        over_docs += frame.results.len();
    }
    expect_eq!(over_frames, 1);
    expect_eq!(over_docs, 1);

    // (c) Empty pipeline result emits exactly 1 empty frame.
    let mut empty_stream = db
        .raw_grpc()
        .execute_pipeline(ExecutePipelineRequest {
            database: DATABASE_ROOT.to_string(),
            pipeline_type: Some(execute_pipeline_request::PipelineType::StructuredPipeline(
                PipelineSpec::collection("empty_col").into_structured(),
            )),
            consistency_selector: None,
        })
        .await
        .unwrap()
        .into_inner();
    let mut empty_frames = 0usize;
    while let Some(frame) = empty_stream.message().await.unwrap() {
        empty_frames += 1;
        expect_that!(frame.results.is_empty(), eq(true));
    }
    expect_eq!(empty_frames, 1);
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
            blocker_db
                .commit_writes(
                    WriteBatchSpec::new()
                        .put("locks/d2", fields!({ "emb": vec_val([0.0, 0.0]) }))
                        .put("locks/h1", fields!({ "v": 2i64 }))
                        .put("locks/h2", fields!({ "v": 2i64 }))
                        .put("locks/h3", fields!({ "v": 2i64 }))
                        .put("locks/h4", fields!({ "v": 2i64 })),
                )
                .await
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
#[tokio::test]
async fn run_query_find_nearest_tie_breaks_by_sorted_non_name_inequality_fields_before_document_name()
 {
    let mut db = TestEnv::new().build_db();
    for (id, a, b) in [
        ("tie_fn/a", 10i64, 1i64),
        ("tie_fn/b", 2i64, 20i64),
        ("tie_fn/c", 2i64, 5i64),
        ("tie_fn/d", 1i64, 99i64),
    ] {
        db.put(id, fields!({ "emb": vec_val([1.0, 0.0]), "a": a, "b": b }))
            .await
            .unwrap();
    }

    // All 4 docs tie at Euclidean distance 0.0; filter mentions `__name__`, then `b`, then `a`.
    // Tie-breaker order must exclude `__name__` from inequality prefix and sort remaining
    // inequality fields lexicographically (`a ASC, b ASC, __name__ ASC`) -> `d, c, b, a`.
    let docs = db
        .query(
            QuerySpec::collection("tie_fn")
                .where_filter(and_filters(vec![
                    field_filter(
                        "__name__",
                        Operator::GreaterThan,
                        Value {
                            value_type: Some(ValueType::ReferenceValue(doc_path("tie_fn/0"))),
                        },
                    ),
                    field_filter("b", Operator::LessThan, 100i64),
                    field_filter("a", Operator::LessThan, 100i64),
                ]))
                .find_nearest(make_find_nearest(
                    "emb",
                    vec_val([1.0, 0.0]),
                    find_nearest::DistanceMeasure::Euclidean,
                    4,
                    None,
                    None,
                )),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&docs.documents), vec!["d", "c", "b", "a"]);
}
