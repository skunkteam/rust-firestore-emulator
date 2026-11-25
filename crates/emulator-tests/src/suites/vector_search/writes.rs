use std::collections::HashMap;

use googleapis::{
    VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY, fields,
    google::firestore::v1::{Value, Write, value::ValueType},
    val,
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{
        WriteBatchSpec, doc_mask, tf_append_missing, tf_increment, tf_remove_all,
        tf_server_timestamp, vec_val,
    },
    env::TestEnv,
    matchers::{assert_doc_wire_eq, assert_status_code},
};

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
