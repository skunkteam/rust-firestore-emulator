use std::collections::HashMap;

use googletest::prelude::*;
use serde_json::json;
use time::macros::datetime;

use super::*;

#[gtest]
fn roundtrip_all_11_storable_value_variants() {
    let ts: Timestamp = datetime!(2026-11-10 15:30:45.123456789 UTC).into();
    let cases: Vec<(Value, serde_json::Value)> = vec![
        (Value::null(), json!({ "nullValue": null })),
        (Value::boolean(true), json!({ "booleanValue": true })),
        (
            Value::integer(i64::MIN),
            json!({ "integerValue": "-9223372036854775808" }),
        ),
        (
            Value::integer(i64::MAX),
            json!({ "integerValue": "9223372036854775807" }),
        ),
        (Value::double(42.5), json!({ "doubleValue": 42.5 })),
        (Value::double(f64::NAN), json!({ "doubleValue": "NaN" })),
        (
            Value::double(f64::INFINITY),
            json!({ "doubleValue": "Infinity" }),
        ),
        (
            Value::double(f64::NEG_INFINITY),
            json!({ "doubleValue": "-Infinity" }),
        ),
        (
            Value::timestamp(ts),
            json!({ "timestampValue": "2026-11-10T15:30:45.123456789Z" }),
        ),
        (Value::string("hello"), json!({ "stringValue": "hello" })),
        (
            Value::bytes(b"\x00\x01\x02\x03".as_slice()),
            json!({ "bytesValue": "AAECAw==" }),
        ),
        (
            Value::reference("projects/p/databases/(default)/documents/users/alice".to_string()),
            json!({ "referenceValue": "projects/p/databases/(default)/documents/users/alice" }),
        ),
        (
            Value {
                value_type: Some(ValueType::GeoPointValue(LatLng {
                    latitude:  52.3676,
                    longitude: 4.9041,
                })),
            },
            json!({ "geoPointValue": { "latitude": 52.3676, "longitude": 4.9041 } }),
        ),
        (
            Value::array(vec![Value::integer(1), Value::double(1.0)]),
            json!({
                "arrayValue": {
                    "values": [
                        { "integerValue": "1" },
                        { "doubleValue": 1.0 }
                    ]
                }
            }),
        ),
        (
            Value::map(HashMap::from([("nested".to_string(), Value::string("ok"))])),
            json!({
                "mapValue": {
                    "fields": {
                        "nested": { "stringValue": "ok" }
                    }
                }
            }),
        ),
    ];

    for (val, expected_json) in cases {
        let serialized = serde_json::to_value(&val).unwrap();
        expect_eq!(serialized, expected_json);
        let deserialized: Value = serde_json::from_value(serialized).unwrap();
        expect_true!(val.storage_eq(&deserialized));
    }
}

#[gtest]
fn negative_zero_and_flexible_protojson_inputs() {
    let neg_zero = Value::double(-0.0);
    let text = serde_json::to_string(&neg_zero).unwrap();
    expect_eq!(text, r#"{"doubleValue":-0.0}"#);
    let parsed: Value = serde_json::from_str(&text).unwrap();
    expect_true!(neg_zero.storage_eq(&parsed));

    // Unpadded and URL-safe base64, JSON integer for integerValue, "NULL_VALUE" for nullValue
    let from_int: Value = serde_json::from_str(r#"{"integerValue": 42}"#).unwrap();
    expect_true!(from_int.storage_eq(&Value::integer(42)));

    let from_null_str: Value = serde_json::from_str(r#"{"nullValue": "NULL_VALUE"}"#).unwrap();
    expect_true!(from_null_str.storage_eq(&Value::null()));

    let from_unpadded_b64: Value = serde_json::from_str(r#"{"bytesValue": "AAECAw"}"#).unwrap();
    expect_true!(from_unpadded_b64.storage_eq(&Value::bytes(b"\x00\x01\x02\x03".as_slice())));

    // Empty arrayValue / mapValue / geoPointValue objects
    let empty_arr: Value = serde_json::from_str(r#"{"arrayValue": {}}"#).unwrap();
    expect_true!(empty_arr.storage_eq(&Value::array(vec![])));

    let empty_map: Value = serde_json::from_str(r#"{"mapValue": {}}"#).unwrap();
    expect_true!(empty_map.storage_eq(&Value::map(HashMap::new())));
}

#[gtest]
fn rejects_invalid_value_envelopes() {
    expect_that!(serde_json::from_str::<Value>("{}"), err(anything()));
    expect_that!(
        serde_json::from_str::<Value>(r#"{"stringValue":"a","integerValue":"1"}"#),
        err(anything())
    );
    expect_that!(
        serde_json::from_str::<Value>(r#"{"unknownValue": 1}"#),
        err(anything())
    );
    let str_oob = serde_json::from_str::<Value>(r#"{"integerValue": "9223372036854775808"}"#)
        .unwrap_err()
        .to_string();
    expect_that!(
        str_oob.as_str(),
        contains_substring("invalid 64-bit integer '9223372036854775808'")
    );
    let u64_oob = serde_json::from_value::<Value>(json!({
        "integerValue": 9_223_372_036_854_775_808_u64
    }))
    .unwrap_err()
    .to_string();
    expect_that!(
        u64_oob.as_str(),
        contains_substring("integer 9223372036854775808 out of i64 bounds")
    );
}

#[gtest]
fn document_and_structured_query_roundtrip() {
    let ts: Timestamp = datetime!(2026-11-10 12:00:00 UTC).into();
    let doc = Document {
        name: "projects/p/databases/(default)/documents/users/alice".to_string(),
        fields: HashMap::from([("age".to_string(), Value::integer(30))]),
        create_time: Some(ts),
        update_time: Some(ts),
    };
    let doc_json = serde_json::to_value(&doc).unwrap();
    expect_eq!(
        doc_json,
        json!({
            "name": "projects/p/databases/(default)/documents/users/alice",
            "fields": { "age": { "integerValue": "30" } },
            "createTime": "2026-11-10T12:00:00Z",
            "updateTime": "2026-11-10T12:00:00Z"
        })
    );
    let parsed_doc: Document = serde_json::from_value(doc_json).unwrap();
    expect_eq!(parsed_doc.name, doc.name);
    expect_eq!(parsed_doc.create_time, doc.create_time);
    expect_true!(parsed_doc.fields["age"].storage_eq(&Value::integer(30)));

    // Phantom document omits fields, createTime, and updateTime
    let phantom = Document {
        name: "projects/p/databases/(default)/documents/rooms/r1".to_string(),
        ..Default::default()
    };
    expect_eq!(
        serde_json::to_value(&phantom).unwrap(),
        json!({ "name": "projects/p/databases/(default)/documents/rooms/r1" })
    );

    // StructuredQuery roundtrip + missing filter field rejection
    let rq_json = json!({
        "structuredQuery": {
            "from": [{ "collectionId": "users", "allDescendants": false }],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {
                            "fieldFilter": {
                                "field": { "fieldPath": "age" },
                                "op": "GREATER_THAN_OR_EQUAL",
                                "value": { "integerValue": "21" }
                            }
                        },
                        {
                            "unaryFilter": {
                                "op": "IS_NOT_NULL",
                                "field": { "fieldPath": "age" }
                            }
                        }
                    ]
                }
            },
            "orderBy": [{ "field": { "fieldPath": "age" }, "direction": "DESCENDING" }],
            "limit": 10
        }
    });
    let rq: RunQueryRequest = serde_json::from_value(rq_json.clone()).unwrap();
    expect_eq!(serde_json::to_value(&rq).unwrap(), rq_json);

    // Missing field inside fieldFilter must fail deserialization instead of panicking later
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({
            "where": { "fieldFilter": { "op": "EQUAL", "value": { "stringValue": "x" } } }
        })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({
            "where": {}
        })),
        err(anything())
    );
}

#[gtest]
fn structured_query_run_query_and_precondition_validation_edges() {
    use crate::google::firestore::v1::structured_query::FindNearest;

    // Negative offset and limit are rejected
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "offset": -1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "limit": -5 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "limit": 3_000_000_000_i64 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "limit": "not-an-i32" })),
        err(anything())
    );

    // findNearest decodes null -> None, object -> Some(Default::default()), rejects non-objects
    // and unknown fields, and fails serialization when Some.
    let sq_null_nearest: StructuredQuery =
        serde_json::from_value(json!({ "findNearest": null })).unwrap();
    expect_eq!(sq_null_nearest.find_nearest, None);

    let sq_obj_nearest: StructuredQuery = serde_json::from_value(json!({
        "findNearest": { "queryVector": { "mapValue": {} } }
    }))
    .unwrap();
    expect_eq!(sq_obj_nearest.find_nearest, Some(FindNearest::default()));

    for non_obj in [json!(true), json!("abc"), json!(1), json!([])] {
        expect_that!(
            serde_json::from_value::<StructuredQuery>(json!({ "findNearest": non_obj })),
            err(anything())
        );
    }
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({
            "findNearest": {},
            "unknown": 1
        })),
        err(anything())
    );
    let sq_with_nearest = StructuredQuery {
        find_nearest: Some(FindNearest::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&sq_with_nearest), err(anything()));

    // Conflicting oneof consistency_selector variants in RunQueryRequest are rejected
    for conflicting in [
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-10T12:00:00Z",
            "transaction": "AAEC"
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-10T12:00:00Z",
            "newTransaction": {}
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "transaction": "AAEC",
            "newTransaction": {}
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-10T12:00:00Z",
            "transaction": "AAEC",
            "newTransaction": {}
        }),
    ] {
        expect_that!(
            serde_json::from_value::<RunQueryRequest>(conflicting),
            err(anything())
        );
    }

    // Precondition empty, exists, updateTime, and conflicting fields
    let empty_pre: Precondition = serde_json::from_str("{}").unwrap();
    expect_eq!(empty_pre.condition_type, None);
    expect_eq!(serde_json::to_value(empty_pre).unwrap(), json!({}));

    let exists_pre: Precondition = serde_json::from_str(r#"{"exists": false}"#).unwrap();
    expect_eq!(
        exists_pre.condition_type,
        Some(precondition::ConditionType::Exists(false))
    );
    expect_eq!(
        serde_json::to_value(exists_pre).unwrap(),
        json!({ "exists": false })
    );

    let ts: Timestamp = datetime!(2026-11-10 12:00:00.500 UTC).into();
    let ut_pre = Precondition {
        condition_type: Some(precondition::ConditionType::UpdateTime(ts)),
    };
    let ut_round: Precondition =
        serde_json::from_str(&serde_json::to_string(&ut_pre).unwrap()).unwrap();
    expect_eq!(ut_round, ut_pre);

    expect_that!(
        serde_json::from_value::<Precondition>(json!({
            "exists": true,
            "updateTime": "2026-11-10T12:00:00Z"
        })),
        err(anything())
    );

    // Filter edge cases: multiple filter types, missing filter_type, missing UnaryFilter/Order
    // field
    expect_that!(
        serde_json::from_value::<Filter>(json!({
            "fieldFilter": {
                "field": { "fieldPath": "a" },
                "op": "EQUAL",
                "value": { "integerValue": "1" }
            },
            "unaryFilter": {
                "op": "IS_NULL",
                "field": { "fieldPath": "a" }
            }
        })),
        err(anything())
    );
    expect_that!(
        serde_json::to_string(&Filter { filter_type: None }),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<UnaryFilter>(json!({ "op": "IS_NULL" })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Order>(json!({ "direction": "ASCENDING" })),
        err(anything())
    );
}

#[gtest]
fn vector_map_and_document_serialize_keys_deterministically_and_preserve_neg_zero() {
    let vec_val = Value::vector([-0.0, 1.5, f64::INFINITY]);
    let vec_json = serde_json::to_string(&vec_val).unwrap();
    let type_idx = vec_json.find(r#""__type__""#).unwrap();
    let value_idx = vec_json.find(r#""value""#).unwrap();
    expect_lt!(type_idx, value_idx);
    expect_that!(vec_json, contains_substring("-0.0"));

    let parsed_vec: Value = serde_json::from_str(&vec_json).unwrap();
    expect_true!(vec_val.storage_eq(&parsed_vec));

    let doc = Document {
        name: "projects/p/databases/(default)/documents/items/v1".to_string(),
        fields: HashMap::from([
            ("zeta".to_string(), Value::integer(1)),
            ("embedding".to_string(), vec_val.clone()),
            ("alpha".to_string(), Value::string("first")),
        ]),
        create_time: None,
        update_time: None,
    };
    let doc_json = serde_json::to_string(&doc).unwrap();
    let alpha_idx = doc_json.find(r#""alpha""#).unwrap();
    let emb_idx = doc_json.find(r#""embedding""#).unwrap();
    let zeta_idx = doc_json.find(r#""zeta""#).unwrap();
    expect_lt!(alpha_idx, emb_idx);
    expect_lt!(emb_idx, zeta_idx);

    let emb_slice = &doc_json[emb_idx..zeta_idx];
    let emb_type_idx = emb_slice.find(r#""__type__""#).unwrap();
    let emb_value_idx = emb_slice.find(r#""value""#).unwrap();
    expect_lt!(emb_type_idx, emb_value_idx);

    let parsed_doc: Document = serde_json::from_str(&doc_json).unwrap();
    expect_true!(Value::fields_storage_eq(&doc.fields, &parsed_doc.fields));
}

#[gtest]
fn timestamp_offset_bounds_and_rfc3339_conformance() {
    // Offset-shifted boundary timestamps crossing 0001..=9999 UTC must error without panicking.
    for oob in [
        "9999-12-31T23:59:59-01:00",
        "9999-12-31T23:59:59.999999999-00:01",
        "0001-01-01T00:00:00+01:00",
        "0000-12-31T23:59:59Z",
    ] {
        expect_that!(
            serde_json::from_str::<Timestamp>(&format!("\"{oob}\"")),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<Value>(json!({ "timestampValue": oob })),
            err(anything())
        );
    }

    let max_shifted: Timestamp = serde_json::from_str(r#""9999-12-31T23:59:59+01:00""#).unwrap();
    expect_eq!(max_shifted.seconds, MAX_TIMESTAMP_SECONDS - 3600);

    let min_shifted: Timestamp = serde_json::from_str(r#""0001-01-01T00:00:00-01:00""#).unwrap();
    expect_eq!(min_shifted.seconds, MIN_TIMESTAMP_SECONDS + 3600);

    // RFC 3339 allows lowercase 't' and 'z'.
    let lower: Timestamp = serde_json::from_str(r#""2026-11-10t15:30:45z""#).unwrap();
    let upper: Timestamp = serde_json::from_str(r#""2026-11-10T15:30:45Z""#).unwrap();
    expect_eq!(lower, upper);

    let lower_frac: Timestamp = serde_json::from_str(r#""2026-11-10t15:30:45.125+02:00""#).unwrap();
    expect_eq!(lower_frac.seconds, upper.seconds - 7200);
    expect_eq!(lower_frac.nanos, 125_000_000);

    // Non-RFC-3339 ISO 8601 forms must be rejected.
    for non_rfc3339 in [
        "2026-11-10T15:30Z",
        "2026-11-10T15:30:45+0200",
        "20261110T153045Z",
        "2026-W46-2T15:30:45Z",
        "2026-314T15:30:45Z",
        "2026-11-10T15:30:45,5Z",
    ] {
        expect_that!(
            serde_json::from_str::<Timestamp>(&format!("\"{non_rfc3339}\"")),
            err(anything())
        );
    }
}

#[gtest]
fn double_string_validation_rejects_non_protojson_and_overflow() {
    for bad in [
        "inf",
        "-inf",
        "nan",
        "NAN",
        "-NaN",
        "+NaN",
        "+Infinity",
        "infinity",
        "-infinity",
        "1e500",
        "-1e500",
        "1.8e308",
    ] {
        expect_that!(
            serde_json::from_value::<Value>(json!({ "doubleValue": bad })),
            err(anything())
        );
    }

    let neg_zero_str: Value = serde_json::from_value(json!({ "doubleValue": "-0.0" })).unwrap();
    expect_true!(neg_zero_str.storage_eq(&Value::double(-0.0)));

    let neg_zero_short: Value = serde_json::from_value(json!({ "doubleValue": "-0" })).unwrap();
    expect_true!(neg_zero_short.storage_eq(&Value::double(-0.0)));

    let max_finite: Value =
        serde_json::from_value(json!({ "doubleValue": "1.7976931348623157e308" })).unwrap();
    expect_true!(max_finite.storage_eq(&Value::double(f64::MAX)));
}

#[gtest]
fn lat_lng_protojson_bounds_strings_and_non_finite_serialization() {
    let empty: LatLng = serde_json::from_str("{}").unwrap();
    expect_eq!(
        empty,
        LatLng {
            latitude:  0.0,
            longitude: 0.0,
        }
    );

    let nulls: LatLng = serde_json::from_str(r#"{"latitude":null,"longitude":null}"#).unwrap();
    expect_eq!(nulls, empty);

    let bounds: LatLng = serde_json::from_str(r#"{"latitude":-90.0,"longitude":180.0}"#).unwrap();
    expect_eq!(bounds.latitude, -90.0);
    expect_eq!(bounds.longitude, 180.0);

    let from_strings: LatLng =
        serde_json::from_str(r#"{"latitude":"52.3676","longitude":"-4.9041"}"#).unwrap();
    expect_eq!(from_strings.latitude, 52.3676);
    expect_eq!(from_strings.longitude, -4.9041);

    let neg_zero: LatLng = serde_json::from_str(r#"{"latitude":"-0.0","longitude":-0.0}"#).unwrap();
    expect_eq!(neg_zero.latitude.to_bits(), (-0.0_f64).to_bits());
    expect_eq!(neg_zero.longitude.to_bits(), (-0.0_f64).to_bits());

    for bad in [
        json!({ "latitude": 90.0001, "longitude": 0.0 }),
        json!({ "latitude": -90.0001, "longitude": 0.0 }),
        json!({ "latitude": 0.0, "longitude": 180.0001 }),
        json!({ "latitude": 0.0, "longitude": -180.0001 }),
        json!({ "latitude": "NaN", "longitude": 0.0 }),
        json!({ "latitude": 0.0, "longitude": "Infinity" }),
        json!({ "latitude": "-Infinity", "longitude": 0.0 }),
        json!({ "latitude": "1e500", "longitude": 0.0 }),
        json!({ "latitude": 10.0, "extra": 20.0 }),
    ] {
        expect_that!(
            serde_json::from_value::<LatLng>(bad.clone()),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<Value>(json!({ "geoPointValue": bad })),
            err(anything())
        );
    }

    let non_finite = Value::from(LatLng {
        latitude:  f64::NAN,
        longitude: f64::NEG_INFINITY,
    });
    expect_eq!(
        serde_json::to_value(&non_finite).unwrap(),
        json!({
            "geoPointValue": {
                "latitude": "NaN",
                "longitude": "-Infinity"
            }
        })
    );
}

#[gtest]
fn run_query_request_serializes_transaction_and_rejects_unsupported_fields() {
    use crate::google::firestore::v1::{ExplainOptions, RequestOptions, TransactionOptions};

    let rq_json = json!({
        "parent": "projects/p/databases/(default)/documents",
        "structuredQuery": {
            "from": [{ "collectionId": "users", "allDescendants": false }]
        },
        "transaction": "AAEC"
    });
    let rq: RunQueryRequest = serde_json::from_value(rq_json.clone()).unwrap();
    expect_eq!(serde_json::to_value(&rq).unwrap(), rq_json);

    let rq_new_txn = RunQueryRequest {
        consistency_selector: Some(run_query_request::ConsistencySelector::NewTransaction(
            TransactionOptions::READ_WRITE,
        )),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&rq_new_txn), err(anything()));

    let rq_explain = RunQueryRequest {
        explain_options: Some(ExplainOptions::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&rq_explain), err(anything()));

    let rq_req_opts = RunQueryRequest {
        request_options: Some(RequestOptions::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&rq_req_opts), err(anything()));

    let lci_req_opts = ListCollectionIdsRequest {
        request_options: Some(RequestOptions::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&lci_req_opts), err(anything()));

    let var_ref = Value {
        value_type: Some(ValueType::VariableReferenceValue("v".to_string())),
    };
    let var_ref_json = serde_json::to_value(&var_ref).unwrap();
    expect_eq!(var_ref_json, json!({ "variableReferenceValue": "v" }));
    let var_ref_roundtrip: Value = serde_json::from_value(var_ref_json).unwrap();
    expect_true!(var_ref.storage_eq(&var_ref_roundtrip));
}

#[gtest]
fn protojson_null_as_default_and_string_int_normalization() {
    use crate::google::firestore::v1::{
        DocumentMask, ListCollectionIdsResponse, ListDocumentsResponse,
    };

    // RunQueryResponse: skippedResults accepts integer, decimal string, null, and omitted
    for (input, expected) in [
        (json!({ "skippedResults": 5 }), 5),
        (json!({ "skippedResults": "5" }), 5),
        (json!({ "skippedResults": null }), 0),
        (json!({}), 0),
    ] {
        let resp: RunQueryResponse = serde_json::from_value(input).unwrap();
        expect_eq!(resp.skipped_results, expected);
    }
    for bad in [
        json!({ "skippedResults": 3_000_000_000_i64 }),
        json!({ "skippedResults": "not-an-i32" }),
        json!({ "skippedResults": true }),
    ] {
        expect_that!(
            serde_json::from_value::<RunQueryResponse>(bad),
            err(anything())
        );
    }

    // Order: explicit `"direction": null` -> Direction::Unspecified (0)
    let order_null: Order =
        serde_json::from_value(json!({ "field": { "fieldPath": "age" }, "direction": null }))
            .unwrap();
    expect_eq!(order_null.direction, Direction::Unspecified as i32);
    expect_eq!(
        order_null.field,
        Some(FieldReference {
            field_path: "age".to_string(),
        })
    );
    let order_omitted: Order =
        serde_json::from_value(json!({ "field": { "fieldPath": "age" } })).unwrap();
    expect_eq!(order_omitted.direction, Direction::Unspecified as i32);

    // CompositeFilter.filters accepts null when op is present
    let comp: CompositeFilter =
        serde_json::from_value(json!({ "op": "AND", "filters": null })).unwrap();
    expect_eq!(comp.op, composite_filter::Operator::And as i32);
    expect_true!(comp.filters.is_empty());

    // StructuredQuery accepts null on all fields (including string-int offset/limit)
    let sq: StructuredQuery = serde_json::from_value(json!({
        "select": null,
        "from": null,
        "where": null,
        "orderBy": null,
        "startAt": null,
        "endAt": null,
        "offset": null,
        "limit": null,
        "findNearest": null
    }))
    .unwrap();
    expect_eq!(sq, StructuredQuery::default());

    let sq_str_ints: StructuredQuery =
        serde_json::from_value(json!({ "offset": "5", "limit": "10" })).unwrap();
    expect_eq!(sq_str_ints.offset, 5);
    expect_eq!(sq_str_ints.limit, Some(Int32Value { value: 10 }));

    // RunQueryRequest.parent accepts null
    let rq: RunQueryRequest = serde_json::from_value(json!({
        "parent": null,
        "structuredQuery": null,
        "readTime": null,
        "transaction": null
    }))
    .unwrap();
    expect_eq!(rq, RunQueryRequest::default());

    // ListCollectionIdsRequest accepts null and string-int pageSize
    let lci_null: ListCollectionIdsRequest = serde_json::from_value(json!({
        "parent": null,
        "pageSize": null,
        "pageToken": null,
        "readTime": null
    }))
    .unwrap();
    expect_eq!(lci_null, ListCollectionIdsRequest::default());
    let lci_str: ListCollectionIdsRequest =
        serde_json::from_value(json!({ "pageSize": "25" })).unwrap();
    expect_eq!(lci_str.page_size, 25);

    // All 10 build.rs derived structs accept explicit JSON null on non-Option fields
    let arr: ArrayValue = serde_json::from_value(json!({ "values": null })).unwrap();
    expect_eq!(arr, ArrayValue::default());

    let map: MapValue = serde_json::from_value(json!({ "fields": null })).unwrap();
    expect_eq!(map, MapValue::default());

    let doc: Document = serde_json::from_value(json!({
        "name": null,
        "fields": null,
        "createTime": null,
        "updateTime": null
    }))
    .unwrap();
    expect_eq!(doc, Document::default());

    let mask: DocumentMask = serde_json::from_value(json!({ "fieldPaths": null })).unwrap();
    expect_eq!(mask, DocumentMask::default());

    let list_docs: ListDocumentsResponse = serde_json::from_value(json!({
        "documents": null,
        "nextPageToken": null
    }))
    .unwrap();
    expect_eq!(list_docs, ListDocumentsResponse::default());

    let list_cols: ListCollectionIdsResponse = serde_json::from_value(json!({
        "collectionIds": null,
        "nextPageToken": null
    }))
    .unwrap();
    expect_eq!(list_cols, ListCollectionIdsResponse::default());

    let col_sel: CollectionSelector = serde_json::from_value(json!({
        "collectionId": null,
        "allDescendants": null
    }))
    .unwrap();
    expect_eq!(col_sel, CollectionSelector::default());

    let field_ref: FieldReference = serde_json::from_value(json!({ "fieldPath": null })).unwrap();
    expect_eq!(field_ref, FieldReference::default());

    let proj: Projection = serde_json::from_value(json!({ "fields": null })).unwrap();
    expect_eq!(proj, Projection::default());

    let cursor: Cursor = serde_json::from_value(json!({
        "values": null,
        "before": null
    }))
    .unwrap();
    expect_eq!(cursor, Cursor::default());
}

#[gtest]
fn strict_value_and_required_filter_order_fields_reject_null() {
    use crate::google::firestore::v1::DocumentMask;

    // Bare null and null inside Value variant payloads (except nullValue) MUST fail
    expect_that!(serde_json::from_str::<Value>("null"), err(anything()));
    for bad_val in [
        json!({ "stringValue": null }),
        json!({ "booleanValue": null }),
        json!({ "integerValue": null }),
        json!({ "doubleValue": null }),
        json!({ "timestampValue": null }),
        json!({ "bytesValue": null }),
        json!({ "referenceValue": null }),
        json!({ "geoPointValue": null }),
        json!({ "arrayValue": null }),
        json!({ "mapValue": null }),
        json!({ "fieldReferenceValue": null }),
        json!({ "variableReferenceValue": null }),
        json!({ "mapValue": { "fields": { "k": null } } }),
        json!({ "arrayValue": { "values": [null] } }),
    ] {
        expect_that!(serde_json::from_value::<Value>(bad_val), err(anything()));
    }

    // Null values inside containers of Value or required sub-elements MUST fail
    expect_that!(
        serde_json::from_value::<Document>(json!({ "fields": { "k": null } })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<MapValue>(json!({ "fields": { "k": null } })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<ArrayValue>(json!({ "values": [null] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Cursor>(json!({ "values": [null] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<DocumentMask>(json!({ "fieldPaths": [null] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "from": [null] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredQuery>(json!({ "orderBy": [null] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Projection>(json!({ "fields": [null] })),
        err(anything())
    );

    // Required Order, CompositeFilter, FieldFilter, and UnaryFilter fields MUST fail on null or
    // omitted
    expect_that!(
        serde_json::from_value::<Order>(json!({ "field": null, "direction": "ASCENDING" })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Order>(json!({ "field": null })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<CompositeFilter>(json!({ "op": null, "filters": [] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<CompositeFilter>(json!({ "filters": [] })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<CompositeFilter>(json!({ "op": "AND", "filters": [null] })),
        err(anything())
    );
    for bad_ff in [
        json!({ "field": null, "op": "EQUAL", "value": { "integerValue": "1" } }),
        json!({ "field": { "fieldPath": "a" }, "op": null, "value": { "integerValue": "1" } }),
        json!({ "field": { "fieldPath": "a" }, "op": "EQUAL", "value": null }),
    ] {
        expect_that!(
            serde_json::from_value::<FieldFilter>(bad_ff),
            err(anything())
        );
    }
    for bad_uf in [
        json!({ "op": null, "field": { "fieldPath": "a" } }),
        json!({ "op": "IS_NULL", "field": null }),
    ] {
        expect_that!(
            serde_json::from_value::<UnaryFilter>(bad_uf),
            err(anything())
        );
    }
}

#[gtest]
fn proto_enum_serde_preserves_exact_diagnostics_and_roundtrips() {
    // Direction
    let dir_bad_str = serde_json::from_value::<Order>(json!({
        "field": { "fieldPath": "a" },
        "direction": "SIDEWAYS"
    }))
    .unwrap_err()
    .to_string();
    assert!(
        dir_bad_str.contains("DIRECTION_UNSPECIFIED")
            && dir_bad_str.contains("ASCENDING")
            && dir_bad_str.contains("DESCENDING"),
        "unexpected Direction unknown_variant error: {dir_bad_str}"
    );
    for num in [json!(-1), json!(99), json!(u64::MAX)] {
        let err = serde_json::from_value::<Order>(json!({
            "field": { "fieldPath": "a" },
            "direction": num
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!("unknown Direction enum number {num}")),
            "unexpected Direction numeric error: {err}"
        );
    }
    let dir_bad_ty = serde_json::from_value::<Order>(json!({
        "field": { "fieldPath": "a" },
        "direction": true
    }))
    .unwrap_err()
    .to_string();
    assert!(
        dir_bad_ty.contains("a Direction enum string or integer"),
        "unexpected Direction expecting error: {dir_bad_ty}"
    );
    expect_eq!(
        serde_json::to_value(Order {
            field:     Some(FieldReference {
                field_path: "a".to_string(),
            }),
            direction: 999,
        })
        .unwrap()["direction"],
        json!("DIRECTION_UNSPECIFIED")
    );

    // CompositeFilter.Operator
    let comp_bad_str = serde_json::from_value::<CompositeFilter>(json!({
        "op": "XOR",
        "filters": []
    }))
    .unwrap_err()
    .to_string();
    assert!(
        comp_bad_str.contains("OPERATOR_UNSPECIFIED")
            && comp_bad_str.contains("AND")
            && comp_bad_str.contains("OR"),
        "unexpected CompositeFilter.Operator unknown_variant error: {comp_bad_str}"
    );
    for num in [json!(-1), json!(99), json!(u64::MAX)] {
        let err = serde_json::from_value::<CompositeFilter>(json!({
            "op": num,
            "filters": []
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!("unknown CompositeFilter.Operator number {num}")),
            "unexpected CompositeFilter.Operator numeric error: {err}"
        );
    }
    let comp_bad_ty = serde_json::from_value::<CompositeFilter>(json!({
        "op": true,
        "filters": []
    }))
    .unwrap_err()
    .to_string();
    assert!(
        comp_bad_ty.contains("a CompositeFilter.Operator string or integer"),
        "unexpected CompositeFilter.Operator expecting error: {comp_bad_ty}"
    );

    // FieldFilter.Operator
    let field_bad_str = serde_json::from_value::<FieldFilter>(json!({
        "field": { "fieldPath": "a" },
        "op": "LIKE",
        "value": { "integerValue": "1" }
    }))
    .unwrap_err()
    .to_string();
    assert!(
        field_bad_str.contains("OPERATOR_UNSPECIFIED") && field_bad_str.contains("NOT_IN"),
        "unexpected FieldFilter.Operator unknown_variant error: {field_bad_str}"
    );
    for num in [json!(-1), json!(99), json!(u64::MAX)] {
        let err = serde_json::from_value::<FieldFilter>(json!({
            "field": { "fieldPath": "a" },
            "op": num,
            "value": { "integerValue": "1" }
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!("unknown FieldFilter.Operator number {num}")),
            "unexpected FieldFilter.Operator numeric error: {err}"
        );
    }
    let field_bad_ty = serde_json::from_value::<FieldFilter>(json!({
        "field": { "fieldPath": "a" },
        "op": true,
        "value": { "integerValue": "1" }
    }))
    .unwrap_err()
    .to_string();
    assert!(
        field_bad_ty.contains("a FieldFilter.Operator string or integer"),
        "unexpected FieldFilter.Operator expecting error: {field_bad_ty}"
    );

    // UnaryFilter.Operator
    let unary_bad_str = serde_json::from_value::<UnaryFilter>(json!({
        "op": "IS_EMPTY",
        "field": { "fieldPath": "a" }
    }))
    .unwrap_err()
    .to_string();
    assert!(
        unary_bad_str.contains("OPERATOR_UNSPECIFIED") && unary_bad_str.contains("IS_NOT_NULL"),
        "unexpected UnaryFilter.Operator unknown_variant error: {unary_bad_str}"
    );
    for num in [json!(-1), json!(99), json!(u64::MAX)] {
        let err = serde_json::from_value::<UnaryFilter>(json!({
            "op": num,
            "field": { "fieldPath": "a" }
        }))
        .unwrap_err()
        .to_string();
        assert!(
            err.contains(&format!("unknown UnaryFilter.Operator number {num}")),
            "unexpected UnaryFilter.Operator numeric error: {err}"
        );
    }
    let unary_bad_ty = serde_json::from_value::<UnaryFilter>(json!({
        "op": true,
        "field": { "fieldPath": "a" }
    }))
    .unwrap_err()
    .to_string();
    assert!(
        unary_bad_ty.contains("a UnaryFilter.Operator string or integer"),
        "unexpected UnaryFilter.Operator expecting error: {unary_bad_ty}"
    );

    // Positive string + integer (`visit_u64` & `visit_i64`) round-trips across all variants,
    // plus unknown `i32` (`999`) fallback serialization on all three `Operator` types.
    use serde::de::value::I64Deserializer;
    for (variant, wire_str) in [
        (Direction::Unspecified, "DIRECTION_UNSPECIFIED"),
        (Direction::Ascending, "ASCENDING"),
        (Direction::Descending, "DESCENDING"),
    ] {
        let num = variant as i32;
        let via_u64 = serde_json::from_value::<Order>(json!({
            "field": { "fieldPath": "a" },
            "direction": num
        }))
        .unwrap();
        let via_str = serde_json::from_value::<Order>(json!({
            "field": { "fieldPath": "a" },
            "direction": wire_str
        }))
        .unwrap();
        expect_eq!(via_u64.direction, num);
        expect_eq!(via_str.direction, num);
        expect_eq!(
            deserialize_direction(I64Deserializer::<serde_json::Error>::new(i64::from(num)))
                .unwrap(),
            num
        );
        expect_eq!(
            serde_json::to_value(via_u64).unwrap(),
            json!({ "field": { "fieldPath": "a" }, "direction": wire_str })
        );
    }

    for (variant, wire_str) in [
        (
            composite_filter::Operator::Unspecified,
            "OPERATOR_UNSPECIFIED",
        ),
        (composite_filter::Operator::And, "AND"),
        (composite_filter::Operator::Or, "OR"),
    ] {
        let num = variant as i32;
        let via_u64 = serde_json::from_value::<CompositeFilter>(json!({
            "op": num,
            "filters": []
        }))
        .unwrap();
        let via_str = serde_json::from_value::<CompositeFilter>(json!({
            "op": wire_str,
            "filters": []
        }))
        .unwrap();
        expect_eq!(via_u64.op, num);
        expect_eq!(via_str.op, num);
        expect_eq!(
            deserialize_composite_op(I64Deserializer::<serde_json::Error>::new(i64::from(num)))
                .unwrap(),
            num
        );
        expect_eq!(
            serde_json::to_value(via_u64).unwrap(),
            json!({ "op": wire_str, "filters": [] })
        );
    }
    expect_eq!(
        serde_json::to_value(CompositeFilter {
            op:      999,
            filters: vec![],
        })
        .unwrap()["op"],
        json!("OPERATOR_UNSPECIFIED")
    );

    for (variant, wire_str) in [
        (field_filter::Operator::Unspecified, "OPERATOR_UNSPECIFIED"),
        (field_filter::Operator::LessThan, "LESS_THAN"),
        (
            field_filter::Operator::LessThanOrEqual,
            "LESS_THAN_OR_EQUAL",
        ),
        (field_filter::Operator::GreaterThan, "GREATER_THAN"),
        (
            field_filter::Operator::GreaterThanOrEqual,
            "GREATER_THAN_OR_EQUAL",
        ),
        (field_filter::Operator::Equal, "EQUAL"),
        (field_filter::Operator::NotEqual, "NOT_EQUAL"),
        (field_filter::Operator::ArrayContains, "ARRAY_CONTAINS"),
        (field_filter::Operator::In, "IN"),
        (
            field_filter::Operator::ArrayContainsAny,
            "ARRAY_CONTAINS_ANY",
        ),
        (field_filter::Operator::NotIn, "NOT_IN"),
    ] {
        let num = variant as i32;
        let via_u64 = serde_json::from_value::<FieldFilter>(json!({
            "field": { "fieldPath": "a" },
            "op": num,
            "value": { "integerValue": "1" }
        }))
        .unwrap();
        let via_str = serde_json::from_value::<FieldFilter>(json!({
            "field": { "fieldPath": "a" },
            "op": wire_str,
            "value": { "integerValue": "1" }
        }))
        .unwrap();
        expect_eq!(via_u64.op, num);
        expect_eq!(via_str.op, num);
        expect_eq!(
            deserialize_field_op(I64Deserializer::<serde_json::Error>::new(i64::from(num)))
                .unwrap(),
            num
        );
        expect_eq!(
            serde_json::to_value(via_u64).unwrap()["op"],
            json!(wire_str)
        );
    }
    expect_eq!(
        serde_json::to_value(FieldFilter {
            field: Some(FieldReference {
                field_path: "a".to_string(),
            }),
            op:    999,
            value: Some(Value::integer(1)),
        })
        .unwrap()["op"],
        json!("OPERATOR_UNSPECIFIED")
    );

    for (variant, wire_str) in [
        (unary_filter::Operator::Unspecified, "OPERATOR_UNSPECIFIED"),
        (unary_filter::Operator::IsNan, "IS_NAN"),
        (unary_filter::Operator::IsNull, "IS_NULL"),
        (unary_filter::Operator::IsNotNan, "IS_NOT_NAN"),
        (unary_filter::Operator::IsNotNull, "IS_NOT_NULL"),
    ] {
        let num = variant as i32;
        let via_u64 = serde_json::from_value::<UnaryFilter>(json!({
            "op": num,
            "field": { "fieldPath": "a" }
        }))
        .unwrap();
        let via_str = serde_json::from_value::<UnaryFilter>(json!({
            "op": wire_str,
            "field": { "fieldPath": "a" }
        }))
        .unwrap();
        expect_eq!(via_u64.op, num);
        expect_eq!(via_str.op, num);
        expect_eq!(
            deserialize_unary_op(I64Deserializer::<serde_json::Error>::new(i64::from(num)))
                .unwrap(),
            num
        );
        expect_eq!(
            serde_json::to_value(via_u64).unwrap(),
            json!({ "op": wire_str, "field": { "fieldPath": "a" } })
        );
    }
    expect_eq!(
        serde_json::to_value(UnaryFilter {
            op: 999,
            operand_type: Some(unary_filter::OperandType::Field(FieldReference {
                field_path: "a".to_string(),
            })),
        })
        .unwrap()["op"],
        json!("OPERATOR_UNSPECIFIED")
    );
}

#[gtest]
fn aggregation_query_request_and_response_roundtrip() {
    let rt: Timestamp = datetime!(2026-11-24 10:15:30.250 UTC).into();
    let req = RunAggregationQueryRequest {
        parent: "projects/p/databases/(default)/documents".to_string(),
        query_type: Some(
            run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                StructuredAggregationQuery {
                    query_type:   Some(structured_aggregation_query::QueryType::StructuredQuery(
                        StructuredQuery {
                            from: vec![CollectionSelector {
                                collection_id:   "orders".to_string(),
                                all_descendants: false,
                            }],
                            limit: Some(Int32Value { value: 50 }),
                            ..Default::default()
                        },
                    )),
                    aggregations: vec![
                        Aggregation {
                            alias:    "".to_string(),
                            operator: Some(Operator::Count(Count { up_to: None })),
                        },
                        Aggregation {
                            alias:    "capped".to_string(),
                            operator: Some(Operator::Count(Count {
                                up_to: Some(Int64Value { value: 100 }),
                            })),
                        },
                        Aggregation {
                            alias:    "revenue".to_string(),
                            operator: Some(Operator::Sum(Sum {
                                field: Some(FieldReference {
                                    field_path: "totalCents".to_string(),
                                }),
                            })),
                        },
                        Aggregation {
                            alias:    "avg_order".to_string(),
                            operator: Some(Operator::Avg(Avg {
                                field: Some(FieldReference {
                                    field_path: "totalCents".to_string(),
                                }),
                            })),
                        },
                    ],
                },
            ),
        ),
        consistency_selector: Some(
            run_aggregation_query_request::ConsistencySelector::ReadTime(rt),
        ),
        explain_options: None,
        request_options: None,
    };

    let req_json = serde_json::to_value(&req).unwrap();
    expect_eq!(
        req_json,
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "structuredAggregationQuery": {
                "structuredQuery": {
                    "from": [{ "collectionId": "orders", "allDescendants": false }],
                    "limit": 50
                },
                "aggregations": [
                    { "count": {} },
                    { "count": { "upTo": "100" }, "alias": "capped" },
                    { "sum": { "field": { "fieldPath": "totalCents" } }, "alias": "revenue" },
                    { "avg": { "field": { "fieldPath": "totalCents" } }, "alias": "avg_order" }
                ]
            },
            "readTime": "2026-11-24T10:15:30.250Z"
        })
    );
    let roundtrip_req: RunAggregationQueryRequest = serde_json::from_value(req_json).unwrap();
    expect_eq!(roundtrip_req, req);

    // Transaction consistency selector on RunAggregationQueryRequest
    let tx_req = RunAggregationQueryRequest {
        parent: "projects/p/databases/(default)/documents".to_string(),
        query_type: Some(
            run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                StructuredAggregationQuery {
                    query_type:   None,
                    aggregations: vec![Aggregation {
                        alias:    "total".to_string(),
                        operator: Some(Operator::Count(Count { up_to: None })),
                    }],
                },
            ),
        ),
        consistency_selector: Some(
            run_aggregation_query_request::ConsistencySelector::Transaction(vec![0, 1, 2]),
        ),
        explain_options: None,
        request_options: None,
    };
    let tx_json = serde_json::to_value(&tx_req).unwrap();
    expect_eq!(
        tx_json,
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "structuredAggregationQuery": {
                "aggregations": [{ "count": {}, "alias": "total" }]
            },
            "transaction": "AAEC"
        })
    );
    expect_eq!(
        serde_json::from_value::<RunAggregationQueryRequest>(tx_json).unwrap(),
        tx_req
    );

    // RunAggregationQueryResponse full and minimal round-trips
    let resp = RunAggregationQueryResponse {
        result: Some(AggregationResult {
            aggregate_fields: HashMap::from([
                ("field_1".to_string(), Value::integer(12)),
                ("avg_order".to_string(), Value::double(42.5)),
            ]),
        }),
        transaction: vec![0, 1, 2],
        read_time: Some(rt),
        explain_metrics: None,
    };
    let resp_json = serde_json::to_value(&resp).unwrap();
    expect_eq!(
        resp_json,
        json!({
            "result": {
                "aggregateFields": {
                    "avg_order": { "doubleValue": 42.5 },
                    "field_1": { "integerValue": "12" }
                }
            },
            "transaction": "AAEC",
            "readTime": "2026-11-24T10:15:30.250Z"
        })
    );
    let roundtrip_resp: RunAggregationQueryResponse = serde_json::from_value(resp_json).unwrap();
    expect_eq!(roundtrip_resp.transaction, resp.transaction);
    expect_eq!(roundtrip_resp.read_time, resp.read_time);
    expect_true!(Value::fields_storage_eq(
        &roundtrip_resp.result.unwrap().aggregate_fields,
        &resp.result.unwrap().aggregate_fields
    ));

    let min_resp = RunAggregationQueryResponse {
        result: None,
        transaction: Vec::new(),
        read_time: Some(rt),
        explain_metrics: None,
    };
    expect_eq!(
        serde_json::to_value(&min_resp).unwrap(),
        json!({ "readTime": "2026-11-24T10:15:30.250Z" })
    );

    // Unknown fields on response and result are ignored (no deny_unknown_fields)
    let permissive_resp: RunAggregationQueryResponse = serde_json::from_value(json!({
        "result": {
            "aggregateFields": { "c": { "integerValue": "7" } },
            "futureResultField": "ignored"
        },
        "explainMetrics": { "planSummary": {} },
        "transaction": null,
        "readTime": "2026-11-24T10:15:30.250Z",
        "futureResponseField": true
    }))
    .unwrap();
    expect_true!(permissive_resp.transaction.is_empty());
    expect_eq!(permissive_resp.read_time, Some(rt));
    expect_eq!(permissive_resp.explain_metrics, None);
    expect_eq!(
        permissive_resp
            .result
            .unwrap()
            .aggregate_fields
            .get("c")
            .and_then(Value::as_integer),
        Some(7)
    );

    // Sum/Avg omit unset field and accept null/omitted field on deserialize;
    // StructuredAggregationQuery defaults on null/empty
    expect_eq!(
        serde_json::to_value(Sum { field: None }).unwrap(),
        json!({})
    );
    expect_eq!(
        serde_json::to_value(Avg { field: None }).unwrap(),
        json!({})
    );
    expect_eq!(
        serde_json::from_value::<Sum>(json!({ "field": null })).unwrap(),
        Sum { field: None }
    );
    expect_eq!(
        serde_json::from_value::<Avg>(json!({ "field": null })).unwrap(),
        Avg { field: None }
    );
    let saq_empty = StructuredAggregationQuery::default();
    expect_eq!(serde_json::to_value(&saq_empty).unwrap(), json!({}));
    expect_eq!(
        serde_json::from_value::<StructuredAggregationQuery>(json!({
            "structuredQuery": null,
            "aggregations": null
        }))
        .unwrap(),
        saq_empty
    );
    expect_that!(
        serde_json::from_value::<StructuredAggregationQuery>(json!({
            "aggregations": [null]
        })),
        err(anything())
    );

    // Aggregation accepts explicit null for alias and non-selected oneof branches
    let agg_null_fields: Aggregation = serde_json::from_value(json!({
        "count": {},
        "sum": null,
        "avg": null,
        "alias": null
    }))
    .unwrap();
    expect_eq!(
        agg_null_fields,
        Aggregation {
            alias:    String::new(),
            operator: Some(Operator::Count(Count { up_to: None })),
        }
    );
}

#[gtest]
fn run_query_and_aggregation_request_null_and_unsupported_envelope_fields() {
    use crate::google::firestore::v1::{
        ExplainOptions, RequestOptions, TransactionOptions, structured_query::FindNearest,
    };

    // Canonical camelCase null envelope fields are accepted on both RunQueryRequest and
    // RunAggregationQueryRequest
    let rq_nulls: RunQueryRequest = serde_json::from_value(json!({
        "parent": null,
        "structuredQuery": null,
        "readTime": null,
        "transaction": null,
        "newTransaction": null,
        "explainOptions": null,
        "requestOptions": null
    }))
    .unwrap();
    expect_eq!(rq_nulls, RunQueryRequest::default());

    let raq_nulls: RunAggregationQueryRequest = serde_json::from_value(json!({
        "parent": null,
        "structuredAggregationQuery": null,
        "readTime": null,
        "transaction": null,
        "newTransaction": null,
        "explainOptions": null,
        "requestOptions": null
    }))
    .unwrap();
    expect_eq!(raq_nulls, RunAggregationQueryRequest::default());

    // Object values on unsupported envelope & findNearest fields decode to
    // Some(Default::default()), while non-objects are rejected on deserialize for both request
    // types.
    for obj in [json!({}), json!({ "nested": [1, "x", { "ok": true }] })] {
        let rq_new_txn: RunQueryRequest =
            serde_json::from_value(json!({ "newTransaction": obj })).unwrap();
        expect_eq!(
            rq_new_txn.consistency_selector,
            Some(run_query_request::ConsistencySelector::NewTransaction(
                TransactionOptions::default()
            ))
        );
        let raq_new_txn: RunAggregationQueryRequest =
            serde_json::from_value(json!({ "newTransaction": obj })).unwrap();
        expect_eq!(
            raq_new_txn.consistency_selector,
            Some(
                run_aggregation_query_request::ConsistencySelector::NewTransaction(
                    TransactionOptions::default()
                )
            )
        );

        let rq_explain: RunQueryRequest =
            serde_json::from_value(json!({ "explainOptions": obj })).unwrap();
        expect_eq!(rq_explain.explain_options, Some(ExplainOptions::default()));
        let raq_explain: RunAggregationQueryRequest =
            serde_json::from_value(json!({ "explainOptions": obj })).unwrap();
        expect_eq!(raq_explain.explain_options, Some(ExplainOptions::default()));

        let rq_req_opts: RunQueryRequest =
            serde_json::from_value(json!({ "requestOptions": obj })).unwrap();
        expect_eq!(rq_req_opts.request_options, Some(RequestOptions::default()));
        let raq_req_opts: RunAggregationQueryRequest =
            serde_json::from_value(json!({ "requestOptions": obj })).unwrap();
        expect_eq!(
            raq_req_opts.request_options,
            Some(RequestOptions::default())
        );

        let rq_fn: RunQueryRequest = serde_json::from_value(json!({
            "structuredQuery": { "findNearest": obj }
        }))
        .unwrap();
        expect_eq!(
            rq_fn.query_type,
            Some(run_query_request::QueryType::StructuredQuery(
                StructuredQuery {
                    find_nearest: Some(FindNearest::default()),
                    ..Default::default()
                }
            ))
        );
        let raq_fn: RunAggregationQueryRequest = serde_json::from_value(json!({
            "structuredAggregationQuery": {
                "structuredQuery": { "findNearest": obj }
            }
        }))
        .unwrap();
        expect_eq!(
            raq_fn.query_type,
            Some(
                run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                    StructuredAggregationQuery {
                        query_type:   Some(
                            structured_aggregation_query::QueryType::StructuredQuery(
                                StructuredQuery {
                                    find_nearest: Some(FindNearest::default()),
                                    ..Default::default()
                                }
                            )
                        ),
                        aggregations: vec![],
                    }
                )
            )
        );
    }

    for key in ["newTransaction", "explainOptions", "requestOptions"] {
        for non_obj in [json!(true), json!("abc"), json!(1), json!([])] {
            let payload = serde_json::json!({ key: non_obj });
            expect_that!(
                serde_json::from_value::<RunQueryRequest>(payload.clone()),
                err(anything())
            );
            expect_that!(
                serde_json::from_value::<RunAggregationQueryRequest>(payload),
                err(anything())
            );
        }

        // Unknown fields alongside `{}` are still rejected by deny_unknown_fields.
        let with_unknown = serde_json::json!({ key: {}, "unknown": 1 });
        expect_that!(
            serde_json::from_value::<RunQueryRequest>(with_unknown.clone()),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<RunAggregationQueryRequest>(with_unknown),
            err(anything())
        );
    }

    for non_obj in [json!(true), json!("abc"), json!(1), json!([])] {
        expect_that!(
            serde_json::from_value::<RunQueryRequest>(json!({
                "structuredQuery": { "findNearest": non_obj }
            })),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<RunAggregationQueryRequest>(json!({
                "structuredAggregationQuery": {
                    "structuredQuery": { "findNearest": non_obj }
                }
            })),
            err(anything())
        );
    }
    expect_that!(
        serde_json::from_value::<RunQueryRequest>(json!({
            "structuredQuery": { "findNearest": {}, "unknown": 1 }
        })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<RunAggregationQueryRequest>(json!({
            "structuredAggregationQuery": {
                "structuredQuery": { "findNearest": {}, "unknown": 1 }
            }
        })),
        err(anything())
    );

    // Snake-case envelope keys (even null) are rejected by camelCase + deny_unknown_fields
    for snake_key in ["new_transaction", "explain_options", "request_options"] {
        let payload = serde_json::json!({ snake_key: null });
        expect_that!(
            serde_json::from_value::<RunQueryRequest>(payload.clone()),
            err(anything())
        );
        expect_that!(
            serde_json::from_value::<RunAggregationQueryRequest>(payload),
            err(anything())
        );
    }

    // Unsupported RunAggregationQueryRequest fields fail serialization
    let raq_new_txn = RunAggregationQueryRequest {
        consistency_selector: Some(
            run_aggregation_query_request::ConsistencySelector::NewTransaction(
                TransactionOptions::READ_WRITE,
            ),
        ),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&raq_new_txn), err(anything()));

    let raq_explain = RunAggregationQueryRequest {
        explain_options: Some(ExplainOptions::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&raq_explain), err(anything()));

    let raq_req_opts = RunAggregationQueryRequest {
        request_options: Some(RequestOptions::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&raq_req_opts), err(anything()));

    let raq_find_nearest = RunAggregationQueryRequest {
        query_type: Some(
            run_aggregation_query_request::QueryType::StructuredAggregationQuery(
                StructuredAggregationQuery {
                    query_type:   Some(structured_aggregation_query::QueryType::StructuredQuery(
                        StructuredQuery {
                            find_nearest: Some(FindNearest::default()),
                            ..Default::default()
                        },
                    )),
                    aggregations: vec![],
                },
            ),
        ),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&raq_find_nearest), err(anything()));

    // Conflicting oneof consistency_selector variants and malformed base64 transaction are rejected
    for conflicting in [
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-24T10:00:00Z",
            "transaction": "AAEC"
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-24T10:00:00Z",
            "newTransaction": {}
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "transaction": "AAEC",
            "newTransaction": {}
        }),
        json!({
            "parent": "projects/p/databases/(default)/documents",
            "readTime": "2026-11-24T10:00:00Z",
            "transaction": "AAEC",
            "newTransaction": {}
        }),
    ] {
        expect_that!(
            serde_json::from_value::<RunAggregationQueryRequest>(conflicting),
            err(anything())
        );
    }
    expect_that!(
        serde_json::from_value::<RunAggregationQueryRequest>(json!({
            "transaction": "not!valid!base64!"
        })),
        err(anything())
    );
}

#[gtest]
fn aggregation_count_up_to_bounds_and_formats() {
    for (input, expected) in [
        (json!({}), None),
        (json!({ "upTo": null }), None),
        (json!({ "upTo": 0 }), Some(0)),
        (json!({ "upTo": "0" }), Some(0)),
        (json!({ "upTo": 42 }), Some(42)),
        (json!({ "upTo": "42" }), Some(42)),
        (json!({ "upTo": -5 }), Some(-5)),
        (json!({ "upTo": "-5" }), Some(-5)),
        (json!({ "upTo": i64::MIN }), Some(i64::MIN)),
        (json!({ "upTo": i64::MIN.to_string() }), Some(i64::MIN)),
        (json!({ "upTo": i64::MAX }), Some(i64::MAX)),
        (json!({ "upTo": i64::MAX.to_string() }), Some(i64::MAX)),
    ] {
        let count: Count = serde_json::from_value(input).unwrap();
        expect_eq!(count.up_to.map(|v| v.value), expected);
        let encoded = serde_json::to_value(count).unwrap();
        match expected {
            Some(n) => expect_eq!(encoded, json!({ "upTo": n.to_string() })),
            None => expect_eq!(encoded, json!({})),
        }
    }

    for bad in [
        json!({ "upTo": "9223372036854775808" }),
        json!({ "upTo": 9_223_372_036_854_775_808_u64 }),
        json!({ "upTo": "-9223372036854775809" }),
        json!({ "upTo": 1.5 }),
        json!({ "upTo": true }),
        json!({ "upTo": "not-an-i64" }),
    ] {
        expect_that!(serde_json::from_value::<Count>(bad), err(anything()));
    }
    let up_to_str_oob = serde_json::from_value::<Count>(json!({ "upTo": "9223372036854775808" }))
        .unwrap_err()
        .to_string();
    expect_that!(
        up_to_str_oob.as_str(),
        contains_substring("invalid 64-bit integer '9223372036854775808'")
    );
    let up_to_u64_oob =
        serde_json::from_value::<Count>(json!({ "upTo": 9_223_372_036_854_775_808_u64 }))
            .unwrap_err()
            .to_string();
    expect_that!(
        up_to_u64_oob.as_str(),
        contains_substring("integer 9223372036854775808 out of i64 bounds")
    );
}

#[gtest]
fn aggregation_result_sorted_and_empty_aggregate_fields() {
    let empty = AggregationResult::default();
    expect_eq!(serde_json::to_value(&empty).unwrap(), json!({}));
    expect_eq!(
        serde_json::from_value::<AggregationResult>(json!({})).unwrap(),
        empty
    );
    expect_eq!(
        serde_json::from_value::<AggregationResult>(json!({ "aggregateFields": null })).unwrap(),
        empty
    );
    expect_that!(
        serde_json::from_value::<AggregationResult>(json!({
            "aggregateFields": { "k": null }
        })),
        err(anything())
    );

    let result = AggregationResult {
        aggregate_fields: HashMap::from([
            ("zeta".to_string(), Value::integer(99)),
            ("alpha".to_string(), Value::double(-0.0)),
            ("field_1".to_string(), Value::null()),
        ]),
    };
    let encoded = serde_json::to_string(&result).unwrap();
    let alpha_idx = encoded.find(r#""alpha""#).unwrap();
    let field1_idx = encoded.find(r#""field_1""#).unwrap();
    let zeta_idx = encoded.find(r#""zeta""#).unwrap();
    expect_lt!(alpha_idx, field1_idx);
    expect_lt!(field1_idx, zeta_idx);
    expect_that!(encoded, contains_substring("-0.0"));
    expect_that!(encoded, contains_substring(r#""integerValue":"99""#));

    let decoded: AggregationResult = serde_json::from_str(&encoded).unwrap();
    expect_true!(Value::fields_storage_eq(
        &decoded.aggregate_fields,
        &result.aggregate_fields
    ));
}

#[gtest]
fn aggregation_validation_errors_and_deny_unknown_fields() {
    use crate::google::firestore::v1::ExplainMetrics;

    // Missing or multiple operators on Aggregation fail deserialize
    for bad_agg in [
        json!({}),
        json!({ "alias": "only_alias" }),
        json!({ "count": null, "sum": null, "avg": null }),
        json!({ "count": {}, "sum": { "field": { "fieldPath": "a" } } }),
        json!({ "count": {}, "avg": { "field": { "fieldPath": "a" } } }),
        json!({ "sum": { "field": { "fieldPath": "a" } }, "avg": { "field": { "fieldPath": "a" } } }),
        json!({ "count": {}, "sum": {}, "avg": {} }),
    ] {
        expect_that!(
            serde_json::from_value::<Aggregation>(bad_agg),
            err(anything())
        );
    }

    // Missing operator on Aggregation fails serialize
    expect_that!(
        serde_json::to_string(&Aggregation {
            alias:    "x".to_string(),
            operator: None,
        }),
        err(anything())
    );
    expect_that!(
        serde_json::to_string(&Aggregation::default()),
        err(anything())
    );

    // explainMetrics on RunAggregationQueryResponse fails serialize
    let resp_explain = RunAggregationQueryResponse {
        explain_metrics: Some(ExplainMetrics::default()),
        ..Default::default()
    };
    expect_that!(serde_json::to_string(&resp_explain), err(anything()));

    // deny_unknown_fields across request and query aggregation types
    expect_that!(
        serde_json::from_value::<RunQueryRequest>(json!({ "unknown": 1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<RunAggregationQueryRequest>(json!({ "unknown": 1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<RunAggregationQueryRequest>(json!({
            "structured_aggregation_query": {}
        })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredAggregationQuery>(json!({ "unknown": 1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<StructuredAggregationQuery>(json!({
            "structured_query": {}
        })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Aggregation>(json!({ "count": {}, "unknown": 1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Count>(json!({ "upTo": "1", "unknown": 1 })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Count>(json!({ "up_to": "1" })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Sum>(json!({
            "field": { "fieldPath": "a" },
            "unknown": 1
        })),
        err(anything())
    );
    expect_that!(
        serde_json::from_value::<Avg>(json!({
            "field": { "fieldPath": "a" },
            "unknown": 1
        })),
        err(anything())
    );
}

mod proptests {
    use proptest::prelude::*;

    use super::*;

    fn arb_valid_timestamp() -> impl Strategy<Value = Timestamp> {
        (
            MIN_TIMESTAMP_SECONDS..=MAX_TIMESTAMP_SECONDS,
            0_i32..1_000_000_000_i32,
        )
            .prop_map(|(seconds, nanos)| Timestamp { seconds, nanos })
    }

    fn arb_protojson_f64() -> impl Strategy<Value = f64> {
        prop_oneof![
            Just(0.0),
            Just(-0.0),
            Just(f64::NAN),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
            (-1_000_000_i64..=1_000_000_i64).prop_map(|n| (n as f64) * 0.0625),
        ]
    }

    fn arb_storable_value() -> impl Strategy<Value = Value> {
        let leaf = prop_oneof![
            Just(Value::null()),
            any::<bool>().prop_map(Value::boolean),
            any::<i64>().prop_map(Value::integer),
            arb_protojson_f64().prop_map(Value::double),
            arb_valid_timestamp().prop_map(Value::timestamp),
            "[a-zA-Z0-9_/-]{0,16}".prop_map(Value::string),
            prop::collection::vec(any::<u8>(), 0..12).prop_map(|b| Value::bytes(Bytes::from(b))),
            "[a-z]{1,4}/[a-z0-9]{1,4}".prop_map(|tail| {
                Value::reference(format!("projects/p/databases/(default)/documents/{tail}"))
            }),
            (-700_i32..=700_i32, -1400_i32..=1400_i32).prop_map(|(lat, lon)| {
                Value {
                    value_type: Some(ValueType::GeoPointValue(LatLng {
                        latitude:  f64::from(lat) * 0.125,
                        longitude: f64::from(lon) * 0.125,
                    })),
                }
            }),
        ];
        leaf.prop_recursive(2, 12, 3, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..3).prop_map(Value::array),
                prop::collection::hash_map("[a-d]{1,3}", inner, 0..3).prop_map(Value::map),
            ]
        })
    }

    fn arb_filter() -> impl Strategy<Value = Filter> {
        let leaf = prop_oneof![
            (
                "[a-d]{1,4}",
                prop_oneof![
                    Just(field_filter::Operator::LessThan as i32),
                    Just(field_filter::Operator::LessThanOrEqual as i32),
                    Just(field_filter::Operator::GreaterThan as i32),
                    Just(field_filter::Operator::GreaterThanOrEqual as i32),
                    Just(field_filter::Operator::Equal as i32),
                    Just(field_filter::Operator::NotEqual as i32),
                    Just(field_filter::Operator::ArrayContains as i32),
                    Just(field_filter::Operator::In as i32),
                ],
                arb_storable_value(),
            )
                .prop_map(|(field_path, op, val)| Filter {
                    filter_type: Some(filter::FilterType::FieldFilter(FieldFilter {
                        field: Some(FieldReference { field_path }),
                        op,
                        value: Some(val),
                    })),
                }),
            (
                "[a-d]{1,4}",
                prop_oneof![
                    Just(unary_filter::Operator::IsNan as i32),
                    Just(unary_filter::Operator::IsNull as i32),
                    Just(unary_filter::Operator::IsNotNan as i32),
                    Just(unary_filter::Operator::IsNotNull as i32),
                ],
            )
                .prop_map(|(field_path, op)| Filter {
                    filter_type: Some(filter::FilterType::UnaryFilter(UnaryFilter {
                        op,
                        operand_type: Some(unary_filter::OperandType::Field(FieldReference {
                            field_path,
                        })),
                    })),
                }),
        ];
        leaf.prop_recursive(2, 6, 3, |inner| {
            (
                prop_oneof![
                    Just(composite_filter::Operator::And as i32),
                    Just(composite_filter::Operator::Or as i32),
                ],
                prop::collection::vec(inner, 1..=3),
            )
                .prop_map(|(op, filters)| Filter {
                    filter_type: Some(filter::FilterType::CompositeFilter(CompositeFilter {
                        op,
                        filters,
                    })),
                })
        })
    }

    fn arb_structured_query() -> impl Strategy<Value = StructuredQuery> {
        (
            prop::option::of(
                prop::collection::vec("[a-d]{1,4}", 1..=3).prop_map(|paths| Projection {
                    fields: paths
                        .into_iter()
                        .map(|field_path| FieldReference { field_path })
                        .collect(),
                }),
            ),
            prop::collection::vec(
                ("[a-d]{1,4}", any::<bool>()).prop_map(|(collection_id, all_descendants)| {
                    CollectionSelector {
                        collection_id,
                        all_descendants,
                    }
                }),
                0..=2,
            ),
            prop::option::of(arb_filter()),
            prop::collection::vec(
                (
                    "[a-d]{1,4}",
                    prop_oneof![
                        Just(Direction::Ascending as i32),
                        Just(Direction::Descending as i32),
                    ],
                )
                    .prop_map(|(field_path, direction)| Order {
                        field: Some(FieldReference { field_path }),
                        direction,
                    }),
                0..=2,
            ),
            prop::option::of(
                (
                    prop::collection::vec(arb_storable_value(), 1..=2),
                    any::<bool>(),
                )
                    .prop_map(|(values, before)| Cursor { values, before }),
            ),
            0_i32..=100_i32,
            prop::option::of(0_i32..=100_i32),
        )
            .prop_map(
                |(select, from, r#where, order_by, start_at, offset, limit)| StructuredQuery {
                    select,
                    from,
                    r#where,
                    order_by,
                    start_at,
                    end_at: None,
                    offset,
                    limit: limit.map(|value| Int32Value { value }),
                    find_nearest: None,
                },
            )
    }

    proptest! {
        #[test]
        fn proptest_document_protojson_roundtrip(
            col in "[a-d]{1,4}",
            doc_id in "[a-z0-9]{1,6}",
            fields in prop::collection::hash_map("[a-f]{1,4}", arb_storable_value(), 0..6),
            create_time in prop::option::of(arb_valid_timestamp()),
            update_time in prop::option::of(arb_valid_timestamp()),
        ) {
            let doc = Document {
                name: format!("projects/p/databases/(default)/documents/{col}/{doc_id}"),
                fields,
                create_time,
                update_time,
            };
            let json_str = serde_json::to_string(&doc).unwrap();
            let roundtripped: Document = serde_json::from_str(&json_str).unwrap();
            prop_assert_eq!(&roundtripped.name, &doc.name);
            prop_assert_eq!(roundtripped.create_time, doc.create_time);
            prop_assert_eq!(roundtripped.update_time, doc.update_time);
            prop_assert!(Value::fields_storage_eq(&roundtripped.fields, &doc.fields));
        }

        #[test]
        fn proptest_structured_query_protojson_roundtrip(sq in arb_structured_query()) {
            let json_str = serde_json::to_string(&sq).unwrap();
            let roundtripped: StructuredQuery = serde_json::from_str(&json_str).unwrap();
            let roundtripped_twice: StructuredQuery =
                serde_json::from_str(&serde_json::to_string(&roundtripped).unwrap()).unwrap();
            prop_assert_eq!(&roundtripped, &sq);
            prop_assert_eq!(&roundtripped_twice, &roundtripped);
        }
    }
}
