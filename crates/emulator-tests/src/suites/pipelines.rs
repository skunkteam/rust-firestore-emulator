use std::collections::HashMap;

use googleapis::{
    fields,
    google::firestore::v1::{Pipeline, Value, value::ValueType},
    val,
};
use googletest::prelude::*;
use rstest::rstest;
use tonic::Code;

use crate::harness::{
    builders::{PipelineSpec, SortDirection, doc_path, field_ref_val, fn_val},
    env::TestEnv,
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

#[gtest]
#[tokio::test]
async fn execute_pipeline_covers_all_ten_implemented_stages_and_expression_functions() {
    let mut db = TestEnv::enterprise().build_db();

    db.put(
        "orders/o1",
        fields!({
            "customer": "Alice",
            "region": "eu",
            "qty": 2i64,
            "price": 15i64,
            "discount": 4.25f64,
            "active": true,
            "tags": ["vip", "fast"],
            "csv": "red,green,blue",
        }),
    )
    .await
    .unwrap();
    db.put(
        "orders/o2",
        fields!({
            "customer": "Bob",
            "region": "eu",
            "qty": 3i64,
            "price": 10i64,
            "discount": -2.5f64,
            "active": true,
            "tags": ["std"],
            "csv": "mono",
        }),
    )
    .await
    .unwrap();
    db.put(
        "orders/o3",
        fields!({
            "customer": "Cara",
            "region": "us",
            "qty": 5i64,
            "price": 20i64,
            "discount": 9.0f64,
            "active": false,
            "tags": ["vip"],
            "csv": "a,b",
        }),
    )
    .await
    .unwrap();
    db.put(
        "orders/o4",
        fields!({
            "customer": "Dan",
            "region": "us",
            "qty": 1i64,
            "price": 50i64,
            "active": true,
            "tags": ["new"],
            "csv": "x,y",
        }),
    )
    .await
    .unwrap();

    // Pipeline 1: `collection` -> `where` (and/or/not + comparison) -> `add_fields` (math, string,
    // array, is_absent, is_error) -> `remove_fields` -> `sort` -> `offset` -> `limit` -> `select`
    let p1 = db
        .pipeline(
            PipelineSpec::collection("orders")
                .where_expr(fn_val(
                    "and",
                    vec![
                        field_ref_val("active"),
                        fn_val(
                            "or",
                            vec![
                                fn_val(
                                    "greater_than_or_equal",
                                    vec![field_ref_val("price"), val!(10i64)],
                                ),
                                fn_val("not", vec![val!(true)]),
                            ],
                        ),
                    ],
                ))
                .add_fields(HashMap::from([
                    (
                        "subtotal".to_string(),
                        fn_val(
                            "multiply",
                            vec![field_ref_val("qty"), field_ref_val("price")],
                        ),
                    ),
                    (
                        "upper_name".to_string(),
                        fn_val("to_upper", vec![field_ref_val("customer")]),
                    ),
                    (
                        "lower_region".to_string(),
                        fn_val("to_lower", vec![field_ref_val("region")]),
                    ),
                    (
                        "label".to_string(),
                        fn_val(
                            "string_concat",
                            vec![
                                field_ref_val("customer"),
                                val!(":"),
                                field_ref_val("region"),
                            ],
                        ),
                    ),
                    (
                        "name_len".to_string(),
                        fn_val("char_length", vec![field_ref_val("customer")]),
                    ),
                    (
                        "parts".to_string(),
                        fn_val("split", vec![field_ref_val("csv"), val!(",")]),
                    ),
                    (
                        "tag_count".to_string(),
                        fn_val("array_length", vec![field_ref_val("tags")]),
                    ),
                    (
                        "merged_tags".to_string(),
                        fn_val(
                            "array_concat",
                            vec![field_ref_val("tags"), fn_val("array", vec![val!("extra")])],
                        ),
                    ),
                    (
                        "no_discount".to_string(),
                        fn_val("is_absent", vec![field_ref_val("discount")]),
                    ),
                    (
                        "div_zero_caught".to_string(),
                        fn_val(
                            "is_error",
                            vec![fn_val("divide", vec![val!(10i64), val!(0i64)])],
                        ),
                    ),
                ]))
                .remove_fields(["csv"])
                .sort([
                    ("subtotal", SortDirection::Desc),
                    ("__name__", SortDirection::Asc),
                ])
                .offset(1)
                .limit(2)
                .select(HashMap::from([
                    ("label".to_string(), field_ref_val("label")),
                    ("subtotal".to_string(), field_ref_val("subtotal")),
                    ("upper_name".to_string(), field_ref_val("upper_name")),
                    ("lower_region".to_string(), field_ref_val("lower_region")),
                    ("name_len".to_string(), field_ref_val("name_len")),
                    ("parts".to_string(), field_ref_val("parts")),
                    ("tag_count".to_string(), field_ref_val("tag_count")),
                    ("merged_tags".to_string(), field_ref_val("merged_tags")),
                    ("no_discount".to_string(), field_ref_val("no_discount")),
                    (
                        "div_zero_caught".to_string(),
                        field_ref_val("div_zero_caught"),
                    ),
                ])),
        )
        .await
        .expect("ExecutePipeline multi-stage");

    // Active orders have subtotals: o4=50, o1=30, o2=30 -> after sort(subtotal desc, __name__ asc)
    // + offset(1) + limit(2): [o1, o2]
    expect_that!(p1.execution_time, some(anything()));
    expect_eq!(short_ids(&p1.results), vec!["o1", "o2"]);
    assert_doc_wire_eq(
        &p1.results[0],
        "orders/o1",
        &fields!({
            "label": "Alice:eu",
            "subtotal": 30i64,
            "upper_name": "ALICE",
            "lower_region": "eu",
            "name_len": 5i64,
            "parts": ["red", "green", "blue"],
            "tag_count": 2i64,
            "merged_tags": ["vip", "fast", "extra"],
            "no_discount": false,
            "div_zero_caught": true,
        }),
    );

    // Pipeline 2: remaining numeric functions (`add`, `subtract`, `divide`, `mod`, `pow`, `abs`,
    // `ceil`, `floor`, `round`, `sqrt`, `log10`, `ln`) and comparisons (`equal`, `not_equal`,
    // `less_than`, `less_than_or_equal`, `greater_than`)
    let math_out = db
        .pipeline(
            PipelineSpec::collection("orders")
                .where_expr(fn_val(
                    "and",
                    vec![
                        fn_val("equal", vec![field_ref_val("customer"), val!("Alice")]),
                        fn_val(
                            "and",
                            vec![
                                fn_val("not_equal", vec![field_ref_val("qty"), val!(99i64)]),
                                fn_val(
                                    "and",
                                    vec![
                                        fn_val(
                                            "less_than",
                                            vec![field_ref_val("qty"), val!(10i64)],
                                        ),
                                        fn_val(
                                            "and",
                                            vec![
                                                fn_val(
                                                    "less_than_or_equal",
                                                    vec![field_ref_val("qty"), val!(2i64)],
                                                ),
                                                fn_val(
                                                    "greater_than",
                                                    vec![field_ref_val("price"), val!(10i64)],
                                                ),
                                            ],
                                        ),
                                    ],
                                ),
                            ],
                        ),
                    ],
                ))
                .select(HashMap::from([
                    (
                        "sum2".to_string(),
                        fn_val("add", vec![val!(7i64), val!(3i64)]),
                    ),
                    (
                        "diff".to_string(),
                        fn_val("subtract", vec![val!(10i64), val!(4i64)]),
                    ),
                    (
                        "quot".to_string(),
                        fn_val("divide", vec![val!(20i64), val!(4i64)]),
                    ),
                    (
                        "rem".to_string(),
                        fn_val("mod", vec![val!(17i64), val!(5i64)]),
                    ),
                    (
                        "power".to_string(),
                        fn_val("pow", vec![val!(2i64), val!(5i64)]),
                    ),
                    ("abs_v".to_string(), fn_val("abs", vec![val!(-3.5f64)])),
                    ("ceil_v".to_string(), fn_val("ceil", vec![val!(4.25f64)])),
                    ("floor_v".to_string(), fn_val("floor", vec![val!(4.75f64)])),
                    ("round_v".to_string(), fn_val("round", vec![val!(4.6f64)])),
                    ("sqrt_v".to_string(), fn_val("sqrt", vec![val!(9.0f64)])),
                    ("log10_v".to_string(), fn_val("log10", vec![val!(100.0f64)])),
                    ("ln_one".to_string(), fn_val("ln", vec![val!(1.0f64)])),
                ])),
        )
        .await
        .unwrap();
    expect_eq!(math_out.results.len(), 1);
    assert_doc_wire_eq(
        &math_out.results[0],
        "orders/o1",
        &fields!({
            "sum2": 10i64,
            "diff": 6i64,
            "quot": 5i64,
            "rem": 2i64,
            "power": 32.0f64,
            "abs_v": 3.5f64,
            "ceil_v": 5.0f64,
            "floor_v": 4.0f64,
            "round_v": 5.0f64,
            "sqrt_v": 3.0f64,
            "log10_v": 2.0f64,
            "ln_one": 0.0f64,
        }),
    );

    // Pipeline 3: `distinct` and `aggregate` (`count`, `sum`, `average`, `minimum`, `maximum` +
    // `group_by`)
    let distinct_regions = db
        .pipeline(
            PipelineSpec::collection("orders")
                .sort_asc("region")
                .distinct(HashMap::from([(
                    "region".to_string(),
                    field_ref_val("region"),
                )])),
        )
        .await
        .unwrap();
    expect_eq!(distinct_regions.results.len(), 2);
    expect_eq!(
        distinct_regions.results[0].fields.get("region"),
        Some(&val!("eu"))
    );
    expect_eq!(
        distinct_regions.results[1].fields.get("region"),
        Some(&val!("us"))
    );

    let grouped = db
        .pipeline(PipelineSpec::collection("orders").aggregate(
            HashMap::from([
                ("n".to_string(), fn_val("count", vec![])),
                (
                    "total_qty".to_string(),
                    fn_val("sum", vec![field_ref_val("qty")]),
                ),
                (
                    "avg_price".to_string(),
                    fn_val("average", vec![field_ref_val("price")]),
                ),
                (
                    "min_price".to_string(),
                    fn_val("minimum", vec![field_ref_val("price")]),
                ),
                (
                    "max_price".to_string(),
                    fn_val("maximum", vec![field_ref_val("price")]),
                ),
            ]),
            Some(HashMap::from([(
                "region".to_string(),
                field_ref_val("region"),
            )])),
        ))
        .await
        .unwrap();
    expect_eq!(grouped.results.len(), 2);
    // Bucket 0 is `eu` (sorted by group key): o1 (qty=2, price=15), o2 (qty=3, price=10)
    expect_eq!(
        grouped.results[0].fields,
        fields!({
            "region": "eu",
            "n": 2i64,
            "total_qty": 5i64,
            "avg_price": 12.5f64,
            "min_price": 10i64,
            "max_price": 15i64,
        })
    );
    // Bucket 1 is `us`: o3 (qty=5, price=20), o4 (qty=1, price=50)
    expect_eq!(
        grouped.results[1].fields,
        fields!({
            "region": "us",
            "n": 2i64,
            "total_qty": 6i64,
            "avg_price": 35.0f64,
            "min_price": 20i64,
            "max_price": 50i64,
        })
    );
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_rejects_standard_edition_with_failed_precondition() {
    let mut db = TestEnv::new().build_db(); // STANDARD edition
    db.put("orders/o1", fields!({ "x": 1i64 })).await.unwrap();

    assert_status_code(
        db.pipeline(PipelineSpec::collection("orders")).await,
        Code::FailedPrecondition,
    );
}

#[rstest]
#[case::collection_group(PipelineSpec::empty().raw_stage("collection_group", vec![val!("orders")]))]
#[case::documents(PipelineSpec::empty().raw_stage("documents", vec![Value::reference("/orders/o1".to_string())]))]
#[case::unknown_followup_stage(PipelineSpec::collection("orders").raw_stage("sample", vec![val!(1i64)]))]
#[case::unknown_function(PipelineSpec::collection("orders").add_fields(HashMap::from([("x".to_string(), fn_val("regex_match", vec![val!("a")]))])))]
#[case::pipeline_value_direct(PipelineSpec::collection("orders").add_fields(HashMap::from([(
    "x".to_string(),
    Value {
        value_type: Some(ValueType::PipelineValue(Pipeline::default())),
    },
)])))]
#[case::pipeline_value_inside_is_error(PipelineSpec::collection("orders").add_fields(HashMap::from([(
    "x".to_string(),
    fn_val(
        "is_error",
        vec![Value {
            value_type: Some(ValueType::PipelineValue(Pipeline::default())),
        }],
    ),
)])))]
#[tokio::test]
async fn execute_pipeline_returns_unimplemented_on_unsupported_stages_and_functions(
    #[case] spec: PipelineSpec,
) {
    let mut db = TestEnv::enterprise().build_db();
    db.put("orders/o1", fields!({ "x": 1i64 })).await.unwrap();

    assert_status_code(db.pipeline(spec).await, Code::Unimplemented);
}

#[rstest]
#[case::empty_stages(PipelineSpec::empty())]
#[case::divide_by_zero(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("divide", vec![val!(1i64), val!(0i64)]))])))]
#[case::negative_sqrt(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("sqrt", vec![val!(-1.0f64)]))])))]
#[case::modulo_by_zero(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("mod", vec![val!(1i64), val!(0i64)]))])))]
#[case::divide_i64_min_neg_one(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("divide", vec![val!(i64::MIN), val!(-1i64)]))])))]
#[case::mod_i64_min_neg_one(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("mod", vec![val!(i64::MIN), val!(-1i64)]))])))]
#[case::add_i64_overflow(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("add", vec![val!(i64::MAX), val!(1i64)]))])))]
#[case::subtract_i64_overflow(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("subtract", vec![val!(i64::MIN), val!(1i64)]))])))]
#[case::multiply_i64_overflow(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("multiply", vec![val!(i64::MAX), val!(2i64)]))])))]
#[case::abs_i64_min(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("abs", vec![val!(i64::MIN)]))])))]
#[case::add_three_args(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("add", vec![val!(1i64), val!(2i64), val!(3i64)]))])))]
#[case::multiply_three_args(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("multiply", vec![val!(2i64), val!(3i64), val!(4i64)]))])))]
#[case::pow_zero_neg_one(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("pow", vec![val!(0i64), val!(-1i64)]))])))]
#[case::pow_negative_fractional(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("pow", vec![val!(-2.0f64), val!(0.5f64)]))])))]
#[case::where_non_boolean(PipelineSpec::collection("orders").where_expr(field_ref_val("x")))]
#[case::aggregate_non_map_second_arg(PipelineSpec::collection("orders").raw_stage("aggregate", vec![val!({}), val!(123i64)]))]
#[case::sort_empty_args(PipelineSpec::collection("orders").raw_stage("sort", vec![]))]
#[case::sort_invalid_direction(PipelineSpec::collection("orders").raw_stage("sort", vec![val!({ "expression": field_ref_val("x"), "direction": "sideways" })]))]
#[case::select_conflicting_prefix(PipelineSpec::collection("orders").select(HashMap::from([("a".to_string(), val!(1i64)), ("a.b".to_string(), val!(2i64))])))]
#[case::add_fields_conflicting_prefix(PipelineSpec::collection("orders").add_fields(HashMap::from([("a".to_string(), val!(1i64)), ("`a`.b".to_string(), val!(2i64))])))]
#[case::collection_empty_path(PipelineSpec::empty().raw_stage("collection", vec![val!("")]))]
#[case::collection_root_slash(PipelineSpec::empty().raw_stage("collection", vec![val!("/")]))]
#[case::collection_even_segments_doc_path(PipelineSpec::empty().raw_stage("collection", vec![val!("/orders/o1")]))]
#[case::collection_empty_segment(PipelineSpec::empty().raw_stage("collection", vec![val!("/a//b")]))]
#[case::collection_reserved_segment(PipelineSpec::empty().raw_stage("collection", vec![val!("/__reserved__")]))]
#[tokio::test]
async fn execute_pipeline_returns_invalid_argument_on_malformed_stages_or_domain_errors(
    #[case] spec: PipelineSpec,
) {
    let mut db = TestEnv::enterprise().build_db();
    db.put("orders/o1", fields!({ "x": 1i64 })).await.unwrap();

    assert_status_code(db.pipeline(spec).await, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_arithmetic_precision_pow_and_is_error() {
    let mut db = TestEnv::enterprise().build_db();
    let large = 9_007_199_254_740_993_i64;
    db.put("nums/n1", fields!({ "large": large, "neg_large": -large }))
        .await
        .unwrap();

    let out = db
        .pipeline(PipelineSpec::collection("nums").select(HashMap::from([
            (
                "pow_neg".to_string(),
                fn_val("pow", vec![val!(2i64), val!(-1i64)]),
            ),
            (
                "abs_large".to_string(),
                fn_val("abs", vec![field_ref_val("neg_large")]),
            ),
            (
                "ceil_large".to_string(),
                fn_val("ceil", vec![field_ref_val("large")]),
            ),
            (
                "floor_large".to_string(),
                fn_val("floor", vec![field_ref_val("large")]),
            ),
            (
                "round_large".to_string(),
                fn_val("round", vec![field_ref_val("large")]),
            ),
            (
                "add_mixed".to_string(),
                fn_val("add", vec![val!(1i64), val!(0.5f64)]),
            ),
            (
                "pow_ovf".to_string(),
                fn_val("pow", vec![val!(1.0e200f64), val!(2.0f64)]),
            ),
            (
                "pow_big_exp".to_string(),
                fn_val("pow", vec![val!(2i64), val!(1i64 << 32)]),
            ),
            (
                "err_add".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("add", vec![val!(i64::MAX), val!(1i64)])],
                ),
            ),
            (
                "err_sub".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("subtract", vec![val!(i64::MIN), val!(1i64)])],
                ),
            ),
            (
                "err_mul".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("multiply", vec![val!(i64::MAX), val!(2i64)])],
                ),
            ),
            (
                "err_div_min".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("divide", vec![val!(i64::MIN), val!(-1i64)])],
                ),
            ),
            (
                "err_mod_min".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("mod", vec![val!(i64::MIN), val!(-1i64)])],
                ),
            ),
            (
                "err_abs_min".to_string(),
                fn_val("is_error", vec![fn_val("abs", vec![val!(i64::MIN)])]),
            ),
            (
                "err_pow_ovf".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("pow", vec![val!(1.0e200f64), val!(2.0f64)])],
                ),
            ),
            (
                "err_pow_zero".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("pow", vec![val!(0i64), val!(-1i64)])],
                ),
            ),
            (
                "err_pow_neg".to_string(),
                fn_val(
                    "is_error",
                    vec![fn_val("pow", vec![val!(-2.0f64), val!(0.5f64)])],
                ),
            ),
        ])))
        .await
        .unwrap();

    expect_eq!(out.results.len(), 1);
    assert_doc_wire_eq(
        &out.results[0],
        "nums/n1",
        &fields!({
            "pow_neg": 0.5f64,
            "abs_large": large,
            "ceil_large": large,
            "floor_large": large,
            "round_large": large,
            "add_mixed": 1.5f64,
            "pow_ovf": f64::INFINITY,
            "pow_big_exp": f64::INFINITY,
            "err_add": true,
            "err_sub": true,
            "err_mul": true,
            "err_div_min": true,
            "err_mod_min": true,
            "err_abs_min": true,
            "err_pow_ovf": false,
            "err_pow_zero": true,
            "err_pow_neg": true,
        }),
    );
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_boolean_short_circuit_and_kleene_logic() {
    let mut db = TestEnv::enterprise().build_db();
    db.put("flags/f1", fields!({ "n": null })).await.unwrap();

    let div_zero_eq = fn_val(
        "equal",
        vec![fn_val("divide", vec![val!(1i64), val!(0i64)]), val!(1i64)],
    );

    let out = db
        .pipeline(PipelineSpec::collection("flags").select(HashMap::from([
            (
                "and_var".to_string(),
                fn_val("and", vec![val!(true), val!(true), val!(false)]),
            ),
            (
                "or_var".to_string(),
                fn_val("or", vec![val!(false), val!(false), val!(true)]),
            ),
            (
                "and_sc".to_string(),
                fn_val("and", vec![val!(false), div_zero_eq.clone()]),
            ),
            (
                "or_sc".to_string(),
                fn_val("or", vec![val!(true), div_zero_eq]),
            ),
            (
                "not_null".to_string(),
                fn_val("not", vec![field_ref_val("n")]),
            ),
            (
                "not_absent".to_string(),
                fn_val("not", vec![field_ref_val("missing")]),
            ),
            (
                "and_false_null".to_string(),
                fn_val("and", vec![val!(false), field_ref_val("n")]),
            ),
            (
                "and_null_false".to_string(),
                fn_val("and", vec![field_ref_val("n"), val!(false)]),
            ),
            (
                "and_true_null".to_string(),
                fn_val("and", vec![val!(true), field_ref_val("n")]),
            ),
            (
                "and_absent_true".to_string(),
                fn_val("and", vec![field_ref_val("missing"), val!(true)]),
            ),
            (
                "or_true_null".to_string(),
                fn_val("or", vec![val!(true), field_ref_val("n")]),
            ),
            (
                "or_null_true".to_string(),
                fn_val("or", vec![field_ref_val("n"), val!(true)]),
            ),
            (
                "or_false_null".to_string(),
                fn_val("or", vec![val!(false), field_ref_val("n")]),
            ),
            (
                "or_false_absent".to_string(),
                fn_val("or", vec![val!(false), field_ref_val("missing")]),
            ),
        ])))
        .await
        .unwrap();

    expect_eq!(out.results.len(), 1);
    assert_doc_wire_eq(
        &out.results[0],
        "flags/f1",
        &fields!({
            "and_var": false,
            "or_var": true,
            "and_sc": false,
            "or_sc": true,
            "not_null": null,
            "not_absent": null,
            "and_false_null": false,
            "and_null_false": false,
            "and_true_null": null,
            "and_absent_true": null,
            "or_true_null": true,
            "or_null_true": true,
            "or_false_null": null,
            "or_false_absent": null,
        }),
    );
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_where_filter_array_null_and_split() {
    let mut db = TestEnv::enterprise().build_db();
    db.put(
        "items/d1",
        fields!({ "flag": true, "n": null, "arr": [1i64] }),
    )
    .await
    .unwrap();
    db.put("items/d2", fields!({ "flag": false }))
        .await
        .unwrap();
    db.put("items/d3", fields!({ "flag": null })).await.unwrap();
    db.put("items/d4", fields!({ "other": 1i64 }))
        .await
        .unwrap();

    let all_true = db
        .pipeline(
            PipelineSpec::collection("items")
                .where_expr(val!(true))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(all_true.results.len(), 4);

    let filtered = db
        .pipeline(
            PipelineSpec::collection("items")
                .where_expr(field_ref_val("flag"))
                .select(HashMap::from([
                    (
                        "len_null".to_string(),
                        fn_val("array_length", vec![field_ref_val("n")]),
                    ),
                    (
                        "len_absent".to_string(),
                        fn_val("array_length", vec![field_ref_val("missing")]),
                    ),
                    (
                        "cat_null".to_string(),
                        fn_val(
                            "array_concat",
                            vec![field_ref_val("arr"), field_ref_val("n")],
                        ),
                    ),
                    (
                        "cat_absent".to_string(),
                        fn_val(
                            "array_concat",
                            vec![field_ref_val("arr"), field_ref_val("missing")],
                        ),
                    ),
                    (
                        "split_chars".to_string(),
                        fn_val("split", vec![val!("abc"), val!("")]),
                    ),
                    (
                        "split_empty".to_string(),
                        fn_val("split", vec![val!(""), val!("")]),
                    ),
                ])),
        )
        .await
        .unwrap();

    expect_eq!(filtered.results.len(), 1);
    assert_doc_wire_eq(
        &filtered.results[0],
        "items/d1",
        &fields!({
            "len_null": null,
            "len_absent": null,
            "cat_null": null,
            "cat_absent": null,
            "split_chars": ["a", "b", "c"],
            "split_empty": [""],
        }),
    );
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_aggregates_and_expression_sort() {
    let mut db = TestEnv::enterprise().build_db();
    db.put(
        "line_items/d2",
        fields!({ "id": 2i64, "tag": null, "opt": null, "qty": 3i64, "price": 5i64 }),
    )
    .await
    .unwrap();
    db.put(
        "line_items/d1",
        fields!({ "id": 1i64, "opt": 10i64, "qty": 2i64, "price": 10i64 }),
    )
    .await
    .unwrap();
    db.put(
        "line_items/d3",
        fields!({ "id": 3i64, "tag": "a", "opt": 20i64, "qty": 1i64, "price": 100i64 }),
    )
    .await
    .unwrap();

    let agg = db
        .pipeline(PipelineSpec::collection("line_items").aggregate(
            HashMap::from([
                ("total".to_string(), fn_val("count", vec![])),
                (
                    "non_null_opt".to_string(),
                    fn_val("count", vec![field_ref_val("opt")]),
                ),
                (
                    "min_opt".to_string(),
                    fn_val("minimum", vec![field_ref_val("opt")]),
                ),
                (
                    "max_opt".to_string(),
                    fn_val("maximum", vec![field_ref_val("opt")]),
                ),
                (
                    "sum_rev".to_string(),
                    fn_val(
                        "sum",
                        vec![fn_val(
                            "multiply",
                            vec![field_ref_val("qty"), field_ref_val("price")],
                        )],
                    ),
                ),
                (
                    "avg_rev".to_string(),
                    fn_val(
                        "average",
                        vec![fn_val(
                            "multiply",
                            vec![field_ref_val("qty"), field_ref_val("price")],
                        )],
                    ),
                ),
                (
                    "min_rev".to_string(),
                    fn_val(
                        "minimum",
                        vec![fn_val(
                            "multiply",
                            vec![field_ref_val("qty"), field_ref_val("price")],
                        )],
                    ),
                ),
                (
                    "max_rev".to_string(),
                    fn_val(
                        "maximum",
                        vec![fn_val(
                            "multiply",
                            vec![field_ref_val("qty"), field_ref_val("price")],
                        )],
                    ),
                ),
            ]),
            None,
        ))
        .await
        .unwrap();

    expect_eq!(agg.results.len(), 1);
    expect_eq!(
        agg.results[0].fields,
        fields!({
            "total": 3i64,
            "non_null_opt": 2i64,
            "min_opt": 10i64,
            "max_opt": 20i64,
            "sum_rev": 135i64,
            "avg_rev": 45.0f64,
            "min_rev": 15i64,
            "max_rev": 100i64,
        })
    );

    let sorted_tag = db
        .pipeline(
            PipelineSpec::collection("line_items")
                .sort([("tag", SortDirection::Asc), ("id", SortDirection::Asc)])
                .select(HashMap::from([("id".to_string(), field_ref_val("id"))])),
        )
        .await
        .unwrap();
    let tag_ids: Vec<_> = sorted_tag
        .results
        .iter()
        .map(|d| d.name.rsplit('/').next().unwrap())
        .collect();
    expect_eq!(tag_ids, vec!["d1", "d2", "d3"]);

    let sorted_tag_desc = db
        .pipeline(
            PipelineSpec::collection("line_items")
                .sort([("tag", SortDirection::Desc), ("id", SortDirection::Asc)])
                .select(HashMap::from([("id".to_string(), field_ref_val("id"))])),
        )
        .await
        .unwrap();
    let tag_desc_ids: Vec<_> = sorted_tag_desc
        .results
        .iter()
        .map(|d| d.name.rsplit('/').next().unwrap())
        .collect();
    expect_eq!(tag_desc_ids, vec!["d3", "d1", "d2"]);

    let sorted_expr = db
        .pipeline(PipelineSpec::collection("line_items").raw_stage(
            "sort",
            vec![val!({
                "expression": fn_val("multiply", vec![field_ref_val("qty"), field_ref_val("price")]),
                "direction": "descending",
            })],
        ))
        .await
        .unwrap();
    let rev_ids: Vec<_> = sorted_expr
        .results
        .iter()
        .map(|d| d.name.rsplit('/').next().unwrap())
        .collect();
    expect_eq!(rev_ids, vec!["d3", "d1", "d2"]);
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_nested_and_escaped_projection_mutations() {
    let mut db = TestEnv::enterprise().build_db();
    db.put(
        "profiles/p1",
        fields!({
            "user": { "name": "Ada", "secret": "hidden" },
            "a.b": "literal",
        }),
    )
    .await
    .unwrap();

    let added = db
        .pipeline(
            PipelineSpec::collection("profiles")
                .add_fields(HashMap::from([
                    ("user.role".to_string(), val!("admin")),
                    ("prefs.theme".to_string(), val!("dark")),
                ]))
                .remove_fields(["user.secret", "`a.b`", "__name__"]),
        )
        .await
        .unwrap();

    expect_eq!(added.results.len(), 1);
    expect_eq!(added.results[0].name, "");
    expect_eq!(
        added.results[0].fields,
        fields!({
            "user": { "name": "Ada", "role": "admin" },
            "prefs": { "theme": "dark" },
        })
    );

    let selected = db
        .pipeline(PipelineSpec::collection("profiles").select(HashMap::from([
            ("user.name".to_string(), field_ref_val("user.name")),
            ("`a.b`".to_string(), field_ref_val("`a.b`")),
        ])))
        .await
        .unwrap();

    expect_eq!(selected.results.len(), 1);
    assert_doc_wire_eq(
        &selected.results[0],
        "profiles/p1",
        &fields!({
            "user": { "name": "Ada" },
            "a.b": "literal",
        }),
    );
}

#[gtest]
#[tokio::test]
async fn execute_pipeline_absent_null_nan_and_cross_type_comparisons() {
    let mut db = TestEnv::enterprise().build_db();
    db.put(
        "cmp_docs/d_absent",
        fields!({
            "num": 5i64,
            "str": "abc",
            "arr_num": [1i64, 2i64],
            "arr_str": [1i64, "a"],
        }),
    )
    .await
    .unwrap();
    db.put(
        "cmp_docs/d_null",
        fields!({
            "opt": null,
            "opt2": null,
            "num": 5i64,
            "nan1": f64::NAN,
            "nan2": f64::NAN,
        }),
    )
    .await
    .unwrap();
    db.put("cmp_docs/d_val", fields!({ "opt": 3i64, "num": 5i64 }))
        .await
        .unwrap();

    let absent_matrix = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val(
                    "equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("cmp_docs/d_absent")),
                    ],
                ))
                .select(HashMap::from([
                    (
                        "abs_abs_eq".to_string(),
                        fn_val("equal", vec![field_ref_val("m1"), field_ref_val("m2")]),
                    ),
                    (
                        "abs_abs_ne".to_string(),
                        fn_val("not_equal", vec![field_ref_val("m1"), field_ref_val("m2")]),
                    ),
                    (
                        "abs_abs_lt".to_string(),
                        fn_val("less_than", vec![field_ref_val("m1"), field_ref_val("m2")]),
                    ),
                    (
                        "abs_abs_lte".to_string(),
                        fn_val(
                            "less_than_or_equal",
                            vec![field_ref_val("m1"), field_ref_val("m2")],
                        ),
                    ),
                    (
                        "abs_abs_gt".to_string(),
                        fn_val(
                            "greater_than",
                            vec![field_ref_val("m1"), field_ref_val("m2")],
                        ),
                    ),
                    (
                        "abs_abs_gte".to_string(),
                        fn_val(
                            "greater_than_or_equal",
                            vec![field_ref_val("m1"), field_ref_val("m2")],
                        ),
                    ),
                    (
                        "abs_null_eq".to_string(),
                        fn_val("equal", vec![field_ref_val("m1"), val!(null)]),
                    ),
                    (
                        "abs_null_ne".to_string(),
                        fn_val("not_equal", vec![field_ref_val("m1"), val!(null)]),
                    ),
                    (
                        "null_abs_lt".to_string(),
                        fn_val("less_than", vec![val!(null), field_ref_val("m1")]),
                    ),
                    (
                        "null_abs_lte".to_string(),
                        fn_val("less_than_or_equal", vec![val!(null), field_ref_val("m1")]),
                    ),
                    (
                        "abs_five_lt".to_string(),
                        fn_val("less_than", vec![field_ref_val("m1"), field_ref_val("num")]),
                    ),
                    (
                        "five_abs_gt".to_string(),
                        fn_val(
                            "greater_than",
                            vec![field_ref_val("num"), field_ref_val("m1")],
                        ),
                    ),
                    (
                        "abs_five_ne".to_string(),
                        fn_val("not_equal", vec![field_ref_val("m1"), field_ref_val("num")]),
                    ),
                    (
                        "five_str_lt".to_string(),
                        fn_val(
                            "less_than",
                            vec![field_ref_val("num"), field_ref_val("str")],
                        ),
                    ),
                    (
                        "five_str_gt".to_string(),
                        fn_val(
                            "greater_than",
                            vec![field_ref_val("num"), field_ref_val("str")],
                        ),
                    ),
                    (
                        "five_str_ne".to_string(),
                        fn_val(
                            "not_equal",
                            vec![field_ref_val("num"), field_ref_val("str")],
                        ),
                    ),
                    (
                        "arr_cross_lt".to_string(),
                        fn_val(
                            "less_than",
                            vec![field_ref_val("arr_num"), field_ref_val("arr_str")],
                        ),
                    ),
                ])),
        )
        .await
        .unwrap();

    expect_eq!(absent_matrix.results.len(), 1);
    assert_doc_wire_eq(
        &absent_matrix.results[0],
        "cmp_docs/d_absent",
        &fields!({
            "abs_abs_eq": true,
            "abs_abs_ne": false,
            "abs_abs_lt": false,
            "abs_abs_lte": true,
            "abs_abs_gt": false,
            "abs_abs_gte": true,
            "abs_null_eq": false,
            "abs_null_ne": true,
            "null_abs_lt": false,
            "null_abs_lte": false,
            "abs_five_lt": false,
            "five_abs_gt": false,
            "abs_five_ne": true,
            "five_str_lt": false,
            "five_str_gt": false,
            "five_str_ne": true,
            "arr_cross_lt": true,
        }),
    );

    let null_nan_matrix = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val(
                    "equal",
                    vec![
                        field_ref_val("__name__"),
                        Value::reference(doc_path("cmp_docs/d_null")),
                    ],
                ))
                .select(HashMap::from([
                    (
                        "null_null_eq".to_string(),
                        fn_val("equal", vec![field_ref_val("opt"), field_ref_val("opt2")]),
                    ),
                    (
                        "null_null_lte".to_string(),
                        fn_val(
                            "less_than_or_equal",
                            vec![field_ref_val("opt"), field_ref_val("opt2")],
                        ),
                    ),
                    (
                        "null_five_lt".to_string(),
                        fn_val(
                            "less_than",
                            vec![field_ref_val("opt"), field_ref_val("num")],
                        ),
                    ),
                    (
                        "null_five_ne".to_string(),
                        fn_val(
                            "not_equal",
                            vec![field_ref_val("opt"), field_ref_val("num")],
                        ),
                    ),
                    (
                        "nan_nan_eq".to_string(),
                        fn_val("equal", vec![field_ref_val("nan1"), field_ref_val("nan2")]),
                    ),
                    (
                        "nan_nan_lte".to_string(),
                        fn_val(
                            "less_than_or_equal",
                            vec![field_ref_val("nan1"), field_ref_val("nan2")],
                        ),
                    ),
                    (
                        "nan_five_lt".to_string(),
                        fn_val(
                            "less_than",
                            vec![field_ref_val("nan1"), field_ref_val("num")],
                        ),
                    ),
                    (
                        "five_nan_gt".to_string(),
                        fn_val(
                            "greater_than",
                            vec![field_ref_val("num"), field_ref_val("nan1")],
                        ),
                    ),
                    (
                        "nan_five_ne".to_string(),
                        fn_val(
                            "not_equal",
                            vec![field_ref_val("nan1"), field_ref_val("num")],
                        ),
                    ),
                ])),
        )
        .await
        .unwrap();

    expect_eq!(null_nan_matrix.results.len(), 1);
    assert_doc_wire_eq(
        &null_nan_matrix.results[0],
        "cmp_docs/d_null",
        &fields!({
            "null_null_eq": true,
            "null_null_lte": true,
            "null_five_lt": false,
            "null_five_ne": true,
            "nan_nan_eq": true,
            "nan_nan_lte": true,
            "nan_five_lt": false,
            "five_nan_gt": false,
            "nan_five_ne": true,
        }),
    );

    let where_missing_eq_null = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val("equal", vec![field_ref_val("missing"), val!(null)])),
        )
        .await
        .unwrap();
    expect_eq!(where_missing_eq_null.results.len(), 0);

    let where_opt_eq_null = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val("equal", vec![field_ref_val("opt"), val!(null)])),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&where_opt_eq_null.results), vec!["d_null"]);

    let where_opt_ne_null = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val("not_equal", vec![field_ref_val("opt"), val!(null)]))
                .sort_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&where_opt_ne_null.results),
        vec!["d_absent", "d_val"]
    );

    let where_opt_lt_5 = db
        .pipeline(
            PipelineSpec::collection("cmp_docs")
                .where_expr(fn_val("less_than", vec![field_ref_val("opt"), val!(5i64)])),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&where_opt_lt_5.results), vec!["d_val"]);
}
