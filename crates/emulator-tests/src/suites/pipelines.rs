use std::collections::HashMap;

use googleapis::{fields, google::firestore::v1::Value, val};
use googletest::prelude::*;
use rstest::rstest;
use tonic::Code;

use crate::harness::{
    builders::{PipelineSpec, field_ref_val, fn_val},
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
                .sort_asc("__name__")
                .sort_desc("subtotal")
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

    // Active orders have subtotals: o4=50, o1=30, o2=30 -> after sort_asc(__name__) +
    // sort_desc(subtotal) + offset(1) + limit(2): [o1, o2]
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
            "power": 32i64,
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
#[case::divide_by_zero(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("divide", vec![val!(1i64), val!(0i64)]))]))) ]
#[case::negative_sqrt(PipelineSpec::collection("orders").add_fields(HashMap::from([("z".to_string(), fn_val("sqrt", vec![val!(-1.0f64)]))]))) ]
#[tokio::test]
async fn execute_pipeline_returns_invalid_argument_on_malformed_stages_or_domain_errors(
    #[case] spec: PipelineSpec,
) {
    let mut db = TestEnv::enterprise().build_db();
    db.put("orders/o1", fields!({ "x": 1i64 })).await.unwrap();

    assert_status_code(db.pipeline(spec).await, Code::InvalidArgument);
}
