use googleapis::{
    fields,
    google::firestore::v1::{
        RunQueryRequest, StructuredQuery, Value, run_query_request,
        structured_query::{
            CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference, Filter,
            FindNearest, Order, UnaryFilter, composite_filter, field_filter::Operator,
            filter::FilterType, unary_filter,
        },
        value::ValueType,
    },
    val,
};
use googletest::prelude::*;
use rstest::rstest;
use tonic::Code;

use crate::harness::{
    builders::{QuerySpec, and_filters, doc_path, field_filter, or_filters, unary_filter},
    env::{DOCUMENTS_ROOT, TestEnv},
    matchers::{assert_doc_semantic_eq, assert_doc_wire_eq, assert_status_code, short_ids},
    stream::collect_run_query,
};

#[gtest]
#[tokio::test]
async fn structured_query_filters_cross_type_numerics_cursors_offsets_and_projections() {
    let mut db = TestEnv::new().build_db();

    db.put(
        "products/p1",
        fields!({ "score": 10i64, "tags": ["rust", "db"], "opt": null, "metric": f64::NAN }),
    )
    .await
    .unwrap();
    db.put(
        "products/p2",
        fields!({ "score": 10.5f64, "tags": ["db"], "opt": "ok", "metric": 1.5f64 }),
    )
    .await
    .unwrap();
    db.put(
        "products/p3",
        fields!({ "score": 20i64, "tags": ["web"], "opt": "ok", "metric": 2.5f64 }),
    )
    .await
    .unwrap();
    db.put(
        "products/p4",
        fields!({ "score": 20.0f64, "tags": ["rust"], "metric": 0.0f64 }),
    )
    .await
    .unwrap();

    // Cross-type numeric range: score > 10 matches 10.5, 20, 20.0 (ordered by score ASC, __name__
    // ASC)
    let gt_ten = db
        .query(
            QuerySpec::collection("products")
                .where_filter(field_filter("score", Operator::GreaterThan, 10i64))
                .order_asc("score"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&gt_ten.documents), vec!["p2", "p3", "p4"]);

    // Cross-type numeric equality: score == 20.0 matches both integer 20 (p3) and double 20.0 (p4)
    let eq_twenty = db
        .query(QuerySpec::collection("products").where_filter(field_filter(
            "score",
            Operator::Equal,
            20.0f64,
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&eq_twenty.documents), vec!["p3", "p4"]);
    // `p4` stores `20.0f64`, which is semantically equal (`PartialEq`) to `20i64`
    assert_doc_semantic_eq(
        &eq_twenty.documents[1],
        "products/p4",
        &fields!({ "score": 20i64, "tags": ["rust"], "metric": 0.0f64 }),
    );

    // Composite OR + AND + ArrayContains + In
    let composite = db
        .query(
            QuerySpec::collection("products").where_filter(and_filters(vec![
                field_filter("tags", Operator::ArrayContains, "rust"),
                or_filters(vec![
                    field_filter("score", Operator::In, val!([10i64, 99i64])),
                    field_filter("score", Operator::LessThanOrEqual, 15i64),
                ]),
            ])),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&composite.documents), vec!["p1"]);

    // Unary filters: IsNull, IsNotNull, IsNan, IsNotNan (Standard Edition semantics)
    let nulls = db
        .query(
            QuerySpec::collection("products")
                .where_filter(unary_filter("opt", unary_filter::Operator::IsNull)),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&nulls.documents), vec!["p1"]);

    let not_nulls = db
        .query(
            QuerySpec::collection("products")
                .where_filter(unary_filter("opt", unary_filter::Operator::IsNotNull)),
        )
        .await
        .unwrap();
    // p4 is missing `opt` so in Standard Edition IsNotNull only matches p2 and p3
    expect_eq!(short_ids(&not_nulls.documents), vec!["p2", "p3"]);

    let nans = db
        .query(
            QuerySpec::collection("products")
                .where_filter(unary_filter("metric", unary_filter::Operator::IsNan)),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&nans.documents), vec!["p1"]);

    let not_nans = db
        .query(
            QuerySpec::collection("products")
                .where_filter(unary_filter("metric", unary_filter::Operator::IsNotNan))
                .order_asc("metric"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&not_nans.documents), vec!["p4", "p2", "p3"]);

    // Cursors (`start_at` inclusive vs exclusive, `end_at`), `offset`, `limit`, and `select`
    // projection
    let windowed = db
        .query(
            QuerySpec::collection("products")
                .order_asc("score")
                .start_at(vec![val!(10i64)], false) // start AFTER 10 -> p2 (10.5), p3 (20), p4 (20.0)
                .end_at(vec![val!(20i64)], false) // end AT (inclusive) 20 -> p2, p3, p4
                .offset(1)
                .limit(1)
                .select(["score"]),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&windowed.documents), vec!["p3"]);
    assert_doc_wire_eq(
        &windowed.documents[0],
        "products/p3",
        &fields!({ "score": 20i64 }),
    );

    // Reversed order + limit (`limitToLast(2)` SDK wire pattern)
    let mut last_two = db
        .query(
            QuerySpec::collection("products")
                .order_desc("score")
                .order_desc("__name__")
                .limit(2),
        )
        .await
        .unwrap()
        .documents;
    last_two.reverse();
    expect_eq!(short_ids(&last_two), vec!["p3", "p4"]);
}

#[gtest]
#[tokio::test]
async fn enterprise_vs_standard_missing_field_and_collection_group_and_empty_sentinel() {
    let mut db = TestEnv::new().build_db();

    db.put("orgs/o1/members/m1", fields!({ "role": "admin" }))
        .await
        .unwrap();
    db.put("orgs/o2/members/m2", fields!({ "role": "viewer" }))
        .await
        .unwrap();
    db.put("orgs/o2/members/m3", fields!({})).await.unwrap();

    // Collection-group query across all `members` subcollections
    let all_members = db
        .query(QuerySpec::collection_group("members"))
        .await
        .unwrap();
    expect_eq!(short_ids(&all_members.documents), vec!["m1", "m2", "m3"]);

    // Standard Edition: `role != "admin"` skips `m3` (missing `role`)
    let ne_std = db
        .query(
            QuerySpec::collection_group("members")
                .where_filter(field_filter("role", Operator::NotEqual, "admin"))
                .order_asc("role"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ne_std.documents), vec!["m2"]);

    // Switch to Enterprise Edition via Admin PATCH: `role != "admin"` now includes missing `m3`!
    db.set_admin_mode(Some("ENTERPRISE"), None).await;
    let ne_ent = db
        .query(
            QuerySpec::collection_group("members")
                .where_filter(field_filter("role", Operator::NotEqual, "admin"))
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ne_ent.documents), vec!["m2", "m3"]);

    // Empty query result emits exactly 1 sentinel frame (`document: None`, `read_time: Some(...)`)
    let empty = db
        .query(QuerySpec::collection("orgs").where_filter(field_filter(
            "__name__",
            Operator::Equal,
            Value::reference(doc_path("orgs/does_not_exist")),
        )))
        .await
        .unwrap();
    expect_that!(empty.documents.is_empty(), eq(true));
    expect_eq!(empty.raw_frame_count, 1);
    expect_that!(empty.read_time, some(anything()));
}

#[rstest]
#[case::not_in(Operator::NotIn, val!(["a", "b"]))]
#[case::array_contains_any(Operator::ArrayContainsAny, val!(["a", "b"]))]
#[tokio::test]
async fn unimplemented_field_filter_operators_return_unimplemented(
    #[case] op: Operator,
    #[case] rhs: Value,
) {
    let mut db = TestEnv::enterprise().build_db();
    db.put("items/i1", fields!({ "tag": "c", "tags": ["c"] }))
        .await
        .unwrap();

    let res = db
        .query(QuerySpec::collection("items").where_filter(field_filter("tag", op, rhs)))
        .await;
    assert_status_code(res, Code::Unimplemented);
}

#[gtest]
#[tokio::test]
async fn structured_query_rejects_invalid_from_parent_operator_and_duplicate_array_contains() {
    let mut db = TestEnv::new().build_db();
    db.put("items/i1", fields!({ "tags": ["a", "b"], "x": 1i64 }))
        .await
        .unwrap();

    // 1. Empty `from` selector -> InvalidArgument
    let empty_from = collect_run_query(
        db.raw_grpc()
            .run_query(RunQueryRequest {
                parent: DOCUMENTS_ROOT.to_string(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    StructuredQuery::default(),
                )),
                consistency_selector: None,
                explain_options: None,
                request_options: None,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await;
    assert_status_code(empty_from, Code::InvalidArgument);

    // 2. Collection `parent` instead of Root/Document -> InvalidArgument
    let col_parent = collect_run_query(
        db.raw_grpc()
            .run_query(RunQueryRequest {
                parent: format!("{DOCUMENTS_ROOT}/items"),
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    StructuredQuery {
                        from: vec![CollectionSelector {
                            collection_id:   "sub".to_string(),
                            all_descendants: false,
                        }],
                        ..Default::default()
                    },
                )),
                consistency_selector: None,
                explain_options: None,
                request_options: None,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await;
    assert_status_code(col_parent, Code::InvalidArgument);

    // 3. `Order` missing `field` -> InvalidArgument
    let missing_order_field = collect_run_query(
        db.raw_grpc()
            .run_query(RunQueryRequest {
                parent: DOCUMENTS_ROOT.to_string(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    StructuredQuery {
                        from: vec![CollectionSelector {
                            collection_id:   "items".to_string(),
                            all_descendants: false,
                        }],
                        order_by: vec![Order {
                            field:     None,
                            direction: Direction::Ascending as i32,
                        }],
                        ..Default::default()
                    },
                )),
                consistency_selector: None,
                explain_options: None,
                request_options: None,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await;
    assert_status_code(missing_order_field, Code::InvalidArgument);

    // 4. `Operator::Unspecified` and duplicate `ARRAY_CONTAINS` in Standard Edition ->
    //    InvalidArgument
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "x",
            Operator::Unspecified,
            1i64,
        )))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("tags", Operator::ArrayContains, "a"),
                field_filter("tags", Operator::ArrayContains, "b"),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );

    // 5. `find_nearest` with default/invalid fields -> InvalidArgument
    let nearest = collect_run_query(
        db.raw_grpc()
            .run_query(RunQueryRequest {
                parent: DOCUMENTS_ROOT.to_string(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    StructuredQuery {
                        from: vec![CollectionSelector {
                            collection_id:   "items".to_string(),
                            all_descendants: false,
                        }],
                        find_nearest: Some(FindNearest::default()),
                        ..Default::default()
                    },
                )),
                consistency_selector: None,
                explain_options: None,
                request_options: None,
            })
            .await
            .unwrap()
            .into_inner(),
    )
    .await;
    assert_status_code(nearest, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn run_query_rejects_malformed_filter_subfields_with_invalid_argument() {
    let mut db = TestEnv::new().build_db();
    let filters = [
        Filter { filter_type: None },
        Filter {
            filter_type: Some(FilterType::FieldFilter(FieldFilter {
                field: None,
                op:    Operator::Equal as i32,
                value: Some(val!(1i64)),
            })),
        },
        Filter {
            filter_type: Some(FilterType::FieldFilter(FieldFilter {
                field: Some(FieldReference {
                    field_path: "x".to_string(),
                }),
                op:    Operator::Equal as i32,
                value: None,
            })),
        },
        Filter {
            filter_type: Some(FilterType::UnaryFilter(UnaryFilter {
                op: unary_filter::Operator::IsNull as i32,
                operand_type: None,
            })),
        },
        Filter {
            filter_type: Some(FilterType::CompositeFilter(CompositeFilter {
                op:      composite_filter::Operator::And as i32,
                filters: vec![Filter { filter_type: None }],
            })),
        },
    ];

    for filter in filters {
        let res = collect_run_query(
            db.raw_grpc()
                .run_query(RunQueryRequest {
                    parent: DOCUMENTS_ROOT.to_string(),
                    query_type: Some(run_query_request::QueryType::StructuredQuery(
                        StructuredQuery {
                            from: vec![CollectionSelector {
                                collection_id:   "items".to_string(),
                                all_descendants: false,
                            }],
                            r#where: Some(filter),
                            ..Default::default()
                        },
                    )),
                    consistency_selector: None,
                    explain_options: None,
                    request_options: None,
                })
                .await
                .unwrap()
                .into_inner(),
        )
        .await;
        assert_status_code(res, Code::InvalidArgument);
    }
}

#[gtest]
#[tokio::test]
async fn structured_query_implicit_order_by_and_cursor_tie_breakers() {
    let mut db = TestEnv::new().build_db();
    for (id, tie, a, b) in [
        ("tie/a", 1i64, 1i64, 10i64),
        ("tie/b", 1i64, 2i64, 5i64),
        ("tie/c", 2i64, 10i64, 1i64),
        ("tie/d", 2i64, 5i64, 2i64),
    ] {
        db.put(id, fields!({ "tie": tie, "a": a, "b": b }))
            .await
            .unwrap();
    }

    // 1. Explicit `orderBy('tie', DESC)` appends implicit `__name__ DESC` in Standard Edition,
    //    allowing both tie-breaking and a 2-value cursor `[tie, __name__]`.
    let desc_ties = db
        .query(QuerySpec::collection("tie").order_desc("tie"))
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_ties.documents), vec!["d", "c", "b", "a"]);

    let desc_after = db
        .query(QuerySpec::collection("tie").order_desc("tie").start_at(
            vec![
                val!(2i64),
                Value {
                    value_type: Some(ValueType::ReferenceValue(doc_path("tie/d"))),
                },
            ],
            false,
        ))
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_after.documents), vec!["c", "b", "a"]);

    // 2. Inequality on `__name__` plus `a > 0`: `a` orders before `__name__` (`b, d, c`).
    let name_and_a = db
        .query(QuerySpec::collection("tie").where_filter(and_filters(vec![
            field_filter(
                "__name__",
                Operator::GreaterThan,
                Value {
                    value_type: Some(ValueType::ReferenceValue(doc_path("tie/a"))),
                },
            ),
            field_filter("a", Operator::GreaterThan, 0i64),
        ])))
        .await
        .unwrap();
    expect_eq!(short_ids(&name_and_a.documents), vec!["b", "d", "c"]);

    // 3. Multiple inequalities (`b > 0 AND a > 0`) order fields lexicographically (`a` then `b`).
    let multi_ineq = db
        .query(QuerySpec::collection("tie").where_filter(and_filters(vec![
            field_filter("b", Operator::GreaterThan, 0i64),
            field_filter("a", Operator::GreaterThan, 0i64),
        ])))
        .await
        .unwrap();
    expect_eq!(short_ids(&multi_ineq.documents), vec!["a", "b", "d", "c"]);

    // 4. Duplicate inequality on the same field deduplicates in implicit `order_by` so a 2-value
    //    cursor `[a, __name__]` aligns cleanly.
    let dup_ineq_cursor = db
        .query(
            QuerySpec::collection("tie")
                .where_filter(and_filters(vec![
                    field_filter("a", Operator::GreaterThan, 0i64),
                    field_filter("a", Operator::LessThan, 10i64),
                ]))
                .start_at(
                    vec![
                        val!(1i64),
                        Value {
                            value_type: Some(ValueType::ReferenceValue(doc_path("tie/a"))),
                        },
                    ],
                    false,
                ),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&dup_ineq_cursor.documents), vec!["b", "d"]);

    // 5. Explicit `orderBy('a', DESC)` combined with `b > 0` appends `b DESC, __name__ DESC`.
    db.put("explicit/x1", fields!({ "a": 5i64, "b": 10i64 }))
        .await
        .unwrap();
    db.put("explicit/x2", fields!({ "a": 5i64, "b": 1i64 }))
        .await
        .unwrap();
    db.put("explicit/x3", fields!({ "a": 9i64, "b": 2i64 }))
        .await
        .unwrap();
    let explicit_with_ineq = db
        .query(
            QuerySpec::collection("explicit")
                .where_filter(field_filter("b", Operator::GreaterThan, 0i64))
                .order_desc("a"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&explicit_with_ineq.documents),
        vec!["x3", "x1", "x2"]
    );

    // 6. Enterprise Edition: direct collection with `orderBy('tie', DESC)` does NOT synthesize
    //    `__name__` (so a 2-value cursor is rejected), whereas `collection_group` DOES append
    //    `__name__ DESC` (`all_descendants: true`).
    let mut edb = TestEnv::enterprise().build_db();
    edb.put("items/p1", fields!({ "tie": 1i64 })).await.unwrap();
    edb.put("items/p2", fields!({ "tie": 1i64 })).await.unwrap();
    edb.put("roots/r1/grp/c", fields!({ "tie": 1i64 }))
        .await
        .unwrap();
    edb.put("roots/r2/grp/d", fields!({ "tie": 1i64 }))
        .await
        .unwrap();

    let ent_direct_cursor = edb
        .query(QuerySpec::collection("items").order_desc("tie").start_at(
            vec![
                val!(1i64),
                Value {
                    value_type: Some(ValueType::ReferenceValue(doc_path("items/p2"))),
                },
            ],
            false,
        ))
        .await;
    assert_status_code(ent_direct_cursor, Code::InvalidArgument);

    let ent_cg = edb
        .query(QuerySpec::collection_group("grp").order_desc("tie"))
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_cg.documents), vec!["d", "c"]);
}
