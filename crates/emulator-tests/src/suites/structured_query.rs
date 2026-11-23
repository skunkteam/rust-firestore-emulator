use googleapis::{
    fields,
    google::{
        firestore::v1::{
            Cursor, Function, Pipeline, RunQueryRequest, StructuredQuery, Value, run_query_request,
            structured_query::{
                CollectionSelector, CompositeFilter, Direction, FieldFilter, FieldReference,
                Filter, FindNearest, Order, Projection, UnaryFilter, composite_filter,
                field_filter::Operator, filter::FilterType, find_nearest, unary_filter,
            },
            value::ValueType,
        },
        protobuf::Timestamp,
        r#type::LatLng,
    },
    val,
};
use googletest::prelude::*;
use tonic::Code;

use crate::harness::{
    builders::{
        AggregationSpec, QuerySpec, and_filters, doc_path, field_filter, or_filters, unary_filter,
    },
    contention::ContentionProbe,
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

#[gtest]
#[tokio::test]
async fn array_contains_any_and_not_in_across_queries_aggregations_and_listen() {
    let mut db = TestEnv::new().build_db();
    db.put(
        "items/d_match",
        fields!({
            "num": 42i64,
            "zero": -0.0f64,
            "tags": ["rust", "db"],
            "arr_num": [42i64, -0.0f64],
            "arr_vals": [1i64, 2i64],
            "metric": 10i64,
        }),
    )
    .await
    .unwrap();
    db.put(
        "items/d_null",
        fields!({
            "num": null,
            "tags": [null],
            "arr_vals": [3i64],
            "metric": 20i64,
        }),
    )
    .await
    .unwrap();
    db.put(
        "items/d_nan",
        fields!({
            "num": f64::NAN,
            "tags": [f64::NAN],
            "metric": 30i64,
        }),
    )
    .await
    .unwrap();
    db.put(
        "items/d_nested_nan",
        fields!({
            "tags": [{ "nested": f64::NAN }],
            "metric": 40i64,
        }),
    )
    .await
    .unwrap();
    db.put(
        "items/d_missing",
        fields!({
            "metric": 50i64,
        }),
    )
    .await
    .unwrap();

    // 1. Standard Edition `ARRAY_CONTAINS_ANY`: cross-type 42.0 / 0 matches `d_match`, nested `{
    //    nested: NaN }` matches `d_nested_nan`, while top-level `null` and `NaN` do NOT match on
    //    Standard.
    let std_aca = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "arr_num",
            Operator::ArrayContainsAny,
            val!([42.0f64, 99i64]),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&std_aca.documents), vec!["d_match"]);

    let std_aca_zero = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "arr_num",
            Operator::ArrayContainsAny,
            val!([0i64]),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&std_aca_zero.documents), vec!["d_match"]);

    let std_aca_null_nan = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "tags",
            Operator::ArrayContainsAny,
            val!([null, f64::NAN]),
        )))
        .await
        .unwrap();
    expect_that!(std_aca_null_nan.documents, is_empty());

    let std_aca_nested_nan = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "tags",
            Operator::ArrayContainsAny,
            Value::array(vec![Value::map(fields!({ "nested": f64::NAN }))]),
        )))
        .await
        .unwrap();
    expect_eq!(
        short_ids(&std_aca_nested_nan.documents),
        vec!["d_nested_nan"]
    );

    // 2. Standard Edition `NOT_IN`: excludes missing (`d_missing`, `d_nested_nan`), excludes `null`
    //    document (`d_null`), and excludes all docs when `[null]` is in `NOT_IN`; `NaN != NaN`
    //    matches.
    let std_not_in_one = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "num",
            Operator::NotIn,
            val!([1i64]),
        )))
        .await
        .unwrap();
    expect_eq!(
        short_ids(&std_not_in_one.documents),
        vec!["d_nan", "d_match"]
    );

    let std_not_in_null = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "num",
            Operator::NotIn,
            val!([1i64, null]),
        )))
        .await
        .unwrap();
    expect_that!(std_not_in_null.documents, is_empty());

    let std_not_in_nan = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "num",
            Operator::NotIn,
            val!([f64::NAN]),
        )))
        .await
        .unwrap();
    expect_eq!(
        short_ids(&std_not_in_nan.documents),
        vec!["d_nan", "d_match"]
    );

    // Nested array operand in `IN [[1, 2]]`
    let nested_in = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "arr_vals",
            Operator::In,
            Value::array(vec![Value::array(vec![val!(1i64), val!(2i64)])]),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&nested_in.documents), vec!["d_match"]);

    // Raw `< null` / `<= null` / `<= NaN` on Standard vs Enterprise
    for (val_rhs, expected_std) in [(Value::null(), false), (Value::double(f64::NAN), false)] {
        let std_range = db
            .query(QuerySpec::collection("items").where_filter(field_filter(
                "num",
                Operator::LessThanOrEqual,
                val_rhs,
            )))
            .await
            .unwrap();
        assert_eq!(!std_range.documents.is_empty(), expected_std);
    }

    // Aggregations (`count`, `sum`, `avg`) with `NOT_IN` & `ARRAY_CONTAINS_ANY`
    let agg_not_in = db
        .aggregate(
            QuerySpec::collection("items").where_filter(field_filter(
                "num",
                Operator::NotIn,
                val!([42.0f64]),
            )),
            AggregationSpec::new()
                .count("n")
                .sum("sum_m", "metric")
                .avg("avg_m", "metric"),
        )
        .await
        .unwrap();
    expect_eq!(agg_not_in.fields.get("n"), Some(&val!(1i64)));
    expect_eq!(agg_not_in.fields.get("sum_m"), Some(&val!(30i64)));
    expect_eq!(agg_not_in.fields.get("avg_m"), Some(&val!(30.0f64)));

    // Real-time `Listen` on `ARRAY_CONTAINS_ANY` and `NOT_IN`
    let (mut aca_listen, aca_init) = db
        .listen_query(QuerySpec::collection("items").where_filter(field_filter(
            "tags",
            Operator::ArrayContainsAny,
            val!(["rust", "go"]),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&aca_init.changed_docs), vec!["d_match"]);

    db.put(
        "items/d_added",
        fields!({ "tags": ["go"], "metric": 60i64 }),
    )
    .await
    .unwrap();
    let add_batch = aca_listen.next_batch().await.unwrap();
    expect_eq!(add_batch.was_reset, true);
    expect_eq!(
        short_ids(&add_batch.changed_docs),
        vec!["d_added", "d_match"]
    );

    db.put(
        "items/d_added",
        fields!({ "tags": ["go", "extra"], "metric": 61i64 }),
    )
    .await
    .unwrap();
    let mod_batch = aca_listen.next_batch().await.unwrap();
    expect_eq!(mod_batch.was_reset, false);
    expect_eq!(short_ids(&mod_batch.changed_docs), vec!["d_added"]);

    db.delete("items/d_added", None).await.unwrap();
    let del_batch = aca_listen.next_batch().await.unwrap();
    expect_eq!(del_batch.was_reset, true);
    expect_eq!(short_ids(&del_batch.changed_docs), vec!["d_match"]);
    aca_listen.close().await;

    let (mut not_in_listen, not_in_init) = db
        .listen_query(QuerySpec::collection("items").where_filter(field_filter(
            "num",
            Operator::NotIn,
            val!([999i64]),
        )))
        .await
        .unwrap();
    expect_eq!(
        short_ids(&not_in_init.changed_docs),
        vec!["d_nan", "d_match"]
    );

    db.put("items/d_live", fields!({ "num": 7i64 }))
        .await
        .unwrap();
    let ni_add = not_in_listen.next_batch().await.unwrap();
    expect_eq!(
        short_ids(&ni_add.changed_docs),
        vec!["d_nan", "d_live", "d_match"]
    );

    db.put("items/d_live", fields!({ "num": 999i64 }))
        .await
        .unwrap();
    let ni_leave = not_in_listen.next_batch().await.unwrap();
    expect_eq!(short_ids(&ni_leave.changed_docs), vec!["d_nan", "d_match"]);
    not_in_listen.close().await;
    db.delete("items/d_live", None).await.unwrap();

    // 3. Enterprise Edition: missing fields match `NOT_IN`, `null`/`NaN` match `ARRAY_CONTAINS_ANY`
    //    & `NOT_IN`, `< null` / `<= null` / `<= NaN`, and 5,000-element arrays execute cleanly.
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;

    let ent_aca_null_nan = db
        .query(
            QuerySpec::collection("items")
                .where_filter(field_filter(
                    "tags",
                    Operator::ArrayContainsAny,
                    val!([null, f64::NAN]),
                ))
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ent_aca_null_nan.documents),
        vec!["d_nan", "d_null"]
    );

    let ent_not_in_null = db
        .query(
            QuerySpec::collection("items")
                .where_filter(field_filter("num", Operator::NotIn, val!([1i64, null])))
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ent_not_in_null.documents),
        vec!["d_match", "d_missing", "d_nan", "d_nested_nan"]
    );

    let ent_not_in_nan = db
        .query(
            QuerySpec::collection("items")
                .where_filter(field_filter("num", Operator::NotIn, val!([f64::NAN])))
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ent_not_in_nan.documents),
        vec!["d_match", "d_missing", "d_nested_nan", "d_null"]
    );

    let ent_le_null = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "num",
            Operator::LessThanOrEqual,
            Value::null(),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_le_null.documents), vec!["d_null"]);

    let large_vals: Vec<Value> = (1..=5000).map(Value::integer).collect();
    let ent_5k_not_in = db
        .query(
            QuerySpec::collection("items")
                .where_filter(field_filter(
                    "num",
                    Operator::NotIn,
                    Value::array(large_vals.clone()),
                ))
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(
        short_ids(&ent_5k_not_in.documents),
        vec!["d_missing", "d_nan", "d_nested_nan", "d_null"]
    );

    let mut large_tags: Vec<Value> = (1..=4999).map(|i| Value::string(format!("t{i}"))).collect();
    large_tags.push(Value::string("rust"));
    let ent_5k_aca = db
        .query(QuerySpec::collection("items").where_filter(field_filter(
            "tags",
            Operator::ArrayContainsAny,
            Value::array(large_tags),
        )))
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_5k_aca.documents), vec!["d_match"]);
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn array_contains_any_and_not_in_transactional_locking_and_optimistic_aborts() {
    let mut db = TestEnv::new().build_db();
    db.put("tx/a", fields!({ "tag": "keep", "tags": ["x"] }))
        .await
        .unwrap();
    db.put("tx/b", fields!({ "tag": "drop", "tags": ["y"] }))
        .await
        .unwrap();
    db.put("tx/c", fields!({ "other": 1i64 })).await.unwrap();

    // Pessimistic Enterprise: `NOT_IN` locks missing-field match `tx/c` in addition to `tx/a`.
    db.set_admin_mode(Some("ENTERPRISE"), Some("PESSIMISTIC"))
        .await;
    {
        let probe = ContentionProbe::new(&db);
        let mut txn = probe.actor_a.clone().begin_rw_txn().await.unwrap();
        let res = txn
            .query(
                QuerySpec::collection("tx")
                    .where_filter(field_filter("tag", Operator::NotIn, val!(["drop"])))
                    .order_asc("__name__"),
            )
            .await
            .unwrap();
        expect_eq!(short_ids(&res.documents), vec!["a", "c"]);

        let mut actor_b = probe.actor_b.clone();
        let mut blocked = ContentionProbe::spawn_op(async move {
            actor_b.put("tx/c", fields!({ "tag": "now-present" })).await
        });
        blocked.assert_pending().await;

        txn.rollback().await.unwrap();
        blocked.expect_ready().await.unwrap();
    }

    // Optimistic Enterprise: concurrent insert matching `ARRAY_CONTAINS_ANY` aborts commit.
    db.set_admin_mode(Some("ENTERPRISE"), Some("OPTIMISTIC"))
        .await;
    {
        let mut txn = db.begin_rw_txn().await.unwrap();
        let res = txn
            .query(QuerySpec::collection("tx").where_filter(field_filter(
                "tags",
                Operator::ArrayContainsAny,
                val!(["x", "z"]),
            )))
            .await
            .unwrap();
        expect_eq!(short_ids(&res.documents), vec!["a"]);

        db.put("tx/d", fields!({ "tags": ["z"] })).await.unwrap();
        txn.put("tx/side", fields!({ "ok": true }));
        assert_status_code(txn.commit().await, Code::Aborted);
    }
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

    // Unset `Value::default()` in FieldFilter.value, ArrayValue.values, and Cursor.values
    let unset_queries = [
        StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   "items".to_string(),
                all_descendants: false,
            }],
            r#where: Some(field_filter("x", Operator::Equal, Value::default())),
            ..Default::default()
        },
        StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   "items".to_string(),
                all_descendants: false,
            }],
            r#where: Some(field_filter(
                "x",
                Operator::In,
                Value::array(vec![Value::default()]),
            )),
            ..Default::default()
        },
        StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   "items".to_string(),
                all_descendants: false,
            }],
            order_by: vec![Order {
                field:     Some(FieldReference {
                    field_path: "x".to_string(),
                }),
                direction: Direction::Ascending as i32,
            }],
            start_at: Some(Cursor {
                before: true,
                values: vec![Value::default()],
            }),
            ..Default::default()
        },
    ];
    for sq in unset_queries {
        let res = collect_run_query(
            db.raw_grpc()
                .run_query(RunQueryRequest {
                    parent: DOCUMENTS_ROOT.to_string(),
                    query_type: Some(run_query_request::QueryType::StructuredQuery(sq)),
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

    // Malformed `Listen` target (empty OR) returns InvalidArgument over the stream without
    // panicking.
    let listen_err = db
        .listen_query(QuerySpec::collection("items").where_filter(or_filters(vec![])))
        .await;
    assert_status_code(listen_err, Code::InvalidArgument);
}

#[gtest]
#[tokio::test]
async fn structured_query_validation_matrix_standard_vs_enterprise() {
    let mut db = TestEnv::new().build_db();
    db.put("items/d1", fields!({ "a": 1i64, "tags": ["x"] }))
        .await
        .unwrap();

    // 1. Empty AND succeeds while empty OR returns InvalidArgument; non-array & empty ArrayValue
    //    fail.
    db.query(QuerySpec::collection("items").where_filter(and_filters(vec![])))
        .await
        .unwrap();
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(or_filters(vec![])))
            .await,
        Code::InvalidArgument,
    );
    for op in [Operator::In, Operator::ArrayContainsAny, Operator::NotIn] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter("a", op, 1i64)))
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter(
                "a",
                op,
                Value::array(vec![]),
            )))
            .await,
            Code::InvalidArgument,
        );
    }

    // 2. Standard array caps (`> 30` IN/ARRAY_CONTAINS_ANY, `> 10` NOT_IN), negation exclusivity,
    //    NOT_IN + OR/IN/ARRAY_CONTAINS_ANY, DNF bounds (`16 * 16 = 256`, same-field over-count, OR
    //    with 1 ARRAY_CONTAINS per branch allowed).
    let vals_31: Vec<Value> = (0..31).map(Value::integer).collect();
    let vals_11: Vec<Value> = (0..11).map(Value::integer).collect();
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "a",
            Operator::In,
            Value::array(vals_31.clone()),
        )))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "tags",
            Operator::ArrayContainsAny,
            Value::array(vals_31),
        )))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "a",
            Operator::NotIn,
            Value::array(vals_11),
        )))
        .await,
        Code::InvalidArgument,
    );

    // Negation exclusivity (`!=`, `NOT_IN`, `IS_NOT_NULL`, `IS_NOT_NAN`)
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::NotEqual, 1i64),
                unary_filter("b", unary_filter::Operator::IsNotNull),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::NotIn, val!([1i64])),
                unary_filter("b", unary_filter::Operator::IsNotNan),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );

    // NOT_IN combined with OR / IN / ARRAY_CONTAINS_ANY
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(or_filters(vec![
            field_filter("a", Operator::NotIn, val!([1i64])),
            field_filter("b", Operator::Equal, 2i64),
        ])))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::NotIn, val!([1i64])),
                field_filter("b", Operator::In, val!([2i64])),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::NotIn, val!([1i64])),
                field_filter("tags", Operator::ArrayContainsAny, val!(["x"])),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );

    // DNF: 16 * 16 = 256, conservative same-field over-count, and distinct int/double and +0/-0
    // literals reject on Standard; OR with 1 ARRAY_CONTAINS per branch passes.
    let vals_16: Vec<Value> = (0..16).map(Value::integer).collect();
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::In, Value::array(vals_16.clone())),
                field_filter("b", Operator::In, Value::array(vals_16)),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );
    let mut int_and_double_15: Vec<Value> = (0..15).map(Value::integer).collect();
    int_and_double_15.extend((0..15).map(|i| Value::double(i as f64)));
    let mut float_plus_minus_zero: Vec<Value> =
        (1..=14).map(|i| Value::double(i as f64 + 0.5)).collect();
    float_plus_minus_zero.push(Value::double(0.0));
    float_plus_minus_zero.push(Value::double(-0.0));
    for over_30_dedup in [int_and_double_15.clone(), float_plus_minus_zero.clone()] {
        assert_status_code(
            db.query(
                QuerySpec::collection("items").where_filter(and_filters(vec![
                    field_filter("a", Operator::In, Value::array(over_30_dedup)),
                    field_filter("b", Operator::In, val!([1i64, 2i64])),
                ])),
            )
            .await,
            Code::InvalidArgument,
        );
    }
    assert_status_code(
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter(
                    "a",
                    Operator::In,
                    Value::array((1..=6).map(Value::integer).collect()),
                ),
                field_filter(
                    "a",
                    Operator::In,
                    Value::array((2..=7).map(Value::integer).collect()),
                ),
            ])),
        )
        .await,
        Code::InvalidArgument,
    );
    db.query(QuerySpec::collection("items").where_filter(or_filters(vec![
        field_filter("tags", Operator::ArrayContains, "x"),
        field_filter("tags", Operator::ArrayContainsAny, val!(["y"])),
    ])))
    .await
    .unwrap();

    // 3. `__name__` constraints, Datastore `__id<i64>__`, and reserved/empty FieldPaths.
    let ds_ref = Value::reference(format!("{DOCUMENTS_ROOT}/items/__id-9223372036854775808__"));
    let ds_zero_ref = Value::reference(format!("{DOCUMENTS_ROOT}/items/__id0__"));
    let ds_zero_anc_ref = Value::reference(format!("{DOCUMENTS_ROOT}/items/__id0__/sub/d1"));
    let other_db_ref =
        Value::reference("projects/test-project/databases/other/documents/items/d1".to_string());
    for ds_ok in [
        format!("{DOCUMENTS_ROOT}/items/__id1__"),
        format!("{DOCUMENTS_ROOT}/items/__id-9223372036854775808__"),
    ] {
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "__name__",
            Operator::Equal,
            Value::reference(ds_ok),
        )))
        .await
        .unwrap();
    }
    db.query(QuerySpec::collection("items").where_filter(field_filter(
        "ref",
        Operator::Equal,
        ds_zero_ref.clone(),
    )))
    .await
    .unwrap();
    db.query(
        QuerySpec::collection("items")
            .order_asc("__name__")
            .start_at(vec![ds_zero_ref.clone()], true),
    )
    .await
    .unwrap();
    for bad_zero_name in [ds_zero_ref.clone(), ds_zero_anc_ref.clone()] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter(
                "__name__",
                Operator::Equal,
                bad_zero_name,
            )))
            .await,
            Code::InvalidArgument,
        );
    }
    for bad_ds_ref in [
        format!("{DOCUMENTS_ROOT}/items/__id-0__"),
        format!("{DOCUMENTS_ROOT}/items/__id01__"),
        format!("{DOCUMENTS_ROOT}/items/__id-01__"),
    ] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter(
                "ref",
                Operator::Equal,
                Value::reference(bad_ds_ref),
            )))
            .await,
            Code::InvalidArgument,
        );
    }

    // Backtick-quoted `` `__name__` `` resolves to DocumentName across filter, order_by, select,
    // and GetDocument mask.
    let valid_doc_ref = Value::reference(doc_path("items/d1"));
    db.query(
        QuerySpec::collection("items")
            .where_filter(field_filter(
                "`__name__`",
                Operator::Equal,
                valid_doc_ref.clone(),
            ))
            .order_asc("`__name__`")
            .select(["`__name__`"]),
    )
    .await
    .unwrap();
    db.get_masked("items/d1", ["`__name__`"]).await.unwrap();
    assert_status_code(
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "`__name__`",
            Operator::Equal,
            1i64,
        )))
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .order_asc("`__name__`"),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("`__name__`")
                .start_at(vec![val!("not-a-ref")], true),
        )
        .await,
        Code::InvalidArgument,
    );

    // Out-of-range GeoPointValue / TimestampValue and unsupported operand ValueTypes fail in both
    // filters and cursors.
    for bad_operand in [
        Value::from(LatLng {
            latitude:  999.0,
            longitude: 0.0,
        }),
        Value::from(LatLng {
            latitude:  0.0,
            longitude: 181.0,
        }),
        Value::from(Timestamp {
            seconds: 0,
            nanos:   -1,
        }),
        Value::from(Timestamp {
            seconds: 253_402_300_800,
            nanos:   0,
        }),
        Value {
            value_type: Some(ValueType::FieldReferenceValue("a".to_string())),
        },
        Value {
            value_type: Some(ValueType::VariableReferenceValue("a".to_string())),
        },
        Value {
            value_type: Some(ValueType::FunctionValue(Function::default())),
        },
        Value {
            value_type: Some(ValueType::PipelineValue(Pipeline::default())),
        },
    ] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter(
                "a",
                Operator::Equal,
                bad_operand.clone(),
            )))
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(
                QuerySpec::collection("items")
                    .order_asc("a")
                    .start_at(vec![bad_operand], true),
            )
            .await,
            Code::InvalidArgument,
        );
    }

    for bad_name_filter in [
        unary_filter("__name__", unary_filter::Operator::IsNotNull),
        field_filter("__name__", Operator::ArrayContains, ds_ref.clone()),
        field_filter(
            "__name__",
            Operator::ArrayContainsAny,
            Value::array(vec![ds_ref.clone()]),
        ),
        field_filter(
            "__name__",
            Operator::Equal,
            Value::reference(DOCUMENTS_ROOT.to_string()),
        ),
        field_filter("__name__", Operator::Equal, "not-a-ref"),
        field_filter("__name__", Operator::Equal, other_db_ref.clone()),
        field_filter(
            "__name__",
            Operator::In,
            Value::array(vec![Value::string("bad")]),
        ),
        field_filter(
            "__name__",
            Operator::NotIn,
            Value::array(vec![other_db_ref.clone()]),
        ),
    ] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(bad_name_filter))
                .await,
            Code::InvalidArgument,
        );
    }

    for bad_path in ["__custom__", "a.__name__", "a..b"] {
        assert_status_code(
            db.query(QuerySpec::collection("items").where_filter(field_filter(
                bad_path,
                Operator::Equal,
                1i64,
            )))
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(
                QuerySpec::collection("items")
                    .where_filter(unary_filter(bad_path, unary_filter::Operator::IsNull)),
            )
            .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(QuerySpec::collection("items").order_asc(bad_path))
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.query(QuerySpec::collection("items").select([bad_path]))
                .await,
            Code::InvalidArgument,
        );
        assert_status_code(
            db.get_masked("items/d1", [bad_path]).await,
            Code::InvalidArgument,
        );
    }

    // 4. Standard duplicate `order_by`, `__name__`-not-last, and `__name__` cursor validation.
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("a")
                .order_desc("a"),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .order_asc("a"),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .where_filter(field_filter("a", Operator::GreaterThan, 0i64))
                .order_asc("__name__"),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .start_at(vec![val!("not-a-ref")], true),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .start_at(vec![other_db_ref.clone()], true),
        )
        .await,
        Code::InvalidArgument,
    );

    // Raw Projection check (empty projection fields array vs invalid field)
    let raw_proj = collect_run_query(
        db.raw_grpc()
            .run_query(RunQueryRequest {
                parent: DOCUMENTS_ROOT.to_string(),
                query_type: Some(run_query_request::QueryType::StructuredQuery(
                    StructuredQuery {
                        from: vec![CollectionSelector {
                            collection_id:   "items".to_string(),
                            all_descendants: false,
                        }],
                        select: Some(Projection {
                            fields: vec![FieldReference {
                                field_path: "__custom__".to_string(),
                            }],
                        }),
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
    assert_status_code(raw_proj, Code::InvalidArgument);

    // Standard `findNearest` with inequality + `ORDER BY __name__` rejects `__name__`-not-last,
    // `ORDER BY b DESC` tie-breaks synthesized inequality `a` in `DESC`, and `OR + inequality`
    // excludes docs missing the inequality field.
    db.put(
        "vec_std/v1",
        fields!({ "a": 10i64, "b": 5i64, "emb": Value::vector([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put(
        "vec_std/v2",
        fields!({ "a": 20i64, "b": 5i64, "emb": Value::vector([1.0, 0.0]) }),
    )
    .await
    .unwrap();
    db.put(
        "vec_std/v3",
        fields!({ "b": 1i64, "emb": Value::vector([1.0, 0.0]) }),
    )
    .await
    .unwrap();

    let fn_spec = FindNearest {
        vector_field: Some(FieldReference {
            field_path: "emb".to_string(),
        }),
        query_vector: Some(Value::vector([0.0, 0.0])),
        distance_measure: find_nearest::DistanceMeasure::Euclidean as i32,
        limit: Some(googleapis::google::protobuf::Int32Value { value: 10 }),
        distance_result_field: String::new(),
        distance_threshold: None,
    };
    assert_status_code(
        db.query(
            QuerySpec::collection("vec_std")
                .where_filter(field_filter("a", Operator::GreaterThan, 0i64))
                .order_asc("__name__")
                .find_nearest(fn_spec.clone()),
        )
        .await,
        Code::InvalidArgument,
    );
    let std_fn_desc = db
        .query(
            QuerySpec::collection("vec_std")
                .where_filter(field_filter("a", Operator::GreaterThan, 0i64))
                .order_desc("b")
                .find_nearest(fn_spec.clone()),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&std_fn_desc.documents), vec!["v2", "v1"]);

    let or_fn_filter = or_filters(vec![
        field_filter("a", Operator::GreaterThan, 0i64),
        field_filter("b", Operator::Equal, 1i64),
    ]);
    let std_fn_or = db
        .query(
            QuerySpec::collection("vec_std")
                .where_filter(or_fn_filter.clone())
                .find_nearest(fn_spec.clone()),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&std_fn_or.documents), vec!["v1", "v2"]);

    // 5. Switch to Enterprise Edition: `__id-9223372036854775808__` succeeds, > 30 DNF disjunctions
    //    (including int/double & +0/-0) and `findNearest` + `__name__` / `OR` succeed, while
    //    non-ReferenceValue and cross-database cursor `__name__` values still fail.
    db.set_admin_mode(Some("ENTERPRISE"), None).await;
    for dedup_vals in [int_and_double_15, float_plus_minus_zero] {
        db.query(
            QuerySpec::collection("items").where_filter(and_filters(vec![
                field_filter("a", Operator::In, Value::array(dedup_vals)),
                field_filter("b", Operator::In, val!([1i64, 2i64])),
            ])),
        )
        .await
        .unwrap();
    }
    db.query(
        QuerySpec::collection("vec_std")
            .where_filter(field_filter("a", Operator::GreaterThan, 0i64))
            .order_asc("__name__")
            .find_nearest(fn_spec.clone()),
    )
    .await
    .unwrap();
    let ent_fn_or = db
        .query(
            QuerySpec::collection("vec_std")
                .where_filter(or_fn_filter)
                .find_nearest(fn_spec),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_fn_or.documents), vec!["v3", "v1", "v2"]);
    db.query(QuerySpec::collection("items").where_filter(field_filter(
        "__name__",
        Operator::Equal,
        ds_ref,
    )))
    .await
    .unwrap();
    for ent_zero in [ds_zero_ref, ds_zero_anc_ref] {
        db.query(QuerySpec::collection("items").where_filter(field_filter(
            "__name__",
            Operator::Equal,
            ent_zero,
        )))
        .await
        .unwrap();
    }
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .start_at(vec![val!("not-a-ref")], true),
        )
        .await,
        Code::InvalidArgument,
    );
    assert_status_code(
        db.query(
            QuerySpec::collection("items")
                .order_asc("__name__")
                .start_at(vec![other_db_ref.clone()], true),
        )
        .await,
        Code::InvalidArgument,
    );
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
    //    while cursor lengths are validated BEFORE implicit synthesis: a 2-value cursor requires
    //    explicit 2-field `orderBy('tie', DESC).orderBy('__name__', DESC)`.
    let desc_ties = db
        .query(QuerySpec::collection("tie").order_desc("tie"))
        .await
        .unwrap();
    expect_eq!(short_ids(&desc_ties.documents), vec!["d", "c", "b", "a"]);

    let desc_cursor_missing_name = db
        .query(QuerySpec::collection("tie").order_desc("tie").start_at(
            vec![
                val!(2i64),
                Value {
                    value_type: Some(ValueType::ReferenceValue(doc_path("tie/d"))),
                },
            ],
            false,
        ))
        .await;
    let err_status = assert_status_code(desc_cursor_missing_name, Code::InvalidArgument);
    assert!(err_status.message().contains("Cursor has too many values."));

    let desc_after = db
        .query(
            QuerySpec::collection("tie")
                .order_desc("tie")
                .order_desc("__name__")
                .start_at(
                    vec![
                        val!(2i64),
                        Value {
                            value_type: Some(ValueType::ReferenceValue(doc_path("tie/d"))),
                        },
                    ],
                    false,
                ),
        )
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

    // 4. Duplicate inequality on the same field deduplicates in implicit `order_by`, while a
    //    2-value cursor `[a, __name__]` requires explicit `order_asc("a").order_asc("__name__")`.
    let dup_cursor_without_order = db
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
        .await;
    let err_status = assert_status_code(dup_cursor_without_order, Code::InvalidArgument);
    assert!(err_status.message().contains("Cursor has too many values."));

    let dup_ineq_cursor = db
        .query(
            QuerySpec::collection("tie")
                .where_filter(and_filters(vec![
                    field_filter("a", Operator::GreaterThan, 0i64),
                    field_filter("a", Operator::LessThan, 10i64),
                ]))
                .order_asc("a")
                .order_asc("__name__")
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

    // 6. Enterprise Edition: neither direct collections nor `collection_group` (`all_descendants`)
    //    synthesize implicit `__name__` ordering (verified against live Cloud Firestore): extra
    //    cursor values are rejected unless `__name__` is explicitly ordered, and explicit
    //    `__name__` direction controls tie-breaking.
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

    let ent_cg_2val_cursor = edb
        .query(
            QuerySpec::collection_group("grp")
                .order_desc("tie")
                .start_at(
                    vec![
                        val!(1i64),
                        Value {
                            value_type: Some(ValueType::ReferenceValue(doc_path("roots/r2/grp/d"))),
                        },
                    ],
                    false,
                ),
        )
        .await;
    assert_status_code(ent_cg_2val_cursor, Code::InvalidArgument);

    let ent_cg_no_order_cursor = edb
        .query(QuerySpec::collection_group("grp").start_at(
            vec![Value {
                value_type: Some(ValueType::ReferenceValue(doc_path("roots/r1/grp/c"))),
            }],
            true,
        ))
        .await;
    assert_status_code(ent_cg_no_order_cursor, Code::InvalidArgument);

    let ent_cg = edb
        .query(QuerySpec::collection_group("grp").order_desc("tie"))
        .await
        .unwrap();
    let mut ent_cg_ids = short_ids(&ent_cg.documents);
    ent_cg_ids.sort();
    expect_eq!(ent_cg_ids, vec!["c", "d"]);

    let ent_cg_explicit_desc = edb
        .query(
            QuerySpec::collection_group("grp")
                .order_desc("tie")
                .order_desc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_cg_explicit_desc.documents), vec!["d", "c"]);

    let ent_cg_explicit_asc = edb
        .query(
            QuerySpec::collection_group("grp")
                .order_desc("tie")
                .order_asc("__name__"),
        )
        .await
        .unwrap();
    expect_eq!(short_ids(&ent_cg_explicit_asc.documents), vec!["c", "d"]);
}
