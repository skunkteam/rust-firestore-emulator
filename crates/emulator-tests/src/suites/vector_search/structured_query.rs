use googleapis::{
    fields,
    google::firestore::v1::{
        Value,
        structured_query::{field_filter::Operator, find_nearest},
        value::ValueType,
    },
    val,
};
use googletest::prelude::*;
use tonic::Code;

use super::make_find_nearest;
use crate::harness::{
    builders::{AggregationSpec, QuerySpec, and_filters, doc_path, field_filter, vec_val},
    env::TestEnv,
    matchers::{assert_doc_wire_eq, assert_status_code, short_ids},
};

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
