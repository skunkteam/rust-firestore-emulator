// Note: `tokio_test` is needed because the emulator spawns tasks (e.g. for timeouts), but the tests
// should still be synchronous.

use std::{fmt::Display, sync::Arc};

use googleapis::google::{
    firestore::v1::{
        Cursor, Document, Precondition, StructuredQuery, TransactionOptions, Value, Write,
        precondition,
        structured_query::{
            self, CollectionSelector, find_nearest::DistanceMeasure as ProtoDistanceMeasure,
        },
        transaction_options, write,
    },
    protobuf::{DoubleValue, Int32Value},
};
use googletest::{Result, assert_that, gtest, prelude::*};
use itertools::Itertools;
use tokio_test::{assert_ready, assert_ready_ok, task::spawn};

use crate::{
    FirestoreConfig, FirestoreDatabase, FirestoreProject,
    database::{projection::Projection, transaction::TransactionId},
    document::DocumentMeta,
    query::{
        Direction, DistanceMeasure, FindNearest, Query, QueryBuilder,
        filter::{CompositeFilter, CompositeOperator, FieldFilter, FieldOperator, Filter},
    },
    read_consistency::ReadConsistency,
    reference::{CollectionRef, RootRef},
};

fn fresh_database() -> Result<Arc<FirestoreDatabase>> {
    let database_ref: RootRef = "projects/my-project/databases/my-database".parse()?;
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    Ok(assert_ready!(spawn(project.database(&database_ref)).poll()))
}

fn start_rw_txn(database: &Arc<FirestoreDatabase>) -> TransactionId {
    assert_ready_ok!(
        spawn(database.new_txn(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        }))
        .poll()
    )
}

fn insert_docs<Id: Display, Fields: IntoIterator<Item = (&'static str, Value)>>(
    database: &Arc<FirestoreDatabase>,
    collection_id: &str,
    ids: impl IntoIterator<Item = Id>,
    contents: impl Fn(Id) -> Fields,
) -> Result<()> {
    for i in ids {
        let name = format!("{}/{collection_id}/document-{i}", database.name);
        let fields = contents(i)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let write = Write {
            operation: Some(write::Operation::Update(Document {
                name,
                fields,
                ..Default::default()
            })),
            current_document: Some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(false)),
            }),
            ..Default::default()
        };
        assert_ready_ok!(spawn(database.write(write)).poll());
    }
    Ok(())
}

fn check_matching_docs<'a, Id: Display>(
    database: &Arc<FirestoreDatabase>,
    collection_id: &str,
    docs: impl IntoIterator<Item = &'a Arc<DocumentMeta>> + 'a,
    expected_ids: impl IntoIterator<Item = Id>,
) {
    assert_that!(
        docs.into_iter()
            .map(|d| d.name.to_string())
            .sorted()
            .collect_vec(),
        container_eq(
            expected_ids
                .into_iter()
                .map(|i| format!("{}/{collection_id}/document-{i}", database.name))
                .collect_vec()
        )
    );
}

fn is_locked(meta: &Arc<DocumentMeta>) -> bool {
    spawn(meta.write_owned()).poll().is_pending()
}

type Docs = Vec<Arc<DocumentMeta>>;

fn eval_query(
    database: &Arc<FirestoreDatabase>,
    collection_id: &str,
    query: &mut Query,
) -> Result<(Docs, Docs)> {
    let (_, result) = assert_ready_ok!(spawn(query.once(database)).poll());
    let matched_docnames = result.iter().map(|meta| meta.name.clone()).collect_vec();

    let collection_ref = CollectionRef::new(database.name.clone(), collection_id);
    let collection = assert_ready!(spawn(database.get_collection(&collection_ref)).poll());
    let docs = assert_ready!(spawn(collection.docs()).poll());

    Ok(docs
        .into_iter()
        .partition(|d| matched_docnames.contains(&d.name)))
}

#[gtest]
#[tokio::test]
async fn correct_locking_in_txn_with_cursors() -> Result<()> {
    let database = fresh_database()?;
    let collection_id = "my-collection";
    insert_docs(&database, collection_id, 0..10, |i| {
        [
            ("value", Value::integer(i)),
            ("even", Value::boolean(i % 2 == 0)),
        ]
    })?;

    let txn_id = start_rw_txn(&database);

    let mut query = QueryBuilder::from(
        database.name.clone().into(),
        vec![CollectionSelector {
            all_descendants: false,
            collection_id:   collection_id.to_string(),
        }],
    )
    .consistency(ReadConsistency::Transaction(txn_id))
    .filter(Filter::Field(FieldFilter {
        field: "even".parse()?,
        op:    FieldOperator::Equal,
        value: Value::boolean(true),
    }))
    .order_by("value".parse()?, Direction::Ascending)
    .start_at(Cursor {
        before: true, // inclusive
        values: vec![Value::integer(2)],
    })
    .end_at(Cursor {
        before: true, // exclusive
        values: vec![Value::integer(8)],
    })
    .build()?;

    let (matched_docs, unmatched_docs) = eval_query(&database, collection_id, &mut query)?;

    check_matching_docs(&database, collection_id, &matched_docs, [2, 4, 6]);

    expect_that!(unmatched_docs, len(eq(7)));
    expect_that!(matched_docs, each(predicate(is_locked)));
    expect_that!(unmatched_docs, each(not(predicate(is_locked))));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn correct_locking_in_txn_with_offset_and_limit() -> Result<()> {
    let database = fresh_database()?;
    let collection_id = "my-collection";
    insert_docs(&database, collection_id, 0..10, |i| {
        [
            ("value", Value::integer(i)),
            ("even", Value::boolean(i % 2 == 0)),
        ]
    })?;

    let txn_id = start_rw_txn(&database);

    let mut query = QueryBuilder::from(
        database.name.clone().into(),
        vec![CollectionSelector {
            all_descendants: false,
            collection_id:   collection_id.to_string(),
        }],
    )
    .consistency(ReadConsistency::Transaction(txn_id))
    .filter(Filter::Field(FieldFilter {
        field: "even".parse()?,
        op:    FieldOperator::Equal,
        value: Value::boolean(true),
    }))
    .order_by("value".parse()?, Direction::Ascending)
    .offset(1)
    .limit(3)
    .build()?;

    let (matched_docs, unmatched_docs) = eval_query(&database, collection_id, &mut query)?;

    check_matching_docs(&database, collection_id, &matched_docs, [2, 4, 6]);

    expect_that!(unmatched_docs, len(eq(7)));
    expect_that!(matched_docs, each(predicate(is_locked)));
    expect_that!(unmatched_docs, each(not(predicate(is_locked))));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn correct_locking_in_txn_with_find_nearest() -> Result<()> {
    let database = fresh_database()?;
    let collection_id = "my-collection";
    insert_docs(&database, collection_id, 0..10, |i| {
        [("embedding", Value::vector([i as f64, 0.0]))]
    })?;

    let txn_id = start_rw_txn(&database);

    let mut query = QueryBuilder::from(
        database.name.clone().into(),
        vec![CollectionSelector {
            all_descendants: false,
            collection_id:   collection_id.to_string(),
        }],
    )
    .consistency(ReadConsistency::Transaction(txn_id))
    .find_nearest(FindNearest {
        vector_field: "embedding".parse()?,
        query_vector: vec![3.1, 0.0],
        distance_measure: DistanceMeasure::Euclidean,
        limit: 3,
        distance_result_field: None,
        distance_threshold: None,
    })
    .build()?;

    let (matched_docs, unmatched_docs) = eval_query(&database, collection_id, &mut query)?;

    check_matching_docs(&database, collection_id, &matched_docs, [2, 3, 4]);

    expect_that!(unmatched_docs, len(eq(7)));
    expect_that!(matched_docs, each(predicate(is_locked)));
    expect_that!(unmatched_docs, each(not(predicate(is_locked))));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn find_nearest_distance_measures_and_result_field() -> Result<()> {
    let database = fresh_database()?;
    let col_id = "exact-vectors";
    insert_docs(&database, col_id, 0..4, |i| {
        let v = match i {
            0 => vec![1.0, 0.0],
            1 => vec![0.0, 1.0],
            2 => vec![3.0, 4.0],
            _ => vec![-1.0, 0.0],
        };
        [("embedding", Value::vector(v))]
    })?;

    let from = vec![CollectionSelector {
        all_descendants: false,
        collection_id:   col_id.to_string(),
    }];

    // 1. Euclidean distance from [1.0, 0.0], limit 3
    let mut euclidean_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .find_nearest(FindNearest {
            vector_field: "embedding".parse()?,
            query_vector: vec![1.0, 0.0],
            distance_measure: DistanceMeasure::Euclidean,
            limit: 3,
            distance_result_field: Some("distance".parse()?),
            distance_threshold: None,
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(euclidean_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(ids, eq(&vec!["document-0", "document-1", "document-3"]));

    // Check projected distance field and ensure underlying version is untouched
    let projected_0 = euclidean_query.project(&docs[0]);
    let projected_1 = euclidean_query.project(&docs[1]);
    let projected_3 = euclidean_query.project(&docs[2]);
    assert_that!(
        projected_0.fields.get("distance"),
        some(eq(&Value::double(0.0)))
    );
    assert_that!(
        projected_1
            .fields
            .get("distance")
            .and_then(Value::as_double),
        some(approx_eq(2.0_f64.sqrt()))
    );
    assert_that!(
        projected_3.fields.get("distance"),
        some(eq(&Value::double(2.0)))
    );
    assert_that!(docs[0].fields.get("distance"), none());

    // 2. Cosine distance from [1.0, 0.0], limit 4
    let mut cosine_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .find_nearest(FindNearest {
            vector_field: "embedding".parse()?,
            query_vector: vec![1.0, 0.0],
            distance_measure: DistanceMeasure::Cosine,
            limit: 4,
            distance_result_field: Some("dist".parse()?),
            distance_threshold: None,
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(cosine_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(
        ids,
        eq(&vec![
            "document-0",
            "document-2",
            "document-1",
            "document-3"
        ])
    );
    let dists: Vec<_> = docs
        .iter()
        .map(|d| {
            cosine_query
                .project(d)
                .fields
                .get("dist")
                .and_then(Value::as_double)
                .unwrap()
        })
        .collect();
    assert_that!(dists[0], eq(0.0));
    assert_that!(dists[1], approx_eq(0.4));
    assert_that!(dists[2], approx_eq(1.0));
    assert_that!(dists[3], approx_eq(2.0));

    // 3. DotProduct from [1.0, 1.0], limit 4 (sorted descending, tie-broken by __name__)
    let mut dot_query = QueryBuilder::from(database.name.clone().into(), from)
        .find_nearest(FindNearest {
            vector_field: "embedding".parse()?,
            query_vector: vec![1.0, 1.0],
            distance_measure: DistanceMeasure::DotProduct,
            limit: 4,
            distance_result_field: Some("score".parse()?),
            distance_threshold: None,
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(dot_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(
        ids,
        eq(&vec![
            "document-2",
            "document-0",
            "document-1",
            "document-3"
        ])
    );
    let scores: Vec<_> = docs
        .iter()
        .map(|d| {
            dot_query
                .project(d)
                .fields
                .get("score")
                .and_then(Value::as_double)
                .unwrap()
        })
        .collect();
    assert_that!(scores, eq(&vec![7.0, 1.0, 1.0, -1.0]));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn find_nearest_threshold_prefilter_and_skipped_documents() -> Result<()> {
    let database = fresh_database()?;
    let col_id = "mixed-docs";
    // Insert a mix of valid vectors, wrong dimensions, non-vectors, zero vectors, and missing
    // fields
    insert_docs(&database, col_id, 0..7, |i| match i {
        0 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::vector([1.0, 0.0]))].into()),
            ),
            ("category", Value::string("a")),
            (
                "tags",
                Value::array(vec![Value::string("x"), Value::string("y")]),
            ),
            ("title", Value::string("doc0")),
        ],
        1 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::vector([0.0, 2.0]))].into()),
            ),
            ("category", Value::string("a")),
            (
                "tags",
                Value::array(vec![Value::string("x"), Value::string("z")]),
            ),
            ("title", Value::string("doc1")),
        ],
        2 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::vector([0.0, 5.0]))].into()),
            ),
            ("category", Value::string("b")),
            ("tags", Value::array(vec![Value::string("x")])),
            ("title", Value::string("doc2")),
        ],
        3 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::vector([1.0, 0.0, 0.0]))].into()),
            ),
            ("category", Value::string("a")),
            ("tags", Value::array(vec![Value::string("x")])),
            ("title", Value::string("wrong-dim")),
        ],
        4 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::string("not-a-vector"))].into()),
            ),
            ("category", Value::string("a")),
            ("tags", Value::array(vec![Value::string("x")])),
            ("title", Value::string("not-vector")),
        ],
        5 => vec![
            (
                "vectorQueryFields",
                Value::map([("nested".to_string(), Value::vector([0.0, 0.0]))].into()),
            ),
            ("category", Value::string("a")),
            ("tags", Value::array(vec![Value::string("x")])),
            ("title", Value::string("zero-vec")),
        ],
        _ => vec![
            ("category", Value::string("a")),
            ("tags", Value::array(vec![Value::string("x")])),
            ("title", Value::string("missing-vec")),
        ],
    })?;

    let from = vec![CollectionSelector {
        all_descendants: false,
        collection_id:   col_id.to_string(),
    }];

    // Threshold = 0.0 with Euclidean: only exact match (document-0) passes
    let mut exact_threshold_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![1.0, 0.0],
            distance_measure: DistanceMeasure::Euclidean,
            limit: 10,
            distance_result_field: None,
            distance_threshold: Some(0.0),
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(exact_threshold_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(ids, eq(&vec!["document-0"]));

    // Cosine query: errors with INVALID_ARGUMENT when a matching candidate has a zero-magnitude
    // vector (document-5), but skips wrong-dim (document-3), non-vector (document-4), and missing
    // (document-6) when document-5 is filtered out.
    let mut cosine_err_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![1.0, 0.0],
            distance_measure: DistanceMeasure::Cosine,
            limit: 10,
            distance_result_field: None,
            distance_threshold: None,
        })
        .build()?;
    assert_that!(
        assert_ready!(spawn(cosine_err_query.once(&database)).poll()),
        err(anything())
    );

    let mut cosine_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .filter(Filter::Field(FieldFilter {
            field: "title".parse()?,
            op:    FieldOperator::NotEqual,
            value: Value::string("zero-vec"),
        }))
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![1.0, 0.0],
            distance_measure: DistanceMeasure::Cosine,
            limit: 10,
            distance_result_field: None,
            distance_threshold: None,
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(cosine_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(ids, eq(&vec!["document-0", "document-1", "document-2"]));

    // DotProduct threshold = 2.0 with query_vector [1.0, 1.0]: only dot >= 2.0 (document-2 has 5.0,
    // document-1 has 2.0)
    let mut dot_threshold_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![1.0, 1.0],
            distance_measure: DistanceMeasure::DotProduct,
            limit: 10,
            distance_result_field: None,
            distance_threshold: Some(2.0),
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(dot_threshold_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    assert_that!(ids, eq(&vec!["document-2", "document-1"]));

    // Pre-filtering (category == "a" && tags array-contains "x") + projection ("title",
    // "meta.distance") + nested distance_result_field ("meta.distance")
    let mut prefiltered_query = QueryBuilder::from(database.name.clone().into(), from.clone())
        .select(Projection::try_from(structured_query::Projection {
            fields: vec![
                structured_query::FieldReference {
                    field_path: "title".to_string(),
                },
                structured_query::FieldReference {
                    field_path: "meta.distance".to_string(),
                },
            ],
        })?)
        .filter(Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![
                Filter::Field(FieldFilter {
                    field: "category".parse()?,
                    op:    FieldOperator::Equal,
                    value: Value::string("a"),
                }),
                Filter::Field(FieldFilter {
                    field: "tags".parse()?,
                    op:    FieldOperator::ArrayContains,
                    value: Value::string("x"),
                }),
            ],
        }))
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![0.0, 2.0],
            distance_measure: DistanceMeasure::Euclidean,
            limit: 5,
            distance_result_field: Some("meta.distance".parse()?),
            distance_threshold: Some(2.5),
        })
        .build()?;
    let (_, docs) = assert_ready_ok!(spawn(prefiltered_query.once(&database)).poll());
    let ids: Vec<_> = docs.iter().map(|d| d.name.document_id.as_ref()).collect();
    // document-2 is excluded by category == "a"; document-1 (dist 0.0), document-5 (dist 2.0),
    // document-0 (dist sqrt(5) ≈ 2.236) match
    assert_that!(ids, eq(&vec!["document-1", "document-5", "document-0"]));

    let projected = prefiltered_query.project(&docs[0]);
    assert_that!(
        projected.fields.get("title"),
        some(eq(&Value::string("doc1")))
    );
    assert_that!(projected.fields.get("category"), none());
    assert_that!(
        projected.fields.get("meta"),
        some(eq(&Value::map(
            [("distance".to_string(), Value::double(0.0))].into()
        )))
    );

    // If `select` does not include `distance_result_field`, the distance field is masked out
    let unprojected_dist_query = QueryBuilder::from(database.name.clone().into(), from)
        .select(Projection::try_from(structured_query::Projection {
            fields: vec![structured_query::FieldReference {
                field_path: "title".to_string(),
            }],
        })?)
        .find_nearest(FindNearest {
            vector_field: "vectorQueryFields.nested".parse()?,
            query_vector: vec![0.0, 2.0],
            distance_measure: DistanceMeasure::Euclidean,
            limit: 5,
            distance_result_field: Some("meta.distance".parse()?),
            distance_threshold: None,
        })
        .build()?;
    let masked_doc = unprojected_dist_query.project(&docs[0]);
    assert_that!(
        masked_doc.fields.get("title"),
        some(eq(&Value::string("doc1")))
    );
    assert_that!(masked_doc.fields.get("meta"), none());

    Ok(())
}

#[gtest]
fn find_nearest_validation_errors() -> Result<()> {
    let parent: crate::reference::Ref = "projects/p/databases/d".parse::<RootRef>()?.into();
    let from = vec![CollectionSelector {
        all_descendants: false,
        collection_id:   "col".to_string(),
    }];

    let make_structured = |find_nearest: structured_query::FindNearest| StructuredQuery {
        from: from.clone(),
        find_nearest: Some(find_nearest),
        ..Default::default()
    };

    let valid_fn = structured_query::FindNearest {
        vector_field: Some(structured_query::FieldReference {
            field_path: "embedding".to_string(),
        }),
        query_vector: Some(Value::vector([1.0, 2.0])),
        distance_measure: ProtoDistanceMeasure::Euclidean as i32,
        limit: Some(Int32Value { value: 5 }),
        distance_result_field: String::new(),
        distance_threshold: None,
    };

    // Valid baseline succeeds
    assert_that!(
        Query::from_structured(
            parent.clone(),
            make_structured(valid_fn.clone()),
            ReadConsistency::Default,
            false
        ),
        ok(anything())
    );

    // Invalid limit (0 and > 1000)
    for bad_limit in [0, -1, 1001] {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.limit = Some(Int32Value { value: bad_limit });
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // Empty query_vector, > 2048 dimensions, and non-finite elements (NaN / Infinity)
    for bad_vec in [
        vec![],
        vec![1.0; 2049],
        vec![1.0, f64::NAN],
        vec![1.0, f64::INFINITY],
    ] {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.query_vector = Some(Value::vector(bad_vec));
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // NaN distance_threshold
    {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.distance_threshold = Some(DoubleValue { value: f64::NAN });
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // Cosine with zero-magnitude query_vector
    {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.distance_measure = ProtoDistanceMeasure::Cosine as i32;
        fn_cfg.query_vector = Some(Value::vector([0.0, 0.0]));
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // Unspecified distance_measure
    {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.distance_measure = ProtoDistanceMeasure::Unspecified as i32;
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // Reserved __name__ in vector_field or distance_result_field (plain and backtick-quoted)
    for reserved in ["__name__", "`__name__`"] {
        let mut fn_cfg = valid_fn.clone();
        fn_cfg.vector_field = Some(structured_query::FieldReference {
            field_path: reserved.to_string(),
        });
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );

        let mut fn_cfg = valid_fn.clone();
        fn_cfg.distance_result_field = reserved.to_string();
        assert_that!(
            Query::from_structured(
                parent.clone(),
                make_structured(fn_cfg),
                ReadConsistency::Default,
                false
            ),
            err(anything())
        );
    }

    // Cursors combined with find_nearest
    {
        let mut sq = make_structured(valid_fn.clone());
        sq.start_at = Some(Cursor {
            before: true,
            values: vec![Value::integer(1)],
        });
        assert_that!(
            Query::from_structured(parent.clone(), sq, ReadConsistency::Default, false),
            err(anything())
        );
    }

    // Explicit order_by combined with find_nearest
    {
        let mut sq = make_structured(valid_fn);
        sq.order_by = vec![structured_query::Order {
            field:     Some(structured_query::FieldReference {
                field_path: "title".to_string(),
            }),
            direction: structured_query::Direction::Ascending as i32,
        }];
        assert_that!(
            Query::from_structured(parent, sq, ReadConsistency::Default, false),
            err(anything())
        );
    }

    Ok(())
}
