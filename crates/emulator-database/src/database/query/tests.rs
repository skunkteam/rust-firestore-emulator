// Note: `tokio_test` is needed because the emulator spawns tasks (e.g. for timeouts), but the tests
// should still be synchronous.

use std::{fmt::Display, sync::Arc};

use googleapis::google::firestore::v1::{
    Cursor, Document, Precondition, TransactionOptions, Value, Write, precondition,
    structured_query::CollectionSelector, transaction_options, write,
};
use googletest::prelude::*;
use itertools::Itertools;
use tokio_test::{assert_ready, assert_ready_ok, task::spawn};

use crate::{
    FirestoreConfig, FirestoreDatabase, FirestoreProject, GenericDatabaseError,
    database::{
        lock::{LockPriority, SessionKind},
        transaction::TransactionId,
    },
    document::DocumentMeta,
    query::{
        Direction, DistanceMeasure, FindNearest, Query, QueryBuilder,
        filter::{FieldFilter, FieldOperator, Filter},
    },
    read_consistency::ReadConsistency,
    reference::{CollectionRef, RootRef},
};

fn fresh_database() -> Result<Arc<FirestoreDatabase>> {
    let database_ref: RootRef = "projects/my-project/databases/my-database".parse()?;
    let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
    Ok(assert_ready!(
        spawn(project.database(&database_ref)).poll_until_idle()
    ))
}

fn start_rw_txn(database: &Arc<FirestoreDatabase>) -> TransactionId {
    assert_ready_ok!(
        spawn(database.new_txn(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        }))
        .poll_until_idle()
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
        assert_ready_ok!(spawn(database.write(write)).poll_until_idle());
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

fn is_locked(db: &FirestoreDatabase) -> impl Fn(&Arc<DocumentMeta>) -> bool + '_ {
    move |meta| {
        let session = db.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        spawn(session.acquire_write(&meta.name))
            .poll_until_idle()
            .is_pending()
    }
}

type Docs = Vec<Arc<DocumentMeta>>;

fn eval_query(
    database: &Arc<FirestoreDatabase>,
    collection_id: &str,
    query: &mut Query,
) -> Result<(Docs, Docs)> {
    let (_, result) = assert_ready_ok!(spawn(query.once(database)).poll_until_idle());
    let matched_docnames = result.iter().map(|m| m.version.name.clone()).collect_vec();

    let collection_ref = CollectionRef::new(database.name.clone(), collection_id);
    let collection =
        assert_ready!(spawn(database.get_collection(&collection_ref)).poll_until_idle());
    let docs = assert_ready!(spawn(collection.docs()).poll_until_idle());

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
    expect_that!(matched_docs, each(predicate(is_locked(&database))));
    expect_that!(unmatched_docs, each(not(predicate(is_locked(&database)))));

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
    expect_that!(matched_docs, each(predicate(is_locked(&database))));
    expect_that!(unmatched_docs, each(not(predicate(is_locked(&database)))));

    Ok(())
}

#[gtest]
#[tokio::test]
async fn query_builder_find_nearest_validates_and_executes() -> Result<()> {
    let database = fresh_database()?;
    let collection_id = "vec-collection";
    insert_docs(&database, collection_id, 0..3, |i| {
        [("emb", Value::vector([i as f64, 0.0]))]
    })?;

    let col_ref = CollectionRef::new(database.name.clone(), collection_id);
    let mut query = QueryBuilder::from_collection(col_ref.clone())
        .find_nearest(
            FindNearest::new(
                "emb".parse()?,
                vec![0.2, 0.0],
                DistanceMeasure::Euclidean,
                2,
            )
            .distance_result_field("dist".parse()?)
            .distance_threshold(1.0),
        )
        .build()?;
    assert!(query.has_find_nearest());

    let (_, matches) = assert_ready_ok!(spawn(query.once(&database)).poll_until_idle());
    let projected: Vec<_> = matches.iter().map(|m| query.project(m)).collect();
    expect_eq!(projected.len(), 2);
    expect_eq!(
        projected[0].name,
        format!("{}/{collection_id}/document-0", database.name)
    );
    expect_eq!(projected[0].fields["dist"].as_double(), Some(0.2));
    expect_eq!(
        projected[1].name,
        format!("{}/{collection_id}/document-1", database.name)
    );
    expect_eq!(projected[1].fields["dist"].as_double(), Some(0.8));

    // Invalid FindNearest constraints caught by Query::validate
    for bad_limit in [0, 1001] {
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .find_nearest(FindNearest::new(
                    "emb".parse()?,
                    vec![1.0, 0.0],
                    DistanceMeasure::Euclidean,
                    bad_limit,
                ))
                .build(),
            err(eq(&GenericDatabaseError::invalid_argument(
                "find_nearest limit must be between 1 and 1000"
            )))
        );
    }

    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![],
                DistanceMeasure::Euclidean,
                2,
            ))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cannot have a zero length vector."
        )))
    );

    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .enterprise_edition(false)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![0.0, 0.0],
                DistanceMeasure::Cosine,
                2,
            ))
            .build(),
        err(eq(&GenericDatabaseError::failed_precondition(
            "Cannot compute cosine distance against a vector with a magnitude of zero."
        )))
    );

    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .enterprise_edition(true)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![0.0, 0.0],
                DistanceMeasure::Cosine,
                2,
            ))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "cosine_distance(...) result is NaN."
        )))
    );

    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![1.0, 0.0],
                DistanceMeasure::Euclidean,
                2,
            ))
            .limit(1)
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "A query limit cannot be used with FindNearest"
        )))
    );

    // Filter and cursor malformed vector operand checks
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "emb".parse()?,
                op:    FieldOperator::Equal,
                value: Value::vector([]),
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cannot have a zero length vector."
        )))
    );

    assert_that!(
        QueryBuilder::from_collection(col_ref)
            .order_by("emb".parse()?, Direction::Ascending)
            .start_at(Cursor {
                before: true,
                values: vec![Value::vector([])],
            })
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cannot have a zero length vector."
        )))
    );

    Ok(())
}

#[gtest]
#[tokio::test]
async fn implicit_order_by_matches_firestore_semantics() -> Result<()> {
    use crate::query::filter::{CompositeFilter, CompositeOperator};

    let database = fresh_database()?;
    let col = "implicit-order";
    // Insert documents with ties and distinct fields:
    // document-1: { a: 10, b: 30, tie: 5 }
    // document-2: { a: 10, b: 10, tie: 5 }
    // document-3: { a: 20, b: 5,  tie: 5 }
    for (id, a, b) in [(1, 10, 30), (2, 10, 10), (3, 20, 5)] {
        insert_docs(&database, col, [id], |_| {
            [
                ("a", Value::integer(a)),
                ("b", Value::integer(b)),
                ("tie", Value::integer(5)),
            ]
        })?;
    }

    let col_ref = CollectionRef::new(database.name.clone(), col);
    let ids_of = |mut q: Query| -> Result<Vec<String>> {
        let (_, matches) = assert_ready_ok!(spawn(q.once(&database)).poll_until_idle());
        Ok(matches
            .into_iter()
            .map(|m| m.version.name.document_id.to_string())
            .collect())
    };

    // 1. `orderBy('tie', 'desc')` on Standard Edition appends `__name__ DESC`: document-3, -2, -1
    let q_desc = QueryBuilder::from_collection(col_ref.clone())
        .order_by("tie".parse()?, Direction::Descending)
        .build()?;
    assert_eq!(
        ids_of(q_desc)?,
        vec!["document-3", "document-2", "document-1"]
    );

    // 2. Enterprise Edition `all_descendants` does NOT synthesize implicit `__name__` ordering
    //    (matching live Cloud Firestore): `order_by` remains unchanged, extra cursor values are
    //    rejected, and explicit `(tie DESC, __name__ ASC/DESC)` cursors obey their direction.
    let ent_from = vec![CollectionSelector {
        collection_id:   col.to_string(),
        all_descendants: true,
    }];
    let q_ent_no_order = QueryBuilder::from(database.name.clone().into(), ent_from.clone())
        .enterprise_edition(true)
        .build()?;
    assert!(q_ent_no_order.order_by().is_empty());

    let q_ent_desc = QueryBuilder::from(database.name.clone().into(), ent_from.clone())
        .enterprise_edition(true)
        .order_by("tie".parse()?, Direction::Descending)
        .build()?;
    assert_eq!(q_ent_desc.order_by().len(), 1);
    let mut ent_desc_ids = ids_of(q_ent_desc)?;
    ent_desc_ids.sort();
    assert_eq!(ent_desc_ids, vec!["document-1", "document-2", "document-3"]);

    let doc2_ref = format!("{}/{col}/document-2", database.name);
    assert_that!(
        QueryBuilder::from(database.name.clone().into(), ent_from.clone())
            .enterprise_edition(true)
            .order_by("tie".parse()?, Direction::Descending)
            .start_at(Cursor {
                values: vec![Value::integer(5), Value::reference(doc2_ref.clone())],
                before: true,
            })
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cursor has too many values."
        )))
    );
    assert_that!(
        QueryBuilder::from(database.name.clone().into(), ent_from.clone())
            .enterprise_edition(true)
            .start_at(Cursor {
                values: vec![Value::reference(doc2_ref.clone())],
                before: true,
            })
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cursor has too many values."
        )))
    );

    let q_ent_explicit_desc = QueryBuilder::from(database.name.clone().into(), ent_from.clone())
        .enterprise_edition(true)
        .order_by("tie".parse()?, Direction::Descending)
        .order_by("__name__".parse()?, Direction::Descending)
        .start_at(Cursor {
            values: vec![Value::integer(5), Value::reference(doc2_ref.clone())],
            before: true,
        })
        .build()?;
    assert_eq!(
        ids_of(q_ent_explicit_desc)?,
        vec!["document-2", "document-1"]
    );

    let q_ent_explicit_asc = QueryBuilder::from(database.name.clone().into(), ent_from)
        .enterprise_edition(true)
        .order_by("tie".parse()?, Direction::Descending)
        .order_by("__name__".parse()?, Direction::Ascending)
        .start_at(Cursor {
            values: vec![Value::integer(5), Value::reference(doc2_ref)],
            before: true,
        })
        .build()?;
    assert_eq!(
        ids_of(q_ent_explicit_asc)?,
        vec!["document-2", "document-3"]
    );

    // 3. `WHERE __name__ > ... AND a > 0` orders by `a ASC, __name__ ASC`, not `__name__` first
    let doc1_ref = format!("{}/{col}/document-1", database.name);
    let q_name_and_field = QueryBuilder::from_collection(col_ref.clone())
        .filter(Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![
                Filter::Field(FieldFilter {
                    field: "__name__".parse()?,
                    op:    FieldOperator::GreaterThan,
                    value: Value::reference(doc1_ref.clone()),
                }),
                Filter::Field(FieldFilter {
                    field: "a".parse()?,
                    op:    FieldOperator::GreaterThan,
                    value: Value::integer(0),
                }),
            ],
        }))
        .build()?;
    assert_eq!(ids_of(q_name_and_field)?, vec!["document-2", "document-3"]);

    // 4. `WHERE b > 0 AND a > 0` orders lexicographically (`a ASC, b ASC, __name__ ASC`)
    let q_lex = QueryBuilder::from_collection(col_ref.clone())
        .filter(Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![
                Filter::Field(FieldFilter {
                    field: "b".parse()?,
                    op:    FieldOperator::GreaterThan,
                    value: Value::integer(0),
                }),
                Filter::Field(FieldFilter {
                    field: "a".parse()?,
                    op:    FieldOperator::GreaterThan,
                    value: Value::integer(0),
                }),
            ],
        }))
        .build()?;
    assert_eq!(
        ids_of(q_lex)?,
        vec!["document-2", "document-1", "document-3"]
    );

    // 5. Duplicate inequality on same field (`a > 0 AND a < 30`) deduplicates `a`, while cursor
    //    length is validated against explicit `order_by`: omitting explicit `order_by` rejects a
    //    2-value cursor, and specifying explicit `(a, __name__)` succeeds.
    let dup_filter = Filter::Composite(CompositeFilter {
        op:      CompositeOperator::And,
        filters: vec![
            Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }),
            Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::LessThan,
                value: Value::integer(30),
            }),
        ],
    });
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(dup_filter.clone())
            .start_at(Cursor {
                before: false,
                values: vec![Value::integer(10), Value::reference(doc1_ref.clone())],
            })
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cursor has too many values."
        )))
    );
    let q_dup_cursor = QueryBuilder::from_collection(col_ref.clone())
        .filter(dup_filter)
        .order_by("a".parse()?, Direction::Ascending)
        .order_by("__name__".parse()?, Direction::Ascending)
        .start_at(Cursor {
            before: false, // startAfter (10, document-1) -> document-2, document-3
            values: vec![Value::integer(10), Value::reference(doc1_ref)],
        })
        .build()?;
    assert_eq!(ids_of(q_dup_cursor)?, vec!["document-2", "document-3"]);

    // 6. Explicit `orderBy('a', 'desc')` + inequality `b > 0` appends `b DESC, __name__ DESC`
    let q_partial = QueryBuilder::from_collection(col_ref)
        .filter(Filter::Field(FieldFilter {
            field: "b".parse()?,
            op:    FieldOperator::GreaterThan,
            value: Value::integer(0),
        }))
        .order_by("a".parse()?, Direction::Descending)
        .build()?;
    assert_eq!(
        ids_of(q_partial)?,
        vec!["document-3", "document-1", "document-2"]
    );

    Ok(())
}

#[gtest]
fn query_operand_vector_validation() {
    use std::collections::HashMap;

    use googleapis::{VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY};

    use super::validate_query_operand_value;

    let zero_d = Value::vector([]);
    let over_2048 = Value::vector(std::iter::repeat_n(1.0, 2049));
    let non_double = Value::map(HashMap::from([
        (
            VECTOR_MAP_TYPE_KEY.to_string(),
            Value::string(VECTOR_MAP_TYPE_SENTINEL),
        ),
        (
            VECTOR_MAP_VALUE_KEY.to_string(),
            Value::array(vec![Value::integer(1)]),
        ),
    ]));
    let nan_vec = Value::vector([f64::NAN, 0.0]);

    for enterprise in [false, true] {
        assert_that!(
            validate_query_operand_value(&zero_d, enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Cannot have a zero length vector."
            )))
        );
        assert_that!(
            validate_query_operand_value(&over_2048, enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Vectors must be at most 2048 dimensions."
            )))
        );
        assert_that!(
            validate_query_operand_value(&non_double, enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Vector must only contain values of type 'double'"
            )))
        );
        let nested_arr = Value::array(vec![zero_d.clone()]);
        assert_that!(
            validate_query_operand_value(&nested_arr, enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Cannot have a zero length vector."
            )))
        );
        let nested_map = Value::map(HashMap::from([("v".to_string(), zero_d.clone())]));
        assert_that!(
            validate_query_operand_value(&nested_map, enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Cannot have a zero length vector."
            )))
        );

        // Unset Value payloads and malformed ReferenceValues inside arrays and maps fail cleanly.
        assert_that!(
            validate_query_operand_value(&Value::default(), enterprise),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Cannot convert firestore.v1.Value with type unset."
            )))
        );
        for bad_ref in [
            "projects/p/databases/d/documents/col",
            "projects/p/databases/d/documents/col/doc/sub",
            "projects/p/databases/d/documents/col/__custom__",
        ] {
            let arr_ref = Value::array(vec![Value::reference(bad_ref.to_string())]);
            assert_that!(
                validate_query_operand_value(&arr_ref, enterprise),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
            let map_ref = Value::map(HashMap::from([(
                "r".to_string(),
                Value::reference(bad_ref.to_string()),
            )]));
            assert_that!(
                validate_query_operand_value(&map_ref, enterprise),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }

        let docs_root_ref = Value::reference("projects/p/databases/d/documents".to_string());
        assert!(validate_query_operand_value(&docs_root_ref, enterprise).is_ok());

        let ds_ref = Value::reference(
            "projects/p/databases/(default)/documents/col/__id-9223372036854775808__".to_string(),
        );
        assert!(validate_query_operand_value(&ds_ref, enterprise).is_ok());

        use googleapis::google::{
            firestore::v1::{Function, Pipeline, value::ValueType},
            protobuf::Timestamp,
            r#type::LatLng,
        };
        for bad_geo in [
            LatLng {
                latitude:  90.0001,
                longitude: 0.0,
            },
            LatLng {
                latitude:  -90.0001,
                longitude: 0.0,
            },
            LatLng {
                latitude:  0.0,
                longitude: 180.0001,
            },
            LatLng {
                latitude:  0.0,
                longitude: -180.0001,
            },
            LatLng {
                latitude:  f64::NAN,
                longitude: 0.0,
            },
            LatLng {
                latitude:  0.0,
                longitude: f64::INFINITY,
            },
        ] {
            assert_that!(
                validate_query_operand_value(&Value::from(bad_geo), enterprise),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
        assert!(
            validate_query_operand_value(
                &Value::from(LatLng {
                    latitude:  -90.0,
                    longitude: 180.0,
                }),
                enterprise,
            )
            .is_ok()
        );

        for bad_ts in [
            Timestamp {
                seconds: 0,
                nanos:   -1,
            },
            Timestamp {
                seconds: 0,
                nanos:   1_000_000_000,
            },
            Timestamp {
                seconds: -62_135_596_801,
                nanos:   0,
            },
            Timestamp {
                seconds: 253_402_300_800,
                nanos:   0,
            },
        ] {
            assert_that!(
                validate_query_operand_value(&Value::from(bad_ts), enterprise),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
        for ok_ts in [
            Timestamp {
                seconds: -62_135_596_800,
                nanos:   0,
            },
            Timestamp {
                seconds: 253_402_300_799,
                nanos:   999_999_999,
            },
        ] {
            assert!(validate_query_operand_value(&Value::from(ok_ts), enterprise).is_ok());
        }

        for unsupported_vt in [
            ValueType::FieldReferenceValue("a".to_string()),
            ValueType::VariableReferenceValue("a".to_string()),
            ValueType::FunctionValue(Function::default()),
            ValueType::PipelineValue(Pipeline::default()),
        ] {
            let unsupported = Value {
                value_type: Some(unsupported_vt),
            };
            assert_that!(
                validate_query_operand_value(&unsupported, enterprise),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    assert!(validate_query_operand_value(&nan_vec, false).is_ok());
    assert_that!(
        validate_query_operand_value(&nan_vec, true),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Vector cannot contain NaN values."
        )))
    );
}

#[gtest]
fn dnf_summary_clamping_overflow_guard_and_conservatism() {
    use super::{
        DnfSummary,
        filter::{CompositeFilter, CompositeOperator, UnaryFilter, UnaryOperator},
    };

    let eq_filter = |n: i64| {
        Filter::Field(FieldFilter {
            field: "a".parse().unwrap(),
            op:    FieldOperator::Equal,
            value: Value::integer(n),
        })
    };
    let in_filter = |field: &str, vals: Vec<Value>| {
        Filter::Field(FieldFilter {
            field: field.parse().unwrap(),
            op:    FieldOperator::In,
            value: Value::array(vals),
        })
    };
    let ac_filter = |field: &str| {
        Filter::Field(FieldFilter {
            field: field.parse().unwrap(),
            op:    FieldOperator::ArrayContains,
            value: Value::string("x"),
        })
    };
    let aca_filter = |field: &str, vals: Vec<Value>| {
        Filter::Field(FieldFilter {
            field: field.parse().unwrap(),
            op:    FieldOperator::ArrayContainsAny,
            value: Value::array(vals),
        })
    };

    // Unary & empty AND
    assert_eq!(
        DnfSummary::from_filter(&Filter::Unary(UnaryFilter {
            field: "x".parse().unwrap(),
            op:    UnaryOperator::IsNull,
        })),
        DnfSummary {
            disjunctions: 1,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![],
        })),
        DnfSummary {
            disjunctions: 1,
            max_array_contains: 0,
        }
    );

    // unique_value_count uses storage/wire equality (`1 != 1.0`, `0.0 != -0.0`, `NaN == NaN`)
    // and clamps > 31 to 31.
    assert_eq!(
        DnfSummary::from_filter(&in_filter(
            "a",
            vec![Value::integer(1), Value::double(1.0), Value::integer(2)]
        )),
        DnfSummary {
            disjunctions: 3,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&in_filter(
            "a",
            vec![Value::double(0.0), Value::double(-0.0)]
        )),
        DnfSummary {
            disjunctions: 2,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&in_filter("a", vec![Value::integer(1), Value::integer(1)])),
        DnfSummary {
            disjunctions: 1,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&in_filter(
            "a",
            vec![
                Value::double(f64::NAN),
                Value::double(f64::from_bits(0xfff8_0000_0000_0000))
            ]
        )),
        DnfSummary {
            disjunctions: 1,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&in_filter("a", (0..35).map(Value::integer).collect())),
        DnfSummary {
            disjunctions: 31,
            max_array_contains: 0,
        }
    );

    // 16 * 16 = 256 u8-overflow regression: saturates and clamps cleanly to 31.
    let sixteen_a = in_filter("a", (0..16).map(Value::integer).collect());
    let sixteen_b = in_filter("b", (0..16).map(Value::integer).collect());
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![sixteen_a, sixteen_b],
        })),
        DnfSummary {
            disjunctions: 31,
            max_array_contains: 0,
        }
    );

    // OR with 1 ArrayContains per branch stays at max_array_contains = 1; AND of 2 or 3 clamps to
    // 2.
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::Or,
            filters: vec![
                ac_filter("t1"),
                aca_filter("t2", vec![Value::integer(1), Value::integer(2)])
            ],
        })),
        DnfSummary {
            disjunctions: 3,
            max_array_contains: 1,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![ac_filter("t1"), ac_filter("t2"), ac_filter("t3")],
        })),
        DnfSummary {
            disjunctions: 1,
            max_array_contains: 2,
        }
    );

    // OR sums disjunctions and clamps at 31; conservative same-field over-counts count branches.
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::Or,
            filters: (0..31).map(|_| eq_filter(1)).collect(),
        })),
        DnfSummary {
            disjunctions: 31,
            max_array_contains: 0,
        }
    );
    assert_eq!(
        DnfSummary::from_filter(&Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![
                in_filter("a", (1..=6).map(Value::integer).collect()),
                in_filter("a", (2..=7).map(Value::integer).collect()),
            ],
        })),
        DnfSummary {
            disjunctions: 31,
            max_array_contains: 0,
        }
    );
}

#[gtest]
#[tokio::test]
async fn query_builder_validation_and_escaped_dot_sorting() -> Result<()> {
    use crate::query::filter::{CompositeFilter, CompositeOperator, UnaryFilter, UnaryOperator};

    let database = fresh_database()?;
    let col_ref = CollectionRef::new(database.name.clone(), "escaped-dots");
    let valid_doc = format!("{}/escaped-dots/doc-1", database.name);
    let other_proj_doc = "projects/other/databases/my-database/documents/escaped-dots/doc-1";
    let other_db_doc = "projects/my-project/databases/other/documents/escaped-dots/doc-1";

    // Escaped dot sorting (`a.b` < `` `a.b` ``) in both regular implicit order_by and findNearest
    // tie-breaking.
    insert_docs(&database, "escaped-dots", [1, 2], |i| {
        if i == 1 {
            [
                (
                    "a",
                    Value::map(std::collections::HashMap::from([(
                        "b".to_string(),
                        Value::integer(20),
                    )])),
                ),
                ("a.b", Value::integer(1)),
                ("emb", Value::vector([1.0, 0.0])),
            ]
        } else {
            [
                (
                    "a",
                    Value::map(std::collections::HashMap::from([(
                        "b".to_string(),
                        Value::integer(10),
                    )])),
                ),
                ("a.b", Value::integer(99)),
                ("emb", Value::vector([1.0, 0.0])),
            ]
        }
    })?;

    let escaped_filter = Filter::Composite(CompositeFilter {
        op:      CompositeOperator::And,
        filters: vec![
            Filter::Field(FieldFilter {
                field: "`a.b`".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }),
            Filter::Field(FieldFilter {
                field: "a.b".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }),
        ],
    });
    let mut q_regular = QueryBuilder::from_collection(col_ref.clone())
        .filter(escaped_filter.clone())
        .build()?;
    let (_, regular_matches) = assert_ready_ok!(spawn(q_regular.once(&database)).poll_until_idle());
    assert_eq!(
        regular_matches
            .iter()
            .map(|m| m.version.name.document_id.as_str())
            .collect_vec(),
        vec!["document-2", "document-1"]
    );

    // Empty `order_by` Standard `find_nearest` safely skips `__name__`-last check and tie-breaks by
    // segment order (`a.b` before `` `a.b` ``).
    let mut q_vec = QueryBuilder::from_collection(col_ref.clone())
        .filter(escaped_filter)
        .find_nearest(FindNearest::new(
            "emb".parse()?,
            vec![1.0, 0.0],
            DistanceMeasure::Euclidean,
            5,
        ))
        .build()?;
    let (_, vec_matches) = assert_ready_ok!(spawn(q_vec.once(&database)).poll_until_idle());
    assert_eq!(
        vec_matches
            .iter()
            .map(|m| m.version.name.document_id.as_str())
            .collect_vec(),
        vec!["document-2", "document-1"]
    );

    // Explicit `orderBy('a', DESC)` rejects 2-value cursor without explicit `__name__`.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("a.b".parse()?, Direction::Descending)
            .end_at(Cursor {
                before: true,
                values: vec![Value::integer(10), Value::reference(valid_doc.clone())],
            })
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Cursor has too many values."
        )))
    );

    // Cursor `__name__` slot type & root validation across Standard and Enterprise.
    for enterprise in [false, true] {
        assert!(
            QueryBuilder::from_collection(col_ref.clone())
                .enterprise_edition(enterprise)
                .order_by("__name__".parse()?, Direction::Ascending)
                .start_at(Cursor {
                    before: true,
                    values: vec![Value::reference(database.name.to_string())],
                })
                .build()
                .is_ok()
        );
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .enterprise_edition(enterprise)
                .order_by("__name__".parse()?, Direction::Ascending)
                .start_at(Cursor {
                    before: true,
                    values: vec![Value::string("not-a-ref")],
                })
                .build(),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Cursor __key__ value is not a document reference."
            )))
        );
        for wrong_root in [other_proj_doc, other_db_doc] {
            assert_that!(
                QueryBuilder::from_collection(col_ref.clone())
                    .enterprise_edition(enterprise)
                    .order_by("__name__".parse()?, Direction::Ascending)
                    .start_at(Cursor {
                        before: true,
                        values: vec![Value::reference(wrong_root.to_string())],
                    })
                    .build(),
                err(eq(&GenericDatabaseError::invalid_argument(
                    "The cursor key is in a different database than the query"
                )))
            );
        }
    }

    // Standard duplicate `order_by` and `__name__`-not-last on both regular and `find_nearest`.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("a".parse()?, Direction::Ascending)
            .order_by("a".parse()?, Direction::Descending)
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("a".parse()?, Direction::Ascending)
            .order_by("a".parse()?, Direction::Ascending)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![1.0, 0.0],
                DistanceMeasure::Euclidean,
                2,
            ))
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("__name__".parse()?, Direction::Ascending)
            .order_by("a".parse()?, Direction::Ascending)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![1.0, 0.0],
                DistanceMeasure::Euclidean,
                2,
            ))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "order by clause cannot contain more fields after the key"
        )))
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }))
            .order_by("__name__".parse()?, Direction::Ascending)
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "order by clause cannot contain more fields after the key"
        )))
    );

    // Array caps (30 ok vs 31 err; 10 ok vs 11 err), non-array/empty, empty OR vs empty AND.
    for (op, cap) in [
        (FieldOperator::In, 30),
        (FieldOperator::ArrayContainsAny, 30),
        (FieldOperator::NotIn, 10),
    ] {
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "x".parse()?,
                    op,
                    value: Value::integer(1),
                }))
                .build(),
            err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
        );
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "x".parse()?,
                    op,
                    value: Value::array(vec![]),
                }))
                .build(),
            err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
        );
        assert!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "x".parse()?,
                    op,
                    value: Value::array((0..cap).map(Value::integer).collect()),
                }))
                .build()
                .is_ok()
        );
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "x".parse()?,
                    op,
                    value: Value::array((0..=cap).map(Value::integer).collect()),
                }))
                .build(),
            err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
        );
    }

    assert!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Composite(CompositeFilter {
                op:      CompositeOperator::And,
                filters: vec![],
            }))
            .build()
            .is_ok()
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Composite(CompositeFilter {
                op:      CompositeOperator::Or,
                filters: vec![],
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Composite filter must have at least one sub-filter."
        )))
    );

    // Negation exclusivity & NOT_IN disjunction exclusion.
    let neg_filters = [
        Filter::Field(FieldFilter {
            field: "a".parse()?,
            op:    FieldOperator::NotEqual,
            value: Value::integer(1),
        }),
        Filter::Field(FieldFilter {
            field: "b".parse()?,
            op:    FieldOperator::NotIn,
            value: Value::array(vec![Value::integer(1)]),
        }),
        Filter::Unary(UnaryFilter {
            field: "c".parse()?,
            op:    UnaryOperator::IsNotNan,
        }),
        Filter::Unary(UnaryFilter {
            field: "d".parse()?,
            op:    UnaryOperator::IsNotNull,
        }),
    ];
    for single in &neg_filters {
        assert!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(single.clone())
                .build()
                .is_ok()
        );
    }
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Composite(CompositeFilter {
                op:      CompositeOperator::And,
                filters: vec![neg_filters[0].clone(), neg_filters[2].clone()],
            }))
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );
    for disj in [
        Filter::Composite(CompositeFilter {
            op:      CompositeOperator::Or,
            filters: vec![Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::Equal,
                value: Value::integer(1),
            })],
        }),
        Filter::Field(FieldFilter {
            field: "a".parse()?,
            op:    FieldOperator::In,
            value: Value::array(vec![Value::integer(1)]),
        }),
        Filter::Field(FieldFilter {
            field: "a".parse()?,
            op:    FieldOperator::ArrayContainsAny,
            value: Value::array(vec![Value::integer(1)]),
        }),
    ] {
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Composite(CompositeFilter {
                    op:      CompositeOperator::And,
                    filters: vec![neg_filters[1].clone(), disj],
                }))
                .build(),
            err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
        );
    }

    // Standard `__name__` filter constraints and reserved path rejections.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "__name__".parse()?,
                op:    FieldOperator::Equal,
                value: Value::reference(format!("{}/items/__id0__", database.name)),
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Document ID of type long must be non-zero"
        )))
    );
    assert!(
        QueryBuilder::from_collection(col_ref.clone())
            .enterprise_edition(true)
            .filter(Filter::Field(FieldFilter {
                field: "__name__".parse()?,
                op:    FieldOperator::Equal,
                value: Value::reference(format!("{}/items/__id0__", database.name)),
            }))
            .build()
            .is_ok()
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "__name__".parse()?,
                op:    FieldOperator::Equal,
                value: Value::reference(database.name.to_string()),
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "Key path is empty."
        )))
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Unary(UnaryFilter {
                field: "__name__".parse()?,
                op:    UnaryOperator::IsNotNull,
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "__key__ filter value must be a Key"
        )))
    );
    for bad_op in [
        FieldOperator::ArrayContains,
        FieldOperator::ArrayContainsAny,
    ] {
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "__name__".parse()?,
                    op:    bad_op,
                    value: Value::array(vec![Value::reference(valid_doc.clone())]),
                }))
                .build(),
            err(eq(&GenericDatabaseError::invalid_argument(
                "the name __key__ is reserved"
            )))
        );
    }
    for wrong_root in [other_proj_doc, other_db_doc] {
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "__name__".parse()?,
                    op:    FieldOperator::Equal,
                    value: Value::reference(wrong_root.to_string()),
                }))
                .build(),
            err(eq(&GenericDatabaseError::invalid_argument(
                "__key__ filter reference must match the target project and database"
            )))
        );
        assert_that!(
            QueryBuilder::from_collection(col_ref.clone())
                .filter(Filter::Field(FieldFilter {
                    field: "__name__".parse()?,
                    op:    FieldOperator::In,
                    value: Value::array(vec![
                        Value::reference(valid_doc.clone()),
                        Value::reference(wrong_root.to_string()),
                    ]),
                }))
                .build(),
            err(eq(&GenericDatabaseError::invalid_argument(
                "__key__ filter reference must match the target project and database"
            )))
        );
    }
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "__name__".parse()?,
                op:    FieldOperator::NotIn,
                value: Value::array(vec![Value::string("bad")]),
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "__key__ filter value must be a Key"
        )))
    );

    // Reserved field paths on order_by, FieldFilter, and UnaryFilter.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("__custom__".parse()?, Direction::Ascending)
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Unary(UnaryFilter {
                field: "a..b".parse()?,
                op:    UnaryOperator::IsNull,
            }))
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );
    // Backtick-quoted `` `__name__` `` resolves to DocumentName in filters and order_by.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "`__name__`".parse()?,
                op:    FieldOperator::Equal,
                value: Value::integer(1),
            }))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "__key__ filter value must be a Key"
        )))
    );
    for enterprise in [false, true] {
        assert!(
            QueryBuilder::from_collection(col_ref.clone())
                .enterprise_edition(enterprise)
                .filter(Filter::Field(FieldFilter {
                    field: "`__name__`".parse()?,
                    op:    FieldOperator::Equal,
                    value: Value::reference(valid_doc.clone()),
                }))
                .order_by("`__name__`".parse()?, Direction::Ascending)
                .build()
                .is_ok()
        );
    }
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .order_by("__name__".parse()?, Direction::Ascending)
            .order_by("`__name__`".parse()?, Direction::Ascending)
            .build(),
        err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
    );

    // Standard find_nearest enforces `__name__`-last after inequality synthesis, tie-breaks
    // synthesized inequalities with `last_dir`, and filters OR+inequality docs missing the field.
    assert_that!(
        QueryBuilder::from_collection(col_ref.clone())
            .filter(Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }))
            .order_by("__name__".parse()?, Direction::Ascending)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![1.0, 0.0],
                DistanceMeasure::Euclidean,
                5,
            ))
            .build(),
        err(eq(&GenericDatabaseError::invalid_argument(
            "order by clause cannot contain more fields after the key"
        )))
    );
    assert!(
        QueryBuilder::from_collection(col_ref)
            .enterprise_edition(true)
            .filter(Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }))
            .order_by("__name__".parse()?, Direction::Ascending)
            .find_nearest(FindNearest::new(
                "emb".parse()?,
                vec![1.0, 0.0],
                DistanceMeasure::Euclidean,
                5,
            ))
            .build()
            .is_ok()
    );

    let fn_col = CollectionRef::new(database.name.clone(), "fn-std-ineq");
    insert_docs(&database, "fn-std-ineq", [1, 2, 3], |i| match i {
        1 => [
            ("a", Value::integer(10)),
            ("b", Value::integer(5)),
            ("emb", Value::vector([1.0, 0.0])),
        ],
        2 => [
            ("a", Value::integer(20)),
            ("b", Value::integer(5)),
            ("emb", Value::vector([1.0, 0.0])),
        ],
        _ => [
            // Missing `a` so only `b == 1` holds in `OR`.
            ("a_unused", Value::integer(0)),
            ("b", Value::integer(1)),
            ("emb", Value::vector([1.0, 0.0])),
        ],
    })?;
    let mut fn_desc_tie = QueryBuilder::from_collection(fn_col.clone())
        .filter(Filter::Field(FieldFilter {
            field: "a".parse()?,
            op:    FieldOperator::GreaterThan,
            value: Value::integer(0),
        }))
        .order_by("b".parse()?, Direction::Descending)
        .find_nearest(FindNearest::new(
            "emb".parse()?,
            vec![0.0, 0.0],
            DistanceMeasure::Euclidean,
            10,
        ))
        .build()?;
    let (_, desc_tie_matches) =
        assert_ready_ok!(spawn(fn_desc_tie.once(&database)).poll_until_idle());
    assert_eq!(
        desc_tie_matches
            .iter()
            .map(|m| m.version.name.document_id.as_str())
            .collect_vec(),
        vec!["document-2", "document-1"]
    );

    let or_ineq_filter = Filter::Composite(CompositeFilter {
        op:      CompositeOperator::Or,
        filters: vec![
            Filter::Field(FieldFilter {
                field: "a".parse()?,
                op:    FieldOperator::GreaterThan,
                value: Value::integer(0),
            }),
            Filter::Field(FieldFilter {
                field: "b".parse()?,
                op:    FieldOperator::Equal,
                value: Value::integer(1),
            }),
        ],
    });
    let mut fn_or_std = QueryBuilder::from_collection(fn_col.clone())
        .filter(or_ineq_filter.clone())
        .find_nearest(FindNearest::new(
            "emb".parse()?,
            vec![0.0, 0.0],
            DistanceMeasure::Euclidean,
            10,
        ))
        .build()?;
    let (_, std_or_matches) = assert_ready_ok!(spawn(fn_or_std.once(&database)).poll_until_idle());
    assert_eq!(
        std_or_matches
            .iter()
            .map(|m| m.version.name.document_id.as_str())
            .collect_vec(),
        vec!["document-1", "document-2"]
    );
    let mut fn_or_ent = QueryBuilder::from_collection(fn_col)
        .enterprise_edition(true)
        .filter(or_ineq_filter)
        .find_nearest(FindNearest::new(
            "emb".parse()?,
            vec![0.0, 0.0],
            DistanceMeasure::Euclidean,
            10,
        ))
        .build()?;
    let (_, ent_or_matches) = assert_ready_ok!(spawn(fn_or_ent.once(&database)).poll_until_idle());
    assert_eq!(
        ent_or_matches
            .iter()
            .map(|m| m.version.name.document_id.as_str())
            .collect_vec(),
        vec!["document-3", "document-1", "document-2"]
    );

    Ok(())
}

#[gtest]
fn collection_cache_guards_against_foreign_database_roots() -> Result<()> {
    use crate::document::StoredDocumentVersion;

    let root_a: RootRef = "projects/p/databases/db-a".parse()?;
    let root_b: RootRef = "projects/p/databases/db-b".parse()?;
    let col_a = CollectionRef::new(root_a, "items");
    let col_b = CollectionRef::new(root_b, "items");

    let make_doc = |col: CollectionRef| StoredDocumentVersion {
        name: crate::reference::DocumentRef::new(col, "doc-1"),
        create_time: Default::default(),
        update_time: Default::default(),
        fields: Default::default(),
    };
    let doc_a = make_doc(col_a.clone());
    let doc_b = make_doc(col_b);

    // Caching `true` for `db-a/items` must not bleed into `db-b/items`.
    let mut query_own_first = QueryBuilder::from_collection(col_a.clone()).build()?;
    assert!(query_own_first.includes_document(&doc_a)?);
    assert!(!query_own_first.includes_document(&doc_b)?);

    // Checking foreign `db-b/items` first must not poison `collection_cache` with `false` for
    // `db-a/items`.
    let mut query_foreign_first = QueryBuilder::from_collection(col_a).build()?;
    assert!(!query_foreign_first.includes_document(&doc_b)?);
    assert!(query_foreign_first.includes_document(&doc_a)?);

    Ok(())
}

#[tokio::test]
async fn enterprise_all_descendants_empty_order_by_is_deterministic_in_firestore_order()
-> Result<()> {
    use std::collections::HashMap;

    use googleapis::google::firestore::v1::{
        Document, TransactionOptions, Write, transaction_options, write,
    };

    use crate::{database::DocumentRef, query::QueryMatch};

    let database = fresh_database()?;
    database.set_config(true, true);

    // Seed multiple docs per parent and `-` vs `/` parent names (`doc/sub/...` < `doc-1/sub/...`).
    for (col_path, doc_id) in [
        ("parents/doc-1/sub", "d2"),
        ("parents/doc-1/sub", "d1"),
        ("parents/doc/sub", "d3"),
        ("parents/doc/sub", "d1"),
        ("parents/doc/sub", "d2"),
    ] {
        let doc_ref: DocumentRef = format!("{}/{col_path}/{doc_id}", database.name).parse()?;
        database
            .update_doc(
                &doc_ref,
                HashMap::from([("tag".to_string(), Value::string("keep"))]),
                None,
                None,
                None,
            )
            .await?;
    }

    let root_prefix = format!("{}/", database.name);
    let make_query = |consistency: ReadConsistency| -> Result<Query> {
        Ok(QueryBuilder::from(
            database.name.clone().into(),
            vec![CollectionSelector {
                collection_id:   "sub".to_string(),
                all_descendants: true,
            }],
        )
        .enterprise_edition(true)
        .filter(Filter::Field(FieldFilter {
            field: "tag".parse()?,
            op:    FieldOperator::Equal,
            value: Value::string("keep"),
        }))
        .consistency(consistency)
        .build()?)
    };

    // 1. Validate stays unsynthesized (`order_by` empty) while execution orders deterministically
    //    via `DocumentRef::Ord` (`doc/sub/...` before `doc-1/sub/...`, and `d1 < d2 < d3`).
    let mut q = make_query(ReadConsistency::Default)?;
    assert!(q.order_by().is_empty());
    let (_, matches) = q.once(&database).await?;
    let rel_paths = |ms: &[QueryMatch]| {
        ms.iter()
            .map(|m| {
                m.version
                    .name
                    .to_string()
                    .strip_prefix(&root_prefix)
                    .unwrap()
                    .to_string()
            })
            .collect_vec()
    };
    let expected = vec![
        "parents/doc/sub/d1",
        "parents/doc/sub/d2",
        "parents/doc/sub/d3",
        "parents/doc-1/sub/d1",
        "parents/doc-1/sub/d2",
    ];
    assert_eq!(rel_paths(&matches), expected);

    // 2. Under an optimistic transaction, concurrent non-matching inserts that force HashMap
    //    rehashing of `Collection::documents` must NOT permute result order or trigger a false
    //    `OptimisticWriteSkew` abort.
    let txn_id = database
        .new_txn(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        })
        .await?;
    let mut txn_q = make_query(ReadConsistency::Transaction(txn_id))?;
    let (_, txn_matches) = txn_q.once(&database).await?;
    assert_eq!(rel_paths(&txn_matches), expected);

    for i in 0..64 {
        let noise_ref: DocumentRef =
            format!("{}/parents/doc/sub/noise-{i}", database.name).parse()?;
        database
            .update_doc(
                &noise_ref,
                HashMap::from([("tag".to_string(), Value::string("noise"))]),
                None,
                None,
                None,
            )
            .await?;
    }

    let other_ref: DocumentRef = format!("{}/other/out", database.name).parse()?;
    database
        .commit(
            vec![Write {
                operation: Some(write::Operation::Update(Document {
                    name: other_ref.to_string(),
                    fields: HashMap::from([("ok".to_string(), Value::boolean(true))]),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            Some(txn_id),
        )
        .await?;

    Ok(())
}
