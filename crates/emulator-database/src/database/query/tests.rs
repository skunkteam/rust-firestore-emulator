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

#[gtest]
#[tokio::test]
async fn missing_docs_and_list_docs_default_do_not_block_on_held_write_locks() -> Result<()> {
    use googleapis::google::protobuf::Timestamp;

    use crate::{database::DocumentRef, reference::Ref};

    let database = fresh_database()?;
    let t_before = Timestamp::now();

    let existing_ref: DocumentRef = format!("{}/users/existing", database.name).parse()?;
    let phantom_a_ref: DocumentRef = format!("{}/users/phantom-a", database.name).parse()?;
    let child_a_ref: DocumentRef =
        format!("{}/users/phantom-a/sub/child", database.name).parse()?;
    let phantom_b_ref: DocumentRef = format!("{}/users/phantom-b", database.name).parse()?;
    let child_b_ref: DocumentRef =
        format!("{}/users/phantom-b/sub/child", database.name).parse()?;
    let phantom_c_ref: DocumentRef = format!("{}/users/phantom-c", database.name).parse()?;
    let child_c_ref: DocumentRef =
        format!("{}/users/phantom-c/sub/child", database.name).parse()?;

    let write_doc = |doc_ref: &DocumentRef, val: i64| {
        let name = doc_ref.to_string();
        assert_ready_ok!(
            spawn(database.write(Write {
                operation: Some(write::Operation::Update(Document {
                    name,
                    fields: [("v".to_string(), Value::integer(val))].into(),
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .poll_until_idle()
        );
    };

    write_doc(&existing_ref, 1);
    write_doc(&child_a_ref, 2);
    let t_mid = Timestamp::now();

    write_doc(&child_b_ref, 3);
    write_doc(&child_c_ref, 4);
    let t_all = Timestamp::now();

    assert_ready_ok!(spawn(database.delete_doc(&child_c_ref, None)).poll_until_idle());

    // Directly acquire exclusive WriteLeases on the phantom ancestor, its child doc, and a
    // materialized sibling to verify MVCC paths never enter `LockTable` wait queues.
    let lock_session = database.locks.new_session(
        None,
        SessionKind::SingleWrite,
        LockPriority::NextSeq,
        false,
        false,
    );
    let lease_phantom =
        assert_ready_ok!(spawn(lock_session.acquire_write(&phantom_a_ref)).poll_until_idle());
    let lease_child =
        assert_ready_ok!(spawn(lock_session.acquire_write(&child_a_ref)).poll_until_idle());
    let lease_existing =
        assert_ready_ok!(spawn(lock_session.acquire_write(&existing_ref)).poll_until_idle());

    // Confirm a lock-acquiring `get_doc(Default)` blocks on the held write lock.
    {
        let mut blocked_read = spawn(database.get_doc(&phantom_a_ref, ReadConsistency::Default));
        assert!(blocked_read.poll_until_idle().is_pending());
    }

    // `Collection::has_doc_at` evaluates MVCC snapshots directly without touching `LockTable`.
    let sub_a_col = assert_ready!(
        spawn(database.get_collection(&child_a_ref.collection_ref)).poll_until_idle()
    );
    let sub_b_col = assert_ready!(
        spawn(database.get_collection(&child_b_ref.collection_ref)).poll_until_idle()
    );
    let sub_c_col = assert_ready!(
        spawn(database.get_collection(&child_c_ref.collection_ref)).poll_until_idle()
    );
    assert!(!assert_ready!(
        spawn(sub_a_col.has_doc_at(t_before)).poll_until_idle()
    ));
    assert!(assert_ready!(
        spawn(sub_a_col.has_doc_at(t_mid)).poll_until_idle()
    ));
    assert!(!assert_ready!(
        spawn(sub_b_col.has_doc_at(t_mid)).poll_until_idle()
    ));
    assert!(assert_ready!(
        spawn(sub_c_col.has_doc_at(t_all)).poll_until_idle()
    ));
    assert!(!assert_ready!(
        spawn(sub_c_col.has_doc_at(Timestamp::now())).poll_until_idle()
    ));

    // `Query::missing_docs` and `missing_docs_at` under `ReadConsistency::Default` complete
    // immediately without blocking on the held write locks.
    let users_col = CollectionRef::new(database.name.clone(), "users");
    let default_query = QueryBuilder::from_collection(users_col.clone()).build()?;
    let missing_default =
        assert_ready_ok!(spawn(default_query.missing_docs(&database)).poll_until_idle());
    assert_eq!(
        missing_default,
        vec![phantom_a_ref.to_string(), phantom_b_ref.to_string()]
    );

    let missing_after_a = assert_ready_ok!(
        spawn(default_query.missing_docs_at(&database, None, Some(&phantom_a_ref.to_string())))
            .poll_until_idle()
    );
    assert_eq!(missing_after_a, vec![phantom_b_ref.to_string()]);

    // `FirestoreDatabase::list_docs(..., show_missing: true)` under `ReadConsistency::Default`
    // completes immediately without blocking.
    let (listed_default, next_tok) = assert_ready_ok!(
        spawn(database.list_docs(
            Ref::Root(database.name.clone()),
            "users".to_string(),
            0,
            String::new(),
            String::new(),
            None,
            true,
            ReadConsistency::Default,
        ))
        .poll_until_idle()
    );
    assert_eq!(next_tok, "");
    assert_eq!(
        listed_default.iter().map(|d| d.name.as_str()).collect_vec(),
        vec![
            existing_ref.to_string(),
            phantom_a_ref.to_string(),
            phantom_b_ref.to_string(),
        ]
    );
    assert!(listed_default[0].create_time.is_some());
    assert!(listed_default[1].create_time.is_none());
    assert!(listed_default[2].create_time.is_none());

    // Historical `ReadTime` also completes lock-free and filters phantom ancestors by child MVCC
    // existence at the requested timestamp.
    let mid_query = QueryBuilder::from_collection(users_col)
        .consistency(ReadConsistency::ReadTime(t_mid))
        .build()?;
    assert_eq!(
        assert_ready_ok!(spawn(mid_query.missing_docs(&database)).poll_until_idle()),
        vec![phantom_a_ref.to_string()]
    );

    let (listed_all, _) = assert_ready_ok!(
        spawn(database.list_docs(
            Ref::Root(database.name.clone()),
            "users".to_string(),
            0,
            String::new(),
            String::new(),
            None,
            true,
            ReadConsistency::ReadTime(t_all),
        ))
        .poll_until_idle()
    );
    assert_eq!(
        listed_all.iter().map(|d| d.name.as_str()).collect_vec(),
        vec![
            existing_ref.to_string(),
            phantom_a_ref.to_string(),
            phantom_b_ref.to_string(),
            phantom_c_ref.to_string(),
        ]
    );

    drop((lease_phantom, lease_child, lease_existing));
    lock_session.close();

    let unblocked = assert_ready_ok!(
        spawn(database.get_doc(&phantom_a_ref, ReadConsistency::Default)).poll_until_idle()
    );
    assert!(unblocked.is_none());

    Ok(())
}

#[gtest]
#[tokio::test(start_paused = true)]
async fn missing_docs_at_respects_readonly_and_optimistic_mvcc_snapshots_and_rejects_inactive_txns()
-> Result<()> {
    use std::collections::HashMap;

    use googleapis::google::protobuf::Timestamp;

    use crate::database::DocumentRef;

    // 1. ReadOnly (implicit snapshot + explicit historical read_time) and multi-subcollection
    //    phantom ancestor deduplication.
    let db = fresh_database()?;
    let users_col = CollectionRef::new(db.name.clone(), "users");
    let alice_ref: DocumentRef = format!("{}/users/alice", db.name).parse()?;
    let p_multi_ref: DocumentRef = format!("{}/users/p_multi", db.name).parse()?;
    let p_multi_s1: DocumentRef = format!("{}/users/p_multi/sub1/c1", db.name).parse()?;
    let p_multi_s2: DocumentRef = format!("{}/users/p_multi/sub2/c2", db.name).parse()?;
    let p_del_ref: DocumentRef = format!("{}/users/p_del", db.name).parse()?;
    let p_del_sub: DocumentRef = format!("{}/users/p_del/sub/c1", db.name).parse()?;
    let p_after_ref: DocumentRef = format!("{}/users/p_after", db.name).parse()?;
    let p_after_sub: DocumentRef = format!("{}/users/p_after/sub/c1", db.name).parse()?;

    for doc_ref in [&alice_ref, &p_multi_s1, &p_multi_s2, &p_del_sub] {
        db.update_doc(
            doc_ref,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await?;
    }
    let t_past = Timestamp::now();

    let ro_implicit = db
        .new_txn(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(
                transaction_options::ReadOnly::default(),
            )),
        })
        .await?;
    let ro_historical = db
        .new_txn(TransactionOptions {
            mode: Some(transaction_options::Mode::ReadOnly(
                transaction_options::ReadOnly {
                    consistency_selector: Some(
                        transaction_options::read_only::ConsistencySelector::ReadTime(t_past),
                    ),
                },
            )),
        })
        .await?;

    // Mutations after `t_past` and `ro_implicit` snapshot:
    // - Create subchild under `p_after` (must NOT be phantom at `t_past` / `ro_implicit`)
    // - Delete sole subchild under `p_del` (MUST STILL be phantom at `t_past` / `ro_implicit`)
    // - Materialize `p_multi` (MUST STILL be phantom at `t_past` / `ro_implicit`)
    db.update_doc(
        &p_after_sub,
        HashMap::from([("v".to_string(), Value::integer(2))]),
        None,
        None,
        None,
    )
    .await?;
    db.delete_doc(&p_del_sub, None).await?;
    db.update_doc(
        &p_multi_ref,
        HashMap::from([("materialized".to_string(), Value::boolean(true))]),
        None,
        None,
        None,
    )
    .await?;

    let expected_past = vec![p_del_ref.to_string(), p_multi_ref.to_string()];
    for ro_txn in [ro_implicit, ro_historical] {
        let q = QueryBuilder::from_collection(users_col.clone())
            .consistency(ReadConsistency::Transaction(ro_txn))
            .build()?;
        assert_eq!(q.missing_docs(&db).await?, expected_past);
        assert_eq!(
            q.missing_docs_at(&db, None, Some(&p_del_ref.to_string()))
                .await?,
            vec![p_multi_ref.to_string()]
        );
        db.rollback(ro_txn).await?;
    }

    // Default listing sees latest state (`p_after` only).
    let q_default = QueryBuilder::from_collection(users_col).build()?;
    assert_eq!(
        q_default.missing_docs(&db).await?,
        vec![p_after_ref.to_string()]
    );

    // 2. Enterprise Optimistic transaction: MVCC snapshot isolation + write-skew preservation on
    //    observed phantom ancestor.
    let ent_db = fresh_database()?;
    ent_db.set_config(true, true);
    let ent_users = CollectionRef::new(ent_db.name.clone(), "users");
    let ent_seed: DocumentRef = format!("{}/users/seed", ent_db.name).parse()?;
    let ent_p_keep: DocumentRef = format!("{}/users/p_keep", ent_db.name).parse()?;
    let ent_p_keep_s1: DocumentRef = format!("{}/users/p_keep/s1/d1", ent_db.name).parse()?;
    let ent_p_keep_s2: DocumentRef = format!("{}/users/p_keep/s2/d2", ent_db.name).parse()?;
    let ent_p_del: DocumentRef = format!("{}/users/p_del", ent_db.name).parse()?;
    let ent_p_del_sub: DocumentRef = format!("{}/users/p_del/sub/d1", ent_db.name).parse()?;
    let ent_p_after_sub: DocumentRef = format!("{}/users/p_after/sub/d1", ent_db.name).parse()?;

    for doc_ref in [&ent_seed, &ent_p_keep_s1, &ent_p_keep_s2, &ent_p_del_sub] {
        ent_db
            .update_doc(
                doc_ref,
                HashMap::from([("v".to_string(), Value::integer(1))]),
                None,
                None,
                None,
            )
            .await?;
    }

    let opt_txn = start_rw_txn(&ent_db);
    // Pin optimistic `read_time` before concurrent writes.
    let _unused = ent_db
        .get_doc(&ent_seed, ReadConsistency::Transaction(opt_txn))
        .await?;

    // Concurrent writes after `opt_txn` pinned its snapshot:
    ent_db
        .update_doc(
            &ent_p_after_sub,
            HashMap::from([("v".to_string(), Value::integer(2))]),
            None,
            None,
            None,
        )
        .await?;
    ent_db.delete_doc(&ent_p_del_sub, None).await?;

    let opt_q = QueryBuilder::from_collection(ent_users.clone())
        .enterprise_edition(true)
        .consistency(ReadConsistency::Transaction(opt_txn))
        .build()?;
    let opt_missing = opt_q.missing_docs(&ent_db).await?;
    assert_eq!(
        opt_missing,
        vec![ent_p_del.to_string(), ent_p_keep.to_string()]
    );

    // Concurrent materialization of observed phantom ancestor `p_keep` triggers optimistic
    // write-skew abort on commit.
    ent_db
        .update_doc(
            &ent_p_keep,
            HashMap::from([("materialized".to_string(), Value::boolean(true))]),
            None,
            None,
            None,
        )
        .await?;
    let summary_ref: DocumentRef = format!("{}/other/summary", ent_db.name).parse()?;
    let commit_err = ent_db
        .commit(
            vec![Write {
                operation: Some(write::Operation::Update(Document {
                    name: summary_ref.to_string(),
                    fields: HashMap::from([("ok".to_string(), Value::boolean(true))]),
                    ..Default::default()
                })),
                ..Default::default()
            }],
            Some(opt_txn),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(commit_err, GenericDatabaseError::Aborted(_)),
        "expected OptimisticWriteSkew Aborted after phantom ancestor materialization, got \
         {commit_err:?}"
    );

    // 3. Zero-subcollection database: expired and rolled-back `Transaction(id)` are rejected
    //    upfront instead of returning `Ok(vec![])`.
    let empty_db = fresh_database()?;
    let empty_users = CollectionRef::new(empty_db.name.clone(), "users");

    let rb_txn = start_rw_txn(&empty_db);
    empty_db.rollback(rb_txn).await?;
    let q_rb = QueryBuilder::from_collection(empty_users.clone())
        .consistency(ReadConsistency::Transaction(rb_txn))
        .build()?;
    assert!(matches!(
        q_rb.missing_docs(&empty_db).await,
        Err(GenericDatabaseError::InvalidArgument(_))
    ));

    let exp_ro = empty_db.new_txn(TransactionOptions::default()).await?;
    let exp_rw = start_rw_txn(&empty_db);
    let exp_rw_rb = start_rw_txn(&empty_db);
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    empty_db.rollback(exp_rw_rb).await?;

    for expired_txn in [exp_ro, exp_rw, exp_rw_rb] {
        let q_exp = QueryBuilder::from_collection(empty_users.clone())
            .consistency(ReadConsistency::Transaction(expired_txn))
            .build()?;
        assert!(
            matches!(
                q_exp.missing_docs(&empty_db).await,
                Err(GenericDatabaseError::Aborted(_))
            ),
            "expected Aborted for expired transaction with zero subcollections"
        );
    }

    Ok(())
}

#[gtest]
fn require_existing_fields_filters_missing_fields_and_preserves_explicit_nulls_in_prefilters()
-> Result<()> {
    use std::collections::HashMap;

    use crate::document::StoredDocumentVersion;

    let root: RootRef = "projects/p/databases/(default)".parse()?;
    let col = CollectionRef::new(root, "items");
    let make_doc = |id: &str, fields: HashMap<String, Value>| StoredDocumentVersion {
        name: crate::reference::DocumentRef::new(col.clone(), id),
        create_time: Default::default(),
        update_time: Default::default(),
        fields,
    };

    let mut q = QueryBuilder::from_collection(col.clone()).build()?;
    q.require_existing_fields(vec![
        "b".parse()?,
        "a".parse()?,
        "a".parse()?,
        "nested.x".parse()?,
    ]);

    let has_both_null = make_doc(
        "d1",
        HashMap::from([
            ("a".to_string(), Value::null()),
            ("b".to_string(), Value::integer(1)),
            (
                "nested".to_string(),
                Value::map(HashMap::from([("x".to_string(), Value::null())])),
            ),
        ]),
    );
    let missing_b = make_doc(
        "d2",
        HashMap::from([
            ("a".to_string(), Value::integer(1)),
            (
                "nested".to_string(),
                Value::map(HashMap::from([("x".to_string(), Value::integer(2))])),
            ),
        ]),
    );
    let non_map_nested = make_doc(
        "d3",
        HashMap::from([
            ("a".to_string(), Value::integer(1)),
            ("b".to_string(), Value::integer(2)),
            ("nested".to_string(), Value::integer(42)),
        ]),
    );

    assert!(q.matches_prefilters(&has_both_null));
    assert!(!q.matches_prefilters(&missing_b));
    assert!(!q.matches_prefilters(&non_map_nested));
    Ok(())
}
