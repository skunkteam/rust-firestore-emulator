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
    database::transaction::TransactionId,
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

fn is_locked(meta: &Arc<DocumentMeta>) -> bool {
    spawn(meta.write_owned()).poll_until_idle().is_pending()
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
