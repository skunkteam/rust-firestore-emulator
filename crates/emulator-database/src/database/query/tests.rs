// Note: `tokio_test` is needed because the emulator spawns tasks (e.g. for timeouts), but the tests
// should still be synchronous.

use std::{fmt::Display, sync::Arc};

use googleapis::google::firestore::v1::{
    Cursor, Document, Precondition, TransactionOptions, Value, Write, precondition,
    structured_query::CollectionSelector, transaction_options, write,
};
use googletest::{Result, assert_that, gtest, prelude::*};
use itertools::Itertools;
use tokio_test::{assert_ready, assert_ready_ok, task::spawn};

use crate::{
    FirestoreConfig, FirestoreDatabase, FirestoreProject,
    database::transaction::TransactionId,
    document::DocumentMeta,
    query::{
        Direction, Query, QueryBuilder,
        filter::{FieldFilter, FieldOperator, Filter},
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
