use std::{
    collections::{hash_map::Entry, BTreeSet, HashMap},
    sync::{Arc, Weak},
};

use googleapis::google::{
    firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
        structured_aggregation_query::{aggregation, Aggregation},
        *,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use string_cache::DefaultAtom;
use tokio::{
    join,
    sync::{
        broadcast::{self, Receiver},
        RwLock,
    },
};
use tracing::{debug, instrument, Level, Span};

use self::{
    collection::Collection,
    document::{
        DocumentContents, DocumentMeta, DocumentVersion, OwnedDocumentContentsWriteGuard,
        StoredDocumentVersion,
    },
    event::DatabaseEvent,
    field_path::FieldPath,
    query::Query,
    read_consistency::ReadConsistency,
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
    transaction::{RunningTransactions, Transaction, TransactionId},
};
use crate::{
    database::field_path::FieldReference, error::Result, unimplemented, unimplemented_collection,
    unimplemented_option, utils::RwLockHashMapExt, GenericDatabaseError,
};

mod collection;
pub(crate) mod document;
pub mod event;
pub mod field_path;
pub mod projection;
pub mod query;
pub mod read_consistency;
pub mod reference;
mod transaction;

const MAX_EVENT_BACKLOG: usize = 1024;

pub struct FirestoreDatabase {
    pub name: RootRef,
    collections: RwLock<HashMap<DefaultAtom, Arc<Collection>>>,
    transactions: RunningTransactions,
    events: broadcast::Sender<Arc<DatabaseEvent>>,
}

impl FirestoreDatabase {
    pub fn new(name: RootRef) -> Arc<Self> {
        Arc::new_cyclic(|database| FirestoreDatabase {
            name,
            collections: Default::default(),
            transactions: RunningTransactions::new(Weak::clone(database)),
            events: broadcast::channel(MAX_EVENT_BACKLOG).0,
        })
    }

    pub async fn clear(&self) {
        join!(
            async { self.collections.write().await.clear() },
            self.transactions.clear(),
        );
    }

    #[instrument(level = Level::TRACE, skip_all, err, fields(in_txn = consistency.is_transaction(), found))]
    pub async fn get_doc(
        &self,
        name: &DocumentRef,
        consistency: &ReadConsistency,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        debug!(%name);
        let version = if let Some(txn) = self.get_txn_for_consistency(consistency).await? {
            txn.read_doc(name)
                .await?
                .version_for_consistency(consistency)?
                .map(Arc::clone)
        } else {
            self.get_doc_meta(name)
                .await?
                .read()
                .await?
                .version_for_consistency(consistency)?
                .map(Arc::clone)
        };
        Span::current().record("found", version.is_some());
        Ok(version)
    }

    pub async fn get_collection(&self, collection_name: &CollectionRef) -> Arc<Collection> {
        debug_assert_eq!(self.name, collection_name.root_ref);
        Arc::clone(
            &*self
                .collections
                .get_or_insert(&collection_name.collection_id, || {
                    Arc::new(Collection::new(collection_name.clone()))
                })
                .await,
        )
    }

    pub(crate) async fn get_doc_meta(&self, name: &DocumentRef) -> Result<Arc<DocumentMeta>> {
        let meta = self
            .get_collection(&name.collection_ref)
            .await
            .get_doc(name)
            .await;
        Ok(meta)
    }

    pub async fn get_doc_meta_mut_no_txn(
        &self,
        name: &DocumentRef,
    ) -> Result<OwnedDocumentContentsWriteGuard> {
        self.get_doc_meta(name)
            .await?
            .read_owned()
            .await?
            .upgrade()
            .await
    }

    pub(crate) async fn get_txn_for_consistency(
        &self,
        consistency: &ReadConsistency,
    ) -> Result<Option<Arc<Transaction>>> {
        if let ReadConsistency::Transaction(id) = consistency {
            Ok(Some(self.get_txn(id).await?))
        } else {
            Ok(None)
        }
    }

    pub async fn get_txn(
        &self,
        id: &TransactionId,
    ) -> Result<Arc<Transaction>, GenericDatabaseError> {
        self.transactions.get(id).await
    }

    /// Get all the collections that reside directly under the given parent. This means that:
    /// - the IDs will not contain a `/`
    /// - the result will be empty if `parent` is a [`Ref::Collection`].
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn get_collection_ids(&self, parent: &Ref) -> Result<BTreeSet<String>> {
        // Cannot use `filter_map` because of the `await`.
        let mut result = BTreeSet::new();
        if matches!(parent, Ref::Collection(_)) {
            return Ok(result);
        }
        for col in self.get_all_collections().await {
            let Some(path) = col.name.strip_prefix(parent) else {
                continue;
            };
            let id = path.split_once('/').unwrap_or((path, "")).0;
            if !result.contains(id) && col.has_doc().await? {
                result.insert(id.to_string());
            }
        }
        debug!(result_count = result.len());
        Ok(result)
    }

    /// Get all the document IDs that reside directly under the given parent. This differs from
    /// [`Collection::docs`] in that this also includes empty documents with nested collections.
    /// This means that:
    /// - the IDs will not contain a `/`
    /// - IDs may point to documents that do not exist.
    #[instrument(level = Level::TRACE, skip_all)]
    pub async fn get_document_ids(&self, parent: &CollectionRef) -> Result<BTreeSet<String>> {
        // Cannot use `filter_map` because of the `await`.
        let mut result = BTreeSet::new();
        for doc in self.get_collection(parent).await.docs().await {
            result.insert(doc.name.document_id.to_string());
        }
        for col in self.get_all_collections().await {
            let Some(path) = col.name.strip_collection_prefix(parent) else {
                continue;
            };
            let id = path.split_once('/').unwrap_or((path, "")).0;
            if !result.contains(id) && col.has_doc().await? {
                result.insert(id.to_string());
            }
        }
        Ok(result)
    }

    /// Get all collections asap collected into a [`Vec`] in order to keep the read lock time
    /// minimal.
    pub async fn get_all_collections(&self) -> Vec<Arc<Collection>> {
        self.collections
            .read()
            .await
            .values()
            .cloned()
            .collect_vec()
    }

    #[instrument(level = Level::TRACE, skip_all, err)]
    pub async fn run_query(&self, query: &mut Query) -> Result<Vec<Document>> {
        debug!(?query);
        let result = query.once(self).await?;
        debug!(result_count = result.len());
        Ok(result
            .into_iter()
            .map(|version| query.project(&version))
            .collect())
    }

    #[instrument(level = Level::TRACE, skip_all, err)]
    pub async fn run_aggregation_query(
        &self,
        parent: Ref,
        query: StructuredQuery,
        aggregations: Vec<Aggregation>,
        consistency: ReadConsistency,
    ) -> Result<HashMap<String, Value>> {
        unimplemented_option!(query.select);
        let mut query = Query::from_structured(parent, query, consistency)?;
        debug!(?query);
        let docs = query.once(self).await?;
        let result = aggregations
            .into_iter()
            .map(|agg| {
                use aggregation::*;
                let operator = agg.operator.ok_or_else(|| {
                    GenericDatabaseError::invalid_argument("missing operator in aggregation query")
                })?;
                let result = match operator {
                    // Count of documents that match the query.
                    //
                    // The `COUNT(*)` aggregation function operates on the entire document
                    // so it does not require a field reference.
                    Operator::Count(Count { up_to }) => {
                        let max = up_to.map(|v| v.value).unwrap_or(i64::MAX);
                        Value::integer((docs.len() as i64).min(max))
                    }
                    // Sum of the values of the requested field.
                    //
                    // * Only numeric values will be aggregated. All non-numeric values including
                    //   `NULL` are skipped.
                    //
                    // * If the aggregated values contain `NaN`, returns `NaN`. Infinity math
                    //   follows IEEE-754 standards.
                    //
                    // * If the aggregated value set is empty, returns 0.
                    //
                    // * Returns a 64-bit integer if all aggregated numbers are integers and the sum
                    //   result does not overflow. Otherwise, the result is returned as a double.
                    //   Note that even if all the aggregated values are integers, the result is
                    //   returned as a double if it cannot fit within a 64-bit signed integer. When
                    //   this occurs, the returned value will lose precision.
                    //
                    // * When underflow occurs, floating-point aggregation is non-deterministic.
                    //   This means that running the same query repeatedly without any changes to
                    //   the underlying values could produce slightly different results each time.
                    //   In those cases, values should be stored as integers over floating-point
                    //   numbers.
                    Operator::Sum(Sum { field }) => {
                        let field_ref: FieldReference = field
                            .as_ref()
                            .ok_or_else(|| {
                                GenericDatabaseError::invalid_argument(
                                    "missing field reference in SUM operator",
                                )
                            })?
                            .try_into()?;
                        docs.iter()
                            .filter_map(|doc| Some(field_ref.get_value(doc)?.into_owned()))
                            .sum()
                    }
                    // Average of the values of the requested field.
                    //
                    // * Only numeric values will be aggregated. All non-numeric values including
                    //   `NULL` are skipped.
                    //
                    // * If the aggregated values contain `NaN`, returns `NaN`. Infinity math
                    //   follows IEEE-754 standards.
                    //
                    // * If the aggregated value set is empty, returns `NULL`.
                    //
                    // * Always returns the result as a double.
                    Operator::Avg(Avg { field }) => {
                        let field_ref: FieldReference = field
                            .as_ref()
                            .ok_or_else(|| {
                                GenericDatabaseError::invalid_argument(
                                    "missing field reference in AVG operator",
                                )
                            })?
                            .try_into()?;
                        let values = docs
                            .iter()
                            .filter_map(|doc| Some(field_ref.get_value(doc)?.into_owned()))
                            .filter(Value::is_number)
                            .collect_vec();
                        if values.is_empty() {
                            Value::null()
                        } else {
                            let len = values.len();
                            let sum: Value = values.into_iter().sum();
                            Value::double(sum.as_double().unwrap() / len as f64)
                        }
                    }
                };
                Ok((agg.alias, result))
            })
            .try_collect()?;
        Ok(result)
    }

    #[instrument(level = Level::TRACE, skip_all, err)]
    pub async fn commit(
        self: &Arc<Self>,
        writes: Vec<Write>,
        transaction: &TransactionId,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let txn = &self.transactions.get(transaction).await?;
        let time = Timestamp::now();

        let mut write_results = vec![];
        let mut updates = HashMap::new();
        let mut write_guard_cache = HashMap::<DocumentRef, OwnedDocumentContentsWriteGuard>::new();
        // This must be done in two phases. First acquire the lock on all docs, only then start to
        // update them.
        for write in &writes {
            let name = get_doc_name_from_write(write)?;
            if let Entry::Vacant(entry) = write_guard_cache.entry(name.clone()) {
                entry.insert(txn.take_write_guard(&name).await?);
            }
        }

        txn.drop_remaining_guards().await;

        for write in writes {
            let name = get_doc_name_from_write(&write)?;
            let guard = write_guard_cache.get_mut(&name).unwrap();
            let (write_result, version) = self.perform_write(write, guard, time).await?;
            write_results.push(write_result);
            if let Some(version) = version {
                updates.insert(name.clone(), version);
            }
        }

        self.transactions.stop(transaction).await?;

        self.send_event(DatabaseEvent {
            database: Arc::downgrade(self),
            update_time: time,
            updates,
        });

        Ok((time, write_results))
    }

    #[instrument(level = Level::TRACE, skip_all, err)]
    pub async fn perform_write(
        &self,
        write: Write,
        contents: &mut DocumentContents,
        commit_time: Timestamp,
    ) -> Result<(WriteResult, Option<DocumentVersion>)> {
        let Write {
            update_mask,
            update_transforms,
            current_document,
            operation,
        } = write;

        let operation = operation.ok_or(GenericDatabaseError::invalid_argument(
            "missing operation in write",
        ))?;
        let condition = current_document
            .and_then(|cd| cd.condition_type)
            .map(Into::into);
        if let Some(condition) = condition {
            contents.check_precondition(condition)?;
        }
        let mut transform_results = vec![];

        use write::Operation::*;
        let document_version = match operation {
            Update(doc) => {
                debug!(name = %contents.name, "Update");

                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(contents, mask, &doc.fields)?
                } else {
                    doc.fields
                };
                for tf in update_transforms {
                    transform_results.push(apply_transform(
                        &mut fields,
                        tf.field_path,
                        tf.transform_type.ok_or_else(|| {
                            GenericDatabaseError::invalid_argument(
                                "empty transform_type in field_transform",
                            )
                        })?,
                        commit_time,
                    )?);
                }
                contents.maybe_add_version(fields, commit_time).await
            }
            Delete(_) => {
                debug!(name = %contents.name, "Delete");

                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                Ok(contents.delete(commit_time).await)
            }
            Transform(_) => unimplemented!("transform"),
        };
        let (update_time, document_version) = match document_version {
            Ok(version) => (Some(commit_time), Some(version)),
            Err(time) => (Some(time), None),
        };
        Ok((
            WriteResult {
                update_time,
                transform_results,
            },
            document_version,
        ))
    }

    pub async fn new_txn(
        &self,
        transaction_options: Option<TransactionOptions>,
    ) -> Result<TransactionId> {
        use transaction_options::*;
        let retry_id = match transaction_options {
            Some(TransactionOptions {
                mode: None | Some(Mode::ReadOnly(_)),
            }) => {
                unimplemented!("read-only transactions")
            }
            Some(TransactionOptions {
                mode: Some(Mode::ReadWrite(ReadWrite { retry_transaction })),
            }) => retry_transaction,
            None => vec![],
        };

        if retry_id.is_empty() {
            Ok(self.transactions.start().await.id)
        } else {
            let id = retry_id.try_into()?;
            self.transactions.start_with_id(id).await?;
            Ok(id)
        }
    }

    pub async fn rollback(&self, transaction: &TransactionId) -> Result<()> {
        self.transactions.stop(transaction).await?;
        Ok(())
    }

    pub fn send_event(&self, event: DatabaseEvent) {
        // A send operation can only fail if there are no active receivers,
        // which is ok by me.
        let _ = self.events.send(event.into());
    }

    pub fn subscribe(&self) -> Receiver<Arc<DatabaseEvent>> {
        self.events.subscribe()
    }
}

fn apply_updates(
    contents: &mut DocumentContents,
    mask: DocumentMask,
    updated_values: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, GenericDatabaseError> {
    let mut fields = contents
        .current_version()
        .map(|v| v.fields.clone())
        .unwrap_or_default();
    for field_path in mask.field_paths {
        let field_path: FieldPath = field_path.parse()?;
        match field_path.get_value(updated_values) {
            Some(new_value) => field_path.set_value(&mut fields, new_value.clone()),
            None => {
                field_path.delete_value(&mut fields);
            }
        }
    }
    Ok(fields)
}

fn apply_transform(
    fields: &mut HashMap<String, Value>,
    path: String,
    transform: TransformType,
    commit_time: Timestamp,
) -> Result<Value> {
    let field_path: FieldPath = path.parse()?;
    let result = match transform {
        TransformType::SetToServerValue(code) => {
            match ServerValue::try_from(code).map_err(|_| {
                GenericDatabaseError::invalid_argument(format!("invalid server_value: {code}"))
            })? {
                ServerValue::RequestTime => {
                    let new_value = Value::timestamp(commit_time);
                    field_path.set_value(fields, new_value.clone());
                    new_value
                }
                other => unimplemented!(format!("{:?}", other)),
            }
        }
        TransformType::Increment(value) => field_path
            .transform_value(fields, |cur| cur.unwrap_or_else(Value::null) + value)
            .clone(),
        TransformType::Maximum(_) => unimplemented!("TransformType::Maximum"),
        TransformType::Minimum(_) => unimplemented!("TransformType::Minimum"),
        TransformType::AppendMissingElements(elements) => {
            field_path.transform_value(fields, |cur_value| {
                match cur_value.and_then(Value::into_array) {
                    Some(mut array) => {
                        for el in elements.values {
                            if !array.contains(&el) {
                                array.push(el)
                            }
                        }
                        Value::array(array)
                    }
                    None => Value::array(elements.values),
                }
            });
            Value::null()
        }
        TransformType::RemoveAllFromArray(elements) => {
            field_path.transform_value(fields, |cur_value| {
                match cur_value.and_then(Value::into_array) {
                    Some(mut array) => {
                        array.retain(|v| !elements.values.contains(v));
                        Value::array(array)
                    }
                    None => Value::array(vec![]),
                }
            });
            Value::null()
        }
    };
    Ok(result)
}

pub fn get_doc_name_from_write(write: &Write) -> Result<DocumentRef> {
    let operation = write
        .operation
        .as_ref()
        .ok_or(GenericDatabaseError::not_implemented(
            "missing operation in write",
        ))?;
    use write::Operation::*;
    let name: &str = match operation {
        Update(doc) => &doc.name,
        Delete(name) => name,
        Transform(trans) => &trans.document,
    };
    name.parse()
}
