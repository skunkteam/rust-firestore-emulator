use std::{
    collections::{BTreeSet, HashMap, hash_map::Entry},
    sync::{
        Arc, Weak,
        atomic::{self, AtomicBool},
    },
};

use googleapis::google::{
    firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
        structured_aggregation_query::{Aggregation, aggregation},
        structured_query::CollectionSelector,
        *,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use string_cache::DefaultAtom;
use tokio::sync::{
    RwLock,
    broadcast::{self, Receiver},
};
use tracing::{Level, Span, debug, field::display, instrument};

use self::{
    collection::Collection,
    document::{
        DocumentMeta, DocumentVersion, OwnedDocumentContentsWriteGuard, StoredDocumentVersion,
    },
    event::{DatabaseEvent, ProjectEvent},
    field_path::FieldPath,
    projection::{Project, Projection},
    query::{Direction, Query, QueryBuilder},
    read_consistency::ReadConsistency,
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
    transaction::{RunningTransactions, TransactionId},
};
use crate::{
    FirestoreProject, GenericDatabaseError, database::field_path::FieldReference, error::Result,
    unimplemented, unimplemented_bool, unimplemented_collection, unimplemented_option,
    utils::RwLockHashMapExt,
};

mod collection;
pub(crate) mod document;
pub mod event;
pub mod field_path;
pub mod pipeline;
pub mod projection;
pub mod query;
pub mod read_consistency;
pub mod reference;
pub(crate) mod timeouts;
mod transaction;

const MAX_EVENT_BACKLOG: usize = 1024;

#[derive(Debug)]
pub struct FirestoreDatabase {
    project: &'static FirestoreProject,
    pub name: RootRef,
    enterprise_edition: AtomicBool,
    optimistic_concurrency: AtomicBool,
    collections: RwLock<HashMap<DefaultAtom, Arc<Collection>>>,
    transactions: RunningTransactions,
    events: broadcast::Sender<Arc<DatabaseEvent>>,
}

impl FirestoreDatabase {
    pub fn new(
        project: &'static FirestoreProject,
        name: RootRef,
        enterprise_edition: bool,
    ) -> Arc<Self> {
        Arc::new_cyclic(|database| FirestoreDatabase {
            project,
            name,
            enterprise_edition: AtomicBool::new(enterprise_edition),
            optimistic_concurrency: AtomicBool::new(enterprise_edition),
            collections: Default::default(),
            transactions: RunningTransactions::new(Weak::clone(database)),
            events: broadcast::channel(MAX_EVENT_BACKLOG).0,
        })
    }

    #[instrument(level = Level::DEBUG, skip_all, err, fields(txn_id, found))]
    pub async fn get_doc(
        &self,
        name: &DocumentRef,
        consistency: ReadConsistency,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        debug!(%name);
        let version = match consistency {
            ReadConsistency::Default => self
                .get_doc_meta(name)
                .await?
                .read()
                .await?
                .current_version()
                .cloned(),
            ReadConsistency::ReadTime(time) => self
                .get_doc_meta(name)
                .await?
                .read()
                .await?
                .version_at_time(time)
                .cloned(),
            ReadConsistency::Transaction(id) => {
                Span::current().record("txn_id", display(id));
                self.transactions.get(id).await?.read_doc(name).await?
            }
        };
        Span::current().record("found", version.is_some());
        Ok(version)
    }

    async fn get_collection(&self, collection_name: &CollectionRef) -> Arc<Collection> {
        debug_assert_eq!(self.name, collection_name.root_ref);
        Arc::clone(
            &*self
                .collections
                .get_or_insert(&collection_name.collection_id, || {
                    Arc::new(Collection::new(self.project, collection_name.clone()))
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

    /// Get all the collections that reside directly under the given parent. This means that:
    /// - the IDs will not contain a `/`
    /// - the result will be empty if `parent` is a [`Ref::Collection`].
    #[instrument(level = Level::DEBUG, skip_all)]
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
            let id = path.split('/').next().unwrap();
            if !result.contains(id) && col.has_doc().await? {
                result.insert(id.to_string());
            }
        }
        debug!(result_count = result.len());
        Ok(result)
    }

    /// Get all the document IDs that reside directly under the given parent.
    /// This also includes empty documents with nested collections. This means
    /// that:
    /// - the IDs will not contain a `/`
    /// - IDs may point to documents that do not exist.
    #[instrument(level = Level::DEBUG, skip_all)]
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
            let id = path.split('/').next().unwrap();
            if !result.contains(id) && col.has_doc().await? {
                result.insert(id.to_string());
            }
        }
        Ok(result)
    }

    /// Get all collections asap collected into a [`Vec`] in order to keep the read lock time
    /// minimal.
    async fn get_all_collections(&self) -> Vec<Arc<Collection>> {
        self.collections
            .read()
            .await
            .values()
            .cloned()
            .collect_vec()
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn run_query(&self, query: &mut Query) -> Result<(Timestamp, Vec<Document>)> {
        debug!(?query);
        let (read_time, result) = query.once(self).await?;
        debug!(result_count = result.len());
        Ok((
            read_time,
            result
                .into_iter()
                .map(|version| query.project(&version))
                .collect(),
        ))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn run_aggregation_query(
        &self,
        parent: Ref,
        query: StructuredQuery,
        aggregations: Vec<Aggregation>,
        consistency: ReadConsistency,
    ) -> Result<(Timestamp, HashMap<String, Value>)> {
        unimplemented_option!(query.select);
        let mut query =
            Query::from_structured(parent, query, consistency, self.enterprise_edition())?;
        debug!(?query);
        let (read_time, docs) = query.once(self).await?;
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
        Ok((read_time, result))
    }

    pub async fn write(self: &Arc<Self>, write: Write) -> Result<WriteResult> {
        let (result, _) = self.write_with_version(write).await?;
        Ok(result)
    }

    async fn write_with_version(
        self: &Arc<Self>,
        write: Write,
    ) -> Result<(WriteResult, Option<Arc<StoredDocumentVersion>>)> {
        let name = get_doc_name_from_write(&write)?;
        let mut guard = self.get_doc_meta(&name).await?.write_owned().await?;
        let update_time = guard.acquire_time;
        let (result, new_version) = self.perform_write(write, &mut guard, update_time).await?;
        let current_stored = guard.current_version().cloned();
        drop(guard);
        if let Some(new_version) = new_version {
            self.send_event(DatabaseEvent {
                database: Arc::downgrade(self),
                update_time,
                updates: [(name.clone(), new_version)].into(),
            });
        }
        Ok((result, current_stored))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn create_doc(
        self: &Arc<Self>,
        parent: &CollectionRef,
        document_id: &str,
        fields: HashMap<String, Value>,
        mask: Option<DocumentMask>,
    ) -> Result<Document> {
        if document_id.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "document_id is required",
            ));
        }
        let doc_ref: DocumentRef = format!("{parent}/{document_id}").parse()?;
        let projection = mask.map(Projection::try_from).transpose()?;
        let (_, stored) = self
            .write_with_version(Write {
                update_mask: None,
                update_transforms: vec![],
                current_document: Some(Precondition {
                    condition_type: Some(precondition::ConditionType::Exists(false)),
                }),
                operation: Some(write::Operation::Update(Document {
                    name: doc_ref.to_string(),
                    fields,
                    create_time: None,
                    update_time: None,
                })),
            })
            .await?;
        let stored = stored.ok_or_else(|| {
            GenericDatabaseError::internal(format!(
                "created document missing immediately after write: {doc_ref}"
            ))
        })?;
        Ok(projection.project(&stored))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn update_doc(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
        fields: HashMap<String, Value>,
        update_mask: Option<DocumentMask>,
        current_document: Option<Precondition>,
        mask: Option<DocumentMask>,
    ) -> Result<Document> {
        let projection = mask.map(Projection::try_from).transpose()?;
        let (_, stored) = self
            .write_with_version(Write {
                update_mask,
                update_transforms: vec![],
                current_document,
                operation: Some(write::Operation::Update(Document {
                    name: doc_ref.to_string(),
                    fields,
                    create_time: None,
                    update_time: None,
                })),
            })
            .await?;
        Ok(match stored {
            Some(stored) => projection.project(&stored),
            None => Document {
                name: doc_ref.to_string(),
                ..Default::default()
            },
        })
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn delete_doc(
        self: &Arc<Self>,
        doc_ref: &DocumentRef,
        current_document: Option<Precondition>,
    ) -> Result<()> {
        self.write(Write {
            update_mask: None,
            update_transforms: vec![],
            current_document,
            operation: Some(write::Operation::Delete(doc_ref.to_string())),
        })
        .await?;
        Ok(())
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn delete_prefix(self: &Arc<Self>, prefix: &Ref) -> Result<usize> {
        let mut to_delete = BTreeSet::new();
        if let Ref::Document(doc_ref) = prefix {
            let meta = self.get_doc_meta(doc_ref).await?;
            if meta.read().await?.exists() {
                to_delete.insert(doc_ref.clone());
            }
        }
        for col in self.get_all_collections().await {
            let col_as_ref = Ref::Collection(col.name.clone());
            if &col_as_ref == prefix || prefix.is_parent_of(&col_as_ref) {
                for meta in col.docs().await {
                    if meta.read().await?.exists() {
                        to_delete.insert(meta.name.clone());
                    }
                }
            }
        }
        let count = to_delete.len();
        for doc_ref in to_delete {
            self.delete_doc(&doc_ref, None).await?;
        }
        Ok(count)
    }

    #[allow(clippy::too_many_arguments)]
    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn list_docs(
        &self,
        parent: Ref,
        collection_id: String,
        page_size: i32,
        page_token: String,
        order_by: String,
        mask: Option<DocumentMask>,
        show_missing: bool,
        consistency: ReadConsistency,
    ) -> Result<(Vec<Document>, String)> {
        if page_size < 0 {
            return Err(GenericDatabaseError::invalid_argument(
                "page_size must be non-negative",
            ));
        }
        if show_missing && !order_by.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "show_missing cannot be combined with order_by",
            ));
        }
        unimplemented_bool!(show_missing && collection_id.is_empty());

        let parsed_orders = parse_list_order_by(&order_by)?;
        let mut builder = QueryBuilder::from(
            parent,
            vec![CollectionSelector {
                all_descendants: collection_id.is_empty(),
                collection_id:   collection_id.clone(),
            }],
        )
        .enterprise_edition(self.enterprise_edition())
        .select(mask.map(Projection::try_from).transpose()?)
        .consistency(consistency);

        if parsed_orders.is_empty() {
            builder = builder.order_by(FieldReference::DocumentName, Direction::Ascending);
        } else {
            let last_dir = parsed_orders
                .last()
                .map(|(_, d)| *d)
                .unwrap_or(Direction::Ascending);
            let has_doc_name = parsed_orders.iter().any(|(f, _)| f.is_document_name());
            for (field, dir) in parsed_orders {
                builder = builder.order_by(field, dir);
            }
            if !has_doc_name {
                builder = builder.order_by(FieldReference::DocumentName, last_dir);
            }
        }

        let mut query = builder.build()?;
        let (_, found_docs) = query.once(self).await?;
        let mut documents: Vec<Document> = found_docs.iter().map(|v| query.project(v)).collect();

        if show_missing && !collection_id.is_empty() {
            let existing_names: std::collections::HashSet<String> =
                documents.iter().map(|d| d.name.clone()).collect();
            for missing_name in query.missing_docs(self).await? {
                if !existing_names.contains(&missing_name) {
                    documents.push(Document {
                        name: missing_name,
                        ..Default::default()
                    });
                }
            }
            documents.sort_by(|a, b| a.name.split('/').cmp(b.name.split('/')));
        }

        if !page_token.is_empty() {
            if let Some(pos) = documents.iter().position(|d| d.name == page_token) {
                documents.drain(..=pos);
            } else if order_by.is_empty() {
                documents.retain(|d| d.name.split('/').gt(page_token.split('/')));
            }
        }

        let next_page_token = if page_size > 0 && documents.len() > page_size as usize {
            documents.truncate(page_size as usize);
            documents.last().map(|d| d.name.clone()).unwrap_or_default()
        } else {
            String::new()
        };

        Ok((documents, next_page_token))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn commit(
        self: &Arc<Self>,
        writes: Vec<Write>,
        transaction: TransactionId,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let txn = self.transactions.get(transaction).await?;
        let rw_txn = txn.as_read_write();

        // No write-skew possible when no writes, do not abort even if things changed.
        if let Some(rw_txn) = rw_txn
            && !writes.is_empty()
        {
            rw_txn.check_concurrency_violations().await?;
        }

        let mut write_results = vec![];
        let mut updates = HashMap::new();
        let mut write_guard_cache = HashMap::<DocumentRef, OwnedDocumentContentsWriteGuard>::new();
        // This must be done in two phases. First acquire the lock on all docs, only then start to
        // update them.
        for write in &writes {
            let name = get_doc_name_from_write(write)?;
            if let Entry::Vacant(entry) = write_guard_cache.entry(name.clone()) {
                let txn = rw_txn.as_ref().ok_or_else(|| {
                    GenericDatabaseError::invalid_argument(
                        "Cannot modify entities in a read-only transaction",
                    )
                })?;
                entry.insert(txn.get_write_guard(&name).await?);
            }
        }

        if let Some(txn) = rw_txn {
            txn.prepare_commit().await;
        }

        let time = Timestamp::now();

        for write in writes {
            let name = get_doc_name_from_write(&write)?;
            let guard = write_guard_cache.get_mut(&name).unwrap();
            let (write_result, version) = self.perform_write(write, guard, time).await?;
            write_results.push(write_result);
            if let Some(version) = version {
                updates.insert(name.clone(), version);
            }
        }

        self.transactions.remove(transaction).await?;

        self.send_event(DatabaseEvent {
            database: Arc::downgrade(self),
            update_time: time,
            updates,
        });

        Ok((time, write_results))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    async fn perform_write(
        &self,
        write: Write,
        document: &mut OwnedDocumentContentsWriteGuard,
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
            document.check_precondition(condition)?;
        }
        let mut transform_results = vec![];

        use write::Operation::*;
        let (update_time, document_version) = match operation {
            Update(doc) => {
                debug!(name = %document.name, "Update");
                validate_writable_fields(&doc.fields)?;

                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(document, mask, &doc.fields)?
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
                validate_writable_fields(&fields)?;
                document.maybe_update(fields, commit_time).await?
            }
            Delete(_) => {
                debug!(name = %document.name, "Delete");

                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                (commit_time, Some(document.delete(commit_time).await?))
            }
            Transform(_) => unimplemented!("transform"),
        };
        Ok((
            WriteResult {
                update_time: Some(update_time),
                transform_results,
            },
            document_version,
        ))
    }

    pub async fn get_read_time(&self, consistency: ReadConsistency) -> Result<Timestamp> {
        match consistency {
            ReadConsistency::Default => Ok(Timestamp::now()),
            ReadConsistency::Transaction(id) => {
                let txn = self.transactions.get(id).await?;
                Ok(txn.read_time().unwrap_or_else(Timestamp::now))
            }
            ReadConsistency::ReadTime(time) => Ok(time),
        }
    }

    pub async fn new_txn(&self, transaction_options: TransactionOptions) -> Result<TransactionId> {
        use transaction_options::*;
        let id = match transaction_options.mode {
            None => self.transactions.start_read_only(None).await,
            Some(Mode::ReadOnly(ro)) => {
                self.transactions
                    .start_read_only(
                        ro.consistency_selector
                            .map(|read_only::ConsistencySelector::ReadTime(read_time)| read_time),
                    )
                    .await
            }
            Some(Mode::ReadWrite(rw)) if rw.retry_transaction.is_empty() => {
                self.transactions
                    .start_read_write(self.optimistic_concurrency())
                    .await
            }
            Some(Mode::ReadWrite(rw)) => {
                let id = rw.retry_transaction.try_into()?;
                self.transactions
                    .start_read_write_with_id(id, self.optimistic_concurrency())
                    .await?;
                id
            }
        };
        Ok(id)
    }

    pub async fn rollback(&self, transaction: TransactionId) -> Result<()> {
        self.transactions.remove(transaction).await?;
        Ok(())
    }

    pub fn send_event(&self, event: DatabaseEvent) {
        if !event.updates.is_empty() {
            let mut paths: Vec<String> = event.updates.keys().map(ToString::to_string).collect();
            paths.sort();
            self.project
                .send_project_event(ProjectEvent::DocumentsChanged {
                    database: self.name.clone(),
                    paths,
                    update_time: event.update_time,
                });
        }
        // A send operation can only fail if there are no active receivers,
        // which is ok by me.
        let _unused = self.events.send(event.into());
    }

    pub fn subscribe(&self) -> Receiver<Arc<DatabaseEvent>> {
        self.events.subscribe()
    }

    pub fn enterprise_edition(&self) -> bool {
        self.enterprise_edition.load(atomic::Ordering::Relaxed)
    }

    pub fn set_enterprise_edition(&self, enterprise_edition: bool) {
        if self
            .enterprise_edition
            .swap(enterprise_edition, atomic::Ordering::Relaxed)
            != enterprise_edition
        {
            self.project
                .send_project_event(ProjectEvent::DatabaseConfigChanged {
                    database: self.name.clone(),
                });
        }
    }

    pub fn optimistic_concurrency(&self) -> bool {
        self.optimistic_concurrency.load(atomic::Ordering::Relaxed)
    }

    pub fn set_optimistic_concurrency(&self, optimistic_concurrency: bool) {
        if self
            .optimistic_concurrency
            .swap(optimistic_concurrency, atomic::Ordering::Relaxed)
            != optimistic_concurrency
        {
            self.project
                .send_project_event(ProjectEvent::DatabaseConfigChanged {
                    database: self.name.clone(),
                });
        }
    }
}

fn validate_writable_fields(fields: &HashMap<String, Value>) -> Result<()> {
    for value in fields.values() {
        validate_writable_value(value, false)?;
    }
    Ok(())
}

fn validate_writable_value(value: &Value, in_array: bool) -> Result<()> {
    use value::ValueType;
    match &value.value_type {
        None => Err(GenericDatabaseError::invalid_argument(
            "document field value is missing a value_type",
        )),
        Some(ValueType::FieldReferenceValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "fieldReferenceValue is not valid in stored documents",
        )),
        Some(ValueType::FunctionValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "functionValue is not valid in stored documents",
        )),
        Some(ValueType::PipelineValue(_)) => Err(GenericDatabaseError::invalid_argument(
            "pipelineValue is not valid in stored documents",
        )),
        Some(ValueType::ArrayValue(arr)) => {
            if in_array {
                return Err(GenericDatabaseError::invalid_argument(
                    "nested arrays are not supported",
                ));
            }
            for item in &arr.values {
                validate_writable_value(item, true)?;
            }
            Ok(())
        }
        Some(ValueType::MapValue(map)) => validate_writable_fields(&map.fields),
        Some(
            ValueType::NullValue(_)
            | ValueType::BooleanValue(_)
            | ValueType::IntegerValue(_)
            | ValueType::DoubleValue(_)
            | ValueType::TimestampValue(_)
            | ValueType::StringValue(_)
            | ValueType::BytesValue(_)
            | ValueType::ReferenceValue(_)
            | ValueType::GeoPointValue(_),
        ) => Ok(()),
    }
}

fn parse_list_order_by(order_by: &str) -> Result<Vec<(FieldReference, Direction)>> {
    let trimmed = order_by.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }
    let mut orders = Vec::new();
    for raw_clause in trimmed.split(',') {
        let clause = raw_clause.trim();
        if clause.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "empty clause in order_by",
            ));
        }
        let (field_str, direction) = match clause.rsplit_once(char::is_whitespace) {
            Some((f, dir))
                if dir.eq_ignore_ascii_case("asc") || dir.eq_ignore_ascii_case("ascending") =>
            {
                (f.trim(), Direction::Ascending)
            }
            Some((f, dir))
                if dir.eq_ignore_ascii_case("desc") || dir.eq_ignore_ascii_case("descending") =>
            {
                (f.trim(), Direction::Descending)
            }
            _ => (clause, Direction::Ascending),
        };
        orders.push((field_str.parse()?, direction));
    }
    Ok(orders)
}

impl Drop for FirestoreDatabase {
    fn drop(&mut self) {
        debug!("Database \"{}\" dropped", self.name);
    }
}

fn apply_updates(
    document: &mut OwnedDocumentContentsWriteGuard,
    mask: DocumentMask,
    updated_values: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, GenericDatabaseError> {
    let mut fields = document
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

fn get_doc_name_from_write(write: &Write) -> Result<DocumentRef> {
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
