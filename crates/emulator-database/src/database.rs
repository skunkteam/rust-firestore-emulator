use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
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
    Mutex, RwLock,
    broadcast::{self, Receiver},
};
use tracing::{Level, Span, debug, field::display, instrument};

use self::{
    collection::Collection,
    document::{
        DeletedDocumentVersion, DocumentMeta, DocumentPrecondition, DocumentVersion,
        OwnedDocumentContentsWriteGuard, StoredDocumentVersion, check_version_precondition,
    },
    event::{DatabaseEvent, ProjectEvent},
    field_path::FieldPath,
    projection::{Project, Projection},
    query::{Direction, Query, QueryBuilder},
    read_consistency::{ReadConsistency, validate_read_time},
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
    transaction::{ReadWriteTransaction, RunningTransactions, TransactionId},
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
mod validation;
pub(crate) mod vector;

const MAX_EVENT_BACKLOG: usize = 1024;

#[derive(Debug)]
pub struct FirestoreDatabase {
    project: &'static FirestoreProject,
    pub name: RootRef,
    enterprise_edition: AtomicBool,
    optimistic_concurrency: AtomicBool,
    cleared: AtomicBool,
    collections: RwLock<HashMap<DefaultAtom, Arc<Collection>>>,
    transactions: RunningTransactions,
    optimistic_commit_lock: Mutex<()>,
    events: std::sync::Mutex<Option<broadcast::Sender<Arc<DatabaseEvent>>>>,
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
            cleared: AtomicBool::new(false),
            collections: Default::default(),
            transactions: RunningTransactions::new(Weak::clone(database)),
            optimistic_commit_lock: Default::default(),
            events: std::sync::Mutex::new(Some(broadcast::channel(MAX_EVENT_BACKLOG).0)),
        })
    }

    pub(crate) fn mark_cleared(&self) {
        self.cleared.store(true, atomic::Ordering::Release);
        let sender = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(sender);
    }

    pub(crate) fn is_cleared(&self) -> bool {
        self.cleared.load(atomic::Ordering::Acquire)
    }

    fn ensure_owns_root(&self, root_ref: &RootRef) -> Result<()> {
        if root_ref != &self.name {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "resource root {root_ref} does not match database {}",
                self.name
            )));
        }
        Ok(())
    }

    fn ensure_owns_doc(&self, doc_ref: &DocumentRef) -> Result<()> {
        self.ensure_owns_root(&doc_ref.collection_ref.root_ref)
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
            ReadConsistency::ReadTime(time) => {
                let time = validate_read_time(time)?;
                self.get_doc_meta(name)
                    .await?
                    .read()
                    .await?
                    .version_at_time(time)
                    .cloned()
            }
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
        self.ensure_owns_doc(name)?;
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
        self.ensure_owns_root(parent.root())?;
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
        self.ensure_owns_root(&parent.root_ref)?;
        // Cannot use `filter_map` because of the `await`.
        let mut result = BTreeSet::new();
        for doc in self.get_collection(parent).await.docs().await {
            if doc.read().await?.exists() {
                result.insert(doc.name.document_id.to_string());
            }
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
            result.into_iter().map(|m| query.project(&m)).collect(),
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
        self.ensure_owns_root(parent.root())?;
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
                        let mut values = docs
                            .iter()
                            .filter_map(|m| {
                                Some(
                                    query
                                        .effective_numeric_field_value(m, &field_ref)?
                                        .into_owned(),
                                )
                            })
                            .filter(Value::is_number)
                            .peekable();
                        if self.enterprise_edition() && values.peek().is_none() {
                            Value::null()
                        } else {
                            values.sum()
                        }
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
                            .filter_map(|m| {
                                Some(
                                    query
                                        .effective_numeric_field_value(m, &field_ref)?
                                        .into_owned(),
                                )
                            })
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
        self.ensure_owns_doc(&name)?;
        validation::validate_write(&write)?;
        let mut guard = self.get_doc_meta(&name).await?.write_owned().await?;
        if self.is_cleared() {
            return Err(GenericDatabaseError::aborted("database was cleared"));
        }
        let update_time = guard.acquire_time;
        let mut staged = StagedDocument::new(&mut guard);
        let result = self.stage_write(write, &mut staged, update_time)?;
        let new_version = staged.pending.take();
        if let Some(ref version) = new_version {
            guard.maybe_add_version(version)?;
        }
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
        if document_id.contains('/') {
            return Err(GenericDatabaseError::invalid_argument(
                "document_id cannot contain '/'",
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
        self.ensure_owns_root(prefix.root())?;
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
        self.ensure_owns_root(parent.root())?;
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
    pub async fn commit_non_transactional(
        self: &Arc<Self>,
        writes: Vec<Write>,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        self.commit_inner(writes, None).await
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn commit(
        self: &Arc<Self>,
        writes: Vec<Write>,
        transaction: TransactionId,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        self.commit_inner(writes, Some(transaction)).await
    }

    async fn commit_inner(
        self: &Arc<Self>,
        writes: Vec<Write>,
        transaction: Option<TransactionId>,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let mut target_docs = BTreeSet::new();
        for write in &writes {
            let name = get_doc_name_from_write(write)?;
            self.ensure_owns_doc(&name)?;
            validation::validate_write(write)?;
            target_docs.insert(name);
        }

        // Reserve the transaction up front after request-level write validation so validation
        // errors keep the transaction active while concurrent commit/retry calls cannot race.
        // Dropping `commit_guard` on error or cancellation releases held read locks before
        // recording the closed slot for idempotent `Rollback` and `retry_transaction`.
        // Non-transactional commits bypass `RunningTransactions` entirely so they cannot churn
        // the bounded closed-transaction table.
        let commit_guard = transaction
            .map(|txn_id| self.transactions.begin_commit(txn_id, !writes.is_empty()))
            .transpose()?;
        let rw_txn = commit_guard
            .as_ref()
            .and_then(|guard| guard.as_read_write());

        // Serialize optimistic validation-through-commit critical sections so concurrent
        // optimistic transactions cannot pass validation against each other's pre-commit state.
        let _optimistic_guard =
            if rw_txn.is_some_and(ReadWriteTransaction::is_optimistic) && !writes.is_empty() {
                Some(self.optimistic_commit_lock.lock().await)
            } else {
                None
            };

        // Acquire write locks in sorted DocumentRef order to prevent AB-BA deadlocks across
        // concurrent multi-document commits, validate optimistic read sets against those held
        // write guards, and stage all mutations before updating any document so failed writes
        // leave all documents untouched.
        let mut write_guard_cache = BTreeMap::<DocumentRef, OwnedDocumentContentsWriteGuard>::new();
        if let Some(txn) = rw_txn {
            for name in target_docs {
                write_guard_cache.insert(name.clone(), txn.get_write_guard(&name).await?);
            }
            // No write-skew possible when no writes, do not abort even if things changed.
            if !writes.is_empty() {
                txn.check_concurrency_violations(&write_guard_cache).await?;
            }
        } else if commit_guard.is_none() {
            for name in target_docs {
                let guard = self.get_doc_meta(&name).await?.write_owned().await?;
                write_guard_cache.insert(name, guard);
            }
        }
        if self.is_cleared() {
            return Err(GenericDatabaseError::aborted("database was cleared"));
        }

        let time = Timestamp::now();

        let mut staged_docs: BTreeMap<DocumentRef, StagedDocument<'_>> = write_guard_cache
            .iter_mut()
            .map(|(name, guard)| (name.clone(), StagedDocument::new(guard)))
            .collect();
        let mut write_results = Vec::with_capacity(writes.len());

        for write in writes {
            let name = get_doc_name_from_write(&write)?;
            let staged = staged_docs.get_mut(&name).unwrap();
            write_results.push(self.stage_write(write, staged, time)?);
        }

        if let Some(txn) = rw_txn {
            txn.prepare_commit().await;
        }

        let mut updates = HashMap::new();
        for (name, staged) in staged_docs {
            if let Some(version) = staged.pending {
                staged.guard.maybe_add_version(&version)?;
                updates.insert(name, version);
            }
        }
        drop(write_guard_cache);
        drop(_optimistic_guard);
        if let Some(commit_guard) = commit_guard {
            commit_guard.finish_committed();
        }

        self.send_event(DatabaseEvent {
            database: Arc::downgrade(self),
            update_time: time,
            updates,
        });

        Ok((time, write_results))
    }

    #[instrument(level = Level::DEBUG, skip_all, err)]
    fn stage_write(
        &self,
        write: Write,
        document: &mut StagedDocument<'_>,
        commit_time: Timestamp,
    ) -> Result<WriteResult> {
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
        let update_time = match operation {
            Update(doc) => {
                debug!(name = %document.guard.name, "Update");
                validation::validate_writable_fields(&doc.fields)?;
                if let Some(mask) = &update_mask {
                    validation::validate_update_mask(mask)?;
                }
                validation::validate_field_transforms(&update_transforms)?;

                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(
                        document.current_version().map(|v| &v.fields),
                        mask,
                        &doc.fields,
                    )?
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
                document.stage_update(fields, commit_time)?
            }
            Delete(_) => {
                debug!(name = %document.guard.name, "Delete");

                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                document.stage_delete(commit_time)?;
                commit_time
            }
            Transform(_) => unimplemented!("transform"),
        };
        Ok(WriteResult {
            update_time: Some(update_time),
            transform_results,
        })
    }

    pub async fn get_read_time(&self, consistency: ReadConsistency) -> Result<Timestamp> {
        match consistency {
            ReadConsistency::Default => Ok(Timestamp::now()),
            ReadConsistency::Transaction(id) => {
                let txn = self.transactions.get(id).await?;
                Ok(txn.read_time().unwrap_or_else(Timestamp::now))
            }
            ReadConsistency::ReadTime(time) => validate_read_time(time),
        }
    }

    pub async fn new_txn(&self, transaction_options: TransactionOptions) -> Result<TransactionId> {
        use transaction_options::*;
        let id = match transaction_options.mode {
            None => self.transactions.start_read_only(None).await?,
            Some(Mode::ReadOnly(ro)) => {
                self.transactions
                    .start_read_only(
                        ro.consistency_selector
                            .map(|read_only::ConsistencySelector::ReadTime(read_time)| read_time),
                    )
                    .await?
            }
            Some(Mode::ReadWrite(rw)) if rw.retry_transaction.is_empty() => {
                if rw.concurrency_mode != ConcurrencyMode::Unspecified as i32 {
                    crate::unimplemented!("TransactionOptions.read_write.concurrency_mode");
                }
                self.transactions
                    .start_read_write(self.optimistic_concurrency())
                    .await
            }
            Some(Mode::ReadWrite(rw)) => {
                if rw.concurrency_mode != ConcurrencyMode::Unspecified as i32 {
                    crate::unimplemented!("TransactionOptions.read_write.concurrency_mode");
                }
                let id = rw.retry_transaction.try_into()?;
                self.transactions
                    .start_read_write_with_id(id, self.optimistic_concurrency())
                    .await?;
                id
            }
        };
        Ok(id)
    }

    pub fn rollback_sync(&self, transaction: TransactionId) -> Result<()> {
        self.transactions.rollback_sync(transaction)
    }

    pub async fn rollback(&self, transaction: TransactionId) -> Result<()> {
        self.rollback_sync(transaction)
    }

    pub fn send_event(&self, event: DatabaseEvent) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(sender) = events.as_ref() else {
            return;
        };
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
        let _unused = sender.send(event.into());
    }

    pub fn subscribe(&self) -> Receiver<Arc<DatabaseEvent>> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .map_or_else(|| broadcast::channel(1).1, broadcast::Sender::subscribe)
    }

    pub fn enterprise_edition(&self) -> bool {
        self.enterprise_edition.load(atomic::Ordering::Relaxed)
    }

    pub fn set_enterprise_edition(&self, enterprise_edition: bool) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .enterprise_edition
            .swap(enterprise_edition, atomic::Ordering::Relaxed)
            != enterprise_edition
            && events.is_some()
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
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self
            .optimistic_concurrency
            .swap(optimistic_concurrency, atomic::Ordering::Relaxed)
            != optimistic_concurrency
            && events.is_some()
        {
            self.project
                .send_project_event(ProjectEvent::DatabaseConfigChanged {
                    database: self.name.clone(),
                });
        }
    }

    pub fn set_config(&self, enterprise_edition: bool, optimistic_concurrency: bool) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let enterprise_changed = self
            .enterprise_edition
            .swap(enterprise_edition, atomic::Ordering::Relaxed)
            != enterprise_edition;
        let optimistic_changed = self
            .optimistic_concurrency
            .swap(optimistic_concurrency, atomic::Ordering::Relaxed)
            != optimistic_concurrency;
        if (enterprise_changed || optimistic_changed) && events.is_some() {
            self.project
                .send_project_event(ProjectEvent::DatabaseConfigChanged {
                    database: self.name.clone(),
                });
        }
    }
}

fn validate_writable_fields(fields: &HashMap<String, Value>) -> Result<()> {
    validation::validate_writable_fields(fields)
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

struct StagedDocument<'a> {
    guard:   &'a mut OwnedDocumentContentsWriteGuard,
    pending: Option<DocumentVersion>,
}

impl<'a> StagedDocument<'a> {
    fn new(guard: &'a mut OwnedDocumentContentsWriteGuard) -> Self {
        Self {
            guard,
            pending: None,
        }
    }

    fn latest_version(&self) -> Option<&DocumentVersion> {
        self.pending
            .as_ref()
            .or_else(|| self.guard.latest_version())
    }

    fn current_version(&self) -> Option<&Arc<StoredDocumentVersion>> {
        self.latest_version()
            .and_then(DocumentVersion::stored_document)
    }

    fn check_precondition(&self, condition: DocumentPrecondition) -> Result<()> {
        check_version_precondition(&self.guard.name, self.latest_version(), condition)
    }

    fn stage_update(
        &mut self,
        fields: HashMap<String, Value>,
        commit_time: Timestamp,
    ) -> Result<Timestamp> {
        let (update_time, version) = document::stage_stored_version(
            &self.guard.name,
            self.guard.acquire_time,
            self.latest_version(),
            fields,
            commit_time,
        )?;
        if let Some(version) = version {
            if let (Some(orig), Some(staged)) =
                (self.guard.current_version(), version.stored_document())
                && orig.create_time == staged.create_time
                && Value::fields_storage_eq(&orig.fields, &staged.fields)
            {
                self.pending = None;
                return Ok(orig.update_time);
            }
            self.pending = Some(version);
        }
        Ok(update_time)
    }

    fn stage_delete(&mut self, delete_time: Timestamp) -> Result<()> {
        if delete_time < self.guard.acquire_time {
            return Err(GenericDatabaseError::internal(
                "lock should always be acquired before claiming write/update time",
            ));
        }
        self.pending = Some(DocumentVersion::Deleted(Arc::new(DeletedDocumentVersion {
            name: self.guard.name.clone(),
            delete_time,
        })));
        Ok(())
    }
}

fn apply_updates(
    existing_fields: Option<&HashMap<String, Value>>,
    mask: DocumentMask,
    updated_values: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, GenericDatabaseError> {
    let mut fields = existing_fields.cloned().unwrap_or_default();
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
                ServerValue::Unspecified => {
                    return Err(GenericDatabaseError::invalid_argument(
                        "ServerValue type must be specified for set_to_server_value \
                         transformation.",
                    ));
                }
            }
        }
        TransformType::Increment(value) => field_path
            .transform_value(fields, |cur| cur.unwrap_or_else(Value::null) + value)
            .clone(),
        TransformType::Maximum(_) => unimplemented!("TransformType::Maximum"),
        TransformType::Minimum(_) => unimplemented!("TransformType::Minimum"),
        TransformType::AppendMissingElements(elements) => {
            field_path.transform_value(fields, |cur_value| {
                let mut array = cur_value.and_then(Value::into_array).unwrap_or_default();
                for el in elements.values {
                    if !array.contains(&el) {
                        array.push(el);
                    }
                }
                Value::array(array)
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use googleapis::google::firestore::v1::{Document, TransactionOptions, Value, Write, write};

    use super::*;
    use crate::{FirestoreConfig, FirestoreProject, reference::CollectionRef};

    #[tokio::test]
    async fn get_document_ids_excludes_deleted_and_probed_missing_documents() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let col: CollectionRef = "projects/p/databases/(default)/documents/users"
            .parse()
            .unwrap();

        db.create_doc(
            &col,
            "alice",
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
        )
        .await
        .unwrap();
        db.create_doc(
            &col,
            "bob",
            HashMap::from([("v".to_string(), Value::integer(2))]),
            None,
        )
        .await
        .unwrap();

        // Probe a non-existent document and delete `bob`.
        let ghost: DocumentRef = "projects/p/databases/(default)/documents/users/ghost"
            .parse()
            .unwrap();
        assert!(
            db.get_doc(&ghost, ReadConsistency::Default)
                .await
                .unwrap()
                .is_none()
        );
        let bob: DocumentRef = "projects/p/databases/(default)/documents/users/bob"
            .parse()
            .unwrap();
        db.delete_doc(&bob, None).await.unwrap();

        // Phantom doc `diana` has an existing subcollection document, so it should be included.
        let sub_col: CollectionRef = "projects/p/databases/(default)/documents/users/diana/notes"
            .parse()
            .unwrap();
        db.create_doc(
            &sub_col,
            "n1",
            HashMap::from([("v".to_string(), Value::integer(3))]),
            None,
        )
        .await
        .unwrap();

        let ids = db.get_document_ids(&col).await.unwrap();
        assert_eq!(
            ids,
            BTreeSet::from(["alice".to_string(), "diana".to_string()])
        );
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_opposite_order_commits_serialize_without_deadlock() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        let doc_b: DocumentRef = "projects/p/databases/(default)/documents/c/b"
            .parse()
            .unwrap();

        // Hold a read lock on `doc_a` so both commits queue up concurrently before either starts
        // Phase 2 locking.
        let rw_opts = TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        };
        let blocker_txn = db.new_txn(rw_opts.clone()).await.unwrap();
        drop(
            db.get_doc(&doc_a, ReadConsistency::Transaction(blocker_txn))
                .await
                .unwrap(),
        );

        let make_write = |doc: &DocumentRef, v: i64| Write {
            operation: Some(write::Operation::Update(Document {
                name: doc.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(v))]),
                ..Default::default()
            })),
            ..Default::default()
        };

        let db1 = Arc::clone(&db);
        let t1_id = db1.new_txn(rw_opts.clone()).await.unwrap();
        let w1 = vec![make_write(&doc_a, 1), make_write(&doc_b, 1)];
        let h1 = tokio::spawn(async move { db1.commit(w1, t1_id).await });
        tokio::task::yield_now().await;

        let db2 = Arc::clone(&db);
        let t2_id = db2.new_txn(rw_opts).await.unwrap();
        let w2 = vec![make_write(&doc_b, 2), make_write(&doc_a, 2)];
        let h2 = tokio::spawn(async move { db2.commit(w2, t2_id).await });
        tokio::task::yield_now().await;

        db.rollback(blocker_txn).await.unwrap();

        h1.await.unwrap().expect("commit 1 should not deadlock");
        h2.await.unwrap().expect("commit 2 should not deadlock");
    }

    #[tokio::test]
    async fn inflight_commit_rejects_concurrent_retry_transaction_with_same_id() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        db.update_doc(
            &doc_a,
            HashMap::from([("v".to_string(), Value::integer(0))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let rw_opts = TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        };
        let blocker_txn = db.new_txn(rw_opts.clone()).await.unwrap();
        drop(
            db.get_doc(&doc_a, ReadConsistency::Transaction(blocker_txn))
                .await
                .unwrap(),
        );

        let make_write = |doc: &DocumentRef, v: i64| Write {
            operation: Some(write::Operation::Update(Document {
                name: doc.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(v))]),
                ..Default::default()
            })),
            ..Default::default()
        };

        let t1_id = db.new_txn(rw_opts).await.unwrap();
        let mut commit_fut = Box::pin(db.commit(vec![make_write(&doc_a, 1)], t1_id));
        assert!(futures_util::poll!(&mut commit_fut).is_pending());

        let retry_res = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(
                    transaction_options::ReadWrite {
                        retry_transaction: Vec::from(t1_id),
                        ..Default::default()
                    },
                )),
            })
            .await;

        db.rollback(blocker_txn).await.unwrap();
        commit_fut.await.unwrap();

        assert!(
            matches!(retry_res, Err(GenericDatabaseError::FailedPrecondition(_))),
            "in-flight commit must still reserve TransactionId against retry_transaction, got \
             {retry_res:?}"
        );
    }

    #[tokio::test]
    async fn commit_writes_on_read_only_transaction_closes_transaction_and_allows_idempotent_rollback()
     {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        db.update_doc(
            &doc_a,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let ro_id = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadOnly(
                    transaction_options::ReadOnly::default(),
                )),
            })
            .await
            .unwrap();

        let write = Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_a.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(2))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        let err = db.commit(vec![write], ro_id).await.unwrap_err();
        assert!(matches!(err, GenericDatabaseError::InvalidArgument(_)));

        assert!(
            matches!(
                db.get_doc(&doc_a, ReadConsistency::Transaction(ro_id))
                    .await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ),
            "read-only transaction must be closed after rejected write commit"
        );
        assert!(
            db.rollback(ro_id).await.is_ok(),
            "read-only transaction must accept idempotent rollback after rejected write commit"
        );
        assert!(
            db.rollback(ro_id).await.is_ok(),
            "repeated rollback after rejected write commit must remain idempotent"
        );
        assert!(
            matches!(
                db.new_txn(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(
                        transaction_options::ReadWrite {
                            retry_transaction: Vec::from(ro_id),
                            ..Default::default()
                        },
                    )),
                })
                .await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ),
            "read-only transaction must not be retryable"
        );
    }

    #[tokio::test]
    async fn commit_validation_error_preserves_active_read_write_and_read_only_transactions() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        db.update_doc(
            &doc_a,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let invalid_writes = vec![
            Write {
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("__bad__".to_string(), Value::integer(1))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Write {
                update_transforms: vec![document_transform::FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(
                        ServerValue::Unspecified as i32,
                    )),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("v".to_string(), Value::integer(9))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Write {
                update_transforms: vec![document_transform::FieldTransform {
                    field_path:     "ts".to_string(),
                    transform_type: Some(TransformType::SetToServerValue(999)),
                }],
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("v".to_string(), Value::integer(9))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Write {
                current_document: Some(Precondition {
                    condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                        seconds: -1,
                        nanos:   0,
                    })),
                }),
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("v".to_string(), Value::integer(9))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
            Write {
                current_document: Some(Precondition {
                    condition_type: Some(precondition::ConditionType::UpdateTime(Timestamp {
                        seconds: 10,
                        nanos:   1_000_000_000,
                    })),
                }),
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("v".to_string(), Value::integer(9))]),
                    ..Default::default()
                })),
                ..Default::default()
            },
        ];

        // Read-write transaction remains active after every request validation failure.
        let rw_id = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(
                    transaction_options::ReadWrite::default(),
                )),
            })
            .await
            .unwrap();
        for invalid_write in &invalid_writes {
            assert!(matches!(
                db.commit(vec![invalid_write.clone()], rw_id).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(
                db.get_doc(&doc_a, ReadConsistency::Transaction(rw_id))
                    .await
                    .is_ok()
            );
        }
        let valid_write = Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_a.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(2))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        db.commit(vec![valid_write], rw_id).await.unwrap();
        assert!(matches!(
            db.rollback(rw_id).await,
            Err(GenericDatabaseError::Aborted(_))
        ));

        // Read-only transaction also remains active after every request validation failure.
        let ro_id = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadOnly(
                    transaction_options::ReadOnly::default(),
                )),
            })
            .await
            .unwrap();
        for invalid_write in invalid_writes {
            assert!(matches!(
                db.commit(vec![invalid_write], ro_id).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(
                db.get_doc(&doc_a, ReadConsistency::Transaction(ro_id))
                    .await
                    .is_ok()
            );
        }
        db.rollback(ro_id).await.unwrap();
    }

    #[tokio::test]
    async fn non_transactional_commits_do_not_evict_closed_transactions() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        db.update_doc(
            &doc_a,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let rw_failed = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadWrite(
                    transaction_options::ReadWrite::default(),
                )),
            })
            .await
            .unwrap();
        let fail_write = Write {
            current_document: Some(Precondition {
                condition_type: Some(precondition::ConditionType::Exists(false)),
            }),
            operation: Some(write::Operation::Update(Document {
                name: doc_a.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(2))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        assert!(matches!(
            db.commit(vec![fail_write], rw_failed).await,
            Err(GenericDatabaseError::AlreadyExists(_))
        ));

        let ro_closed = db
            .new_txn(TransactionOptions {
                mode: Some(transaction_options::Mode::ReadOnly(
                    transaction_options::ReadOnly::default(),
                )),
            })
            .await
            .unwrap();
        db.rollback(ro_closed).await.unwrap();

        // Execute more than `MAX_CLOSED_TXNS` non-transactional commits; none may enter
        // `RunningTransactions` or evict real closed transactions.
        for i in 0..(transaction::MAX_CLOSED_TXNS + 64) {
            let w = Write {
                operation: Some(write::Operation::Update(Document {
                    name: doc_a.to_string(),
                    fields: HashMap::from([("v".to_string(), Value::integer(i as i64))]),
                    ..Default::default()
                })),
                ..Default::default()
            };
            db.commit_non_transactional(vec![w]).await.unwrap();
        }

        assert!(
            db.rollback(rw_failed).await.is_ok(),
            "non-transactional commits must not evict real failed transaction from closed ring"
        );
        assert!(
            matches!(
                db.new_txn(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(
                        transaction_options::ReadWrite {
                            retry_transaction: Vec::from(ro_closed),
                            ..Default::default()
                        },
                    )),
                })
                .await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ),
            "non-transactional commits must not evict closed read-only transaction metadata"
        );
    }

    #[tokio::test]
    async fn rollback_during_blocked_commit_succeeds_without_aborting_commit_and_aborts_after() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/c/a"
            .parse()
            .unwrap();
        db.update_doc(
            &doc_a,
            HashMap::from([("v".to_string(), Value::integer(1))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let rw_opts = TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        };
        let blocker = db.new_txn(rw_opts.clone()).await.unwrap();
        drop(
            db.get_doc(&doc_a, ReadConsistency::Transaction(blocker))
                .await
                .unwrap(),
        );

        let committer = db.new_txn(rw_opts).await.unwrap();
        let write = Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_a.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(42))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut commit_fut = Box::pin(db.commit(vec![write], committer));
        assert!(futures_util::poll!(&mut commit_fut).is_pending());

        // Rollback while Commit is blocked waiting on locks succeeds immediately without cancelling
        // Commit.
        db.rollback(committer).await.unwrap();
        assert!(futures_util::poll!(&mut commit_fut).is_pending());

        db.rollback(blocker).await.unwrap();
        commit_fut.await.unwrap();

        let after = db
            .get_doc(&doc_a, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.fields["v"].as_integer(), Some(42));

        // After Commit succeeded, Rollback rejects with Aborted.
        assert!(matches!(
            db.rollback(committer).await,
            Err(GenericDatabaseError::Aborted(_))
        ));
    }

    #[tokio::test]
    async fn multi_write_commit_cancelling_back_to_original_fields_is_noop() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let col: CollectionRef = "projects/p/databases/(default)/documents/c"
            .parse()
            .unwrap();
        let created = db
            .create_doc(
                &col,
                "d1",
                HashMap::from([("v".to_string(), Value::integer(1))]),
                None,
            )
            .await
            .unwrap();
        let t0 = created.update_time.unwrap();

        let d1: DocumentRef = "projects/p/databases/(default)/documents/c/d1"
            .parse()
            .unwrap();
        let mut rx = db.subscribe();

        let rw_opts = TransactionOptions {
            mode: Some(transaction_options::Mode::ReadWrite(
                transaction_options::ReadWrite::default(),
            )),
        };
        let t_id = db.new_txn(rw_opts).await.unwrap();
        let make_write = |v: i64| Write {
            operation: Some(write::Operation::Update(Document {
                name: d1.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(v))]),
                ..Default::default()
            })),
            ..Default::default()
        };
        db.commit(vec![make_write(2), make_write(1)], t_id)
            .await
            .unwrap();

        let after = db
            .get_doc(&d1, ReadConsistency::Default)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.update_time, t0);
        if let Ok(ev) = rx.try_recv() {
            assert!(
                !ev.updates.contains_key(&d1),
                "noop multi-write commit must not emit update event for {d1}"
            );
        }
    }

    #[tokio::test]
    async fn new_txn_rejects_explicit_concurrency_mode_as_not_implemented() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        for mode in [
            transaction_options::ConcurrencyMode::Optimistic,
            transaction_options::ConcurrencyMode::Pessimistic,
        ] {
            let err = db
                .new_txn(TransactionOptions {
                    mode: Some(transaction_options::Mode::ReadWrite(
                        transaction_options::ReadWrite {
                            retry_transaction: vec![],
                            concurrency_mode:  mode as i32,
                        },
                    )),
                })
                .await
                .unwrap_err();
            assert!(
                matches!(err, GenericDatabaseError::NotImplemented(_)),
                "expected NotImplemented for concurrency_mode {mode:?}, got {err:?}"
            );
        }
    }
}
