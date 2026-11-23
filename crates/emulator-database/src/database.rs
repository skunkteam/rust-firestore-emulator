use std::{
    collections::{BTreeSet, HashMap},
    sync::{
        Arc, Weak,
        atomic::{self, AtomicBool},
    },
};

use googleapis::google::{
    firestore::v1::{
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

pub use self::lock::{
    ContendedDocumentSnapshot, ContentionEvent, ContentionEventKind, ContentionLockMode,
    ContentionSessionState, ContentionSnapshot, ContentionSseEvent, ContentionSubscriberGuard,
    ContentionSubscription, ContentionSummary, DatabaseContentionSnapshot, MAX_CONTENTION_HISTORY,
    MAX_SNAPSHOT_CONTENDED_DOCS, MAX_SNAPSHOT_DOC_PARTICIPANTS, ReaderSnapshot, SessionKind,
    WaiterSnapshot, WriterSnapshot,
};
use self::{
    collection::Collection,
    document::{DocumentContents, DocumentMeta, StoredDocumentVersion},
    event::{DatabaseEvent, ProjectEvent},
    lock::{LockManager, LockPriority},
    projection::{Project, Projection},
    query::{Direction, Query, QueryBuilder},
    read_consistency::{ReadConsistency, validate_read_time},
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
    transaction::{RunningTransactions, TransactionId},
};
use crate::{
    FirestoreProject, GenericDatabaseError,
    database::field_path::FieldReference,
    error::{AbortReason, Result},
    unimplemented_bool, unimplemented_option,
    utils::RwLockHashMapExt,
};

mod collection;
mod commit;
pub(crate) mod document;
pub mod event;
pub mod field_path;
pub(crate) mod lock;
pub mod pipeline;
pub mod projection;
pub mod query;
pub mod read_consistency;
pub mod reference;
pub(crate) mod timeouts;
pub(crate) mod transaction;
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
    pub(crate) locks: Arc<LockManager>,
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
        let locks = Arc::new(LockManager::with_broadcaster(
            name.clone(),
            project.timeouts,
            project.contention_broadcaster(),
        ));
        let db = Arc::new_cyclic(|database| FirestoreDatabase {
            project,
            name,
            enterprise_edition: AtomicBool::new(enterprise_edition),
            optimistic_concurrency: AtomicBool::new(enterprise_edition),
            cleared: AtomicBool::new(false),
            collections: Default::default(),
            locks: Arc::clone(&locks),
            transactions: RunningTransactions::new(Weak::clone(database), locks, project.timeouts),
            optimistic_commit_lock: Default::default(),
            events: std::sync::Mutex::new(Some(broadcast::channel(MAX_EVENT_BACKLOG).0)),
        });
        db.locks.set_reaper(Arc::downgrade(&db));
        db
    }

    pub(crate) fn mark_cleared(&self) {
        self.cleared.store(true, atomic::Ordering::Release);
        let sender = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        drop(sender);
        self.locks.abort_all(AbortReason::DatabaseCleared);
        self.transactions.clear_all();
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

    pub(crate) fn ensure_owns_doc(&self, doc_ref: &DocumentRef) -> Result<()> {
        self.ensure_owns_root(&doc_ref.collection_ref.root_ref)
    }

    #[instrument(level = Level::DEBUG, skip_all, err, fields(txn_id, found))]
    pub async fn get_doc(
        &self,
        name: &DocumentRef,
        consistency: ReadConsistency,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        debug!(%name);
        self.ensure_owns_doc(name)?;
        let version = match consistency {
            ReadConsistency::Default => {
                if self.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                self.transactions.reap_if_due();
                let session = self.locks.new_session(
                    None,
                    SessionKind::PointRead,
                    LockPriority::NextSeq,
                    false,
                    false,
                );
                let lease = session.acquire_read(name).await?;
                match self.peek_doc_meta(name).await? {
                    Some(meta) => meta.read_with(&lease, |c| c.current_version().cloned())?,
                    None => {
                        lease.check_active()?;
                        None
                    }
                }
            }
            ReadConsistency::ReadTime(time) => {
                if self.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                self.transactions.reap_if_due();
                let time = validate_read_time(time)?;
                self.peek_doc_meta(name)
                    .await?
                    .and_then(|meta| meta.read_mvcc_at(time))
            }
            ReadConsistency::Transaction(id) => {
                Span::current().record("txn_id", display(id));
                self.transactions.get_active(id)?.read_doc(name).await?
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
                    Arc::new(Collection::new(collection_name.clone()))
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

    pub(crate) async fn peek_doc_meta(
        &self,
        name: &DocumentRef,
    ) -> Result<Option<Arc<DocumentMeta>>> {
        self.ensure_owns_doc(name)?;
        let col = self
            .collections
            .read()
            .await
            .get(&name.collection_ref.collection_id)
            .cloned();
        match col {
            Some(col) => Ok(col.peek_doc(name).await),
            None => Ok(None),
        }
    }

    /// Get all the collections that reside directly under the given parent. This means that:
    /// - the IDs will not contain a `/`
    /// - the result will be empty if `parent` is a [`Ref::Collection`].
    #[instrument(level = Level::DEBUG, skip_all)]
    pub async fn get_collection_ids(&self, parent: &Ref) -> Result<BTreeSet<String>> {
        self.ensure_owns_root(parent.root())?;
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
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
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        // Cannot use `filter_map` because of the `await`.
        let mut result = BTreeSet::new();
        let col = self
            .collections
            .read()
            .await
            .get(&parent.collection_id)
            .cloned();
        if let Some(col) = col {
            for doc in col.docs().await {
                if doc.read_latest_uncontended(DocumentContents::exists) {
                    result.insert(doc.name.document_id.to_string());
                }
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
                Ok::<_, GenericDatabaseError>((agg.alias, result))
            })
            .try_collect()?;
        Ok((read_time, result))
    }

    pub async fn write(self: &Arc<Self>, write: Write) -> Result<WriteResult> {
        let (result, _) = self.write_with_version(write).await?;
        Ok(result)
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
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        let mut to_delete = BTreeSet::new();
        if let Ref::Document(doc_ref) = prefix
            && let Some(meta) = self.peek_doc_meta(doc_ref).await?
            && meta.read_latest_uncontended(DocumentContents::exists)
        {
            to_delete.insert(doc_ref.clone());
        }
        for col in self.get_all_collections().await {
            let col_as_ref = Ref::Collection(col.name.clone());
            if &col_as_ref == prefix || prefix.is_parent_of(&col_as_ref) {
                for meta in col.docs().await {
                    if meta.read_latest_uncontended(DocumentContents::exists) {
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

    pub async fn get_read_time(&self, consistency: ReadConsistency) -> Result<Timestamp> {
        match consistency {
            ReadConsistency::Default => {
                if self.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                self.transactions.reap_if_due();
                Ok(Timestamp::now())
            }
            ReadConsistency::Transaction(id) => {
                let txn = self.transactions.get_active(id)?;
                Ok(txn.read_time().unwrap_or_else(Timestamp::now))
            }
            ReadConsistency::ReadTime(time) => {
                if self.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                self.transactions.reap_if_due();
                validate_read_time(time)
            }
        }
    }

    pub async fn new_txn(&self, transaction_options: TransactionOptions) -> Result<TransactionId> {
        use transaction_options::*;
        let id = match transaction_options.mode {
            None => self.transactions.start_read_only(None)?,
            Some(Mode::ReadOnly(ro)) => self.transactions.start_read_only(
                ro.consistency_selector
                    .map(|read_only::ConsistencySelector::ReadTime(read_time)| read_time),
            )?,
            Some(Mode::ReadWrite(rw)) if rw.retry_transaction.is_empty() => {
                if rw.concurrency_mode != ConcurrencyMode::Unspecified as i32 {
                    crate::unimplemented!("TransactionOptions.read_write.concurrency_mode");
                }
                self.transactions
                    .start_read_write(self.optimistic_concurrency())?
            }
            Some(Mode::ReadWrite(rw)) => {
                if rw.concurrency_mode != ConcurrencyMode::Unspecified as i32 {
                    crate::unimplemented!("TransactionOptions.read_write.concurrency_mode");
                }
                let id = rw.retry_transaction.try_into()?;
                self.transactions
                    .start_read_write_with_id(id, self.optimistic_concurrency())?
            }
        };
        Ok(id)
    }

    pub fn rollback_sync(&self, transaction: TransactionId) -> Result<()> {
        self.transactions.rollback(transaction)
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
        if !self.is_cleared() {
            self.locks.abort_all(AbortReason::DatabaseDropped);
        }
        self.transactions.clear_all();
        debug!("Database \"{}\" dropped", self.name);
    }
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
        let h1 = tokio::spawn(async move { db1.commit(w1, Some(t1_id)).await });
        tokio::task::yield_now().await;

        let db2 = Arc::clone(&db);
        let t2_id = db2.new_txn(rw_opts).await.unwrap();
        let w2 = vec![make_write(&doc_b, 2), make_write(&doc_a, 2)];
        let h2 = tokio::spawn(async move { db2.commit(w2, Some(t2_id)).await });
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
        let mut commit_fut = Box::pin(db.commit(vec![make_write(&doc_a, 1)], Some(t1_id)));
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
        let err = db.commit(vec![write], Some(ro_id)).await.unwrap_err();
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
                    transform_type: Some(
                        document_transform::field_transform::TransformType::SetToServerValue(
                            document_transform::field_transform::ServerValue::Unspecified as i32,
                        ),
                    ),
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
                    transform_type: Some(
                        document_transform::field_transform::TransformType::SetToServerValue(999),
                    ),
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
                db.commit(vec![invalid_write.clone()], Some(rw_id)).await,
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
        db.commit(vec![valid_write], Some(rw_id)).await.unwrap();
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
                db.commit(vec![invalid_write], Some(ro_id)).await,
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
            db.commit(vec![fail_write], Some(rw_failed)).await,
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
            db.commit(vec![w], None).await.unwrap();
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
        let mut commit_fut = Box::pin(db.commit(vec![write], Some(committer)));
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
        db.commit(vec![make_write(2), make_write(1)], Some(t_id))
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

    #[tokio::test(start_paused = true)]
    async fn contention_commit_records_write_batch_retry_write_timeout_and_upgrade_abort() {
        use std::time::Duration;

        use futures_util::poll;
        use tokio::time::advance;

        use crate::database::lock::{ContentionEventKind, LockPriority, SessionKind};

        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let d_seq: DocumentRef = "projects/p/databases/(default)/documents/c/seq"
            .parse()
            .unwrap();
        let d1: DocumentRef = "projects/p/databases/(default)/documents/c/d1"
            .parse()
            .unwrap();
        let d2: DocumentRef = "projects/p/databases/(default)/documents/c/d2"
            .parse()
            .unwrap();
        let d_upg: DocumentRef = "projects/p/databases/(default)/documents/c/upg"
            .parse()
            .unwrap();

        let mk_write = |doc_ref: &DocumentRef, val: i64| Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_ref.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(val))]),
                ..Default::default()
            })),
            ..Default::default()
        };

        // 1. WriteBatchRetry: older txn wounds an in-flight 2-doc non-tx batch waiting on d2.
        let older_txn = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        // Materialize older_txn's seq eagerly before launching the batch.
        db.get_doc(&d_seq, ReadConsistency::Transaction(older_txn))
            .await
            .unwrap();

        let blocker = db.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let held_d2 = blocker.acquire_write(&d2).await.unwrap();

        let mut batch_fut = Box::pin(db.commit(vec![mk_write(&d1, 1), mk_write(&d2, 1)], None));
        assert!(poll!(&mut batch_fut).is_pending());

        // Older txn reads d1, wounding the batch session (which holds d1 and awaits d2).
        db.get_doc(&d1, ReadConsistency::Transaction(older_txn))
            .await
            .unwrap();

        // Let batch process the wound, log WRITE_BATCH_RETRY, and re-park.
        tokio::task::yield_now().await;
        assert!(poll!(&mut batch_fut).is_pending());

        drop(held_d2);
        blocker.close();
        db.commit(vec![], Some(older_txn)).await.unwrap();
        batch_fut.await.unwrap();

        let snap1 = db.locks.contention_snapshot();
        assert_eq!(snap1.summary.wounds_total, 1);
        assert_eq!(snap1.summary.write_batch_retries_total, 1);
        assert_eq!(
            snap1.recent_events[0].kind,
            ContentionEventKind::WriteBatchRetry
        );
        assert_eq!(snap1.recent_events[0].attempt, Some(1));
        assert_eq!(snap1.recent_events[1].kind, ContentionEventKind::Wounded);

        // 2. WriteTimeout on batch retry backoff crossing deadline.
        db.locks.clear_contention_history();
        let older_a = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        let older_b = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        db.get_doc(&d_seq, ReadConsistency::Transaction(older_a))
            .await
            .unwrap();
        db.get_doc(&d_seq, ReadConsistency::Transaction(older_b))
            .await
            .unwrap();

        let blocker2 = db.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let held2 = blocker2.acquire_write(&d2).await.unwrap();

        let mut batch2 = Box::pin(db.commit(vec![mk_write(&d1, 2), mk_write(&d2, 2)], None));
        assert!(poll!(&mut batch2).is_pending());

        // First wound at t=0 -> finish yield_now, log retry #1, and park attempt #1 holding d1.
        db.get_doc(&d1, ReadConsistency::Transaction(older_a))
            .await
            .unwrap();
        assert!(poll!(&mut batch2).is_pending());
        db.commit(vec![], Some(older_a)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(poll!(&mut batch2).is_pending());
        assert_eq!(
            db.locks
                .contention_snapshot()
                .summary
                .write_batch_retries_total,
            1
        );

        // Advance near deadline (1999ms), then wound attempt #1: backoff sleep crosses deadline!
        advance(Duration::from_millis(1999)).await;
        db.get_doc(&d1, ReadConsistency::Transaction(older_b))
            .await
            .unwrap();
        let err = batch2.await.unwrap_err();
        assert!(matches!(err, GenericDatabaseError::Aborted(_)));

        drop(held2);
        blocker2.close();
        db.commit(vec![], Some(older_b)).await.unwrap();

        let snap2 = db.locks.contention_snapshot();
        assert_eq!(snap2.summary.write_batch_retries_total, 1);
        assert_eq!(snap2.summary.timeouts_total, 1);
        assert_eq!(
            snap2.recent_events[0].kind,
            ContentionEventKind::WriteTimeout
        );
        assert_eq!(snap2.recent_events[0].attempt, Some(1));

        // 3. UpgradeContention recorded via commit_read_write.
        db.locks.clear_contention_history();
        let t1 = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        let t2 = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        // t1 reads first (older root_seq); both observe None on d_upg.
        db.get_doc(&d_upg, ReadConsistency::Transaction(t1))
            .await
            .unwrap();
        db.get_doc(&d_upg, ReadConsistency::Transaction(t2))
            .await
            .unwrap();

        // Younger t2 tries to commit first -> drops its read hold and parks behind t1.
        let mut t2_commit = Box::pin(db.commit(vec![mk_write(&d_upg, 2)], Some(t2)));
        assert!(poll!(&mut t2_commit).is_pending());

        // Older t1 commits cleanly, updating d_upg's timestamp.
        db.commit(vec![mk_write(&d_upg, 1)], Some(t1))
            .await
            .unwrap();

        // t2 wakes, sees last_updated changed, and aborts with UpgradeContention.
        let t2_err = t2_commit.await.unwrap_err();
        assert!(matches!(t2_err, GenericDatabaseError::Aborted(_)));

        let snap3 = db.locks.contention_snapshot();
        assert_eq!(snap3.summary.upgrade_aborts_total, 1);
        assert_eq!(
            snap3.recent_events[0].kind,
            ContentionEventKind::UpgradeAbort
        );
    }
}
