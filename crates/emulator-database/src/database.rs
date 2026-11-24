use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc, Weak,
        atomic::{self, AtomicBool},
    },
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use googleapis::google::{
    firestore::v1::{
        structured_aggregation_query::{Aggregation, aggregation},
        structured_query::CollectionSelector,
        *,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use prost::Message as _;
use string_cache::DefaultAtom;
use tokio::sync::{
    Mutex, RwLock,
    broadcast::{self, Receiver},
};
use tracing::{Level, Span, debug, field::display, instrument};

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
        let locks = Arc::new(LockManager::new(name.clone(), project.timeouts));
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
                let session = self
                    .locks
                    .new_session(None, LockPriority::NextSeq, false, false);
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
        let enterprise = self.enterprise_edition();
        let is_unordered = query.order_by.is_empty() && query.find_nearest.is_none();
        let mut query = Query::from_structured(parent, query, consistency, enterprise)?;
        debug!(?query);
        let (read_time, mut docs) = query.once(self).await?;
        let has_sum_or_avg = aggregations.iter().any(|agg| {
            matches!(
                agg.operator,
                Some(aggregation::Operator::Sum(_) | aggregation::Operator::Avg(_))
            )
        });
        if enterprise && is_unordered && has_sum_or_avg {
            // Enterprise checked integer sums depend on scan order; sort by canonical `__name__`
            // for deterministic primary-key scan order.
            docs.sort_by(|a, b| a.version.name.cmp_firestore_order(&b.version.name));
        }
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
                    // * Standard Edition returns `0` on empty numeric sets and accumulates integers
                    //   in `i128` (promoting to `DoubleValue` only if the final sum overflows
                    //   `i64`).
                    //
                    // * Enterprise Edition returns `NULL` on empty numeric sets and accumulates
                    //   sequentially in `i64` before any `DoubleValue`, failing with
                    //   `InvalidArgument` (`"Long overflow"`) on intermediate or final `i64`
                    //   overflow.
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
                        if enterprise && values.peek().is_none() {
                            Value::null()
                        } else {
                            sum_aggregation_values(enterprise, values)?
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
                    // * Always returns the result as a double (Enterprise Edition applies the same
                    //   sequential `Long overflow` check to its numerator sum).
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
                            let sum = sum_aggregation_values(enterprise, values)?;
                            Value::double(sum.as_double().unwrap_or(0.0) / len as f64)
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
        if show_missing && !order_by.trim().is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "cannot specify an order when show_missing is true",
            ));
        }
        if self.enterprise_edition() && !order_by.trim().is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "order by is not supported",
            ));
        }
        unimplemented_bool!(show_missing && collection_id.is_empty());

        let parsed_orders = parse_list_order_by(&order_by)?;
        let effective_orders = if parsed_orders.is_empty() {
            vec![(FieldReference::DocumentName, Direction::Ascending)]
        } else {
            let last_dir = parsed_orders
                .last()
                .map_or(Direction::Ascending, |(_, d)| *d);
            let has_doc_name = parsed_orders.iter().any(|(f, _)| f.is_document_name());
            let mut orders = parsed_orders;
            if !has_doc_name {
                orders.push((FieldReference::DocumentName, last_dir));
            }
            orders
        };

        let expected_has_mask = mask.is_some();
        let expected_mask_paths: Vec<String> = mask
            .as_ref()
            .map_or_else(Vec::new, |m| m.field_paths.clone());

        let parent_str = parent.to_string();
        let token_binding = ListDocsTokenBinding {
            parent: &parent_str,
            collection_id: &collection_id,
            show_missing,
            raw_order_by: &order_by,
            has_mask: expected_has_mask,
            mask_field_paths: &expected_mask_paths,
        };
        let decoded_cursor = if page_token.is_empty() {
            None
        } else {
            Some(decode_list_docs_page_token(
                &page_token,
                &token_binding,
                effective_orders.len(),
                parent.root(),
            )?)
        };

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

        for (field, dir) in effective_orders {
            builder = builder.order_by(field, dir);
        }

        if !show_missing {
            if let Some(cursor) = decoded_cursor {
                builder = builder.start_at(cursor);
            }
            if page_size > 0 {
                builder = builder.limit((page_size as usize).saturating_add(1));
            }
            let mut query = builder.build()?;
            let (_, mut found_docs) = query.once(self).await?;
            let next_page_token = if page_size > 0 && found_docs.len() > page_size as usize {
                found_docs.truncate(page_size as usize);
                let last = found_docs.last().ok_or_else(|| {
                    GenericDatabaseError::internal("missing boundary document for list_docs cursor")
                })?;
                let mut values = Vec::with_capacity(query.order_by().len());
                for order in query.order_by() {
                    let val = order.field().get_value(&last.version).ok_or_else(|| {
                        GenericDatabaseError::internal(format!(
                            "missing order_by field '{}' on matched document {}",
                            order.field(),
                            last.version.name
                        ))
                    })?;
                    values.push(val.into_owned());
                }
                encode_list_docs_page_token(
                    &token_binding,
                    Cursor {
                        values,
                        before: false,
                    },
                )
            } else {
                String::new()
            };
            let documents = found_docs.iter().map(|v| query.project(v)).collect();
            return Ok((documents, next_page_token));
        }

        let cursor_name = decoded_cursor
            .as_ref()
            .map(|cursor| {
                cursor
                    .values
                    .first()
                    .and_then(|v| v.as_reference())
                    .ok_or_else(|| {
                        GenericDatabaseError::internal("validated cursor missing reference")
                    })
            })
            .transpose()?;

        let mut query = builder.build()?;
        let (_, found_docs) = query.once(self).await?;
        let mut documents: Vec<Document> = found_docs
            .iter()
            .filter(|v| {
                cursor_name.is_none_or(|cursor| {
                    googleapis::cmp_reference_paths(&v.version.name.to_string(), cursor).is_gt()
                })
            })
            .map(|v| query.project(v))
            .collect();

        let existing_names: HashSet<String> = documents.iter().map(|d| d.name.clone()).collect();
        for missing_name in query.missing_docs_after(self, cursor_name).await? {
            if !existing_names.contains(&missing_name) {
                documents.push(Document {
                    name: missing_name,
                    ..Default::default()
                });
            }
        }
        documents.sort_by(|a, b| googleapis::cmp_reference_paths(&a.name, &b.name));

        let next_page_token = if page_size > 0 && documents.len() > page_size as usize {
            documents.truncate(page_size as usize);
            let last_name = documents
                .last()
                .ok_or_else(|| {
                    GenericDatabaseError::internal(
                        "missing boundary document for show_missing list_docs cursor",
                    )
                })?
                .name
                .clone();
            encode_list_docs_page_token(
                &token_binding,
                Cursor {
                    values: vec![Value::reference(last_name)],
                    before: false,
                },
            )
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

pub(crate) fn sum_enterprise_checked(values: impl IntoIterator<Item = Value>) -> Result<Value> {
    Value::try_checked_sum(values)
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Long overflow"))
}

fn sum_aggregation_values(
    enterprise: bool,
    values: impl IntoIterator<Item = Value>,
) -> Result<Value> {
    if enterprise {
        sum_enterprise_checked(values)
    } else {
        Ok(values.into_iter().sum())
    }
}

const LIST_DOCS_TOKEN_VERSION: u32 = 1;
const MAX_PAGE_TOKEN_LEN: usize = 2 * 1024 * 1024;

struct ListDocsTokenBinding<'a> {
    parent: &'a str,
    collection_id: &'a str,
    show_missing: bool,
    raw_order_by: &'a str,
    has_mask: bool,
    mask_field_paths: &'a [String],
}

#[derive(Clone, PartialEq, prost::Message)]
struct ListDocsPageToken {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    parent: String,
    #[prost(string, tag = "3")]
    collection_id: String,
    #[prost(bool, tag = "4")]
    show_missing: bool,
    #[prost(message, optional, tag = "6")]
    cursor: Option<Cursor>,
    #[prost(string, tag = "7")]
    raw_order_by: String,
    #[prost(bool, tag = "8")]
    has_mask: bool,
    #[prost(string, repeated, tag = "9")]
    mask_field_paths: Vec<String>,
}

fn encode_list_docs_page_token(binding: &ListDocsTokenBinding<'_>, cursor: Cursor) -> String {
    let token = ListDocsPageToken {
        version: LIST_DOCS_TOKEN_VERSION,
        parent: binding.parent.to_owned(),
        collection_id: binding.collection_id.to_owned(),
        show_missing: binding.show_missing,
        cursor: Some(cursor),
        raw_order_by: binding.raw_order_by.to_owned(),
        has_mask: binding.has_mask,
        mask_field_paths: binding.mask_field_paths.to_vec(),
    };
    URL_SAFE_NO_PAD.encode(token.encode_to_vec())
}

fn decode_list_docs_page_token(
    page_token: &str,
    binding: &ListDocsTokenBinding<'_>,
    expected_cursor_len: usize,
    expected_root: &RootRef,
) -> Result<Cursor> {
    if page_token.len() > MAX_PAGE_TOKEN_LEN {
        return Err(GenericDatabaseError::invalid_argument(
            "page_token exceeds maximum allowed length",
        ));
    }
    let raw = URL_SAFE_NO_PAD
        .decode(page_token)
        .map_err(|_| GenericDatabaseError::invalid_argument("invalid page_token"))?;
    let token = ListDocsPageToken::decode(raw.as_slice())
        .map_err(|_| GenericDatabaseError::invalid_argument("invalid page_token"))?;

    if token.version != LIST_DOCS_TOKEN_VERSION
        || token.parent != binding.parent
        || token.collection_id != binding.collection_id
        || token.show_missing != binding.show_missing
        || token.raw_order_by != binding.raw_order_by
        || token.has_mask != binding.has_mask
        || token.mask_field_paths.as_slice() != binding.mask_field_paths
    {
        return Err(GenericDatabaseError::invalid_argument(
            "invalid or mismatched page_token",
        ));
    }

    let cursor = token
        .cursor
        .ok_or_else(|| GenericDatabaseError::invalid_argument("invalid page_token"))?;
    if cursor.before || cursor.values.len() != expected_cursor_len {
        return Err(GenericDatabaseError::invalid_argument("invalid page_token"));
    }
    if binding.show_missing {
        if cursor.values.len() != 1
            || !matches!(
                cursor.values[0].value_type,
                Some(value::ValueType::ReferenceValue(_))
            )
        {
            return Err(GenericDatabaseError::invalid_argument("invalid page_token"));
        }
        query::validate_cursor_key_ref(&cursor.values[0], expected_root)?;
    }
    Ok(cursor)
}

struct BacktickScanner {
    in_backticks: bool,
    escaped: bool,
    at_segment_start: bool,
    saw_non_ws_in_segment: bool,
}

impl BacktickScanner {
    fn new() -> Self {
        Self {
            in_backticks: false,
            escaped: false,
            at_segment_start: true,
            saw_non_ws_in_segment: false,
        }
    }

    fn reset_segment(&mut self) {
        self.at_segment_start = true;
        self.saw_non_ws_in_segment = false;
    }

    /// Advances over `ch` and returns `true` when `ch` is outside backtick-quoted segments.
    fn step(&mut self, ch: char) -> bool {
        if self.in_backticks {
            if self.escaped {
                self.escaped = false;
            } else if ch == '\\' {
                self.escaped = true;
            } else if ch == '`' {
                self.in_backticks = false;
            }
            return false;
        }
        match ch {
            '`' if self.at_segment_start => {
                self.in_backticks = true;
                self.at_segment_start = false;
                self.saw_non_ws_in_segment = true;
                false
            }
            '.' => {
                self.reset_segment();
                true
            }
            _ if ch.is_whitespace() => {
                if self.saw_non_ws_in_segment {
                    self.at_segment_start = false;
                }
                true
            }
            _ => {
                self.at_segment_start = false;
                self.saw_non_ws_in_segment = true;
                true
            }
        }
    }
}

fn split_order_by_clauses(input: &str) -> Vec<&str> {
    let mut clauses = Vec::new();
    let mut start = 0;
    let mut scanner = BacktickScanner::new();

    for (idx, ch) in input.char_indices() {
        if scanner.step(ch) && ch == ',' {
            clauses.push(&input[start..idx]);
            start = idx + 1;
            scanner.reset_segment();
        }
    }
    clauses.push(&input[start..]);
    clauses
}

fn split_clause_field_and_dir(clause: &str) -> Option<(&str, &str)> {
    let mut scanner = BacktickScanner::new();
    for (idx, ch) in clause.char_indices() {
        if scanner.step(ch) && ch.is_whitespace() {
            return Some((clause[..idx].trim_end(), clause[idx..].trim_start()));
        }
    }
    None
}

fn parse_list_order_by(order_by: &str) -> Result<Vec<(FieldReference, Direction)>> {
    let trimmed = order_by.trim();
    if trimmed.is_empty() {
        return Ok(vec![]);
    }
    let mut orders = Vec::new();
    for raw_clause in split_order_by_clauses(trimmed) {
        let clause = raw_clause.trim();
        if clause.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "empty clause in order_by",
            ));
        }
        let (field_str, direction) = match split_clause_field_and_dir(clause) {
            Some((f, dir))
                if dir.eq_ignore_ascii_case("asc") || dir.eq_ignore_ascii_case("ascending") =>
            {
                (f, Direction::Ascending)
            }
            Some((f, dir))
                if dir.eq_ignore_ascii_case("desc") || dir.eq_ignore_ascii_case("descending") =>
            {
                (f, Direction::Descending)
            }
            Some((_, dir)) => {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "invalid order_by direction {dir:?} in clause {clause:?}"
                )));
            }
            None => (clause, Direction::Ascending),
        };
        orders.push((field_str.parse()?, direction));
    }
    Ok(orders)
}

impl Drop for FirestoreDatabase {
    fn drop(&mut self) {
        self.locks.abort_all(AbortReason::DatabaseDropped);
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

    #[test]
    fn parse_list_order_by_handles_quoted_commas_whitespace_and_rejects_invalid_directions() {
        assert_eq!(parse_list_order_by("   ").unwrap().len(), 0);

        let parsed = parse_list_order_by(r"`a,b` desc, x.`y, z` asc, `plain desc`, tail").unwrap();
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[0].0.to_string(), "`a,b`");
        assert!(matches!(parsed[0].1, Direction::Descending));
        assert_eq!(parsed[1].0.to_string(), "x.`y, z`");
        assert!(matches!(parsed[1].1, Direction::Ascending));
        assert_eq!(parsed[2].0.to_string(), "`plain desc`");
        assert!(matches!(parsed[2].1, Direction::Ascending));
        assert_eq!(parsed[3].0.to_string(), "tail");
        assert!(matches!(parsed[3].1, Direction::Ascending));

        let escaped = parse_list_order_by(r"`a\`,b` descending").unwrap();
        assert_eq!(escaped.len(), 1);
        assert_eq!(escaped[0].0.to_string(), r"`a\`,b`");
        assert!(matches!(escaped[0].1, Direction::Descending));

        for bad in [
            "rank sideways",
            "rank asc desc",
            "foo bar",
            "rank asc, ",
            ", rank",
            "a,,b",
            "`unclosed, comma desc",
            "`unclosed",
        ] {
            assert!(
                matches!(
                    parse_list_order_by(bad),
                    Err(GenericDatabaseError::InvalidArgument(_))
                ),
                "expected InvalidArgument for order_by {bad:?}"
            );
        }
    }

    #[tokio::test]
    async fn list_docs_page_token_validation_all_descendants_and_page_size_zero() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        for path in [
            "projects/p/databases/(default)/documents/c1/d1",
            "projects/p/databases/(default)/documents/c1/d2",
            "projects/p/databases/(default)/documents/c2/d3",
        ] {
            let doc_ref: DocumentRef = path.parse().unwrap();
            db.update_doc(
                &doc_ref,
                HashMap::from([("v".to_string(), Value::integer(1))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }

        // Oversized raw token (> 2 MiB) is rejected before decoding.
        let huge = "A".repeat(MAX_PAGE_TOKEN_LEN + 1);
        assert!(matches!(
            db.list_docs(
                Ref::Root(root.clone()),
                "c1".to_string(),
                2,
                huge,
                String::new(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Tampered prost payloads: wrong version, missing cursor, before=true, wrong cursor len,
        // and non-reference / cross-db show_missing cursor.
        let forged_tokens = [
            ListDocsPageToken {
                version: 2,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: false,
                cursor: Some(Cursor {
                    values: vec![Value::reference(
                        "projects/p/databases/(default)/documents/c1/d1".to_string(),
                    )],
                    before: false,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
            ListDocsPageToken {
                version: 1,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: false,
                cursor: None,
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
            ListDocsPageToken {
                version: 1,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: false,
                cursor: Some(Cursor {
                    values: vec![Value::reference(
                        "projects/p/databases/(default)/documents/c1/d1".to_string(),
                    )],
                    before: true,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
            ListDocsPageToken {
                version: 1,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: false,
                cursor: Some(Cursor {
                    values: vec![],
                    before: false,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
            ListDocsPageToken {
                version: 1,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: true,
                cursor: Some(Cursor {
                    values: vec![Value::string("not-a-ref")],
                    before: false,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
            ListDocsPageToken {
                version: 1,
                parent: root.to_string(),
                collection_id: "c1".to_string(),
                show_missing: true,
                cursor: Some(Cursor {
                    values: vec![Value::reference(
                        "projects/p/databases/other/documents/c1/d1".to_string(),
                    )],
                    before: false,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            },
        ];
        for tok in forged_tokens {
            let show_missing = tok.show_missing;
            let encoded = URL_SAFE_NO_PAD.encode(tok.encode_to_vec());
            assert!(
                matches!(
                    db.list_docs(
                        Ref::Root(root.clone()),
                        "c1".to_string(),
                        2,
                        encoded,
                        String::new(),
                        None,
                        show_missing,
                        ReadConsistency::Default,
                    )
                    .await,
                    Err(GenericDatabaseError::InvalidArgument(_))
                ),
                "expected InvalidArgument for forged token (show_missing={show_missing})"
            );
        }

        // Standard base64 (non-URL-safe / padded) is rejected.
        assert!(matches!(
            db.list_docs(
                Ref::Root(root.clone()),
                "c1".to_string(),
                2,
                "AAAA====".to_string(),
                String::new(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Normalized parent ("projects/p/databases/(default)" vs ".../documents"),
        // `collection_id == ""` (all_descendants), and `page_size == 0` with `page_token`.
        let root_no_docs: Ref = "projects/p/databases/(default)".parse().unwrap();
        let root_with_docs: Ref = "projects/p/databases/(default)/documents".parse().unwrap();
        let (p1, tok1) = db
            .list_docs(
                root_no_docs,
                String::new(),
                1,
                String::new(),
                String::new(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(p1.len(), 1);
        assert_eq!(p1[0].name, "projects/p/databases/(default)/documents/c1/d1");
        assert!(!tok1.is_empty());

        let (rest, tok2) = db
            .list_docs(
                root_with_docs,
                String::new(),
                0,
                tok1,
                String::new(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(
            rest.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
            vec![
                "projects/p/databases/(default)/documents/c1/d2",
                "projects/p/databases/(default)/documents/c2/d3",
            ]
        );
        assert_eq!(tok2, "");
    }

    #[tokio::test]
    async fn list_docs_multi_clause_cursor_and_show_missing_skips_prior_ancestors() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        for (id, rank, other) in [
            ("m1", 1i64, 20i64),
            ("m2", 1, 20),
            ("m3", 1, 20),
            ("m4", 1, 10),
            ("m5", 2, 30),
            ("m6", 2, 30),
        ] {
            let doc_ref: DocumentRef =
                format!("projects/p/databases/(default)/documents/multi/{id}")
                    .parse()
                    .unwrap();
            db.update_doc(
                &doc_ref,
                HashMap::from([
                    ("rank".to_string(), Value::integer(rank)),
                    ("other".to_string(), Value::integer(other)),
                ]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }

        // Verify `Query::order_by()` / `Order::field()` / `Order::direction()` with implicit
        // `__name__` inheriting the last explicit `DESC` direction.
        let built = QueryBuilder::from(
            Ref::Root(root.clone()),
            vec![CollectionSelector {
                collection_id:   "multi".to_string(),
                all_descendants: false,
            }],
        )
        .order_by("rank".parse().unwrap(), Direction::Ascending)
        .order_by("other".parse().unwrap(), Direction::Descending)
        .build()
        .unwrap();
        assert_eq!(built.order_by().len(), 3);
        assert_eq!(built.order_by()[0].field().to_string(), "rank");
        assert!(matches!(
            built.order_by()[0].direction(),
            Direction::Ascending
        ));
        assert_eq!(built.order_by()[1].field().to_string(), "other");
        assert!(matches!(
            built.order_by()[1].direction(),
            Direction::Descending
        ));
        assert!(built.order_by()[2].field().is_document_name());
        assert!(matches!(
            built.order_by()[2].direction(),
            Direction::Descending
        ));

        let (p1, tok1) = db
            .list_docs(
                Ref::Root(root.clone()),
                "multi".to_string(),
                2,
                String::new(),
                "rank asc, other desc".to_string(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(
            p1.iter()
                .map(|d| d.name.rsplit('/').next().unwrap())
                .collect::<Vec<_>>(),
            vec!["m3", "m2"]
        );
        let decoded =
            ListDocsPageToken::decode(URL_SAFE_NO_PAD.decode(&tok1).unwrap().as_slice()).unwrap();
        assert_eq!(decoded.raw_order_by, "rank asc, other desc");
        assert_eq!(decoded.cursor.as_ref().unwrap().values.len(), 3);

        // Wrong cursor length for a 3-element effective order must be rejected.
        let short_cursor_tok = URL_SAFE_NO_PAD.encode(
            ListDocsPageToken {
                version: LIST_DOCS_TOKEN_VERSION,
                parent: root.to_string(),
                collection_id: "multi".to_string(),
                show_missing: false,
                cursor: Some(Cursor {
                    values: vec![Value::integer(1), Value::integer(20)],
                    before: false,
                }),
                raw_order_by: "rank asc, other desc".to_string(),
                has_mask: false,
                mask_field_paths: vec![],
            }
            .encode_to_vec(),
        );
        assert!(matches!(
            db.list_docs(
                Ref::Root(root.clone()),
                "multi".to_string(),
                2,
                short_cursor_tok,
                "rank asc, other desc".to_string(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        let (p2, tok2) = db
            .list_docs(
                Ref::Root(root.clone()),
                "multi".to_string(),
                2,
                tok1,
                "rank asc, other desc".to_string(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(
            p2.iter()
                .map(|d| d.name.rsplit('/').next().unwrap())
                .collect::<Vec<_>>(),
            vec!["m1", "m4"]
        );
        let (p3, tok3) = db
            .list_docs(
                Ref::Root(root.clone()),
                "multi".to_string(),
                2,
                tok2,
                "rank asc, other desc".to_string(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(
            p3.iter()
                .map(|d| d.name.rsplit('/').next().unwrap())
                .collect::<Vec<_>>(),
            vec!["m6", "m5"]
        );
        assert_eq!(tok3, "");

        // `show_missing = true` with a cursor must skip prior phantom ancestors before `get_doc`
        // (leaving `peek_doc_meta == None`) and prior real docs before `query.project`.
        let d0_ref: DocumentRef = "projects/p/databases/(default)/documents/miss_skip/d0"
            .parse()
            .unwrap();
        let d1_ref: DocumentRef = "projects/p/databases/(default)/documents/miss_skip/d1"
            .parse()
            .unwrap();
        let d1_sub_ref: DocumentRef = "projects/p/databases/(default)/documents/miss_skip/d1/sub/\
                                       s1"
        .parse()
        .unwrap();
        let d2_ref: DocumentRef = "projects/p/databases/(default)/documents/miss_skip/d2"
            .parse()
            .unwrap();
        let d3_sub_ref: DocumentRef = "projects/p/databases/(default)/documents/miss_skip/d3/sub/\
                                       s1"
        .parse()
        .unwrap();
        for doc_ref in [&d0_ref, &d1_sub_ref, &d2_ref, &d3_sub_ref] {
            db.update_doc(
                doc_ref,
                HashMap::from([("v".to_string(), Value::integer(1))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
        assert!(db.peek_doc_meta(&d1_ref).await.unwrap().is_none());

        let tok_after_d1 = URL_SAFE_NO_PAD.encode(
            ListDocsPageToken {
                version: LIST_DOCS_TOKEN_VERSION,
                parent: root.to_string(),
                collection_id: "miss_skip".to_string(),
                show_missing: true,
                cursor: Some(Cursor {
                    values: vec![Value::reference(d1_ref.to_string())],
                    before: false,
                }),
                raw_order_by: String::new(),
                has_mask: false,
                mask_field_paths: vec![],
            }
            .encode_to_vec(),
        );
        let (miss_after_d1, miss_tok) = db
            .list_docs(
                Ref::Root(root),
                "miss_skip".to_string(),
                10,
                tok_after_d1,
                String::new(),
                None,
                true,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(
            miss_after_d1
                .iter()
                .map(|d| d.name.rsplit('/').next().unwrap())
                .collect::<Vec<_>>(),
            vec!["d2", "d3"]
        );
        assert_eq!(miss_tok, "");
        assert!(
            db.peek_doc_meta(&d1_ref).await.unwrap().is_none(),
            "missing_docs_after must not invoke get_doc on skipped ancestor <= cursor"
        );
    }
}
