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

pub(crate) fn ensure_owns_root(expected: &RootRef, actual: &RootRef) -> Result<()> {
    if actual != expected {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "resource root {actual} does not match database {expected}"
        )));
    }
    Ok(())
}

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
        ensure_owns_root(&self.name, root_ref)
    }

    pub(crate) fn ensure_owns_doc(&self, doc_ref: &DocumentRef) -> Result<()> {
        self.ensure_owns_root(&doc_ref.collection_ref.root_ref)
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG), fields(txn_id, found))]
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

    /// Lists direct child collection IDs underneath a root or document parent in lexicographical
    /// order, applying optional `page_size` and opaque `page_token` pagination.
    #[instrument(level = Level::DEBUG, skip_all, err)]
    pub async fn list_collection_ids(
        &self,
        parent: &Ref,
        page_size: i32,
        page_token: &str,
    ) -> Result<(Vec<String>, String)> {
        self.ensure_owns_root(parent.root())?;
        if self.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
        }
        if matches!(parent, Ref::Collection(_)) {
            let parent_str = parent.to_string();
            return Err(GenericDatabaseError::invalid_argument(format!(
                "Document parent name \"{parent_str}\" lacks \"/\" at index {}.",
                parent_str.len()
            )));
        }
        if page_size < 0 {
            return Err(GenericDatabaseError::invalid_argument(
                "page_size must be greater than or equal to zero.",
            ));
        }

        let parent_str = parent.to_string();
        let after = if page_token.is_empty() {
            None
        } else {
            Some(decode_list_collection_ids_page_token(
                page_token,
                &parent_str,
            )?)
        };

        let all_ids = self.get_collection_ids(parent).await?;
        let iter = match after.as_deref() {
            Some(after_id) => all_ids.range::<str, _>((
                std::ops::Bound::Excluded(after_id),
                std::ops::Bound::Unbounded,
            )),
            None => all_ids.range::<str, _>(..),
        };

        let mut collection_ids: Vec<String> = if page_size > 0 {
            iter.take((page_size as usize).saturating_add(1))
                .cloned()
                .collect()
        } else {
            iter.cloned().collect()
        };

        let next_page_token = if page_size > 0 && collection_ids.len() > page_size as usize {
            collection_ids.truncate(page_size as usize);
            let boundary = collection_ids.last().cloned().ok_or_else(|| {
                GenericDatabaseError::internal(
                    "missing boundary collection ID for list_collection_ids cursor",
                )
            })?;
            encode_list_collection_ids_page_token(parent_str, boundary)
        } else {
            String::new()
        };

        Ok((collection_ids, next_page_token))
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

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
    pub async fn run_query(&self, query: &mut Query) -> Result<(Timestamp, Vec<Document>)> {
        debug!(?query);
        let (read_time, result) = query.once(self).await?;
        debug!(result_count = result.len());
        Ok((
            read_time,
            result.into_iter().map(|m| query.project(&m)).collect(),
        ))
    }

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
    pub async fn create_doc(
        self: &Arc<Self>,
        parent: &CollectionRef,
        document_id: &str,
        fields: HashMap<String, Value>,
        mask: Option<DocumentMask>,
    ) -> Result<Document> {
        reference::validate_document_id_segment(document_id)?;
        let doc_ref = DocumentRef::new(parent.clone(), document_id);
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

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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

    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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
    #[instrument(level = Level::DEBUG, skip_all, err(level = Level::DEBUG))]
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
        self.update_config(Some(enterprise_edition), None);
    }

    pub fn optimistic_concurrency(&self) -> bool {
        self.optimistic_concurrency.load(atomic::Ordering::Relaxed)
    }

    pub fn set_optimistic_concurrency(&self, optimistic_concurrency: bool) {
        self.update_config(None, Some(optimistic_concurrency));
    }

    pub fn set_config(&self, enterprise_edition: bool, optimistic_concurrency: bool) {
        self.update_config(Some(enterprise_edition), Some(optimistic_concurrency));
    }

    fn update_config(&self, enterprise: Option<bool>, optimistic: Option<bool>) {
        let events = self
            .events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let enterprise_changed = enterprise
            .is_some_and(|val| self.enterprise_edition.swap(val, atomic::Ordering::Relaxed) != val);
        let optimistic_changed = optimistic.is_some_and(|val| {
            self.optimistic_concurrency
                .swap(val, atomic::Ordering::Relaxed)
                != val
        });
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

const LIST_COLLECTION_IDS_TOKEN_VERSION: u32 = 1;
const LIST_COLLECTION_IDS_TOKEN_KIND: &str = "list_collection_ids";

#[derive(Clone, PartialEq, prost::Message)]
struct ListCollectionIdsPageToken {
    #[prost(uint32, tag = "1")]
    version: u32,
    #[prost(string, tag = "2")]
    parent: String,
    #[prost(string, tag = "3")]
    last_collection_id: String,
    #[prost(string, tag = "10")]
    kind: String,
}

fn encode_list_collection_ids_page_token(parent: String, last_collection_id: String) -> String {
    let token = ListCollectionIdsPageToken {
        version: LIST_COLLECTION_IDS_TOKEN_VERSION,
        parent,
        last_collection_id,
        kind: LIST_COLLECTION_IDS_TOKEN_KIND.to_string(),
    };
    URL_SAFE_NO_PAD.encode(token.encode_to_vec())
}

fn decode_list_collection_ids_page_token(page_token: &str, parent_str: &str) -> Result<String> {
    if page_token.len() > MAX_PAGE_TOKEN_LEN {
        return Err(GenericDatabaseError::invalid_argument("invalid page token"));
    }
    let raw = URL_SAFE_NO_PAD
        .decode(page_token)
        .map_err(|_| GenericDatabaseError::invalid_argument("invalid page token"))?;
    let token = ListCollectionIdsPageToken::decode(raw.as_slice())
        .map_err(|_| GenericDatabaseError::invalid_argument("invalid page token"))?;
    if token.version != LIST_COLLECTION_IDS_TOKEN_VERSION
        || token.kind != LIST_COLLECTION_IDS_TOKEN_KIND
        || token.parent != parent_str
        || token.last_collection_id.is_empty()
        || token.encode_to_vec() != raw
    {
        return Err(GenericDatabaseError::invalid_argument("invalid page token"));
    }
    Ok(token.last_collection_id)
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
        if !self.is_cleared() {
            self.locks.abort_all(AbortReason::DatabaseDropped);
        }
        self.transactions.clear_all();
        debug!("Database \"{}\" dropped", self.name);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::firestore::v1::{Document, TransactionOptions, Value, Write, write};

    use super::*;
    use crate::{FirestoreConfig, FirestoreProject, reference::CollectionRef};

    #[tokio::test]
    async fn create_doc_validates_document_id_edge_cases() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let col: CollectionRef = "projects/p/databases/(default)/documents/users"
            .parse()
            .unwrap();

        let over_1500_ascii = "a".repeat(1501);
        // 751 2-byte UTF-8 chars = 1502 bytes (exceeds 1500-byte limit despite <1500 chars).
        let over_1500_utf8 = "é".repeat(751);
        for bad_id in [
            "",
            "a/b",
            "a/b/c",
            ".",
            "..",
            over_1500_ascii.as_str(),
            over_1500_utf8.as_str(),
            "__a__",
            "__name__",
            "__id1__",
            "__reserved__",
        ] {
            let err = db
                .create_doc(
                    &col,
                    bad_id,
                    HashMap::from([("v".to_string(), Value::integer(1))]),
                    None,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err.grpc_code(),
                tonic::Code::InvalidArgument,
                "expected InvalidArgument for document_id={bad_id:?}, got {err:?}"
            );
        }

        // Valid boundary cases: exact 1500 bytes, "...", 4 underscores ("____"), and partial
        // double-underscore prefixes/suffixes.
        let exact_1500 = "b".repeat(1500);
        for good_id in [
            "alice",
            "...",
            "____",
            "__leading",
            "trailing__",
            &exact_1500,
        ] {
            let doc = db
                .create_doc(
                    &col,
                    good_id,
                    HashMap::from([("v".to_string(), Value::integer(1))]),
                    None,
                )
                .await
                .unwrap_or_else(|e| panic!("expected valid document_id={good_id:?}, got {e:?}"));
            assert_eq!(doc.name, format!("{col}/{good_id}"));
        }
    }

    #[tokio::test]
    async fn update_config_emits_single_event_only_on_actual_change_when_active() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;
        let mut rx = project.subscribe_project_events();

        // No-op config updates must not emit DatabaseConfigChanged.
        db.set_enterprise_edition(false);
        db.set_optimistic_concurrency(false);
        db.set_config(false, false);
        assert!(rx.try_recv().is_err());

        // Single-flag updates emit one event each.
        db.set_enterprise_edition(true);
        assert!(db.enterprise_edition());
        assert!(matches!(
            rx.try_recv(),
            Ok(ProjectEvent::DatabaseConfigChanged { database }) if database == root
        ));
        assert!(rx.try_recv().is_err());

        db.set_optimistic_concurrency(true);
        assert!(db.optimistic_concurrency());
        assert!(matches!(
            rx.try_recv(),
            Ok(ProjectEvent::DatabaseConfigChanged { database }) if database == root
        ));
        assert!(rx.try_recv().is_err());

        // Changing both flags via set_config emits a single DatabaseConfigChanged event.
        db.set_config(false, false);
        assert!(!db.enterprise_edition());
        assert!(!db.optimistic_concurrency());
        assert!(matches!(
            rx.try_recv(),
            Ok(ProjectEvent::DatabaseConfigChanged { database }) if database == root
        ));
        assert!(rx.try_recv().is_err());

        // Once cleared, config updates must not emit stale DatabaseConfigChanged events.
        db.mark_cleared();
        db.set_config(true, true);
        assert!(rx.try_recv().is_err());
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

    #[tokio::test(start_paused = true)]
    async fn contention_commit_records_consistent_write_timeout_on_retried_write_batch() {
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

        let mk_write = |doc_ref: &DocumentRef, val: i64| Write {
            operation: Some(write::Operation::Update(Document {
                name: doc_ref.to_string(),
                fields: HashMap::from([("v".to_string(), Value::integer(val))]),
                ..Default::default()
            })),
            ..Default::default()
        };

        // Case A: Retried WriteBatch (wound_retries == 1) times out while parked inside
        // `acquire_writes_all_since` -> records cumulative `batch_start.elapsed()`,
        // `attempt: Some(1)`, and `doc_name: Some(d2)`.
        let older_a = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        db.get_doc(&d_seq, ReadConsistency::Transaction(older_a))
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

        let mut batch_parked = Box::pin(db.commit(vec![mk_write(&d1, 1), mk_write(&d2, 1)], None));
        assert!(poll!(&mut batch_parked).is_pending());

        // Advance 500ms, then wound attempt 0 -> retry #1 parks inside acquire_writes_all at
        // t=500ms.
        advance(Duration::from_millis(500)).await;
        db.get_doc(&d1, ReadConsistency::Transaction(older_a))
            .await
            .unwrap();
        assert!(poll!(&mut batch_parked).is_pending());
        db.commit(vec![], Some(older_a)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(poll!(&mut batch_parked).is_pending());
        assert_eq!(
            db.locks
                .contention_snapshot()
                .summary
                .write_batch_retries_total,
            1
        );

        // Advance the remaining 1500ms to deadline (t=2000ms) while parked on d2.
        advance(Duration::from_millis(1500)).await;
        let err = batch_parked.await.unwrap_err();
        assert!(matches!(err, GenericDatabaseError::Aborted(_)));

        drop(held_d2);
        blocker.close();

        let snap_parked = db.locks.contention_snapshot();
        assert_eq!(snap_parked.summary.write_batch_retries_total, 1);
        assert_eq!(snap_parked.summary.timeouts_total, 1);
        let parked_to = &snap_parked.recent_events[0];
        assert_eq!(parked_to.kind, ContentionEventKind::WriteTimeout);
        assert_eq!(parked_to.session_kind, SessionKind::WriteBatch);
        assert_eq!(parked_to.wait_ms, Some(2_000));
        assert_eq!(parked_to.attempt, Some(1));
        assert_eq!(parked_to.doc_name.as_deref(), Some(d2.to_string().as_str()));

        // Case B: Unretried WriteBatch (wound_retries == 0) timing out inside acquire_writes_all
        // keeps `attempt: None` while recording `wait_ms` and `doc_name`.
        db.locks.clear_contention_history();
        let blocker_b = db.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let held_d2_b = blocker_b.acquire_write(&d2).await.unwrap();
        let mut batch_initial = Box::pin(db.commit(vec![mk_write(&d1, 2), mk_write(&d2, 2)], None));
        assert!(poll!(&mut batch_initial).is_pending());

        advance(Duration::from_millis(2000)).await;
        let err_b = batch_initial.await.unwrap_err();
        assert!(matches!(err_b, GenericDatabaseError::Aborted(_)));
        drop(held_d2_b);
        blocker_b.close();

        let snap_initial = db.locks.contention_snapshot();
        assert_eq!(snap_initial.summary.write_batch_retries_total, 0);
        assert_eq!(snap_initial.summary.timeouts_total, 1);
        let initial_to = &snap_initial.recent_events[0];
        assert_eq!(initial_to.kind, ContentionEventKind::WriteTimeout);
        assert_eq!(initial_to.session_kind, SessionKind::WriteBatch);
        assert_eq!(initial_to.wait_ms, Some(2_000));
        assert_eq!(initial_to.attempt, None);
        assert_eq!(
            initial_to.doc_name.as_deref(),
            Some(d2.to_string().as_str())
        );
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

    #[tokio::test]
    async fn list_collection_ids_pagination_and_token_validation() {
        let project = Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let parent_doc: Ref = "projects/p/databases/(default)/documents/parents/doc1"
            .parse()
            .unwrap();
        let other_doc: Ref = "projects/p/databases/(default)/documents/parents/doc2"
            .parse()
            .unwrap();

        // Malformed / raw-string page_token rejected even when parent has no subcollections.
        for bad in ["not-a-valid-token", "c2", "AAAA===="] {
            assert!(matches!(
                db.list_collection_ids(&parent_doc, 2, bad).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }

        // Nonexistent doc parent returns empty list and empty next_page_token.
        assert_eq!(
            db.list_collection_ids(&parent_doc, 0, "").await.unwrap(),
            (vec![], String::new())
        );

        // Seed 5 subcollections out of lexicographic order.
        for col_name in ["c3", "c1", "c5", "c2", "c4"] {
            let doc_ref: DocumentRef =
                format!("projects/p/databases/(default)/documents/parents/doc1/{col_name}/d")
                    .parse()
                    .unwrap();
            db.update_doc(
                &doc_ref,
                HashMap::from([("n".to_string(), Value::string(col_name))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }
        let other_sub: DocumentRef = "projects/p/databases/(default)/documents/parents/doc2/c1/d"
            .parse()
            .unwrap();
        db.update_doc(
            &other_sub,
            HashMap::from([("n".to_string(), Value::string("other"))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        // (a) page_size = 2 across 3 pages returns lexicographic IDs and empty final token.
        let (p1, tok1) = db.list_collection_ids(&parent_doc, 2, "").await.unwrap();
        assert_eq!(p1, vec!["c1", "c2"]);
        assert!(!tok1.is_empty());

        let (p2, tok2) = db.list_collection_ids(&parent_doc, 2, &tok1).await.unwrap();
        assert_eq!(p2, vec!["c3", "c4"]);
        assert!(!tok2.is_empty());

        let (p3, tok3) = db.list_collection_ids(&parent_doc, 2, &tok2).await.unwrap();
        assert_eq!(p3, vec!["c5"]);
        assert_eq!(tok3, "");

        // Changing page_size on follow-up page (including page_size = 0) works cleanly.
        let (rest, rest_tok) = db.list_collection_ids(&parent_doc, 0, &tok1).await.unwrap();
        assert_eq!(rest, vec!["c3", "c4", "c5"]);
        assert_eq!(rest_tok, "");

        // (b) Exact fit page_size == total, page_size > total, and page_size == 0 return empty
        // token.
        for size in [5, 6, 0] {
            let (all, next) = db.list_collection_ids(&parent_doc, size, "").await.unwrap();
            assert_eq!(all, vec!["c1", "c2", "c3", "c4", "c5"]);
            assert_eq!(next, "");
        }

        // (c) Negative page_size rejected with InvalidArgument.
        assert!(matches!(
            db.list_collection_ids(&parent_doc, -1, "").await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // (d) Arbitrary, oversized, tampered, cross-parent, and cross-RPC tokens rejected.
        for bad in ["not-a-valid-token", "c2", "c9"] {
            assert!(matches!(
                db.list_collection_ids(&parent_doc, 2, bad).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        assert!(matches!(
            db.list_collection_ids(&other_doc, 2, &tok1).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        let bad_tokens = [
            ListCollectionIdsPageToken {
                version: 2,
                parent: parent_doc.to_string(),
                last_collection_id: "c2".to_string(),
                kind: LIST_COLLECTION_IDS_TOKEN_KIND.to_string(),
            },
            ListCollectionIdsPageToken {
                version: LIST_COLLECTION_IDS_TOKEN_VERSION,
                parent: parent_doc.to_string(),
                last_collection_id: String::new(),
                kind: LIST_COLLECTION_IDS_TOKEN_KIND.to_string(),
            },
            ListCollectionIdsPageToken {
                version: LIST_COLLECTION_IDS_TOKEN_VERSION,
                parent: parent_doc.to_string(),
                last_collection_id: "c2".to_string(),
                kind: "list_docs".to_string(),
            },
        ];
        for tok in bad_tokens {
            let encoded = URL_SAFE_NO_PAD.encode(tok.encode_to_vec());
            assert!(matches!(
                db.list_collection_ids(&parent_doc, 2, &encoded).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        // Cross-RPC ListDocsPageToken cannot decode as a ListCollectionIdsPageToken.
        let (_, list_docs_tok) = db
            .list_docs(
                parent_doc.clone(),
                "c1".to_string(),
                1,
                String::new(),
                String::new(),
                None,
                false,
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        if !list_docs_tok.is_empty() {
            assert!(matches!(
                db.list_collection_ids(&parent_doc, 2, &list_docs_tok).await,
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        // Non-canonical trailing bytes appended to a valid token are rejected.
        let mut raw_valid = URL_SAFE_NO_PAD.decode(&tok1).unwrap();
        raw_valid.extend_from_slice(&[0x78, 0x01]); // unknown protobuf field tag 15 = 1
        let with_extra = URL_SAFE_NO_PAD.encode(raw_valid);
        assert!(matches!(
            db.list_collection_ids(&parent_doc, 2, &with_extra).await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // (e) Collection-path parent rejected with InvalidArgument.
        let col_parent: Ref = "projects/p/databases/(default)/documents/parents"
            .parse()
            .unwrap();
        assert!(matches!(
            db.list_collection_ids(&col_parent, 0, "").await,
            Err(GenericDatabaseError::InvalidArgument(_))
        ));

        // Boundary collection deleted between pages resumes cleanly at the next collection ID.
        let boundary_doc: DocumentRef = "projects/p/databases/(default)/documents/parents/doc1/c2/\
                                         d"
        .parse()
        .unwrap();
        db.delete_doc(&boundary_doc, None).await.unwrap();
        let (after_del, after_del_tok) =
            db.list_collection_ids(&parent_doc, 2, &tok1).await.unwrap();
        assert_eq!(after_del, vec!["c3", "c4"]);
        assert!(!after_del_tok.is_empty());

        // Normalized root parent ("projects/p/databases/(default)" vs ".../documents") works.
        let root_no_docs: Ref = "projects/p/databases/(default)".parse().unwrap();
        let root_with_docs: Ref = "projects/p/databases/(default)/documents".parse().unwrap();
        assert_eq!(
            db.list_collection_ids(&root_no_docs, 1, "").await.unwrap(),
            (vec!["parents".to_string()], String::new())
        );
        assert_eq!(
            db.list_collection_ids(&root_with_docs, 1, "")
                .await
                .unwrap(),
            (vec!["parents".to_string()], String::new())
        );
    }
}
