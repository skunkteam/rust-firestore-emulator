use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    ops::Deref,
    sync::{Arc, Weak},
};

use googleapis::google::{
    firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
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
use tracing::{info, instrument, Span};

use self::{
    collection::Collection,
    document::{DocumentContents, DocumentMeta, DocumentVersion, OwnedDocumentContentsWriteGuard},
    event::DatabaseEvent,
    field_path::FieldPath,
    query::Query,
    reference::{CollectionRef, DocumentRef, Ref, RootRef},
    transaction::{RunningTransactions, Transaction, TransactionId},
};
use crate::{
    error::Result, unimplemented, unimplemented_collection, unimplemented_option,
    utils::RwLockHashMapExt, GenericDatabaseError,
};

mod collection;
pub(crate) mod document;
pub mod event;
mod field_path;
pub(crate) mod query;
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

    #[instrument(skip_all, err, fields(in_txn = consistency.is_transaction(), found))]
    pub async fn get_doc(
        &self,
        name: &DocumentRef,
        consistency: &ReadConsistency,
    ) -> Result<Option<Document>> {
        info!(%name);
        let version = if let Some(txn) = self.get_txn_for_consistency(consistency).await? {
            txn.read_doc(name)
                .await?
                .version_for_consistency(consistency)?
                .map(|version| version.to_document())
        } else {
            self.get_doc_meta(name)
                .await?
                .read()
                .await?
                .version_for_consistency(consistency)?
                .map(|version| version.to_document())
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

    #[instrument(skip_all, err)]
    pub async fn get_doc_meta(&self, name: &DocumentRef) -> Result<Arc<DocumentMeta>> {
        let meta = self
            .get_collection(&name.collection_ref)
            .await
            .get_doc(name)
            .await;
        Ok(meta)
    }

    #[instrument(skip_all, err)]
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

    #[instrument(skip_all, err)]
    pub async fn get_txn_for_consistency(
        &self,
        consistency: &ReadConsistency,
    ) -> Result<Option<Arc<Transaction>>> {
        if let ReadConsistency::Transaction(id) = consistency {
            Ok(Some(self.get_txn(id).await?))
        } else {
            Ok(None)
        }
    }

    #[instrument(skip_all, err)]
    pub async fn get_txn(
        &self,
        id: &TransactionId,
    ) -> Result<Arc<Transaction>, GenericDatabaseError> {
        self.transactions.get(id).await
    }

    // /// Get all the (deeply nested) collections that reside under the given parent.
    // #[instrument(skip_all)]
    // pub async fn get_collection_ids_deep(&self, parent: &Ref) -> Result<Vec<String>> {
    //     // Cannot use `filter_map` because of the `await`.
    //     let mut result = vec![];
    //     for col in self.get_all_collections().await {
    //         let Some(path) = col.name.strip_prefix(parent) else {
    //             continue;
    //         };
    //         if col.has_doc().await? {
    //             result.push(path.to_string());
    //         }
    //     }
    //     Ok(result)
    // }

    /// Get all the collections that reside directly under the given parent. This means that:
    /// - the IDs will not contain a `/`
    /// - the result will be empty if `parent` is a [`Ref::Collection`].
    #[instrument(skip_all)]
    pub async fn get_collection_ids(&self, parent: &Ref) -> Result<HashSet<String>> {
        // Cannot use `filter_map` because of the `await`.
        let mut result = HashSet::new();
        for col in self.get_all_collections().await {
            let Some(path) = col.name.strip_prefix(parent) else {
                continue;
            };
            let id = path.split_once('/').unwrap_or((path, "")).0;
            if !result.contains(id) && col.has_doc().await? {
                result.insert(id.to_string());
            }
        }
        info!(result_count = result.len());
        Ok(result)
    }

    /// Get all the document IDs that reside directly under the given parent. This differs from
    /// [`Collection::docs`] in that this also includes empty documents with nested collections.
    /// This means that:
    /// - the IDs will not contain a `/`
    /// - IDs may point to documents that do not exist.
    #[instrument(skip_all)]
    pub async fn get_document_ids(&self, parent: &CollectionRef) -> Result<HashSet<String>> {
        // Cannot use `filter_map` because of the `await`.
        let mut result = HashSet::new();
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
    async fn get_all_collections(&self) -> Vec<Arc<Collection>> {
        self.collections
            .read()
            .await
            .values()
            .cloned()
            .collect_vec()
    }

    #[instrument(skip_all, err)]
    pub async fn run_query(
        &self,
        parent: Ref,
        query: StructuredQuery,
        consistency: ReadConsistency,
    ) -> Result<Vec<Document>> {
        let mut query = Query::from_structured(parent, query, consistency)?;
        info!(?query);
        let result = query.once(self).await?;
        info!(result_count = result.len());
        Ok(result.into_iter().map(|t| t.1).collect())
    }

    #[instrument(skip_all, err)]
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
            let (write_result, version) = self.perform_write(write, guard, time.clone()).await?;
            write_results.push(write_result);
            updates.insert(name.clone(), version);
        }

        self.transactions.stop(transaction).await?;

        self.send_event(DatabaseEvent {
            database: Arc::downgrade(self),
            update_time: time.clone(),
            updates,
        });

        Ok((time, write_results))
    }

    #[instrument(skip_all, err)]
    pub async fn perform_write(
        &self,
        write: Write,
        contents: &mut DocumentContents,
        commit_time: Timestamp,
    ) -> Result<(WriteResult, DocumentVersion)> {
        let Write {
            update_mask,
            update_transforms,
            current_document,
            operation,
        } = write;

        let operation = operation.ok_or(GenericDatabaseError::not_implemented(
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
                        &commit_time,
                    )?);
                }
                contents.add_version(fields, commit_time.clone()).await
            }
            Delete(_) => {
                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                contents.delete(commit_time.clone()).await
            }
            Transform(_) => unimplemented!("transform"),
        };
        Ok((
            WriteResult {
                update_time: Some(commit_time),
                transform_results,
            },
            document_version,
        ))
    }

    #[instrument(skip_all, err)]
    pub async fn new_txn(&self) -> Result<TransactionId> {
        Ok(self.transactions.start().await.id)
    }

    #[instrument(skip_all, err)]
    pub async fn new_txn_with_id(&self, id: TransactionId) -> Result<()> {
        self.transactions.start_with_id(id).await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
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
        let field_path: FieldPath = field_path.deref().try_into()?;
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
    commit_time: &Timestamp,
) -> Result<Value> {
    let field_path: FieldPath = path.deref().try_into()?;
    let result = match transform {
        TransformType::SetToServerValue(code) => {
            match ServerValue::try_from(code).map_err(|_| {
                GenericDatabaseError::invalid_argument(format!("invalid server_value: {code}"))
            })? {
                ServerValue::RequestTime => {
                    let new_value = Value::timestamp(commit_time.clone());
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

#[derive(Clone, Debug)]
pub enum ReadConsistency {
    Default,
    ReadTime(Timestamp),
    Transaction(TransactionId),
}

impl ReadConsistency {
    /// Returns `true` if the read consistency is [`Transaction`].
    ///
    /// [`Transaction`]: ReadConsistency::Transaction
    #[must_use]
    pub fn is_transaction(&self) -> bool {
        matches!(self, Self::Transaction(..))
    }
}

macro_rules! impl_try_from_consistency_selector {
    ($lib:ident) => {
        impl TryFrom<Option<$lib::ConsistencySelector>> for ReadConsistency {
            type Error = GenericDatabaseError;

            fn try_from(value: Option<$lib::ConsistencySelector>) -> Result<Self, Self::Error> {
                let result = match value {
                    None => ReadConsistency::Default,
                    Some($lib::ConsistencySelector::ReadTime(time)) => {
                        ReadConsistency::ReadTime(time)
                    }
                    Some($lib::ConsistencySelector::Transaction(id)) => {
                        ReadConsistency::Transaction(id.try_into()?)
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        return Err(GenericDatabaseError::internal(concat!(
                            stringify!($lib),
                            "::ConsistencySelector::NewTransaction should be handled by caller"
                        )));
                    }
                };
                Ok(result)
            }
        }
    };
}

impl_try_from_consistency_selector!(batch_get_documents_request);
impl_try_from_consistency_selector!(get_document_request);
impl_try_from_consistency_selector!(list_documents_request);
impl_try_from_consistency_selector!(run_query_request);
