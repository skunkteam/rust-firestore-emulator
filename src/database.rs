use std::{
    collections::{hash_map::Entry, HashMap},
    ops::Deref,
    sync::{Arc, Weak},
};

use itertools::Itertools;
use prost_types::Timestamp;
use string_cache::DefaultAtom;
use tokio::sync::{
    broadcast::{self, Receiver},
    RwLock,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Result, Status};
use tracing::{info, instrument, Span};

use self::{
    collection::Collection,
    document::{DocumentContents, DocumentMeta, DocumentVersion, OwnedDocumentContentsWriteGuard},
    event::DatabaseEvent,
    field_path::FieldPath,
    listener::Listener,
    query::Query,
    transaction::{RunningTransactions, Transaction, TransactionId},
};
use crate::{
    googleapis::google::firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
        *,
    },
    unimplemented, unimplemented_collection, unimplemented_option,
    utils::{timestamp, RwLockHashMapExt},
};

mod collection;
mod document;
pub mod event;
mod field_path;
mod listener;
mod query;
mod transaction;
mod value;

const MAX_EVENT_BACKLOG: usize = 1024;

pub struct Database {
    collections: RwLock<HashMap<DefaultAtom, Arc<Collection>>>,
    transactions: RunningTransactions,
    events: broadcast::Sender<Arc<DatabaseEvent>>,
}

impl Database {
    pub fn new() -> Arc<Self> {
        Arc::new_cyclic(|database| Database {
            collections: Default::default(),
            transactions: RunningTransactions::new(Weak::clone(database)),
            events: broadcast::channel(MAX_EVENT_BACKLOG).0,
        })
    }
}

impl Database {
    #[instrument(skip_all, err, fields(in_txn = consistency.is_transaction(), found))]
    pub async fn get_doc(
        &self,
        name: &DefaultAtom,
        consistency: &ReadConsistency,
    ) -> Result<Option<Document>> {
        info!(name = name.deref());
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

    pub async fn get_collection(&self, collection_name: &DefaultAtom) -> Arc<Collection> {
        Arc::clone(
            &*self
                .collections
                .get_or_insert(collection_name, || {
                    Arc::new(Collection::new(collection_name.into()))
                })
                .await,
        )
    }

    #[instrument(skip_all, err)]
    pub async fn get_doc_meta(&self, name: &DefaultAtom) -> Result<Arc<DocumentMeta>> {
        let collection = collection_name(name)?;
        let meta = self.get_collection(&collection).await.get_doc(name).await;
        Ok(meta)
    }

    #[instrument(skip_all, err)]
    pub async fn get_doc_meta_mut_no_txn(
        &self,
        name: &DefaultAtom,
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
    pub async fn get_txn(&self, id: &TransactionId) -> Result<Arc<Transaction>, Status> {
        self.transactions.get(id).await
    }

    #[instrument(skip_all)]
    pub async fn get_collection_ids(&self, parent: &DefaultAtom) -> Result<Vec<DefaultAtom>> {
        // Get all collections asap in order to keep the read lock time minimal.
        let all_collections = self
            .collections
            .read()
            .await
            .values()
            .cloned()
            .collect_vec();
        // Cannot use `filter_map` because of the `await`.
        let mut result = vec![];
        for col in all_collections {
            let Some(path) = col
                .name
                .strip_prefix(parent.deref())
                .and_then(|p| p.strip_prefix('/'))
            else {
                continue;
            };
            if col.has_doc().await? {
                result.push(path.into());
            }
        }
        Ok(result)
    }

    #[instrument(skip_all, err)]
    pub async fn run_query(
        &self,
        parent: String,
        query: StructuredQuery,
        consistency: ReadConsistency,
    ) -> Result<Vec<Document>> {
        let mut query = Query::from_structured(parent, query, consistency)?;
        info!(?query);
        query.once(self).await
    }

    #[instrument(skip_all, err)]
    pub async fn commit(
        &self,
        writes: Vec<Write>,
        transaction: &TransactionId,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let txn = &self.transactions.get(transaction).await?;
        let time = timestamp();

        let mut write_results = vec![];
        let mut updates = HashMap::new();
        let mut write_guard_cache = HashMap::<DefaultAtom, OwnedDocumentContentsWriteGuard>::new();
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

        let operation = operation.ok_or(Status::unimplemented("missing operation in write"))?;
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
                            Status::invalid_argument("empty transform_type in field_transform")
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

    #[instrument(skip_all)]
    pub async fn clear(&self) {
        self.transactions.clear().await;
        self.collections.write().await.clear();
    }

    pub fn listen(
        self: &Arc<Database>,
        request_stream: tonic::Streaming<ListenRequest>,
    ) -> ReceiverStream<Result<ListenResponse>> {
        Listener::start(Arc::downgrade(self), request_stream)
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
) -> Result<HashMap<String, Value>, Status> {
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
            match ServerValue::try_from(code)
                .map_err(|_| Status::invalid_argument(format!("invalid server_value: {code}")))?
            {
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

fn collection_name(name: &DefaultAtom) -> Result<DefaultAtom> {
    Ok(name
        .rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))?
        .0
        .into())
}

pub fn get_doc_name_from_write(write: &Write) -> Result<DefaultAtom> {
    let operation = write
        .operation
        .as_ref()
        .ok_or(Status::unimplemented("missing operation in write"))?;
    use write::Operation::*;
    let name: &str = match operation {
        Update(doc) => &doc.name,
        Delete(name) => name,
        Transform(trans) => &trans.document,
    };
    Ok(DefaultAtom::from(name))
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
            type Error = Status;

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
                        return Err(Status::internal(concat!(
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
