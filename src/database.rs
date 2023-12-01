use self::{
    collection::Collection,
    document::{DocumentGuard, DocumentMeta, StoredDocumentVersion},
    field_path::FieldPath,
    query::Query,
    transaction::{RunningTransactions, Transaction, TransactionId},
};
use crate::{
    googleapis::google::firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
        *,
    },
    unimplemented, unimplemented_collection, unimplemented_option,
    utils::RwLockHashMapExt,
};
use futures::future::try_join_all;
use itertools::Itertools;
use prost_types::Timestamp;
use std::{collections::HashMap, ops::Deref, sync::Arc, time::SystemTime};
use tokio::sync::RwLock;
use tonic::{Result, Status};
use tracing::{info, instrument};

mod collection;
mod document;
mod field_path;
mod query;
mod transaction;
mod value;

#[derive(Default)]
pub struct Database {
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    transactions: RunningTransactions,
}

impl Database {
    #[instrument(skip_all, err, fields(in_txn = consistency.is_transaction()))]
    pub async fn get_doc(
        &self,
        name: &str,
        consistency: &ReadConsistency,
    ) -> Result<Option<Document>> {
        info!(name);
        let meta = self.get_doc_meta(name).await?;
        if let Some(txn) = self.get_txn_for_consistency(consistency).await? {
            txn.register_doc(&meta).await;
        }
        let version = self.get_version(&meta, consistency).await?;
        info!(found = version.is_some());
        Ok(version.map(|version| version.to_document()))
    }

    #[instrument(skip_all, err)]
    pub async fn get_doc_meta(&self, name: &str) -> Result<Arc<DocumentMeta>> {
        let (collection, id) = split_name(name)?;
        let collection = self
            .collections
            .get_or_insert(collection, || Collection::new(collection.into()).into())
            .await;
        let meta = collection
            .documents
            .get_or_insert(id, || DocumentMeta::new(name.into()).into())
            .await;
        Ok(Arc::clone(&meta))
    }

    #[instrument(skip_all, err)]
    pub async fn get_doc_meta_mut(&self, name: &str) -> Result<DocumentGuard> {
        self.get_doc_meta(name).await?.wait_lock().await
    }

    async fn get_version(
        &self,
        meta: &Arc<DocumentMeta>,
        consistency: &ReadConsistency,
    ) -> Result<Option<Arc<StoredDocumentVersion>>> {
        Ok(match consistency {
            ReadConsistency::Default => meta.current_version().await,
            ReadConsistency::ReadTime(time) => meta.version_at_time(time).await,
            ReadConsistency::Transaction(_) => meta.current_version().await,
        })
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

    // pub async fn set(
    //     &self,
    //     name: &str,
    //     fields: HashMap<String, Value>,
    //     time: Timestamp,
    //     condition: Option<DocumentPrecondition>,
    // ) -> Result<()> {
    //     let meta = self.get_doc_meta_mut(name).await?;
    //     if let Some(condition) = condition {
    //         meta.check_precondition(condition).await?;
    //     }
    //     meta.add_version(fields, time).await;
    //     Ok(())
    // }

    // pub async fn delete(
    //     &self,
    //     name: &str,
    //     time: Timestamp,
    //     condition: Option<DocumentPrecondition>,
    // ) -> Result<()> {
    //     let meta = self.get_doc_meta_mut(name).await?;
    //     meta.check_precondition(condition.unwrap_or(DocumentPrecondition::Exists))
    //         .await?;
    //     meta.delete(time).await;
    //     Ok(())
    // }

    #[instrument(skip_all)]
    pub async fn get_collection_ids(&self, parent: &str) -> Vec<String> {
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
                .strip_prefix(parent)
                .and_then(|p| p.strip_prefix('/'))
            else {
                continue;
            };
            for doc in col.documents.read().await.values() {
                if doc.exists().await {
                    result.push(path.to_string());
                    break;
                }
            }
        }
        result
    }

    #[instrument(skip_all, err)]
    pub async fn run_query(
        &self,
        parent: String,
        query: StructuredQuery,
        consistency: ReadConsistency,
    ) -> Result<Vec<Document>> {
        let query = Query::from_structured(parent, query, consistency)?;
        info!(?query);
        query.once(self).await
    }

    #[instrument(skip_all, err)]
    pub async fn commit(
        &self,
        writes: Vec<Write>,
        transaction: &TransactionId,
    ) -> Result<(Timestamp, Vec<WriteResult>)> {
        let txn = self.get_txn(transaction).await?;
        let metas = try_join_all(writes.into_iter().map(|write| async {
            let meta = self.get_doc_meta(get_doc_name_from_write(&write)?).await?;
            txn.register_doc(&meta).await;
            Ok((write, meta)) as Result<_, Status>
        }))
        .await?;

        let guards = txn.take_guards().await?;
        let time: Timestamp = SystemTime::now().into();

        let write_results = try_join_all(metas.into_iter().map(
            |(write, meta): (Write, Arc<DocumentMeta>)| {
                let guard = &guards[&meta.name];
                self.perform_write(write, guard, time.clone())
            },
        ))
        .await?;
        Ok((time, write_results))
    }

    #[instrument(skip_all, err)]
    pub async fn perform_write(
        &self,
        write: Write,
        guard: &DocumentGuard,
        commit_time: Timestamp,
    ) -> Result<WriteResult> {
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
            guard.check_precondition(condition).await?;
        }
        let mut transform_results = vec![];

        use write::Operation::*;
        match operation {
            Update(doc) => {
                let mut fields = if let Some(mask) = update_mask {
                    apply_updates(guard, mask, &doc.fields).await?
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
                guard.add_version(fields, commit_time.clone()).await;
            }
            Delete(_) => {
                unimplemented_option!(update_mask);
                unimplemented_collection!(update_transforms);
                guard.delete(commit_time.clone()).await;
            }
            Transform(_) => unimplemented!("transform"),
        }
        Ok(WriteResult {
            update_time: Some(commit_time),
            transform_results,
        })
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
        self.transactions.stop(transaction).await
    }

    #[instrument(skip_all)]
    pub async fn clear(&self) {
        self.transactions.clear().await;
        self.collections.write().await.clear();
    }
}

async fn apply_updates(
    guard: &DocumentGuard,
    mask: DocumentMask,
    updated_values: &HashMap<String, Value>,
) -> Result<HashMap<String, Value>, Status> {
    let mut fields = guard
        .current_version()
        .await
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

fn split_name(name: &str) -> Result<(&str, &str)> {
    name.rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))
}

pub fn get_doc_name_from_write(write: &Write) -> Result<&str> {
    let operation = write
        .operation
        .as_ref()
        .ok_or(Status::unimplemented("missing operation in write"))?;
    use write::Operation::*;
    Ok(match operation {
        Update(doc) => &doc.name,
        Delete(name) => name,
        Transform(trans) => &trans.document,
    })
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
                        )))
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
