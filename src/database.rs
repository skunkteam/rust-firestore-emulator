use self::{
    collection::Collection,
    document::{DocumentGuard, DocumentMeta, StoredDocumentVersion},
    query::Query,
    transaction::{RunningTransactions, Transaction, TransactionId},
};
use crate::{
    database::field_path::FieldPath,
    googleapis::google::firestore::v1::{
        document_transform::field_transform::{ServerValue, TransformType},
        value::ValueType,
        *,
    },
    unimplemented, unimplemented_collection, unimplemented_option,
    utils::RwLockHashMapExt,
};
use futures::future::try_join_all;
use itertools::Itertools;
use prost_types::Timestamp;
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio::sync::{mpsc, RwLock};
use tonic::{Result, Status};

mod collection;
mod document;
mod field_path;
mod query;
mod transaction;

#[derive(Default)]
pub struct Database {
    collections: RwLock<HashMap<String, Arc<Collection>>>,
    transactions: RunningTransactions,
}

impl Database {
    pub async fn get_doc(
        &self,
        name: &str,
        consistency: &ReadConsistency,
    ) -> Result<Option<Document>> {
        let meta = self.get_doc_meta(name).await?;
        let version = self.get_version(&meta, consistency).await?;
        self.get_txn_for_consistency(consistency).await?;
        Ok(version.map(|version| version.to_document()))
    }

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
            if col.documents.read().await.is_empty() {
                continue;
            }
            result.push(path.to_string());
        }
        result
    }

    pub async fn run_query(
        &self,
        parent: String,
        query: StructuredQuery,
        consistency: &ReadConsistency,
        sender: mpsc::Sender<Result<Document>>,
    ) -> Result<()> {
        let query = Query::new(parent, query)?;

        let collections = self
            .collections
            .read()
            .await
            .values()
            .filter(|&col| query.includes_collection(&col.name))
            .map(Arc::clone)
            .collect_vec();

        let limit = query.limit();
        let txn = self.get_txn_for_consistency(consistency).await?;

        tokio::spawn(async move {
            let mut count = 0;
            for col in collections {
                for meta in col.docs().await {
                    let msg = async {
                        let Some(version) = meta.current_version().await else {
                            return Ok(None);
                        };
                        if query.includes_document(&version)? {
                            maybe_register_doc(&txn, &meta).await;
                            Ok(Some(query.project(&version)?))
                        } else {
                            Ok(None)
                        }
                    };
                    let send_result = match msg.await {
                        Ok(Some(document)) => sender.send(Ok(document)),
                        Ok(None) => continue,
                        Err(err) => sender.send(Err(err)),
                    };
                    if send_result.await.is_err() {
                        return;
                    }
                    count += 1;
                    if count >= limit {
                        return;
                    }
                }
            }
        });

        Ok(())
    }

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

    pub async fn new_txn(&self) -> Result<TransactionId> {
        Ok(self.transactions.start().await.id)
    }

    pub async fn rollback(&self, transaction: &TransactionId) -> Result<()> {
        self.transactions.stop(transaction).await
    }

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
        let field_path = FieldPath::new(&field_path)?;
        match field_path.get_value(updated_values) {
            Some(new_value) => field_path.set_value(&mut fields, new_value.clone()),
            None => field_path.delete_value(&mut fields),
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
    let field_path = FieldPath::new(&path)?;
    let result = match transform {
        TransformType::SetToServerValue(code) => {
            match ServerValue::try_from(code)
                .map_err(|_| Status::invalid_argument(format!("invalid server_value: {code}")))?
            {
                ServerValue::RequestTime => {
                    let new_value = wrap_value(ValueType::TimestampValue(commit_time.clone()));
                    field_path.set_value(fields, new_value.clone());
                    new_value
                }
                other => unimplemented!(format!("{:?}", other)),
            }
        }
        TransformType::Increment(value) => {
            let ValueType::IntegerValue(value) = unwrap_value(value) else {
                unimplemented!("TransformType::Increment on doubles");
            };
            field_path
                .try_transform_value(fields, |cur| {
                    Ok(match cur.map(unwrap_value) {
                        Some(ValueType::IntegerValue(int)) => {
                            wrap_value(ValueType::IntegerValue(int.saturating_add(value)))
                        }
                        Some(ValueType::DoubleValue(_)) => {
                            unimplemented!("TransformType::Increment on doubles")
                        }
                        _ => wrap_value(ValueType::IntegerValue(value)),
                    })
                })?
                .clone()
        }
        TransformType::Maximum(_) => unimplemented!("TransformType::Maximum"),
        TransformType::Minimum(_) => unimplemented!("TransformType::Minimum"),
        TransformType::AppendMissingElements(elements) => {
            field_path.transform_value(fields, |cur_value| match cur_value.map(unwrap_value) {
                Some(ValueType::ArrayValue(mut array)) => {
                    for el in elements.values {
                        if !array.values.contains(&el) {
                            array.values.push(el)
                        }
                    }
                    wrap_value(ValueType::ArrayValue(array))
                }
                _ => wrap_value(ValueType::ArrayValue(elements)),
            });
            wrap_value(ValueType::NullValue(0))
        }
        TransformType::RemoveAllFromArray(_) => unimplemented!("TransformType::RemoveAllFromArray"),
    };
    Ok(result)
}

fn split_name(name: &str) -> Result<(&str, &str)> {
    name.rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))
}

async fn maybe_register_doc(txn: &Option<Arc<Transaction>>, meta: &Arc<DocumentMeta>) {
    if let Some(txn) = txn {
        txn.register_doc(meta).await;
    }
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

fn unwrap_value(value: Value) -> ValueType {
    value.value_type.expect("missing value_type in value")
}

fn wrap_value(value_type: ValueType) -> Value {
    Value {
        value_type: Some(value_type),
    }
}

#[derive(Clone)]
pub enum ReadConsistency {
    Default,
    ReadTime(Timestamp),
    Transaction(TransactionId),
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
impl_try_from_consistency_selector!(run_query_request);