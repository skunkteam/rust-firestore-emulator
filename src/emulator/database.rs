use self::{
    document::{DocumentMeta, DocumentPrecondition},
    query::Query,
    transaction::{RunningTransactions, TransactionId},
};
use crate::googleapis::google::firestore::v1::*;
use dashmap::DashMap;
use itertools::Itertools;
use prost_types::Timestamp;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc;
use tonic::{Result, Status};

mod document;
mod field_path;
mod query;
mod transaction;

#[derive(Default)]
pub struct Database {
    collections: DashMap<String, Arc<Collection>>,
    transactions: RunningTransactions,
}

#[derive(Default)]
struct Collection {
    documents: DashMap<String, Arc<DocumentMeta>>,
}

impl Collection {
    fn docs(&self) -> impl Iterator<Item = Arc<DocumentMeta>> + '_ {
        self.documents.iter().map(|entry| entry.value().clone())
    }
}

impl Database {
    pub async fn get_doc(
        &self,
        name: &str,
        consistency: &ConsistencySelector,
    ) -> Result<Option<Document>> {
        let meta = self.get_doc_meta(name)?;
        let version = match consistency {
            ConsistencySelector::Strong => meta.current_version().await,
            ConsistencySelector::ReadTime(time) => meta.version_at_time(time).await,
            ConsistencySelector::Transaction(id) => {
                self.transactions.get(id)?.ensure_lock(&meta)?;
                meta.current_version().await
            }
        };
        Ok(version.map(|version| version.to_document()))
    }

    fn get_doc_meta(&self, name: impl Into<String> + AsRef<str>) -> Result<Arc<DocumentMeta>> {
        let (collection, id) = split_name(name.as_ref())?;
        Ok(self
            .collections
            .entry(collection.to_string())
            .or_default()
            .documents
            .entry(id.to_string())
            .or_insert_with(|| DocumentMeta::new(name.into()).into())
            .clone())
    }

    pub async fn set(
        &self,
        name: impl Into<String> + AsRef<str>,
        fields: HashMap<String, Value>,
        time: Timestamp,
        condition: Option<DocumentPrecondition>,
    ) -> Result<()> {
        let meta = self.get_doc_meta(name)?.wait_lock().await?;
        if let Some(condition) = condition {
            meta.check_precondition(condition).await?;
        }
        meta.add_version(fields, time).await;
        Ok(())
    }

    pub async fn delete(
        &self,
        name: impl Into<String> + AsRef<str>,
        time: Timestamp,
        condition: Option<DocumentPrecondition>,
    ) -> Result<()> {
        let meta = self.get_doc_meta(name)?.wait_lock().await?;
        meta.check_precondition(condition.unwrap_or(DocumentPrecondition::Exists))
            .await?;
        meta.delete(time).await;
        Ok(())
    }

    pub fn get_collection_ids(&self, parent: &str) -> Vec<String> {
        self.collections
            .iter()
            .filter_map(|entry| {
                let key = entry.key();
                let path = key.strip_prefix(parent)?.strip_prefix('/')?;
                if path.is_empty() || entry.value().documents.is_empty() {
                    return None;
                }
                let id = path.split_once('/').map(|(id, _)| id).unwrap_or(path);
                Some(id.to_string())
            })
            .unique()
            .collect()
    }

    // TODO: API niet op basis van GRPC types
    pub fn run_query(
        &self,
        parent: String,
        query: StructuredQuery,
        consistency: &ConsistencySelector,
        sender: mpsc::Sender<Result<Document>>,
    ) -> Result<()> {
        let query = Query::new(parent, query)?;

        let collections = self
            .collections
            .iter()
            .filter_map(|entry| {
                query
                    .includes_collection(entry.key())
                    .then(|| entry.value().clone())
            })
            .collect_vec();

        let limit = query.limit();

        tokio::spawn(async move {
            let mut count = 0;
            for col in collections {
                for meta in col.docs() {
                    let msg = async {
                        let Some(version) = meta.current_version().await else {
                            return Ok(None);
                        };
                        if query.includes_document(&version)? {
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

    pub fn new_txn(&self) -> Result<TransactionId> {
        Ok(*self.transactions.start().key())
    }

    pub fn rollback(&self, transaction: &TransactionId) -> Result<()> {
        self.transactions.stop(transaction)
    }
}

pub fn split_name(name: &str) -> Result<(&str, &str)> {
    name.rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))
}

#[derive(Clone)]
pub enum ConsistencySelector {
    Strong,
    ReadTime(Timestamp),
    Transaction(TransactionId),
}

impl TryFrom<Option<get_document_request::ConsistencySelector>> for ConsistencySelector {
    type Error = Status;

    fn try_from(
        value: Option<get_document_request::ConsistencySelector>,
    ) -> Result<Self, Self::Error> {
        use get_document_request::ConsistencySelector as Other;
        let result = match value {
            None => ConsistencySelector::Strong,
            Some(Other::ReadTime(time)) => ConsistencySelector::ReadTime(time),
            Some(Other::Transaction(id)) => ConsistencySelector::Transaction(id.try_into()?),
        };
        Ok(result)
    }
}

impl TryFrom<Option<batch_get_documents_request::ConsistencySelector>> for ConsistencySelector {
    type Error = Status;

    fn try_from(
        value: Option<batch_get_documents_request::ConsistencySelector>,
    ) -> Result<Self, Self::Error> {
        use batch_get_documents_request::ConsistencySelector as Other;
        let result = match value {
            None => ConsistencySelector::Strong,
            Some(Other::ReadTime(time)) => ConsistencySelector::ReadTime(time),
            Some(Other::Transaction(id)) => ConsistencySelector::Transaction(id.try_into()?),
            Some(Other::NewTransaction(_)) => {
                return Err(Status::internal("batch_get_documents_request::ConsistencySelector::NewTransaction should be handled by caller"));
            }
        };
        Ok(result)
    }
}

impl TryFrom<Option<run_query_request::ConsistencySelector>> for ConsistencySelector {
    type Error = Status;

    fn try_from(
        value: Option<run_query_request::ConsistencySelector>,
    ) -> Result<Self, Self::Error> {
        use run_query_request::ConsistencySelector as Other;
        let result = match value {
            None => ConsistencySelector::Strong,
            Some(Other::ReadTime(time)) => ConsistencySelector::ReadTime(time),
            Some(Other::Transaction(id)) => ConsistencySelector::Transaction(id.try_into()?),
            Some(Other::NewTransaction(_)) => {
                return Err(Status::internal("run_query_request::ConsistencySelector::NewTransaction should be handled by caller"));
            }
        };
        Ok(result)
    }
}
