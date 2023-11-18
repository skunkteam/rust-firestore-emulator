use crate::googleapis::google::firestore::v1::*;
use async_stream::try_stream;
use dashmap::{mapref::entry::Entry, DashMap};
use itertools::Itertools;
use prost_types::Timestamp;
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio_stream::Stream;
use tonic::{Code, Result, Status};

#[derive(Default)]
pub struct Collection {
    documents: DashMap<String, Arc<Document>>,
}

#[derive(Default)]
pub struct Database {
    collections: DashMap<String, Arc<Collection>>,
}

impl Database {
    pub fn get_by_name(&self, name: &str) -> Result<Option<Arc<Document>>> {
        let (collection, id) = split_name(name)?;
        Ok(self.get_doc(collection, id))
    }

    pub fn get_doc(&self, collection: &str, id: &str) -> Option<Arc<Document>> {
        let collection = self.collections.get(collection)?;
        let arc = collection.documents.get(id)?;
        Some(arc.clone())
    }

    pub fn get_collection(&self, collection: impl Into<String>) -> Arc<Collection> {
        self.collections
            .entry(collection.into())
            .or_default()
            .clone()
    }

    pub fn add(
        &self,
        name: String,
        fields: HashMap<String, Value>,
        time: Option<Timestamp>,
    ) -> Result<WriteResult> {
        let (collection, id) = split_name(&name)?;
        let collection = self.get_collection(collection);
        let entry = collection.documents.entry(id.to_string());
        if !matches!(entry, Entry::Vacant(_)) {
            return Err(Status::already_exists(Code::AlreadyExists.description()));
        }
        let new_document = Arc::new(Document {
            update_time: time.clone(),
            create_time: time.clone(),
            fields,
            name,
        });
        entry.insert(new_document);
        Ok(WriteResult {
            update_time: time,
            transform_results: vec![],
        })
    }

    pub fn perform_write(
        &self,
        write: Write,
        commit_time: Option<Timestamp>,
    ) -> Result<WriteResult> {
        let (Some(operation), Some(condition)) = (
            write.operation,
            write.current_document.and_then(|c| c.condition_type),
        ) else {
            todo!("Missing operation or condition");
        };

        use precondition::ConditionType;
        use write::Operation;
        match (operation, condition) {
            (Operation::Update(doc), ConditionType::Exists(false)) => {
                self.add(doc.name, doc.fields, commit_time)
            }
            (Operation::Update(_), ConditionType::Exists(true)) => {
                todo!("update")
            }
            (Operation::Update(_), _) => todo!("update with other precondition"),
            (Operation::Delete(_), _) => todo!("delete"),
            (Operation::Transform(_), _) => todo!("transform"),
        }
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

    pub fn run_query(
        &self,
        RunQueryRequest {
            parent,
            query_type,
            consistency_selector,
        }: RunQueryRequest,
    ) -> Result<impl Stream<Item = Result<RunQueryResponse>>> {
        use run_query_request::*;
        let QueryType::StructuredQuery(query) =
            query_type.ok_or(Status::unimplemented("cannot handle empty query_type"))?;
        if consistency_selector.is_some() {
            return Err(Status::unimplemented(
                "consistency_selector is not supported yet",
            ));
        }
        let StructuredQuery {
            select,
            from,
            r#where,
            order_by,
            start_at,
            end_at,
            offset,
            limit,
        } = query;
        if select.is_some() {
            return Err(Status::unimplemented("select is not supported yet"));
        }
        if r#where.is_some() {
            return Err(Status::unimplemented("where is not supported yet"));
        }
        if !order_by.is_empty() {
            return Err(Status::unimplemented("order_by is not supported yet"));
        }
        if start_at.is_some() {
            return Err(Status::unimplemented("start_at is not supported yet"));
        }
        if end_at.is_some() {
            return Err(Status::unimplemented("end_at is not supported yet"));
        }
        if offset != 0 {
            return Err(Status::unimplemented("offset is not supported yet"));
        }
        if limit.is_some() {
            return Err(Status::unimplemented("limit is not supported yet"));
        }

        let collections = self
            .collections
            .iter()
            .filter_map(move |entry| {
                let path = entry.key();
                let path = path.strip_prefix(&parent)?;
                let path = path.strip_prefix('/')?;
                from.iter()
                    .any(|selector| {
                        if selector.all_descendants {
                            path.starts_with(&selector.collection_id)
                        } else {
                            path == selector.collection_id
                        }
                    })
                    .then(|| entry.clone())
            })
            .collect_vec();

        Ok(try_stream! {
            for collection in collections {
                for entry in collection.documents.iter() {
                    let document = entry.value().clone();
                    yield RunQueryResponse {
                        transaction: vec![],
                        document: Some(Document::clone(&document)),
                        continuation_selector: None,
                        read_time: Some(SystemTime::now().into()),
                        skipped_results:0
                    }
                }
            }
        })
    }
}

pub fn split_name(name: &str) -> Result<(&str, &str)> {
    name.rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))
}
