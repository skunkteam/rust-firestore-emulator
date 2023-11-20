use crate::{emulator::database::query::Query, googleapis::google::firestore::v1::*};
use dashmap::{mapref::entry::Entry, DashMap};
use itertools::Itertools;
use prost_types::Timestamp;
use std::{collections::HashMap, sync::Arc, time::SystemTime};
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Result, Status};

mod query;

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

    pub fn delete(&self, name: String) -> Result<WriteResult> {
        let (collection, id) = split_name(&name)?;
        let collection = self.get_collection(collection);
        collection
            .documents
            .remove(id)
            .ok_or(Status::failed_precondition(format!(
                "document not found: {name}"
            )))?;
        Ok(WriteResult {
            update_time: None,
            transform_results: vec![],
        })
    }

    pub fn perform_write(
        &self,
        write: Write,
        commit_time: Option<Timestamp>,
    ) -> Result<WriteResult> {
        let operation = write
            .operation
            .ok_or(Status::unimplemented("missing operation in write"))?;
        let condition = write.current_document.and_then(|cd| cd.condition_type);

        use precondition::ConditionType::*;
        use write::Operation::*;
        match (operation, condition) {
            (Update(doc), Some(Exists(false))) => self.add(doc.name, doc.fields, commit_time),
            (Update(_), Some(Exists(true))) => todo!("update"),
            (Update(_), _) => todo!("update with other precondition"),
            (Delete(name), _) => self.delete(name),
            (Transform(_), _) => todo!("transform"),
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

        let documents: Vec<_> = collections
            .iter()
            .flat_map(|col| col.documents.iter())
            .filter_map(|entry| {
                let doc = entry.value();
                match query.includes_document(doc) {
                    Ok(true) => Some(Ok(doc.clone())),
                    Ok(false) => None,
                    Err(err) => Some(Err(err)),
                }
            })
            .try_collect()?;

        let limit = query.limit();
        let stream = tokio_stream::iter(documents)
            .map(move |doc| {
                Ok(RunQueryResponse {
                    transaction: vec![],
                    document: Some(query.project(&doc)?),
                    continuation_selector: None,
                    read_time: Some(SystemTime::now().into()),
                    skipped_results: 0,
                })
            })
            .take(limit);

        Ok(stream)
    }
}

pub fn split_name(name: &str) -> Result<(&str, &str)> {
    name.rsplit_once('/')
        .ok_or_else(|| Status::invalid_argument("invalid document path, missing collection-name"))
}
