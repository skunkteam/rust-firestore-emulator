use crate::googleapis::google::firestore::v1::*;
use dashmap::{mapref::entry::Entry, DashMap};
use prost_types::Timestamp;
use std::{collections::HashMap, sync::Arc};
use tonic::{Result, Status};

#[derive(Default)]
pub struct Collection {
    documents: DashMap<String, Arc<Document>>,
}

#[derive(Default)]
pub struct Database {
    collections: DashMap<String, Arc<Collection>>,
}

impl Database {
    pub fn split_name(name: &str) -> Result<(&str, &str)> {
        name.rsplit_once('/').ok_or_else(|| {
            Status::invalid_argument("invalid document path, missing collection-name")
        })
    }

    pub fn get_by_name(&self, name: &str) -> Result<Option<Arc<Document>>> {
        let (collection, id) = Self::split_name(name)?;
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
        let (collection, id) = Self::split_name(&name)?;
        let collection = self.get_collection(collection);
        let entry = collection.documents.entry(id.to_string());
        if !matches!(entry, Entry::Vacant(_)) {
            return Err(Status::already_exists(name));
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
}
