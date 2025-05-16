use googleapis::google::firestore::v1::{Document, DocumentMask, structured_query};
use itertools::Itertools;

use super::field_path::FieldReference;
use crate::{GenericDatabaseError, document::StoredDocumentVersion};

#[derive(Debug)]
pub struct Projection {
    fields: Vec<FieldReference>,
}

pub trait Project {
    fn project(&self, version: &StoredDocumentVersion) -> Document;
}

impl Project for Projection {
    fn project(&self, version: &StoredDocumentVersion) -> Document {
        let mut doc = Document {
            fields: Default::default(),
            create_time: Some(version.create_time),
            update_time: Some(version.update_time),
            name: version.name.to_string(),
        };
        for field in &self.fields {
            match field {
                FieldReference::DocumentName => continue,
                FieldReference::FieldPath(path) => {
                    if let Some(val) = path.get_value(&version.fields) {
                        path.set_value(&mut doc.fields, val.clone());
                    }
                }
            }
        }
        doc
    }
}

impl Project for Option<Projection> {
    fn project(&self, version: &StoredDocumentVersion) -> Document {
        match self {
            Some(projection) => projection.project(version),
            None => version.to_document(),
        }
    }
}

impl TryFrom<structured_query::Projection> for Projection {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::Projection) -> Result<Self, Self::Error> {
        let fields = value.fields.iter().map(TryInto::try_into).try_collect()?;
        Ok(Self { fields })
    }
}

impl TryFrom<DocumentMask> for Projection {
    type Error = GenericDatabaseError;

    fn try_from(value: DocumentMask) -> Result<Self, Self::Error> {
        let fields = value.field_paths.iter().map(|s| s.parse()).try_collect()?;
        Ok(Self { fields })
    }
}
