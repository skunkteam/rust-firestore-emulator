use std::collections::HashMap;

use googleapis::google::firestore::v1::{Document, DocumentMask, Value, structured_query};
use itertools::Itertools;

use super::field_path::{FieldPath, FieldReference};
use crate::{GenericDatabaseError, document::StoredDocumentVersion};

#[derive(Clone, Debug)]
pub struct Projection {
    fields: Vec<FieldReference>,
}

pub trait Project {
    fn project(&self, version: &StoredDocumentVersion) -> Document;
}

fn project_fields(
    fields: &[FieldReference],
    version: &StoredDocumentVersion,
    distance: Option<(&FieldPath, f64)>,
) -> Document {
    let mut doc = Document {
        fields: HashMap::new(),
        create_time: Some(version.create_time),
        update_time: Some(version.update_time),
        name: version.name.to_string(),
    };
    for field in fields {
        let FieldReference::FieldPath(sel) = field else {
            continue;
        };
        if distance.is_none_or(|(dist_path, _)| !sel.starts_with(dist_path))
            && let Some(val) = sel.get_value(&version.fields)
        {
            sel.set_value(&mut doc.fields, val.clone());
        }
    }
    if let Some((dist_path, dist)) = distance
        && fields.iter().any(|field| match field {
            FieldReference::FieldPath(sel) => dist_path.starts_with(sel),
            FieldReference::DocumentName => false,
        })
    {
        dist_path.set_value(&mut doc.fields, Value::double(dist));
    }
    doc
}

pub(crate) fn project_with_distance(
    projection: &Option<Projection>,
    version: &StoredDocumentVersion,
    distance: Option<(&FieldPath, f64)>,
) -> Document {
    match projection {
        Some(proj) => project_fields(&proj.fields, version, distance),
        None => {
            let mut doc = version.to_document();
            if let Some((dist_path, dist)) = distance {
                dist_path.set_value(&mut doc.fields, Value::double(dist));
            }
            doc
        }
    }
}

impl Project for Projection {
    fn project(&self, version: &StoredDocumentVersion) -> Document {
        project_fields(&self.fields, version, None)
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};
    use googletest::prelude::*;

    use super::{Projection, project_with_distance};
    use crate::{
        database::field_path::{FieldPath, FieldReference},
        document::StoredDocumentVersion,
    };

    fn sample_version() -> StoredDocumentVersion {
        StoredDocumentVersion {
            name: "projects/p/databases/d/documents/col/doc1".parse().unwrap(),
            create_time: Timestamp {
                seconds: 10,
                nanos:   0,
            },
            update_time: Timestamp {
                seconds: 20,
                nanos:   0,
            },
            fields: HashMap::from([
                ("title".to_string(), Value::string("hello")),
                ("distance".to_string(), Value::double(999.0)),
                (
                    "meta".to_string(),
                    Value::map(HashMap::from([(
                        "author".to_string(),
                        Value::string("alice"),
                    )])),
                ),
            ]),
        }
    }

    fn make_projection(paths: &[&str]) -> Option<Projection> {
        Some(Projection {
            fields: paths
                .iter()
                .map(|p| p.parse::<FieldReference>().unwrap())
                .collect(),
        })
    }

    fn fp(path: &str) -> FieldPath {
        path.parse::<FieldPath>().unwrap()
    }

    #[gtest]
    fn unmasked_projection_injects_distance() {
        let version = sample_version();
        let dist_path = fp("distance");
        let doc = project_with_distance(&None, &version, Some((&dist_path, 0.25)));
        expect_eq!(doc.fields.get("title"), Some(&Value::string("hello")));
        expect_eq!(doc.fields.get("distance"), Some(&Value::double(0.25)));
        expect_eq!(
            doc.fields
                .get("meta")
                .and_then(|v| v.as_map())
                .and_then(|m| m.get("author")),
            Some(&Value::string("alice"))
        );
    }

    #[gtest]
    fn select_title_and_distance_injects_computed_distance() {
        let version = sample_version();
        let dist_path = fp("distance");
        let proj = make_projection(&["title", "distance"]);
        let doc = project_with_distance(&proj, &version, Some((&dist_path, 0.5)));
        expect_eq!(doc.fields.len(), 2);
        expect_eq!(doc.fields.get("title"), Some(&Value::string("hello")));
        expect_eq!(doc.fields.get("distance"), Some(&Value::double(0.5)));
    }

    #[gtest]
    fn select_title_only_masks_out_distance() {
        let version = sample_version();
        let dist_path = fp("distance");
        let proj = make_projection(&["title"]);
        let doc = project_with_distance(&proj, &version, Some((&dist_path, 0.5)));
        expect_eq!(doc.fields.len(), 1);
        expect_eq!(doc.fields.get("title"), Some(&Value::string("hello")));
        expect_that!(doc.fields.get("distance"), none());
    }

    #[gtest]
    fn select_document_name_only_returns_empty_fields() {
        let version = sample_version();
        let dist_path = fp("distance");
        let proj = make_projection(&["__name__"]);
        let doc = project_with_distance(&proj, &version, Some((&dist_path, 0.5)));
        expect_that!(doc.fields, is_empty());
    }

    #[gtest]
    fn select_parent_map_retains_nested_distance_and_siblings() {
        let version = sample_version();
        let dist_path = fp("meta.distance");
        let proj = make_projection(&["meta"]);
        let doc = project_with_distance(&proj, &version, Some((&dist_path, 1.25)));
        let meta = doc.fields.get("meta").and_then(|v| v.as_map()).unwrap();
        expect_eq!(meta.get("author"), Some(&Value::string("alice")));
        expect_eq!(meta.get("distance"), Some(&Value::double(1.25)));
    }

    #[gtest]
    fn select_descendant_field_returns_empty_when_distance_overwrites_ancestor() {
        let version = sample_version();
        let dist_path = fp("meta");
        let proj = make_projection(&["meta.author"]);
        let doc = project_with_distance(&proj, &version, Some((&dist_path, 0.75)));
        expect_that!(doc.fields, is_empty());
    }
}
