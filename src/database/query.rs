use self::filter::Filter;
use super::{document::StoredDocumentVersion, field_path::FieldReference};
use crate::{
    googleapis::google::firestore::v1::{structured_query::CollectionSelector, *},
    unimplemented, unimplemented_option,
};
use itertools::Itertools;
use std::{cmp, ops::Deref, sync::Arc};
use tonic::{Result, Status};

mod filter;

pub struct Query {
    parent: String,
    select: Option<Vec<FieldReference>>,
    from: Vec<CollectionSelector>,
    filter: Option<Filter>,
    order_by: Vec<Order>,
    limit: Option<usize>,
}

impl Query {
    pub fn from_structured(parent: String, query: StructuredQuery) -> Result<Self> {
        let StructuredQuery {
            select,
            from,
            r#where: filter,
            order_by,
            start_at,
            end_at,
            offset,
            limit,
        } = query;
        unimplemented_option!(start_at);
        unimplemented_option!(end_at);
        if offset != 0 {
            unimplemented!("offset")
        }
        let select = select
            .map(|projection| {
                projection
                    .fields
                    .iter()
                    .map(TryInto::try_into)
                    .try_collect()
            })
            .transpose()?;
        let filter: Option<Filter> = filter.map(TryInto::try_into).transpose()?;
        let mut order_by: Vec<Order> = order_by.into_iter().map(TryInto::try_into).try_collect()?;
        if order_by.is_empty() {
            if let Some(filter) = &filter {
                order_by.extend(
                    filter
                        .get_inequality_fields()
                        .into_iter()
                        .map(|field| Order {
                            field: field.clone(),
                            direction: Direction::Ascending,
                        }),
                );
            }
        }
        Ok(Self {
            parent,
            select,
            from,
            filter,
            order_by,
            limit: limit.map(|v| v as usize),
        })
    }

    pub fn includes_collection(&self, path: &str) -> bool {
        let Some(path) = path
            .strip_prefix(&self.parent)
            .and_then(|path| path.strip_prefix('/'))
        else {
            return false;
        };
        self.from.iter().any(|selector| {
            if selector.all_descendants {
                path.starts_with(&selector.collection_id)
            } else {
                path == selector.collection_id
            }
        })
    }

    pub fn includes_document(&self, doc: &StoredDocumentVersion) -> Result<bool> {
        if let Some(filter) = &self.filter {
            if !filter.eval(doc)? {
                return Ok(false);
            }
        }
        for order_by in &self.order_by {
            if order_by.field.get_value(doc).is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn limit(&self) -> usize {
        self.limit.unwrap_or(usize::MAX)
    }

    pub fn project(&self, version: &StoredDocumentVersion) -> Result<Document> {
        let Some(projection) = &self.select else {
            return Ok(version.to_document());
        };

        match &projection[..] {
            [] => Ok(version.to_document()),
            [FieldReference::DocumentName] => Ok(Document {
                fields: Default::default(),
                create_time: Some(version.create_time.clone()),
                update_time: Some(version.update_time.clone()),
                name: version.name.clone(),
            }),
            fields => {
                let mut doc: Document = Default::default();
                for field in fields {
                    let FieldReference::FieldPath(path) = field else {
                        continue;
                    };
                    if let Some(val) = path.get_value(&version.fields) {
                        path.set_value(&mut doc.fields, val.clone());
                    }
                }
                Ok(doc)
            }
        }
    }

    pub fn sort_fn(
        &self,
    ) -> (impl Fn(&Arc<StoredDocumentVersion>, &Arc<StoredDocumentVersion>) -> cmp::Ordering + '_)
    {
        use cmp::Ordering::*;
        let order_by = &self.order_by;
        move |a, b| {
            for order in order_by {
                let a = order
                    .field
                    .get_value(a)
                    .expect("fields used in order_by MUST also be used in filter");
                let b = order
                    .field
                    .get_value(b)
                    .expect("fields used in order_by MUST also be used in filter");
                let result = match a.cmp(&b) {
                    result @ (Less | Greater) => result,
                    Equal => continue,
                };
                return if matches!(order.direction, Direction::Descending) {
                    result.reverse()
                } else {
                    result
                };
            }
            Equal
        }
    }
}

struct Order {
    field: FieldReference,
    direction: Direction,
}

impl TryFrom<structured_query::Order> for Order {
    type Error = Status;

    fn try_from(value: structured_query::Order) -> Result<Self, Self::Error> {
        Ok(Self {
            field: value
                .field
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("order_by without field"))?
                .field_path
                .deref()
                .try_into()?,
            direction: value.direction().try_into()?,
        })
    }
}

enum Direction {
    Ascending,
    Descending,
}

impl TryFrom<structured_query::Direction> for Direction {
    type Error = Status;

    fn try_from(value: structured_query::Direction) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            structured_query::Direction::Unspecified => Err(Status::invalid_argument(
                "Invalid structured_query::Direction",
            )),
            structured_query::Direction::Ascending => Ok(Direction::Ascending),
            structured_query::Direction::Descending => Ok(Direction::Descending),
        }
    }
}
