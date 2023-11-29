use super::{document::StoredDocumentVersion, field_path::FieldReference};
use crate::{
    googleapis::google::firestore::v1::{
        structured_query::{
            composite_filter, field_filter, filter::FilterType, CollectionSelector,
            CompositeFilter, FieldFilter, Filter,
        },
        *,
    },
    unimplemented, unimplemented_option,
};
use itertools::Itertools;
use std::{cmp, ops::Deref, sync::Arc};
use tonic::{Result, Status};

pub struct Query {
    parent: String,
    select: Option<Vec<FieldReference>>,
    from: Vec<CollectionSelector>,
    filter: Option<Filter>, // TODO: create own Filter type with precompiled field references
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
        Ok(Self {
            parent,
            select: select
                .map(|projection| {
                    projection
                        .fields
                        .iter()
                        .map(TryInto::try_into)
                        .try_collect()
                })
                .transpose()?,
            from,
            filter,
            order_by: order_by.into_iter().map(TryInto::try_into).try_collect()?,
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
            if !eval_filter(filter, doc)? {
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

    pub fn project(&self, doc: &StoredDocumentVersion) -> Result<Document> {
        let Some(ref projection) = self.select else {
            return Ok(doc.to_document());
        };

        match &projection[..] {
            [] => Ok(doc.to_document()),
            [FieldReference::DocumentName] => Ok(Document {
                fields: Default::default(),
                create_time: Some(doc.create_time.clone()),
                update_time: Some(doc.update_time.clone()),
                name: doc.name.clone(),
            }),
            _ => unimplemented!("select with fields"),
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
                let result = match order.field.get_value(a).cmp(&order.field.get_value(b)) {
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

fn eval_filter(filter: &Filter, doc: &StoredDocumentVersion) -> Result<bool> {
    match &filter.filter_type {
        None => Ok(true),
        Some(FilterType::CompositeFilter(filter)) => eval_composite_filter(filter, doc),
        Some(FilterType::FieldFilter(filter)) => eval_field_filter(filter, doc),
        Some(FilterType::UnaryFilter(_filter)) => todo!("unary filter"),
    }
}

fn eval_composite_filter(filter: &CompositeFilter, doc: &StoredDocumentVersion) -> Result<bool> {
    use composite_filter::Operator::*;
    let op = filter.op();
    for filter in &filter.filters {
        match (op, eval_filter(filter, doc)?) {
            (And, true) | (Or, false) => continue,
            (And, false) => return Ok(false),
            (Or, true) => return Ok(true),
            (Unspecified, _) => {
                return Err(Status::unimplemented(format!(
                    "composite_filter operation {} not implemented",
                    op.as_str_name()
                )))
            }
        }
    }
    Ok(op == And)
}

fn eval_field_filter(filter: &FieldFilter, doc: &StoredDocumentVersion) -> Result<bool> {
    let field: FieldReference = filter
        .field
        .as_ref()
        .ok_or(Status::invalid_argument("FieldFilter without `field`"))?
        .try_into()?;
    let filter_value = filter
        .value
        .as_ref()
        .ok_or(Status::invalid_argument("FieldFilter without `value`"))?;
    let Some(value) = field.get_value(doc) else {
        return Ok(false);
    };
    let value = value.as_ref();
    use field_filter::Operator::*;
    match filter.op() {
        Equal => Ok(value == filter_value),
        LessThan => Ok(value < filter_value),
        LessThanOrEqual => Ok(value <= filter_value),
        GreaterThan => Ok(value > filter_value),
        GreaterThanOrEqual => Ok(value >= filter_value),
        op => Err(Status::unimplemented(format!(
            "field_filter operation {} not implemented",
            op.as_str_name()
        ))),
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
