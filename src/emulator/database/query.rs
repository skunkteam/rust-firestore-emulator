use super::document::StoredDocumentVersion;
use crate::googleapis::google::firestore::v1::{
    structured_query::{filter::FilterType, *},
    value::ValueType,
    *,
};
use std::borrow::Cow;
use tonic::{Result, Status};

/// The virtual field-name that represents the document-name.
const NAME: &str = "__name__";

/// Datastore allowed numeric IDs where Firestore only allows strings. Numeric
/// IDs are exposed to Firestore as `__idNUM__`, so this is the lowest possible
/// negative numeric value expressed in that format.
///
/// This constant is used to specify startAt/endAt values when querying for all
/// descendants in a single collection.
const REFERENCE_NAME_MIN_ID: &str = "__id-9223372036854775808__";

pub struct Query {
    parent: String,
    query: StructuredQuery,
}

impl Query {
    pub fn new(parent: String, query: StructuredQuery) -> Result<Self> {
        if !query.order_by.is_empty() {
            unimplemented!("order_by");
        }
        if query.start_at.is_some() {
            unimplemented!("start_at");
        }
        if query.end_at.is_some() {
            unimplemented!("end_at");
        }
        if query.offset != 0 {
            unimplemented!("offset");
        }
        Ok(Self { parent, query })
    }

    pub fn includes_collection(&self, path: &str) -> bool {
        let Some(path) = path
            .strip_prefix(&self.parent)
            .and_then(|path| path.strip_prefix('/'))
        else {
            return false;
        };
        self.query.from.iter().any(|selector| {
            if selector.all_descendants {
                path.starts_with(&selector.collection_id)
            } else {
                path == selector.collection_id
            }
        })
    }

    pub fn includes_document(&self, doc: &StoredDocumentVersion) -> Result<bool> {
        let Some(filter) = &self.query.r#where else {
            return Ok(true);
        };
        eval_filter(filter, doc)
    }

    pub fn limit(&self) -> usize {
        self.query.limit.unwrap_or(i32::MAX) as usize
    }

    pub fn project(&self, doc: &StoredDocumentVersion) -> Result<Document> {
        let Some(ref projection) = self.query.select else {
            return Ok(doc.to_document());
        };

        match &projection.fields[..] {
            [] => Ok(doc.to_document()),
            [single_ref] if single_ref.field_path == NAME => Ok(Document {
                fields: Default::default(),
                create_time: Some(doc.create_time.clone()),
                update_time: Some(doc.update_time.clone()),
                name: doc.name.clone(),
            }),
            _ => unimplemented!("select with fields is not supported yet"),
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
    let field = filter
        .field
        .as_ref()
        .ok_or(Status::invalid_argument("FieldFilter without `field`"))?;
    let filter_value = filter
        .value
        .as_ref()
        .ok_or(Status::invalid_argument("FieldFilter without `value`"))?
        .value_type
        .as_ref()
        .ok_or(Status::invalid_argument("Value without `value_type`"))?;
    let Some(value) = get_field(doc, field) else {
        return Ok(false);
    };
    use field_filter::Operator::*;
    match filter.op() {
        Equal => equal(&value, filter_value),
        LessThan => less_than(&value, filter_value),
        LessThanOrEqual => Ok(!less_than(filter_value, &value)?),
        GreaterThan => less_than(filter_value, &value),
        GreaterThanOrEqual => Ok(!less_than(&value, filter_value)?),
        op => Err(Status::unimplemented(format!(
            "field_filter operation {} not implemented",
            op.as_str_name()
        ))),
    }
}

fn get_field<'a>(
    doc: &'a StoredDocumentVersion,
    field: &FieldReference,
) -> Option<Cow<'a, ValueType>> {
    if field.field_path == NAME {
        return Some(Cow::Owned(ValueType::ReferenceValue(doc.name.clone())));
    }
    todo!()
}

fn equal(a: &ValueType, b: &ValueType) -> Result<bool> {
    // TODO: This is not right yet...
    Ok(a == b)
    // match(a,b){
    //     // (ValueType::IntegerValue(a), ValueType::DoubleValue(b))
    //     (a,b) => Ok(a == b)
    // }
}

fn less_than(a: &ValueType, b: &ValueType) -> Result<bool> {
    use ValueType::*;
    match (a, b) {
        (ReferenceValue(a), ReferenceValue(b)) => Ok(prep_ref_for_cmp(a) < prep_ref_for_cmp(b)),
        _ => Err(Status::unimplemented(format!(
            "comparing {a:?} and {b:?} not yet implemented"
        ))),
    }
}

fn prep_ref_for_cmp(path: &str) -> Cow<str> {
    (|| -> Option<Cow<str>> {
        let path = path.strip_suffix(REFERENCE_NAME_MIN_ID)?;
        let collection = path.strip_suffix('/')?;
        let result = match collection.strip_suffix('\0') {
            Some(collection) => Cow::Owned(collection.to_string() + "@"),
            None => Cow::Borrowed(path),
        };
        Some(result)
    })()
    .unwrap_or(Cow::Borrowed(path))
}
