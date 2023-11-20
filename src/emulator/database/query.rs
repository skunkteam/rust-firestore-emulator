use crate::googleapis::google::firestore::v1::{
    structured_query::{filter::FilterType, *},
    value::ValueType,
    *,
};
use std::borrow::Cow;
use tonic::{Result, Status};

const NAME: &str = "__name__";
const REFERENCE_NAME_MIN_ID: &str = "__id-9223372036854775808__";

pub struct Query {
    parent: String,
    query: StructuredQuery,
}

impl Query {
    pub fn new(parent: String, query: StructuredQuery) -> Result<Self> {
        if !query.order_by.is_empty() {
            return Err(Status::unimplemented("order_by is not supported yet"));
        }
        if query.start_at.is_some() {
            return Err(Status::unimplemented("start_at is not supported yet"));
        }
        if query.end_at.is_some() {
            return Err(Status::unimplemented("end_at is not supported yet"));
        }
        if query.offset != 0 {
            return Err(Status::unimplemented("offset is not supported yet"));
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

    pub fn includes_document(&self, doc: &Document) -> Result<bool> {
        let Some(filter) = &self.query.r#where else {
            return Ok(true);
        };
        eval_filter(filter, doc)
    }

    pub fn limit(&self) -> usize {
        self.query.limit.unwrap_or(i32::MAX) as usize
    }

    pub fn project(&self, doc: &Document) -> Result<Document> {
        let Some(ref projection) = self.query.select else {
            return Ok(doc.clone());
        };

        match &projection.fields[..] {
            [] => Ok(doc.clone()),
            [single_ref] if single_ref.field_path == NAME => Ok(Document {
                fields: Default::default(),
                ..doc.clone()
            }),
            _ => Err(Status::unimplemented(
                "select with fields is not supported yet",
            )),
        }
    }
}

fn eval_filter(filter: &Filter, doc: &Document) -> Result<bool> {
    match &filter.filter_type {
        None => Ok(true),
        Some(FilterType::CompositeFilter(filter)) => eval_composite_filter(filter, doc),
        Some(FilterType::FieldFilter(filter)) => eval_field_filter(filter, doc),
        Some(FilterType::UnaryFilter(_filter)) => todo!("unary filter"),
    }
}

fn eval_composite_filter(filter: &CompositeFilter, doc: &Document) -> Result<bool> {
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

fn eval_field_filter(filter: &FieldFilter, doc: &Document) -> Result<bool> {
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

fn get_field<'a>(doc: &'a Document, field: &FieldReference) -> Option<Cow<'a, ValueType>> {
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
