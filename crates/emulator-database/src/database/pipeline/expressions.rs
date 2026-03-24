use googleapis::google::firestore::v1::{Document, Value, value::ValueType};

use super::get_document_value;
use crate::{error::Result, pipeline::function};

/// Recursively evaluates a `Value` Expression against a standard `Document`.
pub(super) fn evaluate<'a>(value: &'a Value, doc: &'a Document) -> Result<Value> {
    maybe_evaluate(value, doc).map(|v| v.unwrap_or_else(Value::null))
}

pub(super) fn maybe_evaluate<'a>(value: &'a Value, doc: &'a Document) -> Result<Option<Value>> {
    match value.value_type() {
        ValueType::FieldReferenceValue(f) => {
            let field_ref = f.parse()?;
            Ok(get_document_value(&field_ref, doc))
        }
        ValueType::FunctionValue(f) => function::evaluate(f, doc),
        _ => Ok(Some(value.clone())),
    }
}
