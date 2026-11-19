use googleapis::google::firestore::v1::{Document, Value, value::ValueType};

use super::get_document_value;
use crate::{GenericDatabaseError, error::Result, pipeline::function};

/// Recursively evaluates a `Value` Expression against a standard `Document`.
pub(super) fn evaluate<'a>(value: &'a Value, doc: &'a Document) -> Result<Value> {
    maybe_evaluate(value, doc).map(|v| v.unwrap_or_else(Value::null))
}

pub(super) fn maybe_evaluate<'a>(value: &'a Value, doc: &'a Document) -> Result<Option<Value>> {
    let Some(vt) = value.value_type.as_ref() else {
        return Err(GenericDatabaseError::invalid_argument(
            "missing value_type in value",
        ));
    };
    match vt {
        ValueType::FieldReferenceValue(f) => {
            let field_ref = f.parse()?;
            Ok(get_document_value(&field_ref, doc))
        }
        ValueType::VariableReferenceValue(_) => crate::unimplemented!("variable_reference_value"),
        ValueType::FunctionValue(f) => function::evaluate(f, doc),
        _ => Ok(Some(value.clone())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variable_reference_value_returns_not_implemented() {
        let val = Value {
            value_type: Some(ValueType::VariableReferenceValue("v".to_string())),
        };
        let doc = Document::default();
        let err = maybe_evaluate(&val, &doc).unwrap_err();
        assert!(matches!(err, GenericDatabaseError::NotImplemented(_)));
    }
}
