use base64::{display::Base64Display, prelude::BASE64_STANDARD_NO_PAD};
use serde::Serializer;

pub(crate) fn as_base64<T: AsRef<[u8]>, S: Serializer>(v: T, s: S) -> Result<S::Ok, S::Error> {
    s.collect_str(&Base64Display::new(v.as_ref(), &BASE64_STANDARD_NO_PAD))
}

pub(crate) fn as_null<T, S: Serializer>(_v: T, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_unit()
}

#[cfg(test)]
mod tests {
    use prost::bytes::Bytes;

    use crate::google::firestore::v1::{value::ValueType, Value};

    #[test]
    fn serialization() {
        let values = [
            Value::null(),
            Value {
                value_type: Some(ValueType::BytesValue(Bytes::from_static(b"\xff\xff\xbe"))),
            },
        ];
        assert_eq!(serde_json::to_string(&values).unwrap(), r#"[null,"//++"]"#);
    }
}
