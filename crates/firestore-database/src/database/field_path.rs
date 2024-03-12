use std::{borrow::Cow, collections::HashMap, convert::Infallible, mem::take, ops::Deref};

use googleapis::google::firestore::v1::*;

use super::document::StoredDocumentVersion;
use crate::{error::Result, GenericDatabaseError};

/// The virtual field-name that represents the document-name.
pub const DOC_NAME: &str = "__name__";

#[derive(Clone, Debug)]
pub enum FieldReference {
    DocumentName,
    FieldPath(FieldPath),
}

impl FieldReference {
    pub fn get_value<'a>(&self, doc: &'a StoredDocumentVersion) -> Option<Cow<'a, Value>> {
        match self {
            Self::DocumentName => Some(Cow::Owned(Value::reference(doc.name.to_string()))),
            Self::FieldPath(field_path) => field_path.get_value(&doc.fields).map(Cow::Borrowed),
        }
    }

    /// Returns `true` if the field reference is [`DocumentName`].
    ///
    /// [`DocumentName`]: FieldReference::DocumentName
    #[must_use]
    pub fn is_document_name(&self) -> bool {
        matches!(self, Self::DocumentName)
    }
}

impl TryFrom<&structured_query::FieldReference> for FieldReference {
    type Error = GenericDatabaseError;

    fn try_from(value: &structured_query::FieldReference) -> Result<Self, Self::Error> {
        value.field_path.deref().try_into()
    }
}

impl TryFrom<&str> for FieldReference {
    type Error = GenericDatabaseError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        match path {
            DOC_NAME => Ok(Self::DocumentName),
            path => Ok(Self::FieldPath((path.try_into())?)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct FieldPath(Vec<String>);

/// Field paths may be used to refer to structured fields of Documents.
/// For `map_value`, the field path is represented by the simple
/// or quoted field names of the containing fields, delimited by `.`. For
/// example, the structured field
/// `"foo" : { map_value: { "x&y" : { string_value: "hello" }}}` would be
/// represented by the field path `foo.x&y`.
///
/// Within a field path, a quoted field name starts and ends with `` ` `` and
/// may contain any character. Some characters, including `` ` ``, must be
/// escaped using a `\`. For example, `` `x&y` `` represents `x&y` and
/// `` `bak\`tik` `` represents `` bak`tik ``.
impl FieldPath {
    pub fn get_value<'a>(&self, fields: &'a HashMap<String, Value>) -> Option<&'a Value> {
        let (first, rest) = self.0.split_first()?;
        rest.iter()
            .try_fold(fields.get(first)?, |prev, key| prev.as_map()?.get(key))
    }

    pub fn set_value(&self, fields: &mut HashMap<String, Value>, new_value: Value) {
        self.transform_value(fields, |_| new_value);
    }

    pub fn delete_value(&self, fields: &mut HashMap<String, Value>) -> Option<Value> {
        match &self.0[..] {
            [] => unreachable!(),
            [key] => fields.remove(key),
            [first, path @ .., last] => path
                .iter()
                .try_fold(fields.get_mut(first)?, |prev, key| {
                    prev.as_map_mut()?.get_mut(key)
                })?
                .as_map_mut()?
                .remove(last),
        }
    }

    pub fn transform_value<'a>(
        &self,
        fields: &'a mut HashMap<String, Value>,
        transform: impl FnOnce(Option<Value>) -> Value,
    ) -> &'a Value {
        self.try_transform_value(fields, |val| Ok(transform(val)) as Result<_, Infallible>)
            .unwrap()
    }

    pub fn try_transform_value<'a, E>(
        &self,
        fields: &'a mut HashMap<String, Value>,
        transform: impl FnOnce(Option<Value>) -> Result<Value, E>,
    ) -> Result<&'a Value, E> {
        {
            let fields: &'a mut HashMap<String, Value> = fields;
            let (last, path) = self.0.split_last().unwrap();
            let fields = path.iter().fold(fields, |parent, key| {
                parent
                    .entry(key.to_string())
                    .and_modify(|cur| {
                        if cur.as_map().is_none() {
                            *cur = Value::map(Default::default())
                        }
                    })
                    .or_insert_with(|| Value::map(Default::default()))
                    .as_map_mut()
                    .unwrap()
            });
            let new_value = transform(fields.remove(last))?;
            Ok(fields.entry(last.to_string()).or_insert(new_value))
        }
    }
}

impl TryFrom<&str> for FieldPath {
    type Error = GenericDatabaseError;

    fn try_from(path: &str) -> Result<Self, Self::Error> {
        if path.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "invalid empty field path",
            ));
        }
        Ok(Self(parse_field_path(path)?))
    }
}

fn parse_field_path(path: &str) -> Result<Vec<String>> {
    let mut elements = vec![];
    let mut cur_element = String::new();
    let mut iter = path.chars();
    let mut inside_backticks = false;
    while let Some(ch) = iter.next() {
        if inside_backticks {
            match ch {
                '`' => {
                    inside_backticks = false;
                    if !matches!(iter.next(), Some('.') | None) {
                        return Err(GenericDatabaseError::invalid_argument(format!(
                            "invalid field path: {path}"
                        )));
                    }
                    elements.push(take(&mut cur_element));
                }
                '\\' => cur_element.push(iter.next().ok_or_else(|| {
                    GenericDatabaseError::invalid_argument(format!("invalid field path: {path}"))
                })?),
                ch => cur_element.push(ch),
            }
        } else {
            match ch {
                '.' => elements.push(take(&mut cur_element)),
                '`' => inside_backticks = true,
                ch => cur_element.push(ch),
            }
        }
    }
    if !cur_element.is_empty() {
        elements.push(cur_element);
    }
    Ok(elements)
}

#[cfg(test)]
mod tests {
    use crate::database::field_path::parse_field_path;

    #[test]
    fn test_parse_field_path() {
        assert_eq!(parse_field_path("a.b.c").unwrap(), ["a", "b", "c"]);
        assert_eq!(parse_field_path("foo.x&y").unwrap(), ["foo", "x&y"]);
        assert_eq!(parse_field_path("foo.`x&y`").unwrap(), ["foo", "x&y"]);
        assert_eq!(
            parse_field_path(r"`bak\`tik`.`x&y`").unwrap(),
            ["bak`tik", "x&y"]
        );
        assert_eq!(
            parse_field_path(r"`bak.\`.tik`.`x&y`").unwrap(),
            ["bak.`.tik", "x&y"]
        );
    }
}
