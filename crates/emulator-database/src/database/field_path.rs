use std::{borrow::Cow, collections::HashMap, convert::Infallible, fmt::Display, str::FromStr};

use googleapis::google::firestore::v1::*;
use itertools::{Itertools, PeekingNext};

use super::document::StoredDocumentVersion;
use crate::{error::Result, GenericDatabaseError};

/// The virtual field-name that represents the document-name.
pub const DOC_NAME: &str = "__name__";

/// A field reference as used in queries. Can refer either to the document name or to a field path.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub enum FieldReference {
    DocumentName,
    FieldPath(FieldPath),
}

impl FieldReference {
    /// Returns the value of the field this [`FieldReference`] points to in the given `doc`, if
    /// found. Returns [`None`] if the field is not found.
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
        value.field_path.parse()
    }
}

impl FromStr for FieldReference {
    type Err = GenericDatabaseError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        match path {
            DOC_NAME => Ok(Self::DocumentName),
            path => Ok(Self::FieldPath((path.parse())?)),
        }
    }
}

impl Display for FieldReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldReference::DocumentName => f.write_str(DOC_NAME),
            FieldReference::FieldPath(path) => path.fmt(f),
        }
    }
}

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
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    /// Walk the path as represented by this [`FieldPath`] into the given object fields and return
    /// the value found at the end of the path, if any. Also returns [`None`] if any intermediate
    /// value was absent or not an object.
    pub fn get_value<'a>(&self, fields: &'a HashMap<String, Value>) -> Option<&'a Value> {
        let (first, rest) = self.0.split_first()?;
        rest.iter()
            .try_fold(fields.get(first)?, |prev, key| prev.as_map()?.get(key))
    }

    /// Walk the path as represented by this [`FieldPath`] into the given object, making sure all
    /// intermediate steps are objects, setting the end of the path to the given value.
    pub fn set_value(&self, fields: &mut HashMap<String, Value>, new_value: Value) {
        self.transform_value(fields, |_| new_value);
    }

    /// Walk the path as represented by this [`FieldPath`] into the given object and takes the value
    /// that is found at the end of the path if any. Also returns [`None`] if any intermediate
    /// value was absent or not an object.
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

    /// Walk the path as represented by this [`FieldPath`] into the given object and transform the
    /// value that is found at the end of the path (if any) using the given function. Returns a
    /// reference to the transformed value.
    pub fn transform_value<'a>(
        &self,
        fields: &'a mut HashMap<String, Value>,
        transform: impl FnOnce(Option<Value>) -> Value,
    ) -> &'a Value {
        let Ok(value) =
            self.try_transform_value(fields, |val| Ok(transform(val)) as Result<_, Infallible>);
        value
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

impl FromStr for FieldPath {
    type Err = GenericDatabaseError;

    fn from_str(path: &str) -> Result<Self, Self::Err> {
        if path.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "invalid empty field path",
            ));
        }
        parse_field_path(path).map(Self)
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

fn parse_field_path(path: &str) -> Result<Vec<String>> {
    let mut iter = path.chars().enumerate().peekable();
    let mut segments = vec![];
    'next_segment: loop {
        // I'm not sure if empty segments are used like this, but this is what they would mean if
        // they were, I think.
        while iter.peeking_next(|&(_, ch)| ch == '.').is_some() {
            segments.push("".to_string());
        }

        let Some((start_pos, first_ch)) = iter.next() else {
            // When we enter the loop or after a `.` we explicitly expect another element, so if we
            // don't find any, we assume a single empty segment.
            segments.push("".to_string());
            return Ok(segments);
        };

        match first_ch {
            // Two possibilities, either the segment begins with a backtick with possibility to
            // escape, or it doesn't and we are sure we don't need to unescape.
            '`' => {
                let mut segment = String::new();
                while let Some((_, ch)) = iter.next() {
                    match ch {
                        // We expect only complete elements to be escaped with backticks,
                        // partial escaping (e.g. "ab`cd`ef") should not occur. This is checked
                        // here.
                        // Normally, we would be as lenient as possible when parsing input, but in
                        // this case, this is particularly important, because of the following bug:
                        // https://github.com/googleapis/nodejs-firestore/issues/2019
                        // which may result in wrong paths because of incorrect escaping in
                        // the Firestore SDK.
                        '`' => {
                            segments.push(segment);
                            match iter.next() {
                                None => return Ok(segments),
                                Some((_, '.')) => continue 'next_segment,
                                Some((pos, ch)) => {
                                    return Err(GenericDatabaseError::invalid_argument(format!(
                                        r"unexpected character {ch:?} after '`' in pos {pos} of field path: {path:?}"
                                    )));
                                }
                            }
                        }
                        '\\' => {
                            // Unconditionally add the next character if we can find it. Complain if
                            // we can't.
                            match iter.next() {
                                Some((_, ch)) => segment.push(ch),
                                None => {
                                    return Err(GenericDatabaseError::invalid_argument(format!(
                                        r"unexpected end after '\' in field path: {path:?}"
                                    )));
                                }
                            }
                        }
                        // All other characters are simply added to the current segment.
                        ch => segment.push(ch),
                    }
                }
                // If we get here, we've depleted the iterator before we got a closing '`'.
                return Err(GenericDatabaseError::invalid_argument(format!(
                    r"missing closing '`' in field path: {path:?}"
                )));
            }

            _ => {
                let end_pos = iter
                    .by_ref()
                    .peeking_take_while(|&(_, ch)| ch != '.')
                    .map(|(pos, _)| pos)
                    .last()
                    .unwrap_or(start_pos);
                segments.push(path[start_pos..=end_pos].to_string());
                // Consume the next '.' or return if we got at the end.
                if iter.next().is_none() {
                    return Ok(segments);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use rstest::rstest;

    use super::*;
    use crate::GenericDatabaseError;

    #[gtest]
    #[rstest]
    #[case("",              &[""])]
    #[case("``",            &[""])]
    #[case("a.b.c",         &["a", "b", "c"])]
    #[case("a.b.c.",        &["a", "b", "c", ""])]
    #[case("foo.x&y",       &["foo", "x&y"])]
    #[case(r"foo.x\y",      &["foo", r"x\y"])]
    #[case("foo.`x&y`",     &["foo", "x&y"])]
    #[case("foo.`x&y`.",    &["foo", "x&y", ""])]
    #[case("foo.`x&y`.``",  &["foo", "x&y", ""])]
    #[case("a.b.c",         &["a", "b", "c"])]
    #[case(r"`bak\`tik`.`x&y`",     &["bak`tik", "x&y"])]
    #[case(r"`bak.\`.tik`.`x&y`",   &["bak.`.tik", "x&y"])]
    #[case("a`b",           &["a`b"])] // Tricky case, should we allow this?
    fn test_parse_field_path_success(#[case] input: &str, #[case] result: &[&str]) {
        assert_that!(parse_field_path(input), ok(eq(result)));
    }

    #[gtest]
    #[rstest]
    #[case(
        r"`missing closing backslash",
        r#"missing closing '`' in field path: "`missing closing backslash""#
    )]
    #[case(
        r"`escaped closing backslash\`",
        r#"missing closing '`' in field path: "`escaped closing backslash\\`""#
    )]
    #[case(
        r"`unescaped `before end`",
        r#"unexpected character 'b' after '`' in pos 12 of field path: "`unescaped `before end`""#
    )]
    #[case(
        r"`end after \",
        r#"unexpected end after '\' in field path: "`end after \\""#
    )]
    fn test_parse_field_path_fail(#[case] input: &str, #[case] msg: &str) {
        assert_that!(
            parse_field_path(input),
            err(eq(&GenericDatabaseError::invalid_argument(msg)))
        );
    }
}
