use std::{
    borrow::Cow,
    collections::HashMap,
    convert::Infallible,
    fmt::{Display, Write},
    str::FromStr,
};

use googleapis::google::firestore::v1::*;
use itertools::{Itertools, PeekingNext};

use super::document::StoredDocumentVersion;
use crate::{GenericDatabaseError, error::Result};

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

    pub(crate) fn validate_query_reference(&self) -> Result<()> {
        match self {
            Self::DocumentName => Ok(()),
            Self::FieldPath(p) => p.validate_non_reserved(),
        }
    }

    pub(crate) fn into_non_reserved_field_path(
        self,
        context_err: &'static str,
    ) -> Result<FieldPath> {
        match self {
            Self::DocumentName => Err(GenericDatabaseError::invalid_argument(context_err)),
            Self::FieldPath(p) => {
                p.validate_non_reserved()?;
                Ok(p)
            }
        }
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
        let parsed: FieldPath = path.parse()?;
        if parsed.0.len() == 1 && parsed.0[0] == DOC_NAME {
            Ok(Self::DocumentName)
        } else {
            Ok(Self::FieldPath(parsed))
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
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct FieldPath(Vec<String>);

impl FieldPath {
    pub(crate) fn starts_with(&self, prefix: &Self) -> bool {
        self.0.starts_with(&prefix.0)
    }

    fn validate_segments(&self, allow_doc_name: bool) -> Result<()> {
        for seg in &self.0 {
            if seg.is_empty() {
                return Err(GenericDatabaseError::invalid_argument(
                    "field path cannot contain empty segments",
                ));
            }
            if super::reference::is_reserved_name(seg) && !(allow_doc_name && seg == DOC_NAME) {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "the name {seg} is reserved"
                )));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_non_reserved(&self) -> Result<()> {
        self.validate_segments(false)
    }

    pub(crate) fn validate_non_reserved_allowing_doc_name(&self) -> Result<()> {
        self.validate_segments(true)
    }

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

fn is_simple_segment(seg: &str) -> bool {
    !seg.is_empty()
        && matches!(seg.as_bytes()[0], b'a'..=b'z' | b'A'..=b'Z' | b'_')
        && seg
            .bytes()
            .all(|b| matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'))
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (idx, seg) in self.0.iter().enumerate() {
            if idx > 0 {
                f.write_char('.')?;
            }
            if is_simple_segment(seg) {
                f.write_str(seg)?;
            } else {
                f.write_char('`')?;
                for ch in seg.chars() {
                    if matches!(ch, '`' | '\\') {
                        f.write_char('\\')?;
                    }
                    f.write_char(ch)?;
                }
                f.write_char('`')?;
            }
        }
        Ok(())
    }
}

fn parse_field_path(path: &str) -> Result<Vec<String>> {
    let mut iter = path.char_indices().peekable();
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
                let end_byte = iter
                    .by_ref()
                    .peeking_take_while(|&(_, ch)| ch != '.')
                    .last()
                    .map_or(start_pos + first_ch.len_utf8(), |(pos, ch)| {
                        pos + ch.len_utf8()
                    });
                segments.push(path[start_pos..end_byte].to_string());
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
    #[case("é",             &["é"])]
    #[case("café",          &["café"])]
    #[case("aéb",           &["aéb"])]
    #[case("α.β",           &["α", "β"])]
    #[case("`é`.foo",       &["é", "foo"])]
    #[case("`café`.price",  &["café", "price"])]
    #[case("🌃.é.αβ",       &["🌃", "é", "αβ"])]
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

    #[gtest]
    fn into_non_reserved_field_path_rejects_reserved_and_empty_segments() {
        let doc_name: FieldReference = DOC_NAME.parse().unwrap();
        assert_that!(
            doc_name.into_non_reserved_field_path("cannot be __name__"),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cannot be __name__"
            )))
        );

        let quoted_doc_name: FieldReference = "`__name__`".parse().unwrap();
        assert_eq!(quoted_doc_name, FieldReference::DocumentName);
        assert_that!(
            quoted_doc_name.into_non_reserved_field_path("cannot be __name__"),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cannot be __name__"
            )))
        );

        let nested_reserved: FieldReference = "a.__custom__.b".parse().unwrap();
        assert_that!(
            nested_reserved.into_non_reserved_field_path("cannot be __name__"),
            err(eq(&GenericDatabaseError::invalid_argument(
                "the name __custom__ is reserved"
            )))
        );

        let empty_seg: FieldReference = "a..b".parse().unwrap();
        assert_that!(
            empty_seg.into_non_reserved_field_path("cannot be __name__"),
            err(eq(&GenericDatabaseError::invalid_argument(
                "field path cannot contain empty segments"
            )))
        );

        let four_underscores: FieldReference = "____".parse().unwrap();
        let parsed = four_underscores
            .into_non_reserved_field_path("cannot be __name__")
            .unwrap();
        assert_eq!(&parsed.0, &["____"]);
        assert!(
            "a.b"
                .parse::<FieldPath>()
                .unwrap()
                .starts_with(&"a".parse::<FieldPath>().unwrap())
        );
    }

    #[gtest]
    fn field_path_orders_by_segment_and_validate_query_reference_allows_doc_name() {
        let a_z: FieldPath = "a.z".parse().unwrap();
        let a_dash_b: FieldPath = "a-b".parse().unwrap();
        assert!(a_z < a_dash_b);

        let escaped_dot: FieldPath = "`a.b`".parse().unwrap();
        let nested: FieldPath = "a.b".parse().unwrap();
        assert!(nested < escaped_dot);

        for ok in [DOC_NAME, "`__name__`", "____"] {
            assert!(
                ok.parse::<FieldReference>()
                    .unwrap()
                    .validate_query_reference()
                    .is_ok()
            );
        }
        for bad in [
            "__custom__",
            "__name__.a",
            "a.__name__",
            "a.__custom__",
            "a..b",
        ] {
            assert_that!(
                bad.parse::<FieldReference>()
                    .unwrap()
                    .validate_query_reference(),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    #[gtest]
    fn validate_non_reserved_allowing_doc_name_permits_doc_name_only() {
        for ok in ["a.__name__", "a.__name__.b", "__name__.a", "____"] {
            assert!(
                ok.parse::<FieldPath>()
                    .unwrap()
                    .validate_non_reserved_allowing_doc_name()
                    .is_ok()
            );
        }
        for bad in ["__custom__", "a.__custom__", "a.__custom__.b", "a..b"] {
            assert_that!(
                bad.parse::<FieldPath>()
                    .unwrap()
                    .validate_non_reserved_allowing_doc_name(),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    #[gtest]
    fn vector_is_not_traversed_or_mutated_as_map() {
        let mut fields = HashMap::from([("embedding".to_string(), Value::vector([1.0, 2.0]))]);

        let emb_path: FieldPath = "embedding".parse().unwrap();
        let value_subpath: FieldPath = "embedding.value".parse().unwrap();
        let type_subpath: FieldPath = "embedding.__type__".parse().unwrap();
        let nested_subpath: FieldPath = "embedding.nested".parse().unwrap();

        assert_that!(
            emb_path.get_value(&fields),
            some(eq(&Value::vector([1.0, 2.0])))
        );
        assert_that!(value_subpath.get_value(&fields), none());
        assert_that!(type_subpath.get_value(&fields), none());

        assert_that!(value_subpath.delete_value(&mut fields), none());
        assert_that!(type_subpath.delete_value(&mut fields), none());
        assert_that!(
            emb_path.get_value(&fields),
            some(eq(&Value::vector([1.0, 2.0])))
        );

        nested_subpath.set_value(&mut fields, Value::string("replaced"));
        assert_that!(
            emb_path.get_value(&fields),
            some(eq(&Value::map(HashMap::from([(
                "nested".to_string(),
                Value::string("replaced")
            )]))))
        );
    }

    #[gtest]
    fn field_path_display_quotes_non_simple_segments() {
        assert_eq!(FieldPath(vec!["".to_string()]).to_string(), "``");
        assert_eq!(
            FieldPath(vec!["a".to_string(), "".to_string(), "b".to_string()]).to_string(),
            "a.``.b"
        );
        assert_eq!(FieldPath(vec!["0abc".to_string()]).to_string(), "`0abc`");
        assert_eq!(FieldPath(vec!["a.b".to_string()]).to_string(), "`a.b`");
        assert_eq!(
            FieldPath(vec![r"bak`tik\end".to_string()]).to_string(),
            r"`bak\`tik\\end`"
        );
        assert_eq!(
            FieldPath(vec!["🔥".to_string(), "é".to_string(), "ok_1".to_string()]).to_string(),
            "`🔥`.`é`.ok_1"
        );
        assert_eq!(
            FieldPath(vec!["__name__".to_string()]).to_string(),
            "__name__"
        );
    }

    mod proptests {
        use std::collections::HashMap;

        use googleapis::google::firestore::v1::Value;
        use proptest::prelude::*;

        use super::{DOC_NAME, FieldPath, FieldReference};

        fn arb_segment() -> impl Strategy<Value = String> {
            prop_oneof![
                Just(String::new()),
                Just(DOC_NAME.to_string()),
                Just("0digit".to_string()),
                "[a-zA-Z0-9_`\\\\.& -]{1,8}",
                prop::collection::vec(
                    prop_oneof![
                        Just('\0'),
                        Just('.'),
                        Just('`'),
                        Just('\\'),
                        Just('0'),
                        Just('a'),
                        Just('_'),
                        Just('é'),
                        Just('🔥'),
                        any::<char>(),
                    ],
                    0..=8,
                )
                .prop_map(|chars| chars.into_iter().collect()),
            ]
        }

        proptest! {
            #[test]
            fn proptest_field_path_escaped_segments_roundtrip(
                segments in prop::collection::vec(arb_segment(), 1..=5),
                leaf_val in any::<i64>(),
            ) {
                let fp = FieldPath(segments.clone());
                let encoded = fp.to_string();
                let parsed: FieldPath = encoded.parse().unwrap();
                prop_assert_eq!(&parsed.0, &segments);

                // Exercise public get_value / set_value / delete_value at the parsed path.
                let mut root = HashMap::new();
                parsed.set_value(&mut root, Value::integer(leaf_val));
                prop_assert_eq!(parsed.get_value(&root), Some(&Value::integer(leaf_val)));
                let deleted = parsed.delete_value(&mut root);
                prop_assert_eq!(deleted, Some(Value::integer(leaf_val)));
                prop_assert_eq!(parsed.get_value(&root), None);
            }

            #[test]
            fn proptest_field_path_display_roundtrip_and_injectivity(
                segs1 in prop::collection::vec(arb_segment(), 1..=5),
                segs2 in prop::collection::vec(arb_segment(), 1..=5),
            ) {
                let p1 = FieldPath(segs1);
                let p2 = FieldPath(segs2);
                let s1 = p1.to_string();
                let s2 = p2.to_string();

                prop_assert_eq!(s1.parse::<FieldPath>().unwrap(), p1.clone());
                prop_assert_eq!(s2.parse::<FieldPath>().unwrap(), p2.clone());
                prop_assert_eq!(p1 == p2, s1 == s2);
            }

            #[test]
            fn proptest_field_path_malformed_inputs_rejected(
                body in "[a-zA-Z0-9_.& -]{0,8}",
                trailing in "[a-zA-Z0-9]",
            ) {
                prop_assert!("".parse::<FieldPath>().is_err());

                let unclosed = format!("`{body}");
                prop_assert!(unclosed.parse::<FieldPath>().is_err());

                let trailing_backslash = format!("`{body}\\");
                prop_assert!(trailing_backslash.parse::<FieldPath>().is_err());

                let junk_after_close = format!("`{body}`{trailing}");
                prop_assert!(junk_after_close.parse::<FieldPath>().is_err());
            }

            #[test]
            fn proptest_field_reference_doc_name_vs_field_path(
                seg in "[a-zA-Z_][a-zA-Z0-9_]{0,7}",
                non_simple in arb_segment(),
            ) {
                let doc_ref: FieldReference = DOC_NAME.parse().unwrap();
                prop_assert!(doc_ref.is_document_name());
                prop_assert_eq!(doc_ref.to_string(), DOC_NAME);

                // Quoted `__name__` also resolves to the document-name pseudo-field in queries.
                let quoted_doc_name: FieldReference = "`__name__`".parse().unwrap();
                prop_assert!(quoted_doc_name.is_document_name());
                prop_assert_eq!(quoted_doc_name.to_string(), DOC_NAME);

                if seg != DOC_NAME {
                    let parsed: FieldReference = seg.parse().unwrap();
                    prop_assert!(!parsed.is_document_name());
                    prop_assert_eq!(parsed.to_string(), seg);
                }

                let fp = FieldPath(vec![non_simple.clone()]);
                let formatted = fp.to_string();
                let parsed_ref: FieldReference = formatted.parse().unwrap();
                if non_simple == DOC_NAME {
                    prop_assert!(parsed_ref.is_document_name());
                } else {
                    prop_assert!(!parsed_ref.is_document_name());
                }
                prop_assert_eq!(parsed_ref.to_string(), formatted);
            }
        }
    }
}
