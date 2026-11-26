use std::{cmp, fmt::Display, iter, str::FromStr};

use string_cache::DefaultAtom;

use crate::GenericDatabaseError;

macro_rules! impl_serde_display_from_str {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl serde::Serialize for $ty {
                fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                where
                    S: serde::Serializer,
                {
                    serializer.collect_str(self)
                }
            }

            impl<'de> serde::Deserialize<'de> for $ty {
                fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
                where
                    D: serde::Deserializer<'de>,
                {
                    struct FromStrVisitor;

                    impl serde::de::Visitor<'_> for FromStrVisitor {
                        type Value = $ty;

                        fn expecting(
                            &self,
                            formatter: &mut std::fmt::Formatter<'_>,
                        ) -> std::fmt::Result {
                            formatter.write_str("a Firestore resource reference string")
                        }

                        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                        where
                            E: serde::de::Error,
                        {
                            v.parse().map_err(E::custom)
                        }
                    }

                    deserializer.deserialize_str(FromStrVisitor)
                }
            }
        )+
    };
}

pub(crate) fn is_reserved_name(s: &str) -> bool {
    s.starts_with("__") && s.ends_with("__") && s.len() >= 5
}

pub(crate) fn is_valid_segment_base(s: &str) -> bool {
    !s.is_empty() && !s.contains('/') && s != "." && s != ".." && s.len() <= 1500
}

fn is_valid_root_segment(s: &str) -> bool {
    is_valid_segment_base(s) && !is_reserved_name(s)
}

pub(crate) fn is_datastore_numeric_id(s: &str) -> bool {
    s.strip_prefix("__id")
        .and_then(|rest| rest.strip_suffix("__"))
        .is_some_and(|n| {
            (n == "0" || (!n.starts_with(['+', '0']) && !n.starts_with("-0")))
                && n.parse::<i64>().is_ok()
        })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InvalidSegmentReason {
    Empty,
    ContainsSlash,
    DotSegment,
    TooLong,
    ReservedName,
}

/// Validates a single collection or document ID segment according to Firestore resource-name
/// rules. Returns `Ok(true)` if the segment is the Datastore sentinel `__id0__`.
fn validate_segment(seg: &str, allow_datastore_id: bool) -> Result<bool, InvalidSegmentReason> {
    if seg.is_empty() {
        return Err(InvalidSegmentReason::Empty);
    }
    if seg.contains('/') {
        return Err(InvalidSegmentReason::ContainsSlash);
    }
    if seg == "." || seg == ".." {
        return Err(InvalidSegmentReason::DotSegment);
    }
    if seg.len() > 1500 {
        return Err(InvalidSegmentReason::TooLong);
    }
    if is_reserved_name(seg) {
        if allow_datastore_id && is_datastore_numeric_id(seg) {
            return Ok(seg == "__id0__");
        }
        return Err(InvalidSegmentReason::ReservedName);
    }
    Ok(false)
}

pub(crate) fn validate_document_id_segment(document_id: &str) -> crate::error::Result<()> {
    match validate_segment(document_id, false) {
        Ok(_) => Ok(()),
        Err(InvalidSegmentReason::Empty) => Err(GenericDatabaseError::invalid_argument(
            "document_id is required",
        )),
        Err(InvalidSegmentReason::ContainsSlash) => Err(GenericDatabaseError::invalid_argument(
            "document_id cannot contain '/'",
        )),
        Err(InvalidSegmentReason::DotSegment) => Err(GenericDatabaseError::invalid_argument(
            "document_id cannot be '.' or '..'",
        )),
        Err(InvalidSegmentReason::TooLong) => Err(GenericDatabaseError::invalid_argument(
            "document_id cannot exceed 1500 bytes",
        )),
        Err(InvalidSegmentReason::ReservedName) => Err(GenericDatabaseError::invalid_argument(
            format!("document_id cannot be a reserved name: {document_id}"),
        )),
    }
}

fn parse_resource_path(
    s: &str,
    allow_datastore_id: bool,
) -> Option<(&str, &str, &str, usize, bool)> {
    // Fixed prefix "projects/"
    let s = s.strip_prefix("projects/")?;
    // Next is the project_id until the next "/"
    let (project_id, s) = s.split_once('/')?;
    if !is_valid_root_segment(project_id) {
        return None;
    }
    // Fixed token "databases/".
    let s = s.strip_prefix("databases/")?;
    // Next is the database_id until the next "/" or the end of the string
    let (database_id, s) = match s.split_once('/') {
        Some((database_id, rest)) => (database_id, rest.strip_prefix("documents")?),
        None => (s, ""),
    };
    if !is_valid_root_segment(database_id) {
        return None;
    }
    if s.is_empty() {
        return Some((project_id, database_id, "", 0, false));
    }
    let rel_path = s.strip_prefix('/')?;
    let mut slash_count = 0;
    let mut has_zero_datastore_id = false;
    for (idx, seg) in rel_path.split('/').enumerate() {
        if idx > 0 {
            slash_count += 1;
        }
        match validate_segment(seg, allow_datastore_id && idx % 2 == 1) {
            Ok(is_zero_id) => has_zero_datastore_id |= is_zero_id,
            Err(_) => return None,
        }
    }
    Some((
        project_id,
        database_id,
        rel_path,
        slash_count,
        has_zero_datastore_id,
    ))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QueryRefMeta<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) database_id: &'a str,
    pub(crate) has_doc_path: bool,
    pub(crate) has_zero_datastore_id: bool,
}

pub(crate) fn parse_query_ref_root(s: &str) -> crate::error::Result<QueryRefMeta<'_>> {
    match parse_resource_path(s, true) {
        Some((project_id, database_id, "", _, _)) => Ok(QueryRefMeta {
            project_id,
            database_id,
            has_doc_path: false,
            has_zero_datastore_id: false,
        }),
        Some((project_id, database_id, _, slash_count, has_zero_datastore_id))
            if slash_count % 2 == 1 =>
        {
            Ok(QueryRefMeta {
                project_id,
                database_id,
                has_doc_path: true,
                has_zero_datastore_id,
            })
        }
        _ => Err(GenericDatabaseError::invalid_argument(format!(
            "Invalid document reference: {s}"
        ))),
    }
}

/// A reference to either the database root (including project name and database name), a single
/// collection or a single document.
///
/// # Examples
///
/// ```
/// # use emulator_database::reference::*;
/// # use emulator_database::*;
/// # fn main() -> Result<(), GenericDatabaseError> {
/// assert_eq!(
///     "projects/my-project/databases/my-database".parse::<Ref>()?,
///     Ref::Root(RootRef::new("my-project", "my-database")),
/// );
/// assert_eq!(
///     "projects/my-project/databases/my-database/documents/my-collection".parse::<Ref>()?,
///     Ref::Collection(CollectionRef::new(
///         RootRef::new("my-project", "my-database"),
///         "my-collection"
///     ))
/// );
/// assert_eq!(
///     "projects/my-project/databases/my-database/documents/my-collection/my-document"
///         .parse::<Ref>()?,
///     Ref::Document(DocumentRef::new(
///         CollectionRef::new(RootRef::new("my-project", "my-database"), "my-collection"),
///         "my-document"
///     ))
/// );
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Ref {
    Root(RootRef),
    Collection(CollectionRef),
    Document(DocumentRef),
}

impl Ref {
    pub fn as_root(&self) -> Option<&RootRef> {
        if let Self::Root(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_collection(&self) -> Option<&CollectionRef> {
        if let Self::Collection(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_document(&self) -> Option<&DocumentRef> {
        if let Self::Document(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn root(&self) -> &RootRef {
        match self {
            Ref::Root(root) => root,
            Ref::Collection(col) => &col.root_ref,
            Ref::Document(doc) => &doc.collection_ref.root_ref,
        }
    }

    /// Returns whether `self` is a (grand)parent of `other`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let root: Ref = "projects/p/databases/d/documents".parse()?;
    /// let collection: Ref = "projects/p/databases/d/documents/collection".parse()?;
    /// let document: Ref = "projects/p/databases/d/documents/collection/document".parse()?;
    /// assert!(root.is_parent_of(&collection));
    /// assert!(root.is_parent_of(&document));
    /// assert!(collection.is_parent_of(&document));
    /// assert!(!root.is_parent_of(&root));
    /// assert!(!document.is_parent_of(&root));
    /// assert!(!collection.is_parent_of(&collection));
    ///
    /// let doc_in_other_col: Ref = "projects/p/databases/d/documents/OTHER/document".parse()?;
    /// assert!(!collection.is_parent_of(&doc_in_other_col));
    /// # Ok(())
    /// # }
    /// ```
    pub fn is_parent_of(&self, other: &Ref) -> bool {
        if self.root() != other.root() {
            return false;
        }
        match (self, other) {
            (_, Ref::Root(_)) => false,
            (Ref::Root(_), _) => true,
            (Ref::Collection(parent), Ref::Collection(child)) => {
                child.strip_collection_prefix(parent).is_some()
            }
            (Ref::Collection(parent), Ref::Document(child)) => {
                &child.collection_ref == parent
                    || child
                        .collection_ref
                        .strip_collection_prefix(parent)
                        .is_some()
            }
            (Ref::Document(parent), Ref::Collection(child)) => {
                child.strip_document_prefix(parent).is_some()
            }
            (Ref::Document(parent), Ref::Document(child)) => {
                child.collection_ref.strip_document_prefix(parent).is_some()
            }
        }
    }
}

impl Display for Ref {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Ref::Root(r) => r.fmt(f),
            Ref::Collection(r) => r.fmt(f),
            Ref::Document(r) => r.fmt(f),
        }
    }
}

impl FromStr for Ref {
    type Err = GenericDatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        fn parse_ref(s: &str) -> Option<Ref> {
            let (project_id, database_id, rel_path, slash_count, _) =
                parse_resource_path(s, false)?;
            let root_ref = RootRef::new(project_id, database_id);
            if rel_path.is_empty() {
                return Some(Ref::Root(root_ref));
            }
            if slash_count.is_multiple_of(2) {
                Some(Ref::Collection(CollectionRef::new(root_ref, rel_path)))
            } else {
                let (collection_id, document_id) = rel_path.rsplit_once('/')?;
                Some(Ref::Document(DocumentRef::new(
                    CollectionRef::new(root_ref, collection_id),
                    document_id,
                )))
            }

            // TODO: add checks:
            // - Maximum depth of subcollections    100
            // - Maximum size for a document name   6 KiB
            // - Constraints on collection IDs
            //    - Must be valid UTF-8 characters
            //    - Cannot contain a forward slash (/)
            // - Constraints on document IDs
            //    - Must be valid UTF-8 characters
            //    - Cannot contain a forward slash (/)
            //    - (If you import Datastore entities into a Firestore database, numeric entity IDs
            //      are exposed as __id[0-9]+__)
        }

        parse_ref(s).ok_or_else(|| {
            GenericDatabaseError::InvalidReference("database/collection/document", s.to_string())
        })
    }
}

impl From<RootRef> for Ref {
    fn from(v: RootRef) -> Self {
        Self::Root(v)
    }
}

impl From<DocumentRef> for Ref {
    fn from(v: DocumentRef) -> Self {
        Self::Document(v)
    }
}

impl From<CollectionRef> for Ref {
    fn from(v: CollectionRef) -> Self {
        Self::Collection(v)
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct RootRef {
    pub project_id:  DefaultAtom,
    pub database_id: DefaultAtom,
}

impl FromStr for RootRef {
    type Err = GenericDatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rf: Ref = s.parse()?;
        rf.as_root()
            .cloned()
            .ok_or_else(|| GenericDatabaseError::InvalidReference("database", s.to_string()))
    }
}

impl Display for RootRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "projects/{}/databases/{}/documents",
            self.project_id, self.database_id,
        )
    }
}

impl RootRef {
    pub fn new(project_id: impl Into<DefaultAtom>, database_id: impl Into<DefaultAtom>) -> Self {
        Self {
            project_id:  project_id.into(),
            database_id: database_id.into(),
        }
    }

    pub fn try_new(project_id: &str, database_id: &str) -> crate::error::Result<Self> {
        if !is_valid_root_segment(project_id) || !is_valid_root_segment(database_id) {
            return Err(GenericDatabaseError::InvalidReference(
                "database",
                format!("projects/{project_id}/databases/{database_id}"),
            ));
        }
        Ok(Self::new(project_id, database_id))
    }
}

/// A reference to a Firestore collection or subcollection.
///
/// Ordered lexicographically by `(root_ref, collection_id segments)` using segment-wise `'/'`
/// comparison rather than flat-string byte ordering so subcollections compare identically to
/// Cloud Firestore resource ordering (for example, `parents/doc/sub` sorts before
/// `parents/doc-1/sub` because `"doc" < "doc-1"`, even though `'-' < '/'` in ASCII).
///
/// Direct `.split('/')` segment comparison is used instead of `googleapis::cmp_reference_segments`
/// so [`Ord`] remains strictly coherent with [`Eq`] and [`Hash`] across all `pub`-constructed
/// instances without `REFERENCE_NAME_MIN_ID` (`"__id-9223372036854775808__"`) sentinel
/// normalization; sentinel normalization lives in `googleapis::cmp_reference_*` for query
/// `ReferenceValue` comparisons, while valid parsed references reject `__.*__` and empty
/// segments.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct CollectionRef {
    pub root_ref:      RootRef,
    pub collection_id: DefaultAtom,
}

impl Ord for CollectionRef {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.root_ref != other.root_ref {
            return self.root_ref.cmp(&other.root_ref);
        }
        if self.collection_id == other.collection_id {
            return cmp::Ordering::Equal;
        }
        self.collection_id
            .split('/')
            .cmp(other.collection_id.split('/'))
    }
}

impl PartialOrd for CollectionRef {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for CollectionRef {
    type Err = GenericDatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rf: Ref = s.parse()?;
        rf.as_collection()
            .cloned()
            .ok_or_else(|| GenericDatabaseError::InvalidReference("collection", s.to_string()))
    }
}

impl Display for CollectionRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.root_ref, self.collection_id,)
    }
}

impl CollectionRef {
    pub fn new(root_ref: RootRef, collection_id: impl Into<DefaultAtom>) -> Self {
        Self {
            root_ref,
            collection_id: collection_id.into(),
        }
    }

    /// Returns the remaining canonical reference string when the given reference `r` has been
    /// removed as prefix. Can also be used to efficiently determine whether the given `r` is a
    /// parent of this collection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let parent: Ref = "projects/p/databases/d/documents/parent".parse()?;
    /// let child: CollectionRef = "projects/p/databases/d/documents/parent/doc/child".parse()?;
    /// assert_eq!(child.strip_prefix(&parent), Some("doc/child"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn strip_prefix(&self, r: &Ref) -> Option<&str> {
        match r {
            Ref::Root(root) => self.strip_root_prefix(root),
            Ref::Collection(col) => self.strip_collection_prefix(col),
            Ref::Document(doc) => self.strip_document_prefix(doc),
        }
    }

    /// Returns the remaining canonical reference string when the given `root` has been removed as
    /// prefix. Note that this becomes a no-op in release mode, because we assume that references
    /// from different databases are never compared to each other.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let parent: RootRef = "projects/p/databases/d/documents".parse()?;
    /// let child: CollectionRef = "projects/p/databases/d/documents/parent/doc/child".parse()?;
    /// assert_eq!(child.strip_root_prefix(&parent), Some("parent/doc/child"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn strip_root_prefix(&self, root: &RootRef) -> Option<&str> {
        (&self.root_ref == root).then_some(&self.collection_id)
    }

    /// Returns the remaining canonical reference string when the given `col` has been removed as
    /// prefix. Can also be used to efficiently determine whether the given `col` is a parent of
    /// this collection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let parent: CollectionRef = "projects/p/databases/d/documents/parent".parse()?;
    /// let child: CollectionRef = "projects/p/databases/d/documents/parent/doc/child".parse()?;
    /// assert_eq!(child.strip_collection_prefix(&parent), Some("doc/child"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn strip_collection_prefix(&self, col: &CollectionRef) -> Option<&str> {
        let rest = self
            .strip_root_prefix(&col.root_ref)?
            .strip_prefix(col.collection_id.as_str())?
            .strip_prefix('/')?;
        Some(rest)
    }

    /// Returns the remaining canonical reference string when the given `doc` has been removed as
    /// prefix. Can also be used to efficiently determine whether the given `doc` is a parent of
    /// this collection.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let parent: DocumentRef = "projects/p/databases/d/documents/parent/doc".parse()?;
    /// let child: CollectionRef = "projects/p/databases/d/documents/parent/doc/child".parse()?;
    /// assert_eq!(child.strip_document_prefix(&parent), Some("child"));
    /// # Ok(())
    /// # }
    /// ```
    pub fn strip_document_prefix(&self, doc: &DocumentRef) -> Option<&str> {
        let rest = self
            .strip_collection_prefix(&doc.collection_ref)?
            .strip_prefix(doc.document_id.as_str())?
            .strip_prefix('/')?;
        Some(rest)
    }

    /// Returns the parent of the collection ref, is either a DocumentRef or a RootRef.
    ///
    /// # Examples
    ///
    /// ```
    /// # use emulator_database::reference::*;
    /// # use emulator_database::*;
    /// # fn main() -> Result<(), GenericDatabaseError> {
    /// let collection: CollectionRef = "projects/p/databases/d/documents/parent/doc/child".parse()?;
    /// let parent: Ref = "projects/p/databases/d/documents/parent/doc".parse()?;
    /// assert_eq!(collection.parent(), parent);
    ///
    /// let collection: CollectionRef = "projects/p/databases/d/documents/parent".parse()?;
    /// let parent: Ref = "projects/p/databases/d/documents".parse()?;
    /// assert_eq!(collection.parent(), parent);
    /// # Ok(())
    /// # }
    /// ```
    pub fn parent(&self) -> Ref {
        let Some((parent_doc, _)) = self.collection_id.rsplit_once('/') else {
            return Ref::Root(self.root_ref.clone());
        };
        let (collection_id, document_id) = parent_doc
            .rsplit_once('/')
            .expect("CollectionRef should always contain pairs of slashes");
        Ref::Document(DocumentRef::new(
            CollectionRef::new(self.root_ref.clone(), collection_id),
            document_id,
        ))
    }
}

/// A reference to a Firestore document.
///
/// Ordered lexicographically by `(root_ref, path segments)` — chaining
/// `collection_ref.collection_id.split('/')` with `document_id` — rather than comparing
/// `collection_ref` first or comparing flat paths byte-by-byte. This matches live Cloud
/// Firestore document ordering across subcollection nesting and punctuation (for example,
/// `col/doc < col/doc/sub/x < col/doc-1 < col/doc-1/sub/x`).
///
/// Same-collection comparisons short-circuit via `O(1)` `DefaultAtom` equality checks to
/// `document_id` ordering without `.split('/')` overhead on `BTreeMap` hot paths.
///
/// Direct `.split('/')` segment comparison is used instead of `googleapis::cmp_reference_segments`
/// so [`Ord`] remains strictly coherent with [`Eq`] and [`Hash`] (and transitive with the
/// same-collection fast path) across all `pub`-constructed instances. `REFERENCE_NAME_MIN_ID`
/// (`"__id-9223372036854775808__"`) sentinel normalization lives in `googleapis::cmp_reference_*`
/// for query `ReferenceValue` comparisons; valid parsed `DocumentRef`s forbid `__.*__` and empty
/// segments, so [`Ord`] matches Cloud Firestore order on all valid document references.
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct DocumentRef {
    pub collection_ref: CollectionRef,
    pub document_id:    DefaultAtom,
}

impl Ord for DocumentRef {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        if self.collection_ref == other.collection_ref {
            return if self.document_id == other.document_id {
                cmp::Ordering::Equal
            } else {
                self.document_id.cmp(&other.document_id)
            };
        }
        if self.collection_ref.root_ref != other.collection_ref.root_ref {
            return self
                .collection_ref
                .root_ref
                .cmp(&other.collection_ref.root_ref);
        }
        self.collection_ref
            .collection_id
            .split('/')
            .chain(iter::once(self.document_id.as_str()))
            .cmp(
                other
                    .collection_ref
                    .collection_id
                    .split('/')
                    .chain(iter::once(other.document_id.as_str())),
            )
    }
}

impl PartialOrd for DocumentRef {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl FromStr for DocumentRef {
    type Err = GenericDatabaseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rf: Ref = s.parse()?;
        rf.as_document()
            .cloned()
            .ok_or_else(|| GenericDatabaseError::InvalidReference("document", s.to_string()))
    }
}
impl Display for DocumentRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.collection_ref, self.document_id,)
    }
}

impl DocumentRef {
    pub fn new(collection_ref: CollectionRef, document_id: impl Into<DefaultAtom>) -> Self {
        Self {
            collection_ref,
            document_id: document_id.into(),
        }
    }
}

impl_serde_display_from_str!(Ref, RootRef, CollectionRef, DocumentRef);

#[cfg(test)]
mod tests {
    use googletest::prelude::*;
    use rstest::rstest;

    use super::*;

    #[gtest]
    #[rstest]
    #[case::root_with_documents_suffix(
        "projects/my-demo-project/databases/database/documents",
        "my-demo-project",
        "database"
    )]
    #[case::root_without_documents_suffix(
        "projects/demo-project/databases/(default)",
        "demo-project",
        "(default)"
    )]
    #[case::collection(
        "projects/proj/databases/database/documents/collection",
        "proj",
        "database",
        "collection"
    )]
    #[case::subcollection(
        "projects/demo-project/databases/(default)/documents/root/doc/sub",
        "demo-project",
        "(default)",
        "root/doc/sub"
    )]
    #[case::document(
        "projects/proj/databases/database/documents/collection/document",
        "proj",
        "database",
        "collection",
        "document"
    )]
    #[case::subdocument(
        "projects/demo-project/databases/(default)/documents/root/doc/sub/doc",
        "demo-project",
        "(default)",
        "root/doc/sub",
        "doc"
    )]
    fn parse_refs(
        #[case] input: &str,
        #[case] project_id: &str,
        #[case] database_id: &str,
        #[case(opt)] collection_id: Option<&str>,
        #[case(opt)] doc_id: Option<&str>,
    ) {
        let root = RootRef::new(project_id, database_id);
        let expected = match (collection_id, doc_id) {
            (None, None) => Ref::Root(root),
            (Some(col), None) => Ref::Collection(CollectionRef::new(root, col)),
            (Some(col), Some(doc)) => {
                Ref::Document(DocumentRef::new(CollectionRef::new(root, col), doc))
            }
            (None, Some(_)) => unreachable!(),
        };
        assert_that!(input.parse::<Ref>(), ok(eq(&expected)));
    }

    #[gtest]
    #[rstest]
    #[case::empty_project("projects//databases/(default)")]
    #[case::empty_project_with_doc("projects//databases/(default)/documents/c/d")]
    #[case::empty_database("projects/p/databases/")]
    #[case::empty_database_with_doc("projects/p/databases//documents/c/d")]
    #[case::trailing_slash_after_database("projects/p/databases/(default)/")]
    fn rejects_empty_project_database_and_trailing_slash(#[case] input: &str) {
        assert_that!(
            input.parse::<Ref>(),
            err(matches_pattern!(GenericDatabaseError::InvalidReference(
                _,
                _
            )))
        );
    }

    #[gtest]
    fn root_ref_try_new_and_from_str_validate_root_segments() {
        let valid_default = RootRef::try_new("demo-project", "(default)").unwrap();
        assert_eq!(
            valid_default,
            "projects/demo-project/databases/(default)"
                .parse::<RootRef>()
                .unwrap()
        );
        assert_eq!(
            valid_default,
            "projects/demo-project/databases/(default)/documents"
                .parse::<RootRef>()
                .unwrap()
        );
        assert_eq!(
            valid_default.to_string(),
            "projects/demo-project/databases/(default)/documents"
        );

        let exact_1500_ascii = "a".repeat(1500);
        let exact_500_cjk = "あ".repeat(500); // 500 * 3 UTF-8 bytes == 1500 bytes
        assert_that!(
            RootRef::try_new(&exact_1500_ascii, "(default)"),
            ok(anything())
        );
        assert_that!(RootRef::try_new("p", &exact_500_cjk), ok(anything()));

        assert_that!(
            RootRef::try_new("p", "d/documents"),
            err(eq(&GenericDatabaseError::InvalidReference(
                "database",
                "projects/p/databases/d/documents".to_string()
            )))
        );
        assert_eq!(
            "projects/p/databases/d/documents"
                .parse::<RootRef>()
                .unwrap(),
            RootRef::new("p", "d")
        );
        for bad_root in [
            "projects/p/databases/d/extra",
            "projects/p/databases/d/documents/documents",
            "projects/d/documents/databases/(default)",
        ] {
            assert_that!(
                bad_root.parse::<RootRef>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
        }

        let over_1500_ascii = "a".repeat(1501);
        let over_1500_utf8 = "é".repeat(751); // 751 * 2 UTF-8 bytes == 1502 bytes (> 1500)
        for (project_id, database_id) in [
            ("p/sub", "d"),
            ("d/documents", "d"),
            ("p", "d/extra"),
            ("p", "."),
            (".", "d"),
            ("p", ".."),
            ("..", "d"),
            ("p", ""),
            ("", "d"),
            ("p", over_1500_ascii.as_str()),
            (over_1500_ascii.as_str(), "d"),
            ("p", over_1500_utf8.as_str()),
            ("p", "__reserved__"),
            ("__reserved__", "d"),
            ("p", "__id123__"),
            ("__id123__", "d"),
        ] {
            let expected_ref = format!("projects/{project_id}/databases/{database_id}");
            assert_that!(
                RootRef::try_new(project_id, database_id),
                err(eq(&GenericDatabaseError::InvalidReference(
                    "database",
                    expected_ref.clone()
                )))
            );
            assert_that!(
                expected_ref.parse::<RootRef>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
            assert_that!(
                format!("{expected_ref}/documents").parse::<RootRef>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
            assert_that!(
                format!("{expected_ref}/documents/col").parse::<CollectionRef>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
            assert_that!(
                format!("{expected_ref}/documents/col/doc").parse::<DocumentRef>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
            assert_that!(
                parse_query_ref_root(&format!("{expected_ref}/documents/col/doc")),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    #[gtest]
    fn serde_roundtrip_and_rejection_for_all_ref_types() {
        let root_str = "projects/p/databases/(default)/documents";
        let col_str = "projects/p/databases/(default)/documents/users";
        let doc_str = "projects/p/databases/(default)/documents/users/alice";

        let root: RootRef = serde_json::from_str(&format!("{root_str:?}")).unwrap();
        expect_eq!(
            serde_json::to_string(&root).unwrap(),
            format!("{root_str:?}")
        );

        let col: CollectionRef = serde_json::from_str(&format!("{col_str:?}")).unwrap();
        expect_eq!(serde_json::to_string(&col).unwrap(), format!("{col_str:?}"));

        let doc: DocumentRef = serde_json::from_str(&format!("{doc_str:?}")).unwrap();
        expect_eq!(serde_json::to_string(&doc).unwrap(), format!("{doc_str:?}"));

        // Escaped JSON string (exercises owned String path in Visitor::visit_str) and Ref Serialize
        let escaped_doc: Ref =
            serde_json::from_str("\"projects/p/databases/(default)/documents/users/al\\u0069ce\"")
                .unwrap();
        expect_eq!(escaped_doc, Ref::Document(doc));
        expect_eq!(
            serde_json::to_string(&escaped_doc).unwrap(),
            format!("{doc_str:?}")
        );
        expect_eq!(
            serde_json::to_string(&Ref::Root(root)).unwrap(),
            format!("{root_str:?}")
        );
        expect_eq!(
            serde_json::to_string(&Ref::Collection(col)).unwrap(),
            format!("{col_str:?}")
        );

        // Invalid path, non-string types, and wrong-kind references fail deserialization cleanly
        expect_that!(
            serde_json::from_str::<Ref>("\"not-a-ref\""),
            err(anything())
        );
        expect_that!(serde_json::from_str::<Ref>("123"), err(anything()));
        expect_that!(serde_json::from_str::<RootRef>("123"), err(anything()));
        expect_that!(
            serde_json::from_str::<CollectionRef>("123"),
            err(anything())
        );
        expect_that!(serde_json::from_str::<DocumentRef>("123"), err(anything()));
        expect_that!(
            serde_json::from_str::<DocumentRef>(&format!("{col_str:?}")),
            err(anything())
        );
        expect_that!(
            serde_json::from_str::<CollectionRef>(&format!("{doc_str:?}")),
            err(anything())
        );
        expect_that!(
            serde_json::from_str::<RootRef>(&format!("{col_str:?}")),
            err(anything())
        );
    }

    #[gtest]
    fn parse_query_ref_root_validates_structure_and_datastore_ids() {
        assert_eq!(
            parse_query_ref_root("projects/p1/databases/d1").unwrap(),
            QueryRefMeta {
                project_id: "p1",
                database_id: "d1",
                has_doc_path: false,
                has_zero_datastore_id: false,
            }
        );
        assert_eq!(
            parse_query_ref_root("projects/p1/databases/d1/documents").unwrap(),
            QueryRefMeta {
                project_id: "p1",
                database_id: "d1",
                has_doc_path: false,
                has_zero_datastore_id: false,
            }
        );
        for doc in [
            "projects/p1/databases/d1/documents/col/doc",
            "projects/p1/databases/d1/documents/c/d/sub/d2",
            "projects/p1/databases/d1/documents/col/____",
        ] {
            assert_eq!(
                parse_query_ref_root(doc).unwrap(),
                QueryRefMeta {
                    project_id: "p1",
                    database_id: "d1",
                    has_doc_path: true,
                    has_zero_datastore_id: false,
                }
            );
        }

        // Datastore numeric IDs (`__id<i64>__`) are permitted in query document (odd) segments
        // across both editions, while strict write `Ref` parsing rejects them. Zero IDs
        // (`__id0__`) are tracked via `has_zero_datastore_id` for Standard `__name__` filters.
        for (ds_doc, expected_zero) in [
            (
                "projects/p1/databases/(default)/documents/col/__id0__",
                true,
            ),
            (
                "projects/p1/databases/(default)/documents/col/__id0__/sub/d1",
                true,
            ),
            (
                "projects/p1/databases/(default)/documents/col/__id123__",
                false,
            ),
            (
                "projects/p1/databases/(default)/documents/col/__id-9223372036854775808__",
                false,
            ),
            (
                "projects/p1/databases/(default)/documents/col/__id123__/sub/__id-456__",
                false,
            ),
        ] {
            assert_eq!(
                parse_query_ref_root(ds_doc).unwrap(),
                QueryRefMeta {
                    project_id: "p1",
                    database_id: "(default)",
                    has_doc_path: true,
                    has_zero_datastore_id: expected_zero,
                }
            );
            assert_that!(
                ds_doc.parse::<Ref>(),
                err(matches_pattern!(GenericDatabaseError::InvalidReference(
                    _,
                    _
                )))
            );
        }

        let too_long_seg = "a".repeat(1501);
        let over_1500 = format!("projects/p1/databases/d1/documents/col/{too_long_seg}");
        for bad in [
            "projects/p1/databases/d1/documents/col",
            "projects/p1/databases/d1/documents/col/doc/sub",
            "projects/p1/databases/d1/documents/__id123__/doc",
            "projects/p1/databases/d1/documents/col/__id-0__",
            "projects/p1/databases/d1/documents/col/__id01__",
            "projects/p1/databases/d1/documents/col/__id-01__",
            "projects/p1/databases/d1/documents/col/__id00__",
            "projects/p1/databases/d1/documents/col/__id+1__",
            "projects/p1/databases/d1/documents/col/__id__",
            "projects/p1/databases/d1/documents/col/__id9223372036854775808__",
            "projects/p1/databases/d1/documents/col/__custom__",
            "projects/p1/databases/d1/documents/__custom__/doc",
            "projects/p1/databases/d1/documents/col/.",
            "projects/p1/databases/d1/documents/col/..",
            "projects/p1/databases/d1/documents/col/",
            over_1500.as_str(),
        ] {
            assert_that!(
                parse_query_ref_root(bad),
                err(matches_pattern!(GenericDatabaseError::InvalidArgument(_)))
            );
        }
    }

    fn assert_strictly_ordered<T: Ord + Eq + Clone + std::fmt::Debug>(ordered: &[T]) {
        for (i, a) in ordered.iter().enumerate() {
            for (j, b) in ordered.iter().enumerate() {
                let expected = i.cmp(&j);
                assert_eq!(
                    a.cmp(b),
                    expected,
                    "cmp mismatch at ({i}, {j}): {a:?} vs {b:?}"
                );
                assert_eq!(
                    a.partial_cmp(b),
                    Some(expected),
                    "partial_cmp mismatch at ({i}, {j}): {a:?} vs {b:?}"
                );
                assert_eq!(a == b, i == j);
            }
        }
        let mut reversed = ordered.to_vec();
        reversed.reverse();
        reversed.sort();
        assert_eq!(reversed, ordered);

        let set: std::collections::BTreeSet<T> = ordered.iter().rev().cloned().collect();
        let collected: Vec<T> = set.into_iter().collect();
        assert_eq!(collected, ordered);
    }

    #[gtest]
    fn document_ref_ord_matches_live_cloud_firestore_order() {
        let root = "projects/p/databases/(default)/documents";
        let ordered: Vec<DocumentRef> = [
            "col/doc",
            "col/doc/mid/m/sub/x",
            "col/doc/mid-1/m/sub/x",
            "col/doc/sub/x",
            "col/doc/sub-1/x",
            "col/doc!1/sub/x",
            "col/doc-1",
            "col/doc-1/sub/x",
            "col/doc.1/sub/x",
            "col/doc0/sub/x",
        ]
        .into_iter()
        .map(|rel| format!("{root}/{rel}").parse().unwrap())
        .collect();

        assert_strictly_ordered(&ordered);
    }

    #[gtest]
    fn collection_ref_ord_segment_and_nesting_order() {
        let root = "projects/p/databases/(default)/documents";
        let ordered: Vec<CollectionRef> = [
            "col",
            "col/doc/sub",
            "col/doc/sub/m/leaf",
            "col/doc/sub-1",
            "col/doc!1/sub",
            "col/doc-1/sub",
            "col/doc.1/sub",
            "col/doc0/sub",
            "col-1",
            "col-1/doc/sub",
        ]
        .into_iter()
        .map(|rel| format!("{root}/{rel}").parse().unwrap())
        .collect();

        assert_strictly_ordered(&ordered);
    }

    #[gtest]
    fn ref_ord_cross_root_and_all_descendants_nesting() {
        let docs_cross_root: Vec<DocumentRef> = [
            "projects/a/databases/(default)/documents/z/z/sub/z",
            "projects/a/databases/db-1/documents/a/a",
            "projects/a/databases/db-1/documents/z/z",
            "projects/a-1/databases/(default)/documents/a/a",
            "projects/a-1/databases/(default)/documents/z/z",
            "projects/b/databases/(default)/documents/a/a",
        ]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
        assert_strictly_ordered(&docs_cross_root);

        let cols_cross_root: Vec<CollectionRef> = [
            "projects/a/databases/(default)/documents/z/z/sub",
            "projects/a/databases/db-1/documents/a",
            "projects/a/databases/db-1/documents/z",
            "projects/a-1/databases/(default)/documents/a",
            "projects/b/databases/(default)/documents/a",
        ]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
        assert_strictly_ordered(&cols_cross_root);

        let mixed_nesting: Vec<DocumentRef> = [
            "projects/p/databases/(default)/documents/col/a",
            "projects/p/databases/(default)/documents/col/a/sub/b",
            "projects/p/databases/(default)/documents/col/c",
        ]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
        assert_strictly_ordered(&mixed_nesting);

        // Even with segment-ordered CollectionRef, collection-group `all_descendants` queries
        // must still sort the final document buffer because CollectionRef("sub") <
        // CollectionRef("sub/a/sub") while DocumentRef("sub/a/sub/x") < DocumentRef("sub/z").
        let parent_col: CollectionRef = "projects/p/databases/(default)/documents/sub"
            .parse()
            .unwrap();
        let nested_col: CollectionRef = "projects/p/databases/(default)/documents/sub/a/sub"
            .parse()
            .unwrap();
        let nested_doc: DocumentRef = "projects/p/databases/(default)/documents/sub/a/sub/x"
            .parse()
            .unwrap();
        let parent_doc: DocumentRef = "projects/p/databases/(default)/documents/sub/z"
            .parse()
            .unwrap();
        expect_lt!(parent_col, &nested_col);
        expect_lt!(nested_doc, &parent_doc);
    }

    #[gtest]
    fn ord_eq_hash_coherence_on_synthetic_edge_cases() {
        let root = RootRef::new("p", "(default)");
        // Covers the transitivity trap that would occur if DocumentRef::Ord normalized
        // "__id-9223372036854775808__" -> "" while fast-pathing same-collection comparisons,
        // plus empty segments and raw '/' inside manually constructed document_id values.
        let ordered_docs = [
            DocumentRef::new(CollectionRef::new(root.clone(), "c"), ""),
            DocumentRef::new(CollectionRef::new(root.clone(), "c/!/d"), "z"),
            DocumentRef::new(CollectionRef::new(root.clone(), "c"), "A"),
            DocumentRef::new(
                CollectionRef::new(root.clone(), "c"),
                "__id-9223372036854775808__",
            ),
            DocumentRef::new(CollectionRef::new(root.clone(), "c/d"), "s"),
            DocumentRef::new(CollectionRef::new(root.clone(), "c"), "d/s"),
        ];
        assert_strictly_ordered(&ordered_docs);

        let mut doc_map = std::collections::BTreeMap::new();
        for (idx, d) in ordered_docs.iter().enumerate() {
            doc_map.insert(d.clone(), idx);
        }
        for (idx, d) in ordered_docs.iter().enumerate() {
            assert_eq!(doc_map.get(d), Some(&idx));
        }

        let ordered_cols = [
            CollectionRef::new(root.clone(), ""),
            CollectionRef::new(root.clone(), "__id-9223372036854775808__"),
            CollectionRef::new(root.clone(), "c"),
            CollectionRef::new(root.clone(), "c/!/d"),
            CollectionRef::new(root.clone(), "c/d"),
            CollectionRef::new(root, "c-1/d"),
        ];
        assert_strictly_ordered(&ordered_cols);

        let mut col_map = std::collections::BTreeMap::new();
        for (idx, c) in ordered_cols.iter().enumerate() {
            col_map.insert(c.clone(), idx);
        }
        for (idx, c) in ordered_cols.iter().enumerate() {
            assert_eq!(col_map.get(c), Some(&idx));
        }
    }

    mod proptests {
        use std::{cmp::Ordering, iter};

        use proptest::{prelude::*, sample::select};

        use super::{CollectionRef, DocumentRef, Ref, RootRef};

        const PROJECT_VOCAB: &[&str] = &["proj-a", "proj-b", "demo-project", "p"];
        const DB_VOCAB: &[&str] = &["(default)", "db-1", "analytics", "staging"];
        const ID_VOCAB: &[&str] = &[
            "users", "orders", "items", "events", "rooms", "msgs", "alpha", "beta", "doc-1",
            "doc_2", "d3", "d4", "a", "b", "__half", "x__y",
        ];
        const VALID_ORDER_VOCAB: &[&str] = &[
            "col", "doc", "doc-1", "doc!1", "doc.1", "doc0", "mid", "mid-1", "sub", "sub-1", "a",
            "b", "z",
        ];
        const SYNTHETIC_SEG_VOCAB: &[&str] = &[
            "",
            "__id-9223372036854775808__",
            "__id0__",
            "A",
            "doc",
            "doc-1",
            "doc!1",
            "doc.1",
            "a/b",
            "/",
            "a",
            "z",
        ];

        fn arb_root() -> impl Strategy<Value = RootRef> {
            (select(PROJECT_VOCAB), select(DB_VOCAB)).prop_map(|(p, d)| RootRef::new(p, d))
        }

        fn arb_synthetic_col() -> impl Strategy<Value = CollectionRef> {
            (
                arb_root(),
                prop::collection::vec(select(SYNTHETIC_SEG_VOCAB), 1..=4),
            )
                .prop_map(|(root, segs)| CollectionRef::new(root, segs.join("/")))
        }

        fn arb_synthetic_doc() -> impl Strategy<Value = DocumentRef> {
            (arb_synthetic_col(), select(SYNTHETIC_SEG_VOCAB))
                .prop_map(|(col, doc_id)| DocumentRef::new(col, doc_id))
        }

        fn arb_parsed_col() -> impl Strategy<Value = CollectionRef> {
            (
                select(PROJECT_VOCAB),
                select(DB_VOCAB),
                prop_oneof![
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 1..=1),
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 3..=3),
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 5..=5),
                ],
            )
                .prop_map(|(p, d, segs)| {
                    format!("projects/{p}/databases/{d}/documents/{}", segs.join("/"))
                        .parse()
                        .unwrap()
                })
        }

        fn arb_parsed_doc() -> impl Strategy<Value = DocumentRef> {
            (
                select(PROJECT_VOCAB),
                select(DB_VOCAB),
                prop_oneof![
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 2..=2),
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 4..=4),
                    prop::collection::vec(select(VALID_ORDER_VOCAB), 6..=6),
                ],
            )
                .prop_map(|(p, d, segs)| {
                    format!("projects/{p}/databases/{d}/documents/{}", segs.join("/"))
                        .parse()
                        .unwrap()
                })
        }

        fn assert_total_order_and_coherence<T: Ord + Eq + Clone + std::fmt::Debug>(
            a: &T,
            b: &T,
            c: &T,
        ) -> Result<(), TestCaseError> {
            for (x, y) in [(a, a), (b, b), (c, c), (a, b), (b, c), (a, c)] {
                let cmp_xy = x.cmp(y);
                let cmp_yx = y.cmp(x);
                prop_assert_eq!(x.partial_cmp(y), Some(cmp_xy));
                prop_assert_eq!(cmp_xy, cmp_yx.reverse());
                prop_assert_eq!(x == y, cmp_xy == Ordering::Equal);
            }

            let mut sorted = [a.clone(), b.clone(), c.clone()];
            sorted.sort();
            prop_assert!(sorted[0] <= sorted[1]);
            prop_assert!(sorted[1] <= sorted[2]);
            prop_assert!(sorted[0] <= sorted[2]);

            let ab = a.cmp(b);
            let bc = b.cmp(c);
            let ac = a.cmp(c);
            if ab != Ordering::Greater && bc != Ordering::Greater {
                if ab == Ordering::Less || bc == Ordering::Less {
                    prop_assert_eq!(ac, Ordering::Less);
                } else {
                    prop_assert_eq!(ac, Ordering::Equal);
                }
            }
            if ab != Ordering::Less && bc != Ordering::Less {
                if ab == Ordering::Greater || bc == Ordering::Greater {
                    prop_assert_eq!(ac, Ordering::Greater);
                } else {
                    prop_assert_eq!(ac, Ordering::Equal);
                }
            }
            Ok(())
        }

        proptest! {
            #[test]
            fn proptest_ref_display_from_str_roundtrip_and_hierarchy(
                project in select(PROJECT_VOCAB),
                database in select(DB_VOCAB),
                segments in prop::collection::vec(select(ID_VOCAB), 0..=6),
            ) {
                let root_bare = format!("projects/{project}/databases/{database}");
                let root_docs = format!("{root_bare}/documents");

                let parsed_bare: Ref = root_bare.parse().unwrap();
                let parsed_docs: Ref = root_docs.parse().unwrap();
                prop_assert_eq!(&parsed_bare, &parsed_docs);
                prop_assert_eq!(&parsed_docs.to_string(), &root_docs);

                if segments.is_empty() {
                    let root_only: RootRef = root_docs.parse().unwrap();
                    prop_assert_eq!(&root_only.to_string(), &root_docs);
                    prop_assert!(root_docs.parse::<CollectionRef>().is_err());
                    prop_assert!(root_docs.parse::<DocumentRef>().is_err());
                } else {
                    let full = format!("{root_docs}/{}", segments.join("/"));
                    let parsed: Ref = full.parse().unwrap();
                    prop_assert_eq!(&parsed.to_string(), &full);
                    prop_assert_eq!(&parsed.to_string().parse::<Ref>().unwrap(), &parsed);
                    prop_assert!(parsed_docs.is_parent_of(&parsed));
                    prop_assert!(!parsed.is_parent_of(&parsed));
                    prop_assert!(!parsed.is_parent_of(&parsed_docs));

                    if segments.len() % 2 == 1 {
                        let col: CollectionRef = full.parse().unwrap();
                        prop_assert_eq!(&col.to_string(), &full);
                        prop_assert!(full.parse::<RootRef>().is_err());
                        prop_assert!(full.parse::<DocumentRef>().is_err());

                        let parent = col.parent();
                        prop_assert!(parent.is_parent_of(&Ref::Collection(col.clone())));
                        prop_assert!(col.strip_prefix(&parent).is_some());
                    } else {
                        let doc: DocumentRef = full.parse().unwrap();
                        prop_assert_eq!(&doc.to_string(), &full);
                        prop_assert!(full.parse::<RootRef>().is_err());
                        prop_assert!(full.parse::<CollectionRef>().is_err());
                    }
                }
            }

            #[test]
            fn proptest_ref_rejects_reserved_and_malformed_ids(
                project in select(PROJECT_VOCAB),
                database in select(DB_VOCAB),
                prefix in prop::collection::vec(select(ID_VOCAB), 0..=3),
                suffix in prop::collection::vec(select(ID_VOCAB), 0..=3),
                bad_id in prop_oneof![
                    Just(".".to_string()),
                    Just("..".to_string()),
                    Just(String::new()),
                    Just("_____".to_string()),
                    Just("__name__".to_string()),
                    select(ID_VOCAB).prop_map(|s| format!("__{s}__")),
                ],
            ) {
                let mut all = prefix;
                all.push(&bad_id);
                all.extend(suffix);
                let path = format!(
                    "projects/{project}/databases/{database}/documents/{}",
                    all.join("/")
                );
                prop_assert!(path.parse::<Ref>().is_err());
            }

            #[test]
            fn proptest_collection_ref_ord_total_order_and_eq_coherence(
                a in arb_synthetic_col(),
                b in arb_synthetic_col(),
                c in arb_synthetic_col(),
            ) {
                assert_total_order_and_coherence(&a, &b, &c)?;
            }

            #[test]
            fn proptest_document_ref_ord_total_order_and_eq_coherence(
                a in arb_synthetic_doc(),
                b in arb_synthetic_doc(),
                c in arb_synthetic_doc(),
                same_col_doc_id in select(SYNTHETIC_SEG_VOCAB),
            ) {
                assert_total_order_and_coherence(&a, &b, &c)?;
                // Explicitly exercise the same-collection fast path against a third arbitrary doc.
                let a_sibling = DocumentRef::new(a.collection_ref.clone(), same_col_doc_id);
                assert_total_order_and_coherence(&a, &a_sibling, &c)?;
            }

            #[test]
            fn proptest_ref_ord_matches_googleapis_cmp_reference_segments_on_valid_refs(
                col_a in arb_parsed_col(),
                col_b in arb_parsed_col(),
                doc_a in arb_parsed_doc(),
                doc_b in arb_parsed_doc(),
            ) {
                let expected_col = col_a.root_ref.cmp(&col_b.root_ref).then_with(|| {
                    googleapis::cmp_reference_segments(
                        col_a.collection_id.split('/'),
                        col_b.collection_id.split('/'),
                    )
                });
                prop_assert_eq!(col_a.cmp(&col_b), expected_col);

                let expected_doc = doc_a
                    .collection_ref
                    .root_ref
                    .cmp(&doc_b.collection_ref.root_ref)
                    .then_with(|| {
                        googleapis::cmp_reference_segments(
                            doc_a
                                .collection_ref
                                .collection_id
                                .split('/')
                                .chain(iter::once(doc_a.document_id.as_str())),
                            doc_b
                                .collection_ref
                                .collection_id
                                .split('/')
                                .chain(iter::once(doc_b.document_id.as_str())),
                        )
                    });
                prop_assert_eq!(doc_a.cmp(&doc_b), expected_doc);
            }
        }
    }
}
