use std::{fmt::Display, str::FromStr};

use serde::Deserialize;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use string_cache::DefaultAtom;

use crate::GenericDatabaseError;

/// A reference to either the database root (including project name and database name), a single
/// collection or a single document.
///
/// # Examples
///
/// ```
/// # use firestore_database::reference::*;
/// # use firestore_database::*;
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
#[derive(Clone, Debug, PartialEq, Eq, DeserializeFromStr, SerializeDisplay)]
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
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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
            // Fixed prefix "projects/"
            let s = s.strip_prefix("projects/")?;
            // Next is the project_id until the next "/"
            let (project_id, s) = s.split_once('/')?;
            // Fixed token "databases/".
            let s = s.strip_prefix("databases/")?;
            // Next is the database_id until the next "/" or the end of the string
            let (database_id, s) = s.split_once('/').unwrap_or((s, ""));
            // If not the end of the string, then the token "documents" is mandatory
            let s = if s.is_empty() {
                s
            } else {
                s.strip_prefix("documents")?
            };
            let root_ref = RootRef::new(project_id, database_id);
            if s.is_empty() {
                return Some(Ref::Root(root_ref));
            }
            // Next up is an alternating path of collection name and document name, we can
            // determine whether this is a collection or a document by counting the slashes.
            let s = s.strip_prefix('/')?;
            let slashes = s.chars().filter(|ch| *ch == '/').count();
            if slashes % 2 == 0 {
                Some(Ref::Collection(CollectionRef::new(root_ref, s)))
            } else {
                let (collection_id, document_id) = s.rsplit_once('/')?;
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
            //    - Must be no longer than 1,500 bytes
            //    - Cannot contain a forward slash (/)
            //    - Cannot solely consist of a single period (.) or double periods (..)
            //    - Cannot match the regular expression __.*__
            // - Constraints on document IDs
            //    - Must be valid UTF-8 characters
            //    - Must be no longer than 1,500 bytes
            //    - Cannot contain a forward slash (/)
            //    - Cannot solely consist of a single period (.) or double periods (..)
            //    - Cannot match the regular expression __.*__
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Deserialize)]
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
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectionRef {
    pub root_ref:      RootRef,
    pub collection_id: DefaultAtom,
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
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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
            .strip_prefix(&*col.collection_id)?
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
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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
            .strip_prefix(&*doc.document_id)?
            .strip_prefix('/')?;
        Some(rest)
    }

    /// Returns the parent of the collection ref, is either a DocumentRef or a RootRef.
    ///
    /// # Examples
    ///
    /// ```
    /// # use firestore_database::reference::*;
    /// # use firestore_database::*;
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

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct DocumentRef {
    pub collection_ref: CollectionRef,
    pub document_id:    DefaultAtom,
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        // With `/documents` suffix
        "projects/my-demo-project/databases/database/documents",
        "my-demo-project",
        "database"
    )]
    #[case(
        // Without `/documents` suffix
        "projects/demo-project/databases/(default)",
        "demo-project",
        "(default)"
    )]
    fn parse_root_ref(#[case] input: &str, #[case] project_id: &str, #[case] database_id: &str) {
        assert_eq!(
            input.parse::<Ref>().unwrap(),
            Ref::Root(RootRef::new(project_id, database_id))
        )
    }

    #[rstest]
    #[case(
        "projects/proj/databases/database/documents/collection",
        "proj",
        "database",
        "collection"
    )]
    #[case(
        "projects/demo-project/databases/(default)/documents/root/doc/sub",
        "demo-project",
        "(default)",
        "root/doc/sub"
    )]
    fn parse_collection_refs(
        #[case] input: &str,
        #[case] project_id: &str,
        #[case] database_id: &str,
        #[case] collection_id: &str,
    ) {
        assert_eq!(
            input.parse::<Ref>().unwrap(),
            Ref::Collection(CollectionRef::new(
                RootRef::new(project_id, database_id),
                collection_id
            ))
        );
    }

    #[rstest]
    #[case(
        "projects/proj/databases/database/documents/collection/document",
        "proj",
        "database",
        "collection",
        "document"
    )]
    #[case(
        "projects/demo-project/databases/(default)/documents/root/doc/sub/doc",
        "demo-project",
        "(default)",
        "root/doc/sub",
        "doc"
    )]
    fn parse_document_refs(
        #[case] input: &str,
        #[case] project_id: &str,
        #[case] database_id: &str,
        #[case] collection_id: &str,
        #[case] doc_id: &str,
    ) {
        assert_eq!(
            input.parse::<Ref>().unwrap(),
            Ref::Document(DocumentRef::new(
                CollectionRef::new(RootRef::new(project_id, database_id), collection_id),
                doc_id
            ))
        );
    }
}
