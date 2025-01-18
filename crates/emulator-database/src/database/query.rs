use std::{
    cmp,
    collections::{HashMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use googleapis::google::firestore::v1::{
    structured_query::{self, CollectionSelector},
    Cursor, Document, StructuredQuery,
};
use itertools::Itertools;
use string_cache::DefaultAtom;

use self::filter::Filter;
use super::{
    collection::Collection,
    document::StoredDocumentVersion,
    field_path::FieldReference,
    projection::{Project, Projection},
    read_consistency::ReadConsistency,
    reference::{CollectionRef, Ref},
    FirestoreDatabase,
};
use crate::{error::Result, reference::DocumentRef, GenericDatabaseError};

mod filter;

#[derive(Debug)]
pub struct QueryBuilder {
    parent: Ref,
    select: Option<Projection>,
    from: Vec<CollectionSelector>,
    filter: Option<Filter>,
    order_by: Vec<Order>,
    start_at: Option<Cursor>,
    end_at: Option<Cursor>,
    offset: usize,
    limit: Option<usize>,
    consistency: ReadConsistency,
}

impl QueryBuilder {
    pub fn from(parent: Ref, from: Vec<CollectionSelector>) -> Self {
        Self {
            parent,
            select: None,
            from,
            filter: None,
            order_by: vec![],
            start_at: None,
            end_at: None,
            offset: 0,
            limit: None,
            consistency: ReadConsistency::Default,
        }
    }

    /// Optional sub-set of the fields to return.
    ///
    /// This acts as a [DocumentMask][google.firestore.v1.DocumentMask] over the
    /// documents returned from a query. When not set, assumes that the caller
    /// wants all fields returned.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn select(mut self, projection: impl Into<Option<Projection>>) -> Self {
        self.select = projection.into();
        self
    }

    /// The filter to apply.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// The order to apply to the query results.
    ///
    /// Firestore allows callers to provide a full ordering, a partial ordering, or
    /// no ordering at all. In all cases, Firestore guarantees a stable ordering
    /// through the following rules:
    ///
    ///   * The `order_by` is required to reference all fields used with an inequality filter.
    ///   * All fields that are required to be in the `order_by` but are not already present are
    ///     appended in lexicographical ordering of the field name.
    ///   * If an order on `__name__` is not specified, it is appended by default.
    ///
    /// Fields are appended with the same sort direction as the last order
    /// specified, or 'ASCENDING' if no order was specified. For example:
    ///
    ///   * `ORDER BY a` becomes `ORDER BY a ASC, __name__ ASC`
    ///   * `ORDER BY a DESC` becomes `ORDER BY a DESC, __name__ DESC`
    ///   * `WHERE a > 1` becomes `WHERE a > 1 ORDER BY a ASC, __name__ ASC`
    ///   * `WHERE __name__ > ... AND a > 1` becomes `WHERE __name__ > ... AND a > 1 ORDER BY a ASC,
    ///     __name__ ASC`
    ///
    /// Subsequent calls will *add* new order by clauses.
    pub fn order_by(mut self, field: FieldReference, direction: Direction) -> Self {
        self.order_by.push(Order { field, direction });
        self
    }

    /// A potential prefix of a position in the result set to start the query at.
    ///
    /// The ordering of the result set is based on the `ORDER BY` clause of the
    /// original query.
    ///
    /// ```sql
    /// SELECT * FROM k WHERE a = 1 AND b > 2 ORDER BY b ASC, __name__ ASC;
    /// ```
    ///
    /// This query's results are ordered by `(b ASC, __name__ ASC)`.
    ///
    /// Cursors can reference either the full ordering or a prefix of the location,
    /// though it cannot reference more fields than what are in the provided
    /// `ORDER BY`.
    ///
    /// Continuing off the example above, attaching the following start cursors
    /// will have varying impact:
    ///
    /// - `START BEFORE (2, /k/123)`: start the query right before `a = 1 AND b > 2 AND __name__ >
    ///   /k/123`.
    /// - `START AFTER (10)`: start the query right after `a = 1 AND b > 10`.
    ///
    /// Unlike `OFFSET` which requires scanning over the first N results to skip,
    /// a start cursor allows the query to begin at a logical position. This
    /// position is not required to match an actual result, it will scan forward
    /// from this position to find the next document.
    ///
    /// Requires:
    ///
    /// * The number of values cannot be greater than the number of fields specified in the `ORDER
    ///   BY` clause.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn start_at(mut self, start_at: Cursor) -> Self {
        self.start_at = Some(start_at);
        self
    }

    /// A potential prefix of a position in the result set to end the query at.
    ///
    /// This is similar to `START_AT` but with it controlling the end position
    /// rather than the start position.
    ///
    /// Requires:
    ///
    /// * The number of values cannot be greater than the number of fields specified in the `ORDER
    ///   BY` clause.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn end_at(mut self, end_at: Cursor) -> Self {
        self.end_at = Some(end_at);
        self
    }

    /// The number of documents to skip before returning the first result.
    ///
    /// This applies after the constraints specified by the `WHERE`, `START AT`, &
    /// `END AT` but before the `LIMIT` clause.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// The maximum number of results to return.
    ///
    /// Applies after all other constraints.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn consistency(mut self, consistency: ReadConsistency) -> Self {
        self.consistency = consistency;
        self
    }

    /// Try to build the final Query struct.
    pub fn build(self) -> Result<Query> {
        let mut query = Query {
            parent: self.parent,
            select: self.select,
            from: self.from,
            filter: self.filter,
            order_by: self.order_by,
            start_at: self.start_at,
            end_at: self.end_at,
            offset: self.offset,
            limit: self.limit,
            consistency: self.consistency,
            collection_cache: Default::default(),
        };
        query.validate()?;
        Ok(query)
    }
}

/// A Firestore query.
#[derive(Debug)]
pub struct Query {
    parent: Ref,

    /// Optional sub-set of the fields to return.
    ///
    /// This acts as a [DocumentMask][google.firestore.v1.DocumentMask] over the
    /// documents returned from a query. When not set, assumes that the caller
    /// wants all fields returned.
    select: Option<Projection>,

    /// The collections to query.
    from: Vec<CollectionSelector>,

    /// The filter to apply.
    filter: Option<Filter>,

    /// The order to apply to the query results.
    ///
    /// Firestore allows callers to provide a full ordering, a partial ordering, or
    /// no ordering at all. In all cases, Firestore guarantees a stable ordering
    /// through the following rules:
    ///
    ///   * The `order_by` is required to reference all fields used with an inequality filter.
    ///   * All fields that are required to be in the `order_by` but are not already present are
    ///     appended in lexicographical ordering of the field name.
    ///   * If an order on `__name__` is not specified, it is appended by default.
    ///
    /// Fields are appended with the same sort direction as the last order
    /// specified, or 'ASCENDING' if no order was specified. For example:
    ///
    ///   * `ORDER BY a` becomes `ORDER BY a ASC, __name__ ASC`
    ///   * `ORDER BY a DESC` becomes `ORDER BY a DESC, __name__ DESC`
    ///   * `WHERE a > 1` becomes `WHERE a > 1 ORDER BY a ASC, __name__ ASC`
    ///   * `WHERE __name__ > ... AND a > 1` becomes `WHERE __name__ > ... AND a > 1 ORDER BY a
    ///     ASC, __name__ ASC`
    order_by: Vec<Order>,

    /// A potential prefix of a position in the result set to start the query at.
    ///
    /// The ordering of the result set is based on the `ORDER BY` clause of the
    /// original query.
    ///
    /// ```sql
    /// SELECT * FROM k WHERE a = 1 AND b > 2 ORDER BY b ASC, __name__ ASC;
    /// ```
    ///
    /// This query's results are ordered by `(b ASC, __name__ ASC)`.
    ///
    /// Cursors can reference either the full ordering or a prefix of the location,
    /// though it cannot reference more fields than what are in the provided
    /// `ORDER BY`.
    ///
    /// Continuing off the example above, attaching the following start cursors
    /// will have varying impact:
    ///
    /// - `START BEFORE (2, /k/123)`: start the query right before `a = 1 AND b > 2 AND __name__ >
    ///   /k/123`.
    /// - `START AFTER (10)`: start the query right after `a = 1 AND b > 10`.
    ///
    /// Unlike `OFFSET` which requires scanning over the first N results to skip,
    /// a start cursor allows the query to begin at a logical position. This
    /// position is not required to match an actual result, it will scan forward
    /// from this position to find the next document.
    ///
    /// Requires:
    ///
    /// * The number of values cannot be greater than the number of fields specified in the `ORDER
    ///   BY` clause.
    start_at: Option<Cursor>,

    /// A potential prefix of a position in the result set to end the query at.
    ///
    /// This is similar to `START_AT` but with it controlling the end position
    /// rather than the start position.
    ///
    /// Requires:
    ///
    /// * The number of values cannot be greater than the number of fields specified in the `ORDER
    ///   BY` clause.
    end_at: Option<Cursor>,

    /// The number of documents to skip before returning the first result.
    ///
    /// This applies after the constraints specified by the `WHERE`, `START AT`, &
    /// `END AT` but before the `LIMIT` clause.
    offset: usize,

    /// The maximum number of results to return.
    ///
    /// Applies after all other constraints.
    limit: Option<usize>,

    consistency: ReadConsistency,

    collection_cache: HashMap<DefaultAtom, bool>,
}

impl Query {
    pub fn from_structured(
        parent: Ref,
        query: StructuredQuery,
        consistency: ReadConsistency,
    ) -> Result<Box<Self>> {
        let StructuredQuery {
            select,
            from,
            r#where: filter,
            order_by,
            start_at,
            end_at,
            offset,
            limit,
        } = query;

        let mut query = Box::new(Self {
            parent,
            select: select.map(Projection::try_from).transpose()?,
            from,
            filter: filter.map(TryInto::try_into).transpose()?,
            order_by: order_by.into_iter().map(TryInto::try_into).try_collect()?,
            start_at,
            end_at,
            offset: offset as usize,
            limit: limit.map(|v| v.value as usize),
            consistency,
            collection_cache: Default::default(),
        });
        query.validate()?;
        Ok(query)
    }

    fn validate(&mut self) -> Result<()> {
        if matches!(self.parent, Ref::Collection(_)) {
            return Err(GenericDatabaseError::invalid_argument(
                "parent must point to a Document or Root",
            ));
        }
        // Validate from clause
        if self.from.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                "Query FROM is mandatory",
            ));
        }
        if let Some(filter) = &self.filter {
            // Ensure order_by is consistent
            if self.order_by.is_empty() {
                self.order_by
                    .extend(filter.get_inequality_fields().map(|field| Order {
                        field:     field.clone(),
                        direction: Direction::Ascending,
                    }));
            }

            filter
                .field_filters()
                .filter(|f| f.op.is_array_contains())
                .at_most_one()
                .map_err(|_| {
                    GenericDatabaseError::invalid_argument(
                        "A maximum of 1 'ARRAY_CONTAINS' filter is allowed per disjunction.",
                    )
                })?;
        }

        if !self.order_by.iter().any(|o| o.field.is_document_name()) {
            self.order_by.push(Order {
                field:     FieldReference::DocumentName,
                direction: Direction::Ascending,
            })
        }
        Ok(())
    }

    pub fn reset_on_update(&self) -> bool {
        self.order_by.iter().any(|o| !o.field.is_document_name())
    }

    pub async fn once(
        &mut self,
        db: &FirestoreDatabase,
    ) -> Result<Vec<Arc<StoredDocumentVersion>>> {
        // First collect all Arc<Collection>s in a Vec to release the collection lock asap.
        let collections = self.applicable_collections(db).await;

        let txn = db.get_txn_for_consistency(self.consistency).await?;

        let mut buffer = vec![];
        for col in collections {
            for meta in col.docs().await {
                let version = match &txn {
                    Some(txn) => txn.read_doc(&meta.name).await?,
                    None => meta.read().await?.current_version().cloned(),
                };
                let Some(version) = version else {
                    continue;
                };
                if !self.includes_document(&version)? {
                    continue;
                }
                if let Some(cursor) = &self.start_at {
                    let exclude = self.doc_on_left_of_cursor(&version, cursor);
                    if exclude {
                        continue;
                    }
                }
                if let Some(cursor) = &self.end_at {
                    let include = self.doc_on_left_of_cursor(&version, cursor);
                    if !include {
                        continue;
                    }
                }
                buffer.push(version);
            }
        }

        buffer.sort_unstable_by(|a, b| self.order_by_cmp(a, b));

        if self.offset > 0 {
            buffer.drain(0..self.offset.min(buffer.len()));
        }

        if let Some(limit) = self.limit {
            buffer.truncate(limit)
        }

        Ok(buffer)
    }

    /// Get the names of missing documents that match this query. A document is missing if it does
    /// not exist, but there are sub-documents nested underneath it.
    pub async fn missing_docs(&self, db: &FirestoreDatabase) -> Result<Vec<String>> {
        let not_supported = [
            (self.from.len() != 1).then_some("multiple 'from' selectors"),
            self.from
                .iter()
                .any(|f| f.all_descendants || f.collection_id.is_empty())
                .then_some("all_descendants or empty collection_ids"),
            self.filter.is_some().then_some("filter"),
            // Only the default "order by document name" is allowed
            (self.order_by.len() > 1).then_some("order_by"),
            self.start_at.is_some().then_some("start_at"),
            self.end_at.is_some().then_some("end_at"),
            (self.offset > 0).then_some("offset"),
            self.limit.is_some().then_some("limit"),
        ]
        .into_iter()
        .flatten()
        .join(", ");
        if !not_supported.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "not supported when querying missing docs: {not_supported}"
            )));
        }

        // Checked above that self.from has exactly one non empty collection_id.
        // Lazy conversion using String rep, maybe replace with dedicated methods on refs.
        let parent: CollectionRef =
            format!("{}/{}", self.parent, self.from[0].collection_id).parse()?;

        let all_docs: HashSet<_> = db
            .get_all_collections()
            .await
            .into_iter()
            .filter_map(|col| {
                let document_id = col
                    .name
                    .strip_collection_prefix(&parent)?
                    .split_once('/')?
                    .0;
                Some(DocumentRef::new(parent.clone(), document_id))
            })
            .collect();
        let mut result = vec![];
        for name in all_docs {
            if db.get_doc(&name, self.consistency).await.is_ok() {
                result.push(name.to_string());
            }
        }
        Ok(result)
    }

    async fn applicable_collections(&mut self, db: &FirestoreDatabase) -> Vec<Arc<Collection>> {
        db.collections
            .read()
            .await
            .values()
            .filter(|&col| self.includes_collection(&col.name))
            .map(Arc::clone)
            .collect_vec()
    }

    fn includes_collection(&mut self, collection: &CollectionRef) -> bool {
        if let Some(&r) = self.collection_cache.get(&collection.collection_id) {
            return r;
        }
        let included = collection.strip_prefix(&self.parent).is_some_and(|path| {
            self.from.iter().any(|selector| {
                if !selector.all_descendants {
                    return path == selector.collection_id;
                }
                if selector.collection_id.is_empty() {
                    // collection_id empty is a special case where all collections should match
                    return true;
                }
                // With all_descendants == true we search for the given collection_id in the
                // remaining path.
                // Invariant: path starts with <COLLECTION-NAME>/... or is empty
                let mut elements = path.split('/');
                loop {
                    let Some(next_id) = elements.next() else {
                        return false;
                    };
                    if next_id == selector.collection_id {
                        return true;
                    }
                    // Strip document id and try again
                    if elements.next().is_none() {
                        return false;
                    }
                }
            })
        });
        self.collection_cache
            .insert(collection.collection_id.clone(), included);
        included
    }

    pub fn includes_document(&mut self, doc: &StoredDocumentVersion) -> Result<bool> {
        if !self.includes_collection(&doc.name.collection_ref) {
            return Ok(false);
        }
        if let Some(filter) = &self.filter {
            if !filter.eval(doc)? {
                return Ok(false);
            }
        }
        for order_by in &self.order_by {
            if order_by.field.get_value(doc).is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn project(&self, version: &StoredDocumentVersion) -> Document {
        self.select.project(version)
    }

    fn order_by_cmp(
        &self,
        a: &Arc<StoredDocumentVersion>,
        b: &Arc<StoredDocumentVersion>,
    ) -> cmp::Ordering {
        use cmp::Ordering::*;
        for order in &self.order_by {
            let a = order.field.get_value(a).expect(
                "fields used in order_by MUST also be used in filter, so cannot be empty here",
            );
            let b = order.field.get_value(b).expect(
                "fields used in order_by MUST also be used in filter, so cannot be empty here",
            );
            let result = match a.cmp(&b) {
                result @ (Less | Greater) => result,
                Equal => continue,
            };
            return match order.direction {
                Direction::Descending => result.reverse(),
                _ => result,
            };
        }
        Equal
    }

    /// Returns true when the given document is "on the left" of the given cursor.
    fn doc_on_left_of_cursor(&self, doc: &Arc<StoredDocumentVersion>, cursor: &Cursor) -> bool {
        use cmp::Ordering::*;
        for (order, cursor_value) in self.order_by.iter().zip(&cursor.values) {
            let doc_value = order.field.get_value(doc).expect(
                "fields used in order_by MUST also be used in filter, so cannot be empty here",
            );
            match (&order.direction, doc_value.deref().cmp(cursor_value)) {
                (Direction::Ascending, Less) | (Direction::Descending, Greater) => return true,
                (Direction::Ascending, Greater) | (Direction::Descending, Less) => return false,
                (_, Equal) => (),
            };
        }
        // When we get here, the doc is equal to the values of the cursor. If the cursur wants to be
        // "before" equal documents we must consider them not to be part of the left
        // partition.
        !cursor.before
    }
}

#[derive(Debug)]
pub struct Order {
    field:     FieldReference,
    direction: Direction,
}

impl TryFrom<structured_query::Order> for Order {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::Order) -> Result<Self, Self::Error> {
        Ok(Self {
            field:     value
                .field
                .as_ref()
                .ok_or_else(|| GenericDatabaseError::invalid_argument("order_by without field"))?
                .field_path
                .parse()?,
            direction: value.direction().into(),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Direction {
    Ascending,
    Descending,
}

impl From<structured_query::Direction> for Direction {
    fn from(value: structured_query::Direction) -> Self {
        match value {
            structured_query::Direction::Unspecified => Direction::Ascending,
            structured_query::Direction::Ascending => Direction::Ascending,
            structured_query::Direction::Descending => Direction::Descending,
        }
    }
}
