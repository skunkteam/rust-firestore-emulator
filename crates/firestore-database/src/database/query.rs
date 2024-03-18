use std::{cmp, collections::HashMap, ops::Deref, sync::Arc};

use googleapis::google::firestore::v1::{structured_query::CollectionSelector, *};
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
use crate::{error::Result, GenericDatabaseError};

mod filter;

/// A Firestore query.
#[derive(Debug)]
pub(crate) struct Query {
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
    ) -> Result<Self> {
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

        let select = select.map(Projection::try_from).transpose()?;
        let filter: Option<Filter> = filter.map(TryInto::try_into).transpose()?;
        let mut order_by: Vec<Order> = order_by.into_iter().map(TryInto::try_into).try_collect()?;
        if order_by.is_empty() {
            if let Some(filter) = &filter {
                order_by.extend(
                    filter
                        .get_inequality_fields()
                        .into_iter()
                        .map(|field| Order {
                            field:     field.clone(),
                            direction: Direction::Ascending,
                        }),
                );
            }
        }
        if !order_by.iter().any(|o| o.field.is_document_name()) {
            order_by.push(Order {
                field:     FieldReference::DocumentName,
                direction: Direction::Ascending,
            })
        }
        Ok(Self {
            parent,
            select,
            from,
            filter,
            order_by,
            start_at,
            end_at,
            offset: offset as usize,
            limit: limit.map(|v| v.value as usize),
            consistency,
            collection_cache: Default::default(),
        })
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

        let txn = db.get_txn_for_consistency(&self.consistency).await?;

        let mut buffer = vec![];
        for col in collections {
            for meta in col.docs().await {
                let version = if let Some(txn) = &txn {
                    txn.read_doc(&meta.name)
                        .await?
                        .current_version()
                        .map(Arc::clone)
                } else {
                    meta.read().await?.current_version().map(Arc::clone)
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
            return if matches!(order.direction, Direction::Descending) {
                result.reverse()
            } else {
                result
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
struct Order {
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
            direction: value.direction().try_into()?,
        })
    }
}

#[derive(Debug)]
enum Direction {
    Ascending,
    Descending,
}

impl TryFrom<structured_query::Direction> for Direction {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::Direction) -> std::prelude::v1::Result<Self, Self::Error> {
        match value {
            structured_query::Direction::Unspecified => Err(
                GenericDatabaseError::invalid_argument("Invalid structured_query::Direction"),
            ),
            structured_query::Direction::Ascending => Ok(Direction::Ascending),
            structured_query::Direction::Descending => Ok(Direction::Descending),
        }
    }
}
