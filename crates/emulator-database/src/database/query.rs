use std::{
    borrow::Cow,
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    ops::Deref,
    sync::Arc,
};

use googleapis::google::{
    firestore::v1::{
        Cursor, Document, StructuredQuery, Value,
        structured_query::{self, CollectionSelector},
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use string_cache::DefaultAtom;

use self::filter::Filter;
pub use self::find_nearest::FindNearest;
pub use super::vector::DistanceMeasure;
use super::{
    FirestoreDatabase,
    collection::Collection,
    document::StoredDocumentVersion,
    field_path::{FieldPath, FieldReference},
    projection::{Project, Projection, project_with_distance},
    read_consistency::{ReadConsistency, validate_read_time},
    reference::{CollectionRef, Ref},
    vector,
};
use crate::{
    GenericDatabaseError,
    database::transaction::{
        ConcurrencyMode, ExecutedQuery, OptimisticState, PessimisticTransactionQuerySupport,
        Transaction,
    },
    document::{DocumentMeta, OwnedDocumentContentsReadGuard, OwnedDocumentContentsWriteGuard},
    error::Result,
    reference::DocumentRef,
};

mod filter;
mod find_nearest;
#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct QueryMatch {
    pub version: Arc<StoredDocumentVersion>,
    pub computed_distance: Option<f64>,
}

#[derive(Debug)]
pub struct QueryBuilder {
    parent: Ref,
    enterprise_edition: bool,
    select: Option<Projection>,
    from: Vec<CollectionSelector>,
    filter: Option<Filter>,
    order_by: Vec<Order>,
    start_at: Option<Cursor>,
    end_at: Option<Cursor>,
    offset: usize,
    limit: Option<usize>,
    find_nearest: Option<FindNearest>,
    consistency: ReadConsistency,
}

impl QueryBuilder {
    pub fn from(parent: Ref, from: Vec<CollectionSelector>) -> Self {
        Self {
            parent,
            enterprise_edition: false,
            select: None,
            from,
            filter: None,
            order_by: vec![],
            start_at: None,
            end_at: None,
            offset: 0,
            limit: None,
            find_nearest: None,
            consistency: ReadConsistency::Default,
        }
    }

    pub fn from_collection(collection: CollectionRef) -> Self {
        Self::from(
            Ref::Root(collection.root_ref),
            vec![CollectionSelector {
                collection_id:   collection.collection_id.to_string(),
                all_descendants: false,
            }],
        )
    }

    /// Determines whether the query corresponds to an Enterprise Edition database.
    pub fn enterprise_edition(mut self, enterprise_edition: bool) -> Self {
        self.enterprise_edition = enterprise_edition;
        self
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

    /// Optional nearest-neighbors vector search configuration.
    ///
    /// Subsequent calls will replace previously set values.
    pub fn find_nearest(mut self, find_nearest: FindNearest) -> Self {
        self.find_nearest = Some(find_nearest);
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
            enterprise_edition: self.enterprise_edition,
            from: self.from,
            filter: self.filter,
            order_by: self.order_by,
            start_at: self.start_at,
            end_at: self.end_at,
            offset: self.offset,
            limit: self.limit,
            find_nearest: self.find_nearest,
            consistency: self.consistency,
            collection_cache: Default::default(),
        };
        query.validate(self.enterprise_edition)?;
        Ok(query)
    }
}

/// A Firestore query.
#[derive(Clone, Debug)]
pub struct Query {
    parent: Ref,

    /// Determines whether the query corresponds to an Enterprise Edition database
    enterprise_edition: bool,

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

    find_nearest: Option<FindNearest>,

    consistency: ReadConsistency,

    collection_cache: HashMap<DefaultAtom, bool>,
}

impl Query {
    pub fn from_structured(
        parent: Ref,
        query: StructuredQuery,
        consistency: ReadConsistency,
        enterprise_edition: bool,
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
            find_nearest,
        } = query;

        if offset < 0 {
            return Err(GenericDatabaseError::invalid_argument(
                "offset cannot be negative",
            ));
        }
        if limit.as_ref().is_some_and(|l| l.value < 0) {
            return Err(GenericDatabaseError::invalid_argument(
                "limit cannot be negative",
            ));
        }

        let find_nearest = find_nearest
            .map(|fn_proto| FindNearest::try_from_proto(fn_proto, enterprise_edition))
            .transpose()?;

        let mut query = Box::new(Self {
            parent,
            select: select.map(Projection::try_from).transpose()?,
            enterprise_edition,
            from,
            filter: filter.map(TryInto::try_into).transpose()?,
            order_by: order_by.into_iter().map(TryInto::try_into).try_collect()?,
            start_at,
            end_at,
            offset: offset as usize,
            limit: limit.map(|v| v.value as usize),
            find_nearest,
            consistency,
            collection_cache: Default::default(),
        });
        query.validate(enterprise_edition)?;
        Ok(query)
    }

    fn validate(&mut self, enterprise_edition: bool) -> Result<()> {
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
            for ff in filter.field_filters() {
                vector::validate_query_operand_value(&ff.value, enterprise_edition)?;
            }
        }
        for cursor in [&self.start_at, &self.end_at].into_iter().flatten() {
            for v in &cursor.values {
                vector::validate_query_operand_value(v, enterprise_edition)?;
            }
        }

        if let Some(spec) = &self.find_nearest {
            spec.validate(enterprise_edition)?;
            if self.limit.is_some() {
                return Err(GenericDatabaseError::invalid_argument(
                    "A query limit cannot be used with FindNearest",
                ));
            }
            if self.offset > 0 {
                return Err(GenericDatabaseError::invalid_argument(
                    "A query offset cannot be used with FindNearest",
                ));
            }
            if self.start_at.is_some() || self.end_at.is_some() {
                return Err(GenericDatabaseError::invalid_argument(
                    "A cursor cannot be used with FindNearest",
                ));
            }
            if !enterprise_edition && let Some(filter) = &self.filter {
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
        } else {
            let last_dir = self
                .order_by
                .last()
                .map_or(Direction::Ascending, |o| o.direction);

            if !enterprise_edition && let Some(filter) = &self.filter {
                for field in filter.sorted_non_name_inequality_fields() {
                    if !self.order_by.iter().any(|o| o.field == field) {
                        self.order_by.push(Order {
                            field,
                            direction: last_dir,
                        });
                    }
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

            let has_all_descendants = self.from.iter().any(|selector| selector.all_descendants);
            if (!enterprise_edition || has_all_descendants)
                && !self.order_by.iter().any(|o| o.field.is_document_name())
            {
                self.order_by.push(Order {
                    field:     FieldReference::DocumentName,
                    direction: last_dir,
                })
            }

            if self
                .start_at
                .as_ref()
                .is_some_and(|c| c.values.len() > self.order_by.len())
                || self
                    .end_at
                    .as_ref()
                    .is_some_and(|c| c.values.len() > self.order_by.len())
            {
                return Err(GenericDatabaseError::invalid_argument(
                    "Cursor has too many values.",
                ));
            }
        }
        Ok(())
    }

    pub fn has_find_nearest(&self) -> bool {
        self.find_nearest.is_some()
    }

    pub fn reset_on_update(&self) -> bool {
        self.find_nearest.is_none() && self.order_by.iter().any(|o| !o.field.is_document_name())
    }

    pub(crate) fn affects_offset_window(&mut self, collection: &CollectionRef) -> bool {
        self.offset > 0 && self.includes_collection(collection)
    }

    pub fn reset_consistency_to_default(&mut self) {
        self.consistency = ReadConsistency::Default;
    }

    pub async fn once(&mut self, db: &FirestoreDatabase) -> Result<(Timestamp, Vec<QueryMatch>)> {
        let txn; // If we find a transaction, we borrow from it.
        let lock_strategy = match self.consistency {
            ReadConsistency::Default => LockStrategy::ReadTime(Timestamp::now()),
            ReadConsistency::ReadTime(timestamp) => {
                LockStrategy::ReadTime(validate_read_time(timestamp)?)
            }
            ReadConsistency::Transaction(transaction_id) => {
                txn = db.transactions.get(transaction_id).await?;
                LockStrategy::from_transaction(&txn).await
            }
        };
        self.once_with_strategy(db, lock_strategy).await
    }

    pub(crate) async fn once_for_validation(
        &mut self,
        db: &FirestoreDatabase,
        held_write_guards: &BTreeMap<DocumentRef, OwnedDocumentContentsWriteGuard>,
    ) -> Result<(Timestamp, Vec<QueryMatch>)> {
        self.once_with_strategy(db, LockStrategy::ValidationReadOnly(held_write_guards))
            .await
    }

    async fn once_with_strategy(
        &mut self,
        db: &FirestoreDatabase,
        mut lock_strategy: LockStrategy<'_>,
    ) -> Result<(Timestamp, Vec<QueryMatch>)> {
        // First collect all Arc<Collection>s in a Vec to release the collection lock asap.
        let mut collections = self.applicable_collections(db).await;
        collections.sort_by(|a, b| a.name.cmp(&b.name));

        let result = if let Some(spec) = self.find_nearest.clone() {
            let mut scanned_guards = vec![];
            let mut candidates = vec![];
            for col in collections {
                let mut docs = col.docs().await;
                docs.sort_by(|a, b| a.name.cmp_firestore_order(&b.name));
                for meta in docs {
                    if let Some(mut doc) = lock_strategy.read(&meta).await? {
                        if self.enterprise_edition
                            && let Some(guard) = doc.guard.as_ref()
                        {
                            scanned_guards.push(Arc::clone(guard));
                        }
                        if !self.matches_prefilters(&doc.version)? {
                            continue;
                        }
                        let Some(dist) =
                            self.evaluate_find_nearest_candidate(&spec, &doc.version.fields)?
                        else {
                            continue;
                        };
                        doc.computed_distance = Some(dist);
                        candidates.push(doc);
                    }
                }
            }

            vector::select_and_sort_nearest(&mut candidates, Some(spec.limit), |a, b| {
                let da = a.computed_distance.unwrap_or(f64::NAN);
                let db = b.computed_distance.unwrap_or(f64::NAN);
                spec.distance_measure
                    .cmp_distance(da, db)
                    .then_with(|| self.find_nearest_tie_cmp(&a.version, &b.version))
            });

            if self.enterprise_edition {
                lock_strategy.retain_pessimistic_guards(scanned_guards.into_iter());
            }

            lock_strategy
                .confirm_all(candidates.into_iter(), self)
                .await
        } else {
            let mut buffer = vec![];
            for (col_idx, col) in collections.into_iter().enumerate() {
                let mut ordered = col.docs().await.into_iter().enumerate().collect_vec();
                ordered.sort_by(|(_, a), (_, b)| a.name.cmp_firestore_order(&b.name));
                for (orig_idx, meta) in ordered {
                    if let Some(doc) = lock_strategy.read(&meta).await?
                        && self.includes_document(&doc.version)?
                    {
                        buffer.push((col_idx, orig_idx, doc));
                    }
                }
            }

            if self.order_by.is_empty() {
                buffer.sort_unstable_by_key(|&(col_idx, orig_idx, _)| (col_idx, orig_idx));
            } else {
                buffer.sort_unstable_by(|(_, _, a), (_, _, b)| {
                    self.order_by_cmp(&a.version, &b.version)
                });
            }

            if self.offset > 0 {
                buffer.drain(0..self.offset.min(buffer.len()));
            }

            if let Some(limit) = self.limit {
                buffer.truncate(limit)
            }

            // Now make sure that the transaction (if any and if needed) acquires a lock to all
            // resulting documents or remember which results were served in case of optimistic
            // concurrency.
            lock_strategy
                .confirm_all(buffer.into_iter().map(|(_, _, doc)| doc), self)
                .await
        };

        Ok((lock_strategy.read_time(), result))
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
            self.find_nearest.is_some().then_some("find_nearest"),
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

        let mut all_docs = HashSet::new();
        for col in db.get_all_collections().await {
            let Some(rest) = col.name.strip_collection_prefix(&parent) else {
                continue;
            };
            let Some((document_id, _)) = rest.split_once('/') else {
                continue;
            };
            if !col.has_doc().await? {
                continue;
            }
            all_docs.insert(DocumentRef::new(parent.clone(), document_id));
        }
        let mut result = vec![];
        for name in all_docs {
            if db.get_doc(&name, self.consistency).await?.is_none() {
                result.push(name.to_string());
            }
        }
        result.sort();
        Ok(result)
    }

    /// Find all collections that are selected in the FROM clause of the query.
    async fn applicable_collections(&mut self, db: &FirestoreDatabase) -> Vec<Arc<Collection>> {
        db.collections
            .read()
            .await
            .values()
            .filter(|&col| self.includes_collection(&col.name))
            .map(Arc::clone)
            .collect_vec()
    }

    /// Check whether the given collection matches the FROM clause of the query.
    fn includes_collection(&mut self, collection: &CollectionRef) -> bool {
        if let Some(&r) = self.collection_cache.get(&collection.collection_id) {
            return r;
        }
        // The collection name should start with the Query's parent...
        let included = collection
            .strip_prefix(&self.parent)
            .is_some_and(|remaining_path| {
                // ... and it should match one of the FROM selectors:
                self.from.iter().any(|selector| {
                    if !selector.all_descendants {
                        return remaining_path == selector.collection_id;
                    }
                    if selector.collection_id.is_empty() {
                        // collection_id empty is a special case where all collections should match
                        return true;
                    }
                    // Searching for documents where the direct parent collection is equal to the
                    // given collection_id when `all_descendants == true`. "The direct parent
                    // collection" translates to the rightmost collection name in
                    // `remaining_path`. So we check whether the `remaining_path` ends with the
                    // requested `collection_id` and is preceded by either a '/' or nothing (when
                    // `remaining_path == collection_id`).
                    remaining_path
                        .strip_suffix(&selector.collection_id)
                        .is_some_and(|rem| rem.is_empty() || rem.ends_with('/'))
                })
            });
        self.collection_cache
            .insert(collection.collection_id.clone(), included);
        included
    }

    pub(crate) fn matches_prefilters(&mut self, doc: &StoredDocumentVersion) -> Result<bool> {
        if !self.includes_collection(&doc.name.collection_ref) {
            return Ok(false);
        }
        if let Some(filter) = &self.filter
            && !filter.eval(doc, self.enterprise_edition)?
        {
            return Ok(false);
        }
        for order_by in &self.order_by {
            if order_by.field.get_value(doc).is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    pub fn includes_document(&mut self, doc: &StoredDocumentVersion) -> Result<bool> {
        if !self.matches_prefilters(doc)? {
            return Ok(false);
        }
        if let Some(cursor) = &self.start_at
            && self.doc_on_left_of_cursor(doc, cursor)
        {
            return Ok(false);
        }
        if let Some(cursor) = &self.end_at
            && !self.doc_on_left_of_cursor(doc, cursor)
        {
            return Ok(false);
        }
        if let Some(spec) = &self.find_nearest {
            return Ok(self
                .evaluate_find_nearest_candidate(spec, &doc.fields)?
                .is_some());
        }
        Ok(true)
    }

    fn evaluate_find_nearest_candidate(
        &self,
        spec: &FindNearest,
        fields: &HashMap<String, Value>,
    ) -> Result<Option<f64>> {
        let Some(coords) = spec
            .vector_field
            .get_value(fields)
            .and_then(|v| v.iter_vector_f64())
        else {
            return Ok(None);
        };
        if coords.len() != spec.query_vector.len() {
            return Ok(None);
        }
        if !self.enterprise_edition && coords.clone().any(|x| !x.is_finite()) {
            return Ok(None);
        }
        let dist = vector::compute_query_distance(
            spec.distance_measure,
            self.enterprise_edition,
            spec.query_vector.iter().copied(),
            coords,
        )?;
        if let Some(threshold) = spec.distance_threshold {
            if !spec.distance_measure.passes_threshold(dist, threshold) {
                return Ok(None);
            }
        } else if !self.enterprise_edition {
            let within_default_bound = match spec.distance_measure {
                DistanceMeasure::Euclidean | DistanceMeasure::Cosine => dist < f64::INFINITY,
                DistanceMeasure::DotProduct => dist > f64::NEG_INFINITY,
            };
            if !within_default_bound {
                return Ok(None);
            }
        }
        Ok(Some(dist))
    }

    fn find_nearest_tie_cmp(
        &self,
        a: &Arc<StoredDocumentVersion>,
        b: &Arc<StoredDocumentVersion>,
    ) -> cmp::Ordering {
        if !self.enterprise_edition && !self.order_by.is_empty() {
            let ord = self.order_by_cmp(a, b);
            if ord != cmp::Ordering::Equal {
                return ord;
            }
            let name_cmp = a.name.cmp_firestore_order(&b.name);
            return match self.order_by.last().map(|o| o.direction) {
                Some(Direction::Descending) => name_cmp.reverse(),
                _ => name_cmp,
            };
        }
        if let Some(filter) = &self.filter {
            for field in filter.sorted_non_name_inequality_fields() {
                let ord = field.get_value(a).cmp(&field.get_value(b));
                if ord != cmp::Ordering::Equal {
                    return ord;
                }
            }
        }
        a.name.cmp_firestore_order(&b.name)
    }

    pub fn project(&self, item: &QueryMatch) -> Document {
        let distance = self.distance_result_field().zip(item.computed_distance);
        project_with_distance(&self.select, &item.version, distance)
    }

    pub(crate) fn project_base(&self, version: &StoredDocumentVersion) -> Document {
        self.select.project(version)
    }

    pub(crate) fn distance_result_field(&self) -> Option<&FieldPath> {
        self.find_nearest
            .as_ref()
            .and_then(|spec| spec.distance_result_field.as_ref())
    }

    pub(crate) fn effective_numeric_field_value<'a>(
        &self,
        item: &'a QueryMatch,
        field_ref: &FieldReference,
    ) -> Option<Cow<'a, Value>> {
        if let (Some(dist_path), Some(dist)) =
            (self.distance_result_field(), item.computed_distance)
            && let FieldReference::FieldPath(fp) = field_ref
        {
            if fp == dist_path {
                return Some(Cow::Owned(Value::double(dist)));
            }
            if fp.starts_with(dist_path) || dist_path.starts_with(fp) {
                return None;
            }
        }
        field_ref.get_value(&item.version)
    }

    fn order_by_cmp(
        &self,
        a: &Arc<StoredDocumentVersion>,
        b: &Arc<StoredDocumentVersion>,
    ) -> cmp::Ordering {
        use cmp::Ordering::*;
        for order in &self.order_by {
            let a = order.field.get_value(a);
            let b = order.field.get_value(b);
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
    fn doc_on_left_of_cursor(&self, doc: &StoredDocumentVersion, cursor: &Cursor) -> bool {
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

#[derive(Clone, Debug)]
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

#[derive(Debug)]
enum LockStrategy<'a> {
    PessimisticTransaction(PessimisticTransactionQuerySupport<'a>),
    OptimisticTransaction(&'a OptimisticState),
    ValidationReadOnly(&'a BTreeMap<DocumentRef, OwnedDocumentContentsWriteGuard>),
    ReadTime(Timestamp),
}

impl<'a> LockStrategy<'a> {
    async fn from_transaction(txn: &'a Transaction) -> Self {
        if let Some(rw_txn) = txn.as_read_write() {
            match &rw_txn.mode {
                ConcurrencyMode::Pessimistic(state) => {
                    Self::PessimisticTransaction(state.query_support().await)
                }
                ConcurrencyMode::Optimistic(state) => Self::OptimisticTransaction(state),
            }
        } else {
            Self::ReadTime(txn.read_time().unwrap_or_else(Timestamp::now))
        }
    }

    async fn read(&self, meta: &Arc<DocumentMeta>) -> Result<Option<LockStrategyDocument>> {
        let read_time = match self {
            LockStrategy::PessimisticTransaction(txn) => {
                let read_guard = txn.get_read_guard(meta).await?;
                return Ok(read_guard.current_version().cloned().map(|version| {
                    LockStrategyDocument {
                        version,
                        guard: Some(read_guard),
                        computed_distance: None,
                    }
                }));
            }
            LockStrategy::ValidationReadOnly(held_write_guards) => {
                let version = if let Some(wg) = held_write_guards.get(&meta.name) {
                    wg.current_version().cloned()
                } else {
                    meta.read().await?.current_version().cloned()
                };
                return Ok(version.map(|version| LockStrategyDocument {
                    version,
                    guard: None,
                    computed_distance: None,
                }));
            }
            LockStrategy::OptimisticTransaction(state) => *state.read_time,
            LockStrategy::ReadTime(rt) => *rt,
        };
        let read_guard = meta.read().await?;
        Ok(read_guard
            .version_at_time(read_time)
            .cloned()
            .map(|version| LockStrategyDocument {
                version,
                guard: None,
                computed_distance: None,
            }))
    }

    fn retain_pessimistic_guards(
        &mut self,
        guards: impl Iterator<Item = Arc<OwnedDocumentContentsReadGuard>>,
    ) {
        if let LockStrategy::PessimisticTransaction(txn) = self {
            for guard in guards {
                txn.manage_read_guard(guard);
            }
        }
    }

    async fn confirm_all(
        &mut self,
        docs: impl Iterator<Item = LockStrategyDocument>,
        query: &Query,
    ) -> Vec<QueryMatch> {
        let result = docs
            .map(|doc| {
                if let LockStrategy::PessimisticTransaction(txn) = self
                    && let Some(guard) = doc.guard
                {
                    txn.manage_read_guard(guard);
                }
                QueryMatch {
                    version: doc.version,
                    computed_distance: doc.computed_distance,
                }
            })
            .collect_vec();

        if let Self::OptimisticTransaction(state) = self {
            state
                .observed_docs
                .lock()
                .await
                .extend(result.iter().map(|m| m.version.name.clone()));
            let snapshot = result
                .iter()
                .map(|m| (m.version.name.clone(), m.version.update_time))
                .collect();
            let mut verification_query = query.clone();
            verification_query.reset_consistency_to_default();
            state.executed_queries.lock().await.push(ExecutedQuery {
                query: verification_query,
                snapshot,
            });
        }

        result
    }

    fn read_time(self) -> Timestamp {
        match self {
            // Using `Timestamp::now()` is correct here, even if this may be much later than the
            // actual time the documents were read. We can simply take Timestamp::now,
            // because the documents are locked and cannot change while being locked. So
            // we are sure that the documents are unchanged and are still the same at
            // this particular point in time.
            LockStrategy::PessimisticTransaction(_) | LockStrategy::ValidationReadOnly(_) => {
                Timestamp::now()
            }
            LockStrategy::OptimisticTransaction(state) => *state.read_time,
            LockStrategy::ReadTime(timestamp) => timestamp,
        }
    }
}

struct LockStrategyDocument {
    version: Arc<StoredDocumentVersion>,
    guard: Option<Arc<OwnedDocumentContentsReadGuard>>,
    computed_distance: Option<f64>,
}
