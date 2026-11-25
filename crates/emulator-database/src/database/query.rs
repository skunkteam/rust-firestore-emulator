use std::{
    borrow::Cow,
    cmp,
    collections::{BTreeMap, HashMap, HashSet},
    iter,
    ops::Deref,
    sync::Arc,
};

use googleapis::{
    VectorClassification, VectorFormatError, classify_vector_map,
    google::{
        firestore::v1::{
            Cursor, Document, StructuredQuery, Value,
            structured_query::{self, CollectionSelector},
            value::ValueType,
        },
        protobuf::Timestamp,
    },
};
use itertools::Itertools;
use string_cache::DefaultAtom;

use self::filter::{CompositeOperator, FieldOperator, Filter, UnaryOperator};
pub use self::find_nearest::FindNearest;
pub use super::vector::DistanceMeasure;
use super::{
    FirestoreDatabase,
    collection::Collection,
    document::{DocumentMeta, StoredDocumentVersion},
    field_path::{FieldPath, FieldReference},
    lock::{LockSession, ReadLease, WriteLease},
    projection::{Project, Projection, project_with_distance},
    read_consistency::{ReadConsistency, validate_read_time},
    reference::{self, CollectionRef, Ref, RootRef},
    validation, vector,
};
use crate::{
    GenericDatabaseError,
    database::transaction::{ConcurrencyMode, ExecutedQuery, OptimisticState, Transaction},
    error::{AbortReason, CommitError, Result},
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
        query.validate()?;
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

const MAX_STANDARD_DISJUNCTIONS: u8 = 30;
const DNF_DISJUNCTION_CAP: u8 = 31;
const MAX_ARRAY_CONTAINS_CAP: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DnfSummary {
    disjunctions: u8,
    max_array_contains: u8,
}

fn value_dnf_dedup_eq(a: &Value, b: &Value) -> bool {
    (a.is_nan() && b.is_nan()) || a.storage_eq(b)
}

fn unique_value_count(values: &[Value]) -> u8 {
    values
        .iter()
        .enumerate()
        .filter(|&(i, v)| values[..i].iter().all(|prev| !value_dnf_dedup_eq(prev, v)))
        .count()
        .min(DNF_DISJUNCTION_CAP as usize) as u8
}

impl DnfSummary {
    fn from_filter(filter: &Filter) -> Self {
        match filter {
            Filter::Unary(_) => Self {
                disjunctions: 1,
                max_array_contains: 0,
            },
            Filter::Field(ff) => match ff.op {
                FieldOperator::ArrayContains => Self {
                    disjunctions: 1,
                    max_array_contains: 1,
                },
                FieldOperator::In => Self {
                    disjunctions: ff.value.as_array().map_or(1, unique_value_count),
                    max_array_contains: 0,
                },
                FieldOperator::ArrayContainsAny => Self {
                    disjunctions: ff.value.as_array().map_or(1, unique_value_count),
                    max_array_contains: 1,
                },
                _ => Self {
                    disjunctions: 1,
                    max_array_contains: 0,
                },
            },
            Filter::Composite(cf) => match cf.op {
                CompositeOperator::And => cf.filters.iter().fold(
                    Self {
                        disjunctions: 1,
                        max_array_contains: 0,
                    },
                    |acc, sub| {
                        let child = Self::from_filter(sub);
                        Self {
                            disjunctions: acc
                                .disjunctions
                                .saturating_mul(child.disjunctions)
                                .min(DNF_DISJUNCTION_CAP),
                            max_array_contains: acc
                                .max_array_contains
                                .saturating_add(child.max_array_contains)
                                .min(MAX_ARRAY_CONTAINS_CAP),
                        }
                    },
                ),
                CompositeOperator::Or => cf.filters.iter().fold(
                    Self {
                        disjunctions: 0,
                        max_array_contains: 0,
                    },
                    |acc, sub| {
                        let child = Self::from_filter(sub);
                        Self {
                            disjunctions: acc
                                .disjunctions
                                .saturating_add(child.disjunctions)
                                .min(DNF_DISJUNCTION_CAP),
                            max_array_contains: acc
                                .max_array_contains
                                .max(child.max_array_contains),
                        }
                    },
                ),
            },
        }
    }
}

fn validate_query_operand_value(value: &Value, enterprise_edition: bool) -> Result<()> {
    match value.value_type.as_ref() {
        None => Err(GenericDatabaseError::invalid_argument(
            "Cannot convert firestore.v1.Value with type unset.",
        )),
        Some(ValueType::ReferenceValue(s)) => reference::parse_query_ref_root(s).map(|_| ()),
        Some(ValueType::GeoPointValue(geo)) => validation::validate_geo_point(geo),
        Some(ValueType::TimestampValue(ts)) => validation::validate_timestamp(ts),
        Some(
            ValueType::FieldReferenceValue(_)
            | ValueType::VariableReferenceValue(_)
            | ValueType::FunctionValue(_)
            | ValueType::PipelineValue(_),
        ) => Err(GenericDatabaseError::invalid_argument(
            "Value type is not supported in StructuredQuery operands.",
        )),
        Some(ValueType::MapValue(map)) => match classify_vector_map(&map.fields) {
            VectorClassification::Valid(_) => Ok(()),
            VectorClassification::Invalid(VectorFormatError::NanElement) if !enterprise_edition => {
                Ok(())
            }
            VectorClassification::Invalid(err) => Err(GenericDatabaseError::invalid_argument(
                err.as_error_message(),
            )),
            VectorClassification::NotVectorMap => {
                for v in map.fields.values() {
                    validate_query_operand_value(v, enterprise_edition)?;
                }
                Ok(())
            }
        },
        Some(ValueType::ArrayValue(arr)) => {
            for v in &arr.values {
                validate_query_operand_value(v, enterprise_edition)?;
            }
            Ok(())
        }
        Some(
            ValueType::NullValue(_)
            | ValueType::BooleanValue(_)
            | ValueType::IntegerValue(_)
            | ValueType::DoubleValue(_)
            | ValueType::StringValue(_)
            | ValueType::BytesValue(_),
        ) => Ok(()),
    }
}

fn validate_filter_common(filter: &Filter, enterprise_edition: bool) -> Result<()> {
    match filter {
        Filter::Composite(cf) => {
            if cf.op == CompositeOperator::Or && cf.filters.is_empty() {
                return Err(GenericDatabaseError::invalid_argument(
                    "Composite filter must have at least one sub-filter.",
                ));
            }
            for sub in &cf.filters {
                validate_filter_common(sub, enterprise_edition)?;
            }
            Ok(())
        }
        Filter::Unary(uf) => uf.field.validate_query_reference(),
        Filter::Field(ff) => {
            ff.field.validate_query_reference()?;
            if ff.op.is_array_operand() {
                let Some(arr) = ff.value.as_array() else {
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "'{}' requires an ArrayValue.",
                        ff.op.wire_name()
                    )));
                };
                if arr.is_empty() {
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "'{}' requires a non-empty ArrayValue.",
                        ff.op.wire_name()
                    )));
                }
            }
            validate_query_operand_value(&ff.value, enterprise_edition)
        }
    }
}

fn validate_standard_key_ref(value: &Value, expected_root: &RootRef) -> Result<()> {
    let Some(s) = value.as_reference() else {
        return Err(GenericDatabaseError::invalid_argument(
            "__key__ filter value must be a Key",
        ));
    };
    let meta = reference::parse_query_ref_root(s)?;
    if !meta.has_doc_path {
        return Err(GenericDatabaseError::invalid_argument("Key path is empty."));
    }
    if meta.has_zero_datastore_id {
        return Err(GenericDatabaseError::invalid_argument(
            "Document ID of type long must be non-zero",
        ));
    }
    if meta.project_id != expected_root.project_id.as_str()
        || meta.database_id != expected_root.database_id.as_str()
    {
        return Err(GenericDatabaseError::invalid_argument(
            "__key__ filter reference must match the target project and database",
        ));
    }
    Ok(())
}

pub(super) fn validate_cursor_key_ref(value: &Value, expected_root: &RootRef) -> Result<()> {
    let Some(s) = value.as_reference() else {
        return Err(GenericDatabaseError::invalid_argument(
            "Cursor __key__ value is not a document reference.",
        ));
    };
    let meta = reference::parse_query_ref_root(s)?;
    if meta.project_id != expected_root.project_id.as_str()
        || meta.database_id != expected_root.database_id.as_str()
    {
        return Err(GenericDatabaseError::invalid_argument(
            "The cursor key is in a different database than the query",
        ));
    }
    Ok(())
}

#[derive(Debug, Default)]
struct StandardFilterStats {
    negation_count: usize,
    has_not_in: bool,
    has_disjunction_op: bool,
}

fn collect_standard_filter_stats(
    filter: &Filter,
    expected_root: &RootRef,
    stats: &mut StandardFilterStats,
) -> Result<()> {
    match filter {
        Filter::Composite(cf) => {
            if cf.op == CompositeOperator::Or {
                stats.has_disjunction_op = true;
            }
            for sub in &cf.filters {
                collect_standard_filter_stats(sub, expected_root, stats)?;
            }
        }
        Filter::Unary(uf) => {
            if uf.field.is_document_name() {
                return Err(GenericDatabaseError::invalid_argument(
                    "__key__ filter value must be a Key",
                ));
            }
            if matches!(uf.op, UnaryOperator::IsNotNan | UnaryOperator::IsNotNull) {
                stats.negation_count += 1;
            }
        }
        Filter::Field(ff) => {
            match ff.op {
                FieldOperator::In => {
                    if ff.value.as_array().is_some_and(|arr| arr.len() > 30) {
                        return Err(GenericDatabaseError::invalid_argument(
                            "'IN' supports up to 30 comparison values.",
                        ));
                    }
                    stats.has_disjunction_op = true;
                }
                FieldOperator::ArrayContainsAny => {
                    if ff.value.as_array().is_some_and(|arr| arr.len() > 30) {
                        return Err(GenericDatabaseError::invalid_argument(
                            "'ARRAY_CONTAINS_ANY' supports up to 30 comparison values.",
                        ));
                    }
                    stats.has_disjunction_op = true;
                }
                FieldOperator::NotIn => {
                    if ff.value.as_array().is_some_and(|arr| arr.len() > 10) {
                        return Err(GenericDatabaseError::invalid_argument(
                            "'NOT_IN' supports up to 10 comparison values.",
                        ));
                    }
                    stats.has_not_in = true;
                    stats.negation_count += 1;
                }
                FieldOperator::NotEqual => {
                    stats.negation_count += 1;
                }
                _ => {}
            }

            if ff.field.is_document_name() {
                match ff.op {
                    FieldOperator::ArrayContains | FieldOperator::ArrayContainsAny => {
                        return Err(GenericDatabaseError::invalid_argument(
                            "the name __key__ is reserved",
                        ));
                    }
                    FieldOperator::In | FieldOperator::NotIn => {
                        if let Some(arr) = ff.value.as_array() {
                            for el in arr {
                                validate_standard_key_ref(el, expected_root)?;
                            }
                        }
                    }
                    _ => {
                        validate_standard_key_ref(&ff.value, expected_root)?;
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_standard_filter(filter: &Filter, expected_root: &RootRef) -> Result<()> {
    let mut stats = StandardFilterStats::default();
    collect_standard_filter_stats(filter, expected_root, &mut stats)?;
    if stats.negation_count > 1 {
        return Err(GenericDatabaseError::invalid_argument(
            "Only a single 'NOT_EQUAL', 'NOT_IN', 'IS_NOT_NAN', or 'IS_NOT_NULL' filter allowed \
             per query.",
        ));
    }
    if stats.has_not_in && stats.has_disjunction_op {
        return Err(GenericDatabaseError::invalid_argument(
            "'NOT_IN' cannot be used in the same query with 'IN', 'ARRAY_CONTAINS_ANY' or 'OR'.",
        ));
    }
    let dnf = DnfSummary::from_filter(filter);
    if dnf.max_array_contains > 1 {
        return Err(GenericDatabaseError::invalid_argument(
            "A maximum of 1 'ARRAY_CONTAINS' filter is allowed per disjunction.",
        ));
    }
    if dnf.disjunctions > MAX_STANDARD_DISJUNCTIONS {
        return Err(GenericDatabaseError::invalid_argument(
            "Too many disjunctions after normalization; maximum is 30.",
        ));
    }
    Ok(())
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
        query.validate_inner(false)?;
        Ok(query)
    }

    fn synthesize_standard_order_by(&mut self, dir: Direction) {
        if let Some(filter) = &self.filter {
            for field in filter.sorted_non_name_inequality_fields() {
                if !self.order_by.iter().any(|o| o.field == field) {
                    self.order_by.push(Order {
                        field,
                        direction: dir,
                    });
                }
            }
        }
        if !self.order_by.iter().any(|o| o.field.is_document_name()) {
            self.order_by.push(Order {
                field:     FieldReference::DocumentName,
                direction: dir,
            });
        }
    }

    fn validate(&mut self) -> Result<()> {
        self.validate_inner(true)
    }

    fn validate_inner(&mut self, validate_find_nearest_spec: bool) -> Result<()> {
        let enterprise_edition = self.enterprise_edition;
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

        for order in &self.order_by {
            order.field.validate_query_reference()?;
        }
        if let Some(filter) = &self.filter {
            validate_filter_common(filter, enterprise_edition)?;
        }
        for cursor in [&self.start_at, &self.end_at].into_iter().flatten() {
            for v in &cursor.values {
                validate_query_operand_value(v, enterprise_edition)?;
            }
        }

        if !enterprise_edition {
            if let Some(filter) = &self.filter {
                validate_standard_filter(filter, self.parent.root())?;
            }
            for (i, order) in self.order_by.iter().enumerate() {
                if self.order_by[..i]
                    .iter()
                    .any(|prev| prev.field == order.field)
                {
                    return Err(GenericDatabaseError::invalid_argument(format!(
                        "order by clause cannot contain duplicate fields: {}",
                        order.field
                    )));
                }
            }
        }

        if let Some(spec) = &self.find_nearest {
            // `FindNearest::try_from_proto` already validated the spec; only `QueryBuilder::build`
            // needs to validate programmatic `FindNearest` instances here.
            if validate_find_nearest_spec {
                spec.validate(enterprise_edition)?;
            }
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
            if !enterprise_edition {
                let last_dir = self
                    .order_by
                    .last()
                    .map_or(Direction::Ascending, |o| o.direction);
                self.synthesize_standard_order_by(last_dir);
            }
        } else {
            for cursor in [&self.start_at, &self.end_at].into_iter().flatten() {
                if cursor.values.len() > self.order_by.len() {
                    return Err(GenericDatabaseError::invalid_argument(
                        "Cursor has too many values.",
                    ));
                }
                for (order, cursor_val) in self.order_by.iter().zip(&cursor.values) {
                    if order.field.is_document_name() {
                        validate_cursor_key_ref(cursor_val, self.parent.root())?;
                    }
                }
            }

            if !enterprise_edition {
                let last_dir = self
                    .order_by
                    .last()
                    .map_or(Direction::Ascending, |o| o.direction);
                self.synthesize_standard_order_by(last_dir);
            }
        }

        if !enterprise_edition
            && self
                .order_by
                .split_last()
                .is_some_and(|(_, prefix)| prefix.iter().any(|o| o.field.is_document_name()))
        {
            return Err(GenericDatabaseError::invalid_argument(
                "order by clause cannot contain more fields after the key",
            ));
        }

        Ok(())
    }

    pub(crate) fn order_by(&self) -> &[Order] {
        &self.order_by
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
            ReadConsistency::Default => {
                if db.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                db.transactions.reap_if_due();
                LockStrategy::ReadTime(Timestamp::now())
            }
            ReadConsistency::ReadTime(timestamp) => {
                if db.is_cleared() {
                    return Err(AbortReason::DatabaseCleared.into());
                }
                db.transactions.reap_if_due();
                LockStrategy::ReadTime(validate_read_time(timestamp)?)
            }
            ReadConsistency::Transaction(transaction_id) => {
                txn = db.transactions.get_active(transaction_id)?;
                LockStrategy::from_transaction(&txn)
            }
        };
        self.once_with_strategy(db, lock_strategy)
            .await
            .map_err(Into::into)
    }

    pub(crate) async fn once_for_validation(
        &mut self,
        db: &FirestoreDatabase,
        held_write_leases: &BTreeMap<DocumentRef, WriteLease>,
    ) -> std::result::Result<(Timestamp, Vec<QueryMatch>), CommitError> {
        if db.is_cleared() {
            return Err(CommitError::Aborted(AbortReason::DatabaseCleared));
        }
        self.once_with_strategy(db, LockStrategy::ValidationReadOnly(held_write_leases))
            .await
    }

    async fn once_with_strategy(
        &mut self,
        db: &FirestoreDatabase,
        mut lock_strategy: LockStrategy<'_>,
    ) -> std::result::Result<(Timestamp, Vec<QueryMatch>), CommitError> {
        // First collect all Arc<Collection>s in a Vec to release the collection lock asap.
        let mut collections = self.applicable_collections(db).await;
        collections.sort_by(|a, b| a.name.cmp(&b.name));

        let result = if let Some(spec) = self.find_nearest.clone() {
            let mut scanned_leases = vec![];
            let mut candidates = vec![];
            for col in collections {
                let mut docs = col.docs().await;
                docs.sort_by(|a, b| a.name.cmp(&b.name));
                for meta in docs {
                    if let Some(mut doc) = lock_strategy.read(&meta).await? {
                        if self.enterprise_edition
                            && let Some(lease) = doc.lease.take()
                        {
                            scanned_leases.push(lease);
                        }
                        if !self.matches_prefilters(&doc.version) {
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

            let enterprise_tie_fields = if self.enterprise_edition {
                self.filter
                    .as_ref()
                    .map_or_else(Vec::new, Filter::sorted_non_name_inequality_fields)
            } else {
                Vec::new()
            };

            vector::select_and_sort_nearest(&mut candidates, Some(spec.limit), |a, b| {
                let da = a.computed_distance.unwrap_or(f64::NAN);
                let db = b.computed_distance.unwrap_or(f64::NAN);
                spec.distance_measure.cmp_distance(da, db).then_with(|| {
                    self.find_nearest_tie_cmp(&a.version, &b.version, &enterprise_tie_fields)
                })
            });

            if self.enterprise_edition && !scanned_leases.is_empty() {
                lock_strategy.retain_pessimistic_leases(scanned_leases)?;
            }

            lock_strategy
                .confirm_all(candidates.into_iter(), self)
                .await?
        } else {
            let mut buffer = vec![];
            for (col_idx, col) in collections.into_iter().enumerate() {
                let mut ordered = col.docs().await.into_iter().enumerate().collect_vec();
                ordered.sort_by(|(_, a), (_, b)| a.name.cmp(&b.name));
                for (orig_idx, meta) in ordered {
                    if let Some(doc) = lock_strategy.read(&meta).await?
                        && self.includes_document(&doc.version)?
                    {
                        buffer.push((col_idx, orig_idx, doc));
                    }
                }
            }

            if self.order_by.is_empty() {
                if self.from.iter().any(|selector| selector.all_descendants) {
                    // CollectionRef("sub") < CollectionRef("sub/a/sub") even though
                    // DocumentRef("sub/a/sub/x") < DocumentRef("sub/z"), so cross-collection
                    // results still need a final DocumentRef sort.
                    buffer.sort_unstable_by(|(_, _, a), (_, _, b)| {
                        a.version.name.cmp(&b.version.name)
                    });
                } else {
                    buffer.sort_unstable_by_key(|&(col_idx, orig_idx, _)| (col_idx, orig_idx));
                }
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
                .await?
        };

        Ok((lock_strategy.read_time(), result))
    }

    /// Get the names of missing documents that match this query. A document is missing if it does
    /// not exist, but there are sub-documents nested underneath it.
    pub async fn missing_docs(&self, db: &FirestoreDatabase) -> Result<Vec<String>> {
        self.missing_docs_after(db, None).await
    }

    /// Like [`Query::missing_docs`], optionally skipping candidate ancestor paths `<= after_cursor`
    /// in Firestore reference order before locking or reading documents via `db.get_doc`.
    pub(crate) async fn missing_docs_after(
        &self,
        db: &FirestoreDatabase,
        after_cursor: Option<&str>,
    ) -> Result<Vec<String>> {
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
        if db.is_cleared() {
            return Err(AbortReason::DatabaseCleared.into());
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
            if let Some(cursor) = after_cursor {
                let doc_segments = [
                    "projects",
                    parent.root_ref.project_id.as_str(),
                    "databases",
                    parent.root_ref.database_id.as_str(),
                    "documents",
                ]
                .into_iter()
                .chain(parent.collection_id.split('/'))
                .chain(iter::once(document_id));
                if !googleapis::cmp_reference_segments(doc_segments, cursor.split('/')).is_gt() {
                    continue;
                }
            }
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
        result.sort_by(|a, b| googleapis::cmp_reference_paths(a, b));
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
        if &collection.root_ref != self.parent.root() {
            return false;
        }
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

    pub(crate) fn matches_prefilters(&mut self, doc: &StoredDocumentVersion) -> bool {
        if !self.includes_collection(&doc.name.collection_ref) {
            return false;
        }
        if let Some(filter) = &self.filter
            && !filter.eval(doc, self.enterprise_edition)
        {
            return false;
        }
        for order_by in &self.order_by {
            if order_by.field.get_value(doc).is_none() {
                return false;
            }
        }
        true
    }

    pub fn includes_document(&mut self, doc: &StoredDocumentVersion) -> Result<bool> {
        if !self.matches_prefilters(doc) {
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
        enterprise_tie_fields: &[FieldReference],
    ) -> cmp::Ordering {
        if !self.enterprise_edition {
            return self.order_by_cmp(a, b);
        }
        for field in enterprise_tie_fields {
            let ord = field.get_value(a).cmp(&field.get_value(b));
            if ord != cmp::Ordering::Equal {
                return ord;
            }
        }
        a.name.cmp(&b.name)
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
            let result = match &order.field {
                FieldReference::DocumentName => a.name.cmp(&b.name),
                FieldReference::FieldPath(path) => {
                    path.get_value(&a.fields).cmp(&path.get_value(&b.fields))
                }
            };
            if result == Equal {
                continue;
            }
            return match order.direction {
                Direction::Descending => result.reverse(),
                Direction::Ascending => result,
            };
        }
        Equal
    }

    /// Returns true when the given document is "on the left" of the given cursor.
    fn doc_on_left_of_cursor(&self, doc: &StoredDocumentVersion, cursor: &Cursor) -> bool {
        use cmp::Ordering::*;
        for (order, cursor_value) in self.order_by.iter().zip(&cursor.values) {
            let Some(doc_value) = order.field.get_value(doc) else {
                return false;
            };
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

impl Order {
    pub(crate) fn field(&self) -> &FieldReference {
        &self.field
    }

    #[cfg(test)]
    pub(crate) fn direction(&self) -> Direction {
        self.direction
    }
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
    PessimisticTransaction(&'a Arc<LockSession>),
    OptimisticTransaction(&'a OptimisticState),
    ValidationReadOnly(&'a BTreeMap<DocumentRef, WriteLease>),
    ReadTime(Timestamp),
}

impl<'a> LockStrategy<'a> {
    fn from_transaction(txn: &'a Transaction) -> Self {
        if let Some(rw_txn) = txn.as_read_write() {
            match &rw_txn.mode {
                ConcurrencyMode::Pessimistic(state) => {
                    Self::PessimisticTransaction(state.session())
                }
                ConcurrencyMode::Optimistic(state) => Self::OptimisticTransaction(state),
            }
        } else {
            Self::ReadTime(txn.read_time().unwrap_or_else(Timestamp::now))
        }
    }

    async fn read(
        &self,
        meta: &Arc<DocumentMeta>,
    ) -> std::result::Result<Option<LockStrategyDocument>, CommitError> {
        let read_time = match self {
            LockStrategy::PessimisticTransaction(session) => {
                let lease = session.acquire_read(&meta.name).await?;
                let (version, last_updated) =
                    meta.read_with(&lease, |c| (c.current_version().cloned(), c.last_updated()))?;
                return Ok(version.map(|version| LockStrategyDocument {
                    version,
                    lease: Some((lease, last_updated)),
                    computed_distance: None,
                }));
            }
            LockStrategy::ValidationReadOnly(held_write_leases) => {
                let version = if let Some(lease) = held_write_leases.get(&meta.name) {
                    meta.read_with_write_lease(lease, |c| c.current_version().cloned())?
                } else {
                    meta.read_latest_uncontended(|c| c.current_version().cloned())
                };
                return Ok(version.map(|version| LockStrategyDocument {
                    version,
                    lease: None,
                    computed_distance: None,
                }));
            }
            LockStrategy::OptimisticTransaction(state) => *state.read_time,
            LockStrategy::ReadTime(rt) => *rt,
        };
        Ok(meta
            .read_mvcc_at(read_time)
            .map(|version| LockStrategyDocument {
                version,
                lease: None,
                computed_distance: None,
            }))
    }

    fn retain_pessimistic_leases(
        &mut self,
        leases: impl IntoIterator<Item = (ReadLease, Option<Timestamp>)>,
    ) -> std::result::Result<(), CommitError> {
        if let LockStrategy::PessimisticTransaction(session) = self {
            session.retain_reads(leases)?;
        }
        Ok(())
    }

    async fn confirm_all(
        &mut self,
        docs: impl Iterator<Item = LockStrategyDocument>,
        query: &Query,
    ) -> std::result::Result<Vec<QueryMatch>, CommitError> {
        let mut leases_to_retain = Vec::new();
        let mut result = Vec::new();
        for doc in docs {
            if let Some(lease) = doc.lease {
                leases_to_retain.push(lease);
            }
            result.push(QueryMatch {
                version: doc.version,
                computed_distance: doc.computed_distance,
            });
        }
        if let LockStrategy::PessimisticTransaction(session) = self
            && !leases_to_retain.is_empty()
        {
            session.retain_reads(leases_to_retain)?;
        }

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

        Ok(result)
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
    lease: Option<(ReadLease, Option<Timestamp>)>,
    computed_distance: Option<f64>,
}
