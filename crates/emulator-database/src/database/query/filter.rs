use std::iter;

use googleapis::google::firestore::v1::{Value, structured_query};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
    database::{document::StoredDocumentVersion, field_path::FieldReference},
    error::Result,
    unimplemented,
};

#[derive(Debug)]
pub enum Filter {
    Composite(CompositeFilter),
    Field(FieldFilter),
    Unary(UnaryFilter),
}

impl Filter {
    pub fn get_inequality_fields(&self) -> impl Iterator<Item = &FieldReference> {
        self.depth_first().filter_map(|f| match f {
            Filter::Composite(_) => None,
            Filter::Field(f) => f.op.is_inequality().then_some(&f.field),
            Filter::Unary(f) => f.op.is_inequality().then_some(&f.field),
        })
    }

    pub fn field_filters(&self) -> impl Iterator<Item = &FieldFilter> {
        self.depth_first().filter_map(Filter::as_field)
    }

    fn depth_first(&self) -> Box<dyn Iterator<Item = &Filter> + '_> {
        let me = iter::once(self);
        match self {
            Filter::Composite(f) => {
                Box::new(me.chain(f.filters.iter().flat_map(|f| f.depth_first())))
            }
            Filter::Field(_) | Filter::Unary(_) => Box::new(me),
        }
    }

    pub fn eval(&self, version: &StoredDocumentVersion) -> Result<bool> {
        match self {
            Filter::Composite(f) => f.eval(version),
            Filter::Field(f) => f.eval(version),
            Filter::Unary(f) => Ok(f.eval(version)),
        }
    }

    pub fn as_composite(&self) -> Option<&CompositeFilter> {
        if let Self::Composite(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_field(&self) -> Option<&FieldFilter> {
        if let Self::Field(v) = self {
            Some(v)
        } else {
            None
        }
    }

    pub fn as_unary(&self) -> Option<&UnaryFilter> {
        if let Self::Unary(v) = self {
            Some(v)
        } else {
            None
        }
    }
}

impl TryFrom<structured_query::Filter> for Filter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::Filter) -> Result<Self, Self::Error> {
        use structured_query::filter::FilterType::*;
        match value.filter_type.expect("filter without filter_type") {
            CompositeFilter(f) => Ok(Self::Composite(f.try_into()?)),
            FieldFilter(f) => Ok(Self::Field(f.try_into()?)),
            UnaryFilter(f) => Ok(Self::Unary(f.try_into()?)),
        }
    }
}

#[derive(Debug)]
pub struct CompositeFilter {
    pub op:      CompositeOperator,
    pub filters: Vec<Filter>,
}

impl CompositeFilter {
    fn eval(&self, version: &StoredDocumentVersion) -> Result<bool> {
        for filter in &self.filters {
            let filter_result = filter.eval(version)?;
            match (&self.op, filter_result) {
                (CompositeOperator::And, true) | (CompositeOperator::Or, false) => continue,
                (_, result) => return Ok(result),
            }
        }
        Ok(self.op.is_and())
    }
}

impl TryFrom<structured_query::CompositeFilter> for CompositeFilter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::CompositeFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            op:      value.op().try_into()?,
            filters: value
                .filters
                .into_iter()
                .map(Filter::try_from)
                .try_collect()?,
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum CompositeOperator {
    And,
    Or,
}

impl CompositeOperator {
    /// Returns `true` if the composite operator is [`And`].
    ///
    /// [`And`]: CompositeOperator::And
    #[must_use]
    fn is_and(&self) -> bool {
        matches!(self, Self::And)
    }
}

impl TryFrom<structured_query::composite_filter::Operator> for CompositeOperator {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::composite_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::composite_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(GenericDatabaseError::invalid_argument(
                "invalid structured_query::composite_filter::Operator",
            )),
            Other::And => Ok(Self::And),
            Other::Or => Ok(Self::Or),
        }
    }
}

#[derive(Debug)]
pub struct FieldFilter {
    pub field: FieldReference,
    pub op:    FieldOperator,
    pub value: Value,
}

impl FieldFilter {
    fn eval(&self, version: &StoredDocumentVersion) -> Result<bool> {
        let Some(value) = self.field.get_value(version) else {
            return Ok(false);
        };
        let value = value.as_ref();
        if self.op.needs_type_compat() && !value.is_compatible_with(&self.value) {
            return Ok(false);
        }
        use FieldOperator::*;
        Ok(match self.op {
            LessThan => value < &self.value,
            LessThanOrEqual => value <= &self.value,
            GreaterThan => value > &self.value,
            GreaterThanOrEqual => value >= &self.value,

            // Workaround for incompatible `PartialEq` implementation
            #[allow(clippy::double_comparisons)]
            Equal => value <= &self.value && value >= &self.value,

            NotEqual => !value.is_null() && value != &self.value,
            ArrayContains => value
                .as_array()
                .is_some_and(|arr| arr.contains(&self.value)),
            In => self.value.as_array().is_some_and(|arr| arr.contains(value)),
            ArrayContainsAny => unimplemented!("ArrayContainsAny"),
            NotIn => unimplemented!("NotIn"),
        })
    }
}

impl TryFrom<structured_query::FieldFilter> for FieldFilter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::FieldFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            field: value
                .field
                .as_ref()
                .expect("missing field in FieldFilter")
                .field_path
                .parse()?,
            op:    value.op().try_into()?,
            value: value.value.expect("missing value in FieldFilter"),
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FieldOperator {
    /// The given `field` is less than the given `value`.
    ///
    /// Requires:
    ///
    /// * That `field` come first in `order_by`.
    LessThan,
    /// The given `field` is less than or equal to the given `value`.
    ///
    /// Requires:
    ///
    /// * That `field` come first in `order_by`.
    LessThanOrEqual,
    /// The given `field` is greater than the given `value`.
    ///
    /// Requires:
    ///
    /// * That `field` come first in `order_by`.
    GreaterThan,
    /// The given `field` is greater than or equal to the given `value`.
    ///
    /// Requires:
    ///
    /// * That `field` come first in `order_by`.
    GreaterThanOrEqual,
    /// The given `field` is equal to the given `value`.
    Equal,
    /// The given `field` is not equal to the given `value`.
    ///
    /// Requires:
    ///
    /// * No other `NOT_EQUAL`, `NOT_IN`, `IS_NOT_NULL`, or `IS_NOT_NAN`.
    /// * That `field` comes first in the `order_by`.
    NotEqual,
    /// The given `field` is an array that contains the given `value`.
    ArrayContains,
    /// The given `field` is equal to at least one value in the given array.
    ///
    /// Requires:
    ///
    /// * That `value` is a non-empty `ArrayValue`, subject to disjunction limits.
    /// * No `NOT_IN` filters in the same query.
    In,
    /// The given `field` is an array that contains any of the values in the
    /// given array.
    ///
    /// Requires:
    ///
    /// * That `value` is a non-empty `ArrayValue`, subject to disjunction limits.
    /// * No other `ARRAY_CONTAINS_ANY` filters within the same disjunction.
    /// * No `NOT_IN` filters in the same query.
    ArrayContainsAny,
    /// The value of the `field` is not in the given array.
    ///
    /// Requires:
    ///
    /// * That `value` is a non-empty `ArrayValue` with at most 10 values.
    /// * No other `OR`, `IN`, `ARRAY_CONTAINS_ANY`, `NOT_IN`, `NOT_EQUAL`, `IS_NOT_NULL`, or
    ///   `IS_NOT_NAN`.
    /// * That `field` comes first in the `order_by`.
    NotIn,
}

impl FieldOperator {
    pub(crate) fn is_inequality(&self) -> bool {
        match self {
            Self::LessThan
            | Self::LessThanOrEqual
            | Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::NotEqual
            | Self::NotIn => true,
            Self::Equal | Self::ArrayContains | Self::In | Self::ArrayContainsAny => false,
        }
    }

    pub(crate) fn is_array_contains(&self) -> bool {
        match self {
            Self::LessThan
            | Self::LessThanOrEqual
            | Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::Equal
            | Self::NotEqual
            | Self::In
            | Self::NotIn => false,
            Self::ArrayContains | Self::ArrayContainsAny => true,
        }
    }

    pub(crate) fn needs_type_compat(&self) -> bool {
        matches!(
            self,
            Self::LessThan
                | Self::LessThanOrEqual
                | Self::GreaterThan
                | Self::GreaterThanOrEqual
                | Self::Equal
        )
    }
}

impl TryFrom<structured_query::field_filter::Operator> for FieldOperator {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::field_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::field_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(GenericDatabaseError::invalid_argument(
                "invalid structured_query::field_filter::Operator",
            )),
            Other::LessThan => Ok(Self::LessThan),
            Other::LessThanOrEqual => Ok(Self::LessThanOrEqual),
            Other::GreaterThan => Ok(Self::GreaterThan),
            Other::GreaterThanOrEqual => Ok(Self::GreaterThanOrEqual),
            Other::Equal => Ok(Self::Equal),
            Other::NotEqual => Ok(Self::NotEqual),
            Other::ArrayContains => Ok(Self::ArrayContains),
            Other::In => Ok(Self::In),
            Other::ArrayContainsAny => Ok(Self::ArrayContainsAny),
            Other::NotIn => Ok(Self::NotIn),
        }
    }
}

#[derive(Debug)]
pub struct UnaryFilter {
    pub field: FieldReference,
    pub op:    UnaryOperator,
}

impl UnaryFilter {
    fn eval(&self, version: &StoredDocumentVersion) -> bool {
        let Some(value) = self.field.get_value(version) else {
            return false;
        };
        let value = value.as_ref();
        match self.op {
            UnaryOperator::IsNan => value.is_nan(),
            UnaryOperator::IsNull => value.is_null(),
            UnaryOperator::IsNotNan => !value.is_null() && !value.is_nan(),
            UnaryOperator::IsNotNull => !value.is_null(),
        }
    }
}

impl TryFrom<structured_query::UnaryFilter> for UnaryFilter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::UnaryFilter) -> Result<Self, Self::Error> {
        let structured_query::unary_filter::OperandType::Field(field) = value
            .operand_type
            .as_ref()
            .expect("missing operand_type in UnaryFilter");
        Ok(Self {
            field: field.field_path.parse()?,
            op:    value.op().try_into()?,
        })
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug)]
pub enum UnaryOperator {
    /// The given `field` is equal to `NaN`.
    IsNan,
    /// The given `field` is equal to `NULL`.
    IsNull,
    /// The given `field` is not equal to `NaN`.
    ///
    /// Requires:
    ///
    /// * No other `NOT_EQUAL`, `NOT_IN`, `IS_NOT_NULL`, or `IS_NOT_NAN`.
    /// * That `field` comes first in the `order_by`.
    IsNotNan,
    /// The given `field` is not equal to `NULL`.
    ///
    /// Requires:
    ///
    /// * A single `NOT_EQUAL`, `NOT_IN`, `IS_NOT_NULL`, or `IS_NOT_NAN`.
    /// * That `field` comes first in the `order_by`.
    IsNotNull,
}

impl UnaryOperator {
    pub(crate) fn is_inequality(&self) -> bool {
        match self {
            UnaryOperator::IsNan | UnaryOperator::IsNull => false,
            UnaryOperator::IsNotNan | UnaryOperator::IsNotNull => true,
        }
    }
}

impl TryFrom<structured_query::unary_filter::Operator> for UnaryOperator {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::unary_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::unary_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(GenericDatabaseError::invalid_argument(
                "invalid structured_query::unary_filter::Operator",
            )),
            Other::IsNan => Ok(Self::IsNan),
            Other::IsNull => Ok(Self::IsNull),
            Other::IsNotNan => Ok(Self::IsNotNan),
            Other::IsNotNull => Ok(Self::IsNotNull),
        }
    }
}
