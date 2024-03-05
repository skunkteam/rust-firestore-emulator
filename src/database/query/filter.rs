use crate::{
    database::{document::StoredDocumentVersion, field_path::FieldReference, value::Value},
    googleapis::google::firestore::v1::structured_query,
    unimplemented,
};
use itertools::Itertools;
use std::ops::Deref;
use tonic::{Result, Status};

#[derive(Debug)]
pub enum Filter {
    Composite(CompositeFilter),
    Field(FieldFilter),
    Unary(UnaryFilter),
}

impl Filter {
    pub fn get_inequality_fields(&self) -> Vec<&FieldReference> {
        match self {
            Filter::Composite(f) => f.get_inequality_fields(),
            Filter::Field(f) => f.get_inequality_fields(),
            Filter::Unary(f) => f.get_inequality_fields(),
        }
    }

    pub fn eval(&self, version: &StoredDocumentVersion) -> Result<bool> {
        match self {
            Filter::Composite(f) => f.eval(version),
            Filter::Field(f) => f.eval(version),
            Filter::Unary(f) => Ok(f.eval(version)),
        }
    }
}

impl TryFrom<structured_query::Filter> for Filter {
    type Error = Status;

    fn try_from(value: structured_query::Filter) -> Result<Self, Self::Error> {
        use structured_query::filter::FilterType;
        match value.filter_type.expect("filter without filter_type") {
            FilterType::CompositeFilter(f) => Ok(Self::Composite(f.try_into()?)),
            FilterType::FieldFilter(f) => Ok(Self::Field(f.try_into()?)),
            FilterType::UnaryFilter(f) => Ok(Self::Unary(f.try_into()?)),
        }
    }
}

#[derive(Debug)]
pub struct CompositeFilter {
    op:      CompositeOperator,
    filters: Vec<Filter>,
}

impl CompositeFilter {
    pub fn get_inequality_fields(&self) -> Vec<&FieldReference> {
        self.filters
            .iter()
            .flat_map(Filter::get_inequality_fields)
            .collect()
    }

    fn eval(&self, version: &StoredDocumentVersion) -> Result<bool, Status> {
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
    type Error = Status;

    fn try_from(value: structured_query::CompositeFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            op:      value.op().try_into()?,
            filters: value
                .filters
                .into_iter()
                .map(TryInto::try_into)
                .try_collect()?,
        })
    }
}

#[derive(Debug)]
enum CompositeOperator {
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
    type Error = Status;

    fn try_from(value: structured_query::composite_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::composite_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(Status::invalid_argument(
                "invalid structured_query::composite_filter::Operator",
            )),
            Other::And => Ok(Self::And),
            Other::Or => Ok(Self::Or),
        }
    }
}

#[derive(Debug)]
pub struct FieldFilter {
    field: FieldReference,
    op:    FieldOperator,
    value: Value,
}

impl FieldFilter {
    pub fn get_inequality_fields(&self) -> Vec<&FieldReference> {
        if self.op.is_inequality() {
            vec![&self.field]
        } else {
            vec![]
        }
    }

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
    type Error = Status;

    fn try_from(value: structured_query::FieldFilter) -> Result<Self, Self::Error> {
        Ok(Self {
            field: value
                .field
                .as_ref()
                .expect("missing field in FieldFilter")
                .field_path
                .deref()
                .try_into()?,
            op:    value.op().try_into()?,
            value: value.value.expect("missing value in FieldFilter"),
        })
    }
}

#[derive(Debug)]
enum FieldOperator {
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
    pub fn is_inequality(&self) -> bool {
        match self {
            FieldOperator::LessThan
            | FieldOperator::LessThanOrEqual
            | FieldOperator::GreaterThan
            | FieldOperator::GreaterThanOrEqual
            | FieldOperator::NotEqual
            | FieldOperator::NotIn => true,
            FieldOperator::Equal
            | FieldOperator::ArrayContains
            | FieldOperator::In
            | FieldOperator::ArrayContainsAny => false,
        }
    }

    pub fn needs_type_compat(&self) -> bool {
        matches!(
            self,
            FieldOperator::LessThan
                | FieldOperator::LessThanOrEqual
                | FieldOperator::GreaterThan
                | FieldOperator::GreaterThanOrEqual
                | FieldOperator::Equal
        )
    }
}

impl TryFrom<structured_query::field_filter::Operator> for FieldOperator {
    type Error = Status;

    fn try_from(value: structured_query::field_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::field_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(Status::invalid_argument(
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
    field: FieldReference,
    op:    UnaryOperator,
}

impl UnaryFilter {
    pub fn get_inequality_fields(&self) -> Vec<&FieldReference> {
        if self.op.is_inequality() {
            vec![&self.field]
        } else {
            vec![]
        }
    }

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
    type Error = Status;

    fn try_from(value: structured_query::UnaryFilter) -> Result<Self, Self::Error> {
        let structured_query::unary_filter::OperandType::Field(field) = value
            .operand_type
            .as_ref()
            .expect("missing operand_type in UnaryFilter");
        Ok(Self {
            field: field.field_path.deref().try_into()?,
            op:    value.op().try_into()?,
        })
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Debug)]
enum UnaryOperator {
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
    pub fn is_inequality(&self) -> bool {
        match self {
            UnaryOperator::IsNan | UnaryOperator::IsNull => false,
            UnaryOperator::IsNotNan | UnaryOperator::IsNotNull => true,
        }
    }
}

impl TryFrom<structured_query::unary_filter::Operator> for UnaryOperator {
    type Error = Status;

    fn try_from(value: structured_query::unary_filter::Operator) -> Result<Self, Self::Error> {
        use structured_query::unary_filter::Operator as Other;
        match value {
            Other::Unspecified => Err(Status::invalid_argument(
                "invalid structured_query::unary_filter::Operator",
            )),
            Other::IsNan => Ok(Self::IsNan),
            Other::IsNull => Ok(Self::IsNull),
            Other::IsNotNan => Ok(Self::IsNotNan),
            Other::IsNotNull => Ok(Self::IsNotNull),
        }
    }
}
