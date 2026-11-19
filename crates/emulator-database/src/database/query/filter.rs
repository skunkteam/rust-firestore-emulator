use std::{fmt, iter};

use googleapis::google::firestore::v1::{Value, structured_query};
use itertools::Itertools;

use crate::{
    GenericDatabaseError,
    database::{
        document::StoredDocumentVersion,
        field_path::{FieldPath, FieldReference},
    },
};

#[derive(Clone, Debug)]
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

    pub(crate) fn sorted_non_name_inequality_fields(&self) -> Vec<FieldReference> {
        let mut paths: Vec<FieldPath> = self
            .get_inequality_fields()
            .filter_map(|f| match f {
                FieldReference::DocumentName => None,
                FieldReference::FieldPath(p) => Some(p.clone()),
            })
            .collect();
        paths.sort();
        paths.dedup();
        paths.into_iter().map(FieldReference::FieldPath).collect()
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

    pub fn eval(&self, version: &StoredDocumentVersion, is_enterprise: bool) -> bool {
        match self {
            Filter::Composite(f) => f.eval(version, is_enterprise),
            Filter::Field(f) => f.eval(version, is_enterprise),
            Filter::Unary(f) => f.eval(version, is_enterprise),
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
        let filter_type = value.filter_type.ok_or_else(|| {
            GenericDatabaseError::invalid_argument("missing filter_type in Filter")
        })?;
        match filter_type {
            CompositeFilter(f) => Ok(Self::Composite(f.try_into()?)),
            FieldFilter(f) => Ok(Self::Field(f.try_into()?)),
            UnaryFilter(f) => Ok(Self::Unary(f.try_into()?)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CompositeFilter {
    pub op:      CompositeOperator,
    pub filters: Vec<Filter>,
}

impl CompositeFilter {
    fn eval(&self, version: &StoredDocumentVersion, is_enterprise: bool) -> bool {
        for filter in &self.filters {
            let filter_result = filter.eval(version, is_enterprise);
            match (&self.op, filter_result) {
                (CompositeOperator::And, true) | (CompositeOperator::Or, false) => continue,
                (_, result) => return result,
            }
        }
        self.op.is_and()
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[derive(Clone)]
pub struct FieldFilter {
    pub field: FieldReference,
    pub op:    FieldOperator,
    pub value: Value,
}

impl fmt::Debug for FieldFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut ds = f.debug_struct("FieldFilter");
        ds.field("field", &self.field).field("op", &self.op);
        if let Some(values) = self.value.as_array() {
            let prefix = &values[..values.len().min(3)];
            ds.field("array_len", &values.len())
                .field("array_prefix", &prefix);
        } else {
            ds.field("value", &self.value);
        }
        ds.finish()
    }
}

fn value_matches_eq(lhs: &Value, rhs: &Value, is_enterprise: bool) -> bool {
    (is_enterprise || (!lhs.is_null() && !lhs.is_nan())) && lhs == rhs
}

impl FieldFilter {
    fn operand_contains(&self, cand: &Value, is_enterprise: bool) -> bool {
        if !is_enterprise && (cand.is_null() || cand.is_nan()) {
            return false;
        }
        self.value
            .as_array()
            .is_some_and(|arr| arr.iter().any(|rhs| cand == rhs))
    }

    fn eval(&self, version: &StoredDocumentVersion, is_enterprise: bool) -> bool {
        use FieldOperator::*;
        let Some(value) = self.field.get_value(version) else {
            // If the field is missing, it only evaluates to true for `!=` and `NOT_IN` in
            // Enterprise Edition.
            return is_enterprise && matches!(self.op, NotEqual | NotIn);
        };
        let value = value.as_ref();
        if self.op.needs_type_compat()
            && (!value.is_compatible_with(&self.value)
                || (!is_enterprise && (value.is_null() || value.is_nan())))
        {
            return false;
        }
        match self.op {
            LessThan => value < &self.value,
            LessThanOrEqual => value <= &self.value,
            GreaterThan => value > &self.value,
            GreaterThanOrEqual => value >= &self.value,
            Equal => value_matches_eq(value, &self.value, is_enterprise),
            NotEqual => {
                (is_enterprise || (!value.is_null() && !self.value.is_null()))
                    && !value_matches_eq(value, &self.value, is_enterprise)
            }
            ArrayContains => value.as_array().is_some_and(|arr| {
                arr.iter()
                    .any(|el| value_matches_eq(el, &self.value, is_enterprise))
            }),
            In => self.operand_contains(value, is_enterprise),
            ArrayContainsAny => value.as_array().is_some_and(|arr| {
                arr.iter()
                    .any(|el| self.operand_contains(el, is_enterprise))
            }),
            NotIn => {
                (is_enterprise
                    || (!value.is_null()
                        && !self
                            .value
                            .as_array()
                            .is_some_and(|arr| arr.iter().any(Value::is_null))))
                    && !self.operand_contains(value, is_enterprise)
            }
        }
    }
}

impl TryFrom<structured_query::FieldFilter> for FieldFilter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::FieldFilter) -> Result<Self, Self::Error> {
        let field = value
            .field
            .as_ref()
            .ok_or_else(|| GenericDatabaseError::invalid_argument("missing field in FieldFilter"))?
            .field_path
            .parse()?;
        let op = value.op().try_into()?;
        let val = value.value.ok_or_else(|| {
            GenericDatabaseError::invalid_argument("missing value in FieldFilter")
        })?;
        Ok(Self {
            field,
            op,
            value: val,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub(super) const fn wire_name(self) -> &'static str {
        match self {
            Self::LessThan => "LESS_THAN",
            Self::LessThanOrEqual => "LESS_THAN_OR_EQUAL",
            Self::GreaterThan => "GREATER_THAN",
            Self::GreaterThanOrEqual => "GREATER_THAN_OR_EQUAL",
            Self::Equal => "EQUAL",
            Self::NotEqual => "NOT_EQUAL",
            Self::ArrayContains => "ARRAY_CONTAINS",
            Self::In => "IN",
            Self::ArrayContainsAny => "ARRAY_CONTAINS_ANY",
            Self::NotIn => "NOT_IN",
        }
    }

    pub(super) const fn is_array_operand(self) -> bool {
        matches!(self, Self::In | Self::ArrayContainsAny | Self::NotIn)
    }

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

    pub(crate) fn needs_type_compat(&self) -> bool {
        matches!(
            self,
            Self::LessThan | Self::LessThanOrEqual | Self::GreaterThan | Self::GreaterThanOrEqual
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

#[derive(Clone, Debug)]
pub struct UnaryFilter {
    pub field: FieldReference,
    pub op:    UnaryOperator,
}

impl UnaryFilter {
    fn eval(&self, version: &StoredDocumentVersion, is_enterprise: bool) -> bool {
        use UnaryOperator::*;
        let Some(value) = self.field.get_value(version) else {
            // If the field is missing, it only evaluates to true for `!=` in Enterprise Edition
            return is_enterprise && matches!(self.op, IsNotNan | IsNotNull);
        };
        let value = value.as_ref();
        match self.op {
            IsNan => value.is_nan(),
            IsNull => value.is_null(),
            IsNotNan => (is_enterprise || !value.is_null()) && !value.is_nan(),
            IsNotNull => !value.is_null(),
        }
    }
}

impl TryFrom<structured_query::UnaryFilter> for UnaryFilter {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::UnaryFilter) -> Result<Self, Self::Error> {
        let structured_query::unary_filter::OperandType::Field(field) =
            value.operand_type.as_ref().ok_or_else(|| {
                GenericDatabaseError::invalid_argument("missing operand_type in UnaryFilter")
            })?;
        Ok(Self {
            field: field.field_path.parse()?,
            op:    value.op().try_into()?,
        })
    }
}

#[allow(clippy::enum_variant_names)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::{firestore::v1::Value, protobuf::Timestamp};
    use googletest::prelude::*;

    use super::{FieldFilter, FieldOperator};
    use crate::database::document::StoredDocumentVersion;

    #[gtest]
    fn field_filter_cross_type_numeric_equality_and_membership() {
        let version = StoredDocumentVersion {
            name: "projects/p/databases/d/documents/col/doc".parse().unwrap(),
            create_time: Timestamp::default(),
            update_time: Timestamp::default(),
            fields: HashMap::from([
                ("count".to_string(), Value::integer(42)),
                ("big".to_string(), Value::integer(i64::MAX)),
                (
                    "tags".to_string(),
                    Value::array(vec![Value::integer(10), Value::double(f64::NAN)]),
                ),
            ]),
        };

        let eq_filter = FieldFilter {
            field: "count".parse().unwrap(),
            op:    FieldOperator::Equal,
            value: Value::double(42.0),
        };
        expect_that!(eq_filter.eval(&version, false), eq(true));

        let ne_filter = FieldFilter {
            field: "count".parse().unwrap(),
            op:    FieldOperator::NotEqual,
            value: Value::double(42.0),
        };
        expect_that!(ne_filter.eval(&version, false), eq(false));

        let big_filter = FieldFilter {
            field: "big".parse().unwrap(),
            op:    FieldOperator::Equal,
            value: Value::double(i64::MAX as f64),
        };
        expect_that!(big_filter.eval(&version, false), eq(false));

        let array_contains_filter = FieldFilter {
            field: "tags".parse().unwrap(),
            op:    FieldOperator::ArrayContains,
            value: Value::double(10.0),
        };
        expect_that!(array_contains_filter.eval(&version, false), eq(true));

        let in_filter = FieldFilter {
            field: "count".parse().unwrap(),
            op:    FieldOperator::In,
            value: Value::array(vec![Value::double(42.0)]),
        };
        expect_that!(in_filter.eval(&version, false), eq(true));

        let nan_version = StoredDocumentVersion {
            name: "projects/p/databases/d/documents/col/nan".parse().unwrap(),
            create_time: Timestamp::default(),
            update_time: Timestamp::default(),
            fields: HashMap::from([("val".to_string(), Value::double(f64::NAN))]),
        };
        let nan_in_filter = FieldFilter {
            field: "val".parse().unwrap(),
            op:    FieldOperator::In,
            value: Value::array(vec![Value::double(f64::NAN)]),
        };
        expect_that!(nan_in_filter.eval(&nan_version, false), eq(false));
        expect_that!(nan_in_filter.eval(&nan_version, true), eq(true));
    }

    #[gtest]
    fn try_from_missing_required_proto_fields_returns_invalid_argument() {
        use googleapis::google::firestore::v1::structured_query;

        use super::{Filter, GenericDatabaseError};

        assert_that!(
            Filter::try_from(structured_query::Filter { filter_type: None }),
            err(eq(&GenericDatabaseError::invalid_argument(
                "missing filter_type in Filter"
            )))
        );

        assert_that!(
            FieldFilter::try_from(structured_query::FieldFilter {
                field: None,
                op:    structured_query::field_filter::Operator::Equal as i32,
                value: Some(Value::integer(1)),
            }),
            err(eq(&GenericDatabaseError::invalid_argument(
                "missing field in FieldFilter"
            )))
        );

        assert_that!(
            FieldFilter::try_from(structured_query::FieldFilter {
                field: Some(structured_query::FieldReference {
                    field_path: "a".to_string(),
                }),
                op:    structured_query::field_filter::Operator::Equal as i32,
                value: None,
            }),
            err(eq(&GenericDatabaseError::invalid_argument(
                "missing value in FieldFilter"
            )))
        );

        assert_that!(
            super::UnaryFilter::try_from(structured_query::UnaryFilter {
                op: structured_query::unary_filter::Operator::IsNull as i32,
                operand_type: None,
            }),
            err(eq(&GenericDatabaseError::invalid_argument(
                "missing operand_type in UnaryFilter"
            )))
        );
    }

    #[gtest]
    fn sorted_non_name_inequality_fields_orders_escaped_dot_segments_by_parsed_segments() {
        use super::{CompositeFilter, CompositeOperator, Filter};

        let f_escaped = Filter::Field(FieldFilter {
            field: "`a.b`".parse().unwrap(),
            op:    FieldOperator::GreaterThan,
            value: Value::integer(0),
        });
        let f_nested_c = Filter::Field(FieldFilter {
            field: "a.c".parse().unwrap(),
            op:    FieldOperator::GreaterThan,
            value: Value::integer(0),
        });
        let f_nested_b = Filter::Field(FieldFilter {
            field: "a.b".parse().unwrap(),
            op:    FieldOperator::GreaterThan,
            value: Value::integer(0),
        });

        let filter = Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![f_escaped.clone(), f_nested_c],
        });
        assert_eq!(
            filter.sorted_non_name_inequality_fields(),
            vec!["a.c".parse().unwrap(), "`a.b`".parse().unwrap()]
        );

        let order_1 = Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![f_escaped.clone(), f_nested_b.clone()],
        });
        let order_2 = Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![f_nested_b, f_escaped],
        });
        let expected = vec!["a.b".parse().unwrap(), "`a.b`".parse().unwrap()];
        assert_eq!(order_1.sorted_non_name_inequality_fields(), expected);
        assert_eq!(order_2.sorted_non_name_inequality_fields(), expected);
    }

    #[gtest]
    fn field_filter_edition_null_nan_missing_and_array_operators() {
        let make_doc = |fields: HashMap<String, Value>| StoredDocumentVersion {
            name: "projects/p/databases/d/documents/col/doc".parse().unwrap(),
            create_time: Timestamp::default(),
            update_time: Timestamp::default(),
            fields,
        };
        let eval_doc =
            |fields: HashMap<String, Value>, op: FieldOperator, rhs: Value, ent: bool| {
                let doc = make_doc(fields);
                FieldFilter {
                    field: "v".parse().unwrap(),
                    op,
                    value: rhs,
                }
                .eval(&doc, ent)
            };

        // Missing field across all 10 operators: only NotEqual and NotIn match on Enterprise.
        let all_ops = [
            (FieldOperator::LessThan, Value::integer(1), false),
            (FieldOperator::LessThanOrEqual, Value::integer(1), false),
            (FieldOperator::GreaterThan, Value::integer(1), false),
            (FieldOperator::GreaterThanOrEqual, Value::integer(1), false),
            (FieldOperator::Equal, Value::integer(1), false),
            (FieldOperator::NotEqual, Value::integer(1), true),
            (FieldOperator::ArrayContains, Value::integer(1), false),
            (
                FieldOperator::In,
                Value::array(vec![Value::integer(1)]),
                false,
            ),
            (
                FieldOperator::ArrayContainsAny,
                Value::array(vec![Value::integer(1)]),
                false,
            ),
            (
                FieldOperator::NotIn,
                Value::array(vec![Value::null()]),
                true,
            ),
        ];
        for (op, rhs, ent_expected) in all_ops {
            assert!(!eval_doc(HashMap::new(), op, rhs.clone(), false));
            assert_eq!(eval_doc(HashMap::new(), op, rhs, true), ent_expected);
        }

        // Range comparisons (`<`, `<=`, `>`, `>=`) and Standard null/NaN exclusion.
        let one = HashMap::from([("v".to_string(), Value::integer(1))]);
        assert!(eval_doc(
            one.clone(),
            FieldOperator::LessThan,
            Value::integer(2),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::LessThan,
            Value::integer(1),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::LessThanOrEqual,
            Value::integer(1),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::GreaterThan,
            Value::integer(0),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::GreaterThan,
            Value::integer(1),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::GreaterThanOrEqual,
            Value::integer(1),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::LessThan,
            Value::string("a"),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::LessThanOrEqual,
            Value::null(),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::LessThanOrEqual,
            Value::null(),
            true
        ));

        let null_doc = HashMap::from([("v".to_string(), Value::null())]);
        let nan_doc = HashMap::from([("v".to_string(), Value::double(f64::NAN))]);
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::LessThanOrEqual,
            Value::null(),
            false
        ));
        assert!(eval_doc(
            null_doc.clone(),
            FieldOperator::LessThanOrEqual,
            Value::null(),
            true
        ));
        assert!(!eval_doc(
            nan_doc.clone(),
            FieldOperator::LessThanOrEqual,
            Value::double(f64::NAN),
            false
        ));
        assert!(eval_doc(
            nan_doc.clone(),
            FieldOperator::LessThanOrEqual,
            Value::double(f64::NAN),
            true
        ));

        // Equal & NotEqual with null, NaN, and cross-type 0 / -0.0.
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::Equal,
            Value::null(),
            false
        ));
        assert!(eval_doc(
            null_doc.clone(),
            FieldOperator::Equal,
            Value::null(),
            true
        ));
        assert!(!eval_doc(
            nan_doc.clone(),
            FieldOperator::Equal,
            Value::double(f64::NAN),
            false
        ));
        assert!(eval_doc(
            nan_doc.clone(),
            FieldOperator::Equal,
            Value::double(f64::NAN),
            true
        ));

        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::NotEqual,
            Value::integer(1),
            false
        ));
        assert!(eval_doc(
            null_doc.clone(),
            FieldOperator::NotEqual,
            Value::integer(1),
            true
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::NotEqual,
            Value::null(),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::NotEqual,
            Value::null(),
            true
        ));
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::NotEqual,
            Value::null(),
            false
        ));
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::NotEqual,
            Value::null(),
            true
        ));
        assert!(eval_doc(
            nan_doc.clone(),
            FieldOperator::NotEqual,
            Value::double(f64::NAN),
            false
        ));
        assert!(!eval_doc(
            nan_doc.clone(),
            FieldOperator::NotEqual,
            Value::double(f64::NAN),
            true
        ));

        let neg_zero_doc = HashMap::from([("v".to_string(), Value::double(-0.0))]);
        assert!(eval_doc(
            neg_zero_doc.clone(),
            FieldOperator::Equal,
            Value::integer(0),
            false
        ));
        assert!(!eval_doc(
            neg_zero_doc.clone(),
            FieldOperator::NotEqual,
            Value::integer(0),
            false
        ));

        // NotIn across Standard vs Enterprise with null, NaN, and cross-type numerics.
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(1)]),
            false
        ));
        assert!(eval_doc(
            null_doc.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(1)]),
            true
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::null()]),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::null()]),
            true
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(2), Value::null()]),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(2), Value::null()]),
            true
        ));
        assert!(!eval_doc(
            null_doc.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::null()]),
            false
        ));
        assert!(!eval_doc(
            null_doc,
            FieldOperator::NotIn,
            Value::array(vec![Value::null()]),
            true
        ));
        assert!(eval_doc(
            nan_doc.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::double(f64::NAN)]),
            false
        ));
        assert!(!eval_doc(
            nan_doc,
            FieldOperator::NotIn,
            Value::array(vec![Value::double(f64::NAN)]),
            true
        ));
        assert!(!eval_doc(
            neg_zero_doc,
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(0)]),
            false
        ));
        assert!(eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::integer(2)]),
            false
        ));
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::NotIn,
            Value::array(vec![Value::double(1.0)]),
            false
        ));

        // ArrayContains & ArrayContainsAny with null, NaN, nested NaN map, and cross-type numerics.
        let nested_nan = Value::map(HashMap::from([(
            "nested".to_string(),
            Value::double(f64::NAN),
        )]));
        let arr_doc = HashMap::from([(
            "v".to_string(),
            Value::array(vec![
                Value::null(),
                Value::double(f64::NAN),
                Value::double(-0.0),
                Value::integer(42),
                nested_nan.clone(),
            ]),
        )]);
        assert!(!eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            Value::null(),
            false
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            Value::null(),
            true
        ));
        assert!(!eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            Value::double(f64::NAN),
            false
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            Value::double(f64::NAN),
            true
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            nested_nan.clone(),
            false
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContains,
            Value::double(42.0),
            false
        ));

        assert!(!eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::null(), Value::double(f64::NAN)]),
            false
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::null()]),
            true
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::double(f64::NAN)]),
            true
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::integer(0), Value::string("miss")]),
            false
        ));
        assert!(eval_doc(
            arr_doc.clone(),
            FieldOperator::ArrayContainsAny,
            Value::array(vec![nested_nan]),
            false
        ));
        assert!(!eval_doc(
            arr_doc,
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::integer(99)]),
            false
        ));
        // Non-array field value evaluates to false for ArrayContains / ArrayContainsAny.
        assert!(!eval_doc(
            one.clone(),
            FieldOperator::ArrayContains,
            Value::integer(1),
            true
        ));
        assert!(!eval_doc(
            one,
            FieldOperator::ArrayContainsAny,
            Value::array(vec![Value::integer(1)]),
            true
        ));
    }

    #[gtest]
    fn sorted_non_name_inequality_fields_wire_names_and_bounded_debug() {
        use super::{CompositeFilter, CompositeOperator, Filter, UnaryFilter, UnaryOperator};

        let filter = Filter::Composite(CompositeFilter {
            op:      CompositeOperator::And,
            filters: vec![
                Filter::Field(FieldFilter {
                    field: "__name__".parse().unwrap(),
                    op:    FieldOperator::GreaterThan,
                    value: Value::reference("projects/p/databases/d/documents/c/1".to_string()),
                }),
                Filter::Field(FieldFilter {
                    field: "`a.b`".parse().unwrap(),
                    op:    FieldOperator::GreaterThan,
                    value: Value::integer(1),
                }),
                Filter::Field(FieldFilter {
                    field: "a.b".parse().unwrap(),
                    op:    FieldOperator::LessThan,
                    value: Value::integer(10),
                }),
                Filter::Field(FieldFilter {
                    field: "a.b".parse().unwrap(),
                    op:    FieldOperator::GreaterThan,
                    value: Value::integer(0),
                }),
                Filter::Field(FieldFilter {
                    field: "c".parse().unwrap(),
                    op:    FieldOperator::NotIn,
                    value: Value::array(vec![Value::integer(1)]),
                }),
                Filter::Unary(UnaryFilter {
                    field: "d".parse().unwrap(),
                    op:    UnaryOperator::IsNotNull,
                }),
                Filter::Unary(UnaryFilter {
                    field: "e".parse().unwrap(),
                    op:    UnaryOperator::IsNotNan,
                }),
                Filter::Field(FieldFilter {
                    field: "eq_ignored".parse().unwrap(),
                    op:    FieldOperator::Equal,
                    value: Value::integer(5),
                }),
                Filter::Unary(UnaryFilter {
                    field: "null_ignored".parse().unwrap(),
                    op:    UnaryOperator::IsNull,
                }),
            ],
        });

        let sorted = filter.sorted_non_name_inequality_fields();
        assert_eq!(
            sorted,
            vec![
                "a.b".parse().unwrap(),
                "`a.b`".parse().unwrap(),
                "c".parse().unwrap(),
                "d".parse().unwrap(),
                "e".parse().unwrap(),
            ]
        );

        assert_eq!(FieldOperator::LessThan.wire_name(), "LESS_THAN");
        assert_eq!(
            FieldOperator::LessThanOrEqual.wire_name(),
            "LESS_THAN_OR_EQUAL"
        );
        assert_eq!(FieldOperator::GreaterThan.wire_name(), "GREATER_THAN");
        assert_eq!(
            FieldOperator::GreaterThanOrEqual.wire_name(),
            "GREATER_THAN_OR_EQUAL"
        );
        assert_eq!(FieldOperator::Equal.wire_name(), "EQUAL");
        assert_eq!(FieldOperator::NotEqual.wire_name(), "NOT_EQUAL");
        assert_eq!(FieldOperator::ArrayContains.wire_name(), "ARRAY_CONTAINS");
        assert_eq!(FieldOperator::In.wire_name(), "IN");
        assert_eq!(
            FieldOperator::ArrayContainsAny.wire_name(),
            "ARRAY_CONTAINS_ANY"
        );
        assert_eq!(FieldOperator::NotIn.wire_name(), "NOT_IN");

        assert!(FieldOperator::In.is_array_operand());
        assert!(FieldOperator::ArrayContainsAny.is_array_operand());
        assert!(FieldOperator::NotIn.is_array_operand());
        assert!(!FieldOperator::ArrayContains.is_array_operand());
        assert!(!FieldOperator::Equal.is_array_operand());

        let large_arr = FieldFilter {
            field: "tags".parse().unwrap(),
            op:    FieldOperator::In,
            value: Value::array((1001..=1005).map(Value::integer).collect()),
        };
        let large_dbg = format!("{large_arr:?}");
        assert!(large_dbg.contains("array_len: 5"));
        assert!(large_dbg.contains("array_prefix:"));
        assert!(large_dbg.contains("1001"));
        assert!(large_dbg.contains("1003"));
        assert!(!large_dbg.contains("1004"));
        assert!(!large_dbg.contains("1005"));

        let small_arr = FieldFilter {
            field: "tags".parse().unwrap(),
            op:    FieldOperator::NotIn,
            value: Value::array(vec![Value::integer(1), Value::integer(2)]),
        };
        let small_dbg = format!("{small_arr:?}");
        assert!(small_dbg.contains("array_len: 2"));
        assert!(small_dbg.contains('2'));

        let scalar = FieldFilter {
            field: "count".parse().unwrap(),
            op:    FieldOperator::Equal,
            value: Value::integer(42),
        };
        let scalar_dbg = format!("{scalar:?}");
        assert!(scalar_dbg.contains("value:"));
        assert!(scalar_dbg.contains("42"));
        assert!(!scalar_dbg.contains("array_len"));
    }
}
