use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

use googleapis::google::{
    firestore::v1::{
        structured_aggregation_query::{Aggregation, aggregation},
        *,
    },
    protobuf::Timestamp,
};
use itertools::Itertools;
use tracing::{Instrument, debug};

use super::{
    FirestoreDatabase,
    field_path::{FieldPath, FieldReference},
    query::Query,
    read_consistency::ReadConsistency,
    reference::{self, Ref},
};
use crate::{GenericDatabaseError, error::Result};

const MAX_STANDARD_AGGREGATIONS_PER_QUERY: usize = 5;
const MAX_ENTERPRISE_AGGREGATIONS_PER_QUERY: usize = 64;
const MAX_AGGREGATION_ALIAS_BYTES: usize = 1_500;

#[derive(Debug, PartialEq, Eq)]
enum CompiledAggregationOp {
    Count { up_to: Option<i64> },
    Sum(FieldReference),
    Avg(FieldReference),
}

#[derive(Debug, PartialEq, Eq)]
struct CompiledAggregation {
    alias: String,
    op:    CompiledAggregationOp,
}

/// Parses and validates a `SUM`/`AVG` field reference according to edition rules.
fn compile_aggregation_field(
    field: Option<structured_query::FieldReference>,
    op_name: &str,
    name_err: &'static str,
    enterprise: bool,
) -> Result<FieldReference> {
    let field = field.as_ref().ok_or_else(|| {
        GenericDatabaseError::invalid_argument(format!(
            "missing field reference in {op_name} operator"
        ))
    })?;
    let field_ref = FieldReference::try_from(field)?;
    if enterprise {
        match &field_ref {
            FieldReference::DocumentName => Ok(field_ref),
            FieldReference::FieldPath(fp) => {
                fp.validate_non_reserved_allowing_doc_name()?;
                Ok(field_ref)
            }
        }
    } else {
        let fp = field_ref.into_non_reserved_field_path(name_err)?;
        Ok(FieldReference::FieldPath(fp))
    }
}

/// Validates aggregation count, operators, `Count.up_to`, `SUM`/`AVG` fields, and aliases.
fn compile_aggregations(
    aggregations: Vec<Aggregation>,
    enterprise: bool,
) -> Result<Vec<CompiledAggregation>> {
    if aggregations.is_empty() {
        return Err(GenericDatabaseError::invalid_argument(
            "aggregations cannot be empty",
        ));
    }
    let max_aggregations = if enterprise {
        MAX_ENTERPRISE_AGGREGATIONS_PER_QUERY
    } else {
        MAX_STANDARD_AGGREGATIONS_PER_QUERY
    };
    if aggregations.len() > max_aggregations {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "a maximum of {max_aggregations} aggregations are allowed per query"
        )));
    }

    let mut next_auto_alias: usize = 1;
    let mut seen_aliases = HashSet::with_capacity(aggregations.len());
    let mut compiled = Vec::with_capacity(aggregations.len());

    for agg in aggregations {
        let operator = agg.operator.ok_or_else(|| {
            GenericDatabaseError::invalid_argument("missing operator in aggregation query")
        })?;
        let op = match operator {
            aggregation::Operator::Count(aggregation::Count { up_to }) => {
                let up_to = up_to.map(|v| v.value);
                if enterprise {
                    if up_to.is_some() {
                        return Err(GenericDatabaseError::invalid_argument(
                            "The `count_up_to(...)` accumulator is not supported in Enterprise \
                             edition.",
                        ));
                    }
                } else if let Some(limit) = up_to
                    && limit < 0
                {
                    return Err(GenericDatabaseError::invalid_argument(
                        "Count.up_to cannot be negative",
                    ));
                }
                CompiledAggregationOp::Count { up_to }
            }
            aggregation::Operator::Sum(aggregation::Sum { field }) => {
                CompiledAggregationOp::Sum(compile_aggregation_field(
                    field,
                    "SUM",
                    "SUM field cannot be __name__",
                    enterprise,
                )?)
            }
            aggregation::Operator::Avg(aggregation::Avg { field }) => {
                CompiledAggregationOp::Avg(compile_aggregation_field(
                    field,
                    "AVG",
                    "AVG field cannot be __name__",
                    enterprise,
                )?)
            }
        };

        let alias = if agg.alias.is_empty() {
            let generated = format!("field_{next_auto_alias}");
            next_auto_alias += 1;
            generated
        } else {
            if agg.alias.len() > MAX_AGGREGATION_ALIAS_BYTES {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "aggregation alias cannot exceed {MAX_AGGREGATION_ALIAS_BYTES} bytes"
                )));
            }
            if !enterprise && reference::is_reserved_name(&agg.alias) {
                return Err(GenericDatabaseError::invalid_argument(format!(
                    "the name {} is reserved",
                    agg.alias
                )));
            }
            agg.alias
        };

        if !seen_aliases.insert(alias.clone()) {
            return Err(GenericDatabaseError::invalid_argument(format!(
                "duplicate aggregation alias: {alias}"
            )));
        }

        compiled.push(CompiledAggregation { alias, op });
    }

    Ok(compiled)
}

pub(crate) fn sum_enterprise_checked(values: impl IntoIterator<Item = Value>) -> Result<Value> {
    Value::try_checked_sum(values)
        .ok_or_else(|| GenericDatabaseError::invalid_argument("Long overflow"))
}

fn sum_aggregation_values(
    enterprise: bool,
    values: impl IntoIterator<Item = Value>,
) -> Result<Value> {
    if enterprise {
        sum_enterprise_checked(values)
    } else {
        Ok(values.into_iter().sum())
    }
}

impl FirestoreDatabase {
    pub async fn run_aggregation_query(
        &self,
        parent: Ref,
        query: StructuredQuery,
        aggregations: Vec<Aggregation>,
        consistency: ReadConsistency,
    ) -> Result<(Timestamp, HashMap<String, Value>)> {
        let enterprise = self.enterprise_edition();
        let span = tracing::debug_span!(
            "run_aggregation_query",
            agg_count = aggregations.len(),
            enterprise,
            matched_docs = tracing::field::Empty,
        );
        let span_record = span.clone();
        async move {
            let res: Result<(Timestamp, HashMap<String, Value>)> = async {
                self.ensure_owns_root(parent.root())?;
                let is_unordered = query.order_by.is_empty() && query.find_nearest.is_none();
                let mut query = Query::from_structured(parent, query, consistency, enterprise)?;
                let compiled = compile_aggregations(aggregations, enterprise)?;
                if !enterprise {
                    let distance_result_field = query.distance_result_field();
                    let required_fields: Vec<FieldPath> = compiled
                        .iter()
                        .filter_map(|agg| match &agg.op {
                            CompiledAggregationOp::Sum(FieldReference::FieldPath(fp))
                            | CompiledAggregationOp::Avg(FieldReference::FieldPath(fp))
                                if !distance_result_field.is_some_and(|dp| dp.starts_with(fp)) =>
                            {
                                Some(fp.clone())
                            }
                            CompiledAggregationOp::Count { .. }
                            | CompiledAggregationOp::Sum(_)
                            | CompiledAggregationOp::Avg(_) => None,
                        })
                        .collect();
                    query.require_existing_fields(required_fields);
                }
                debug!(?query);
                let (read_time, mut docs) = query.once(self).await?;
                span_record.record("matched_docs", docs.len());
                let has_sum_or_avg = compiled.iter().any(|agg| {
                    matches!(
                        agg.op,
                        CompiledAggregationOp::Sum(_) | CompiledAggregationOp::Avg(_)
                    )
                });
                if enterprise && is_unordered && has_sum_or_avg {
                    // Enterprise checked integer sums depend on scan order; sort by canonical
                    // `__name__` for deterministic primary-key scan order.
                    docs.sort_by(|a, b| a.version.name.cmp(&b.version.name));
                }
                let result = compiled
                    .into_iter()
                    .map(|agg| {
                        let result = match agg.op {
                            // Count of documents that match the query.
                            //
                            // The `COUNT(*)` aggregation function operates on the entire document
                            // so it does not require a field reference.
                            CompiledAggregationOp::Count { up_to } => {
                                let max = up_to.unwrap_or(i64::MAX);
                                Value::integer((docs.len() as i64).min(max))
                            }
                            // Sum of the values of the requested field.
                            //
                            // * Only numeric values will be aggregated. All non-numeric values
                            //   including `NULL` are skipped.
                            //
                            // * If the aggregated values contain `NaN`, returns `NaN`. Infinity
                            //   math follows IEEE-754 standards.
                            //
                            // * Standard Edition returns `0` on empty numeric sets and accumulates
                            //   integers in `i128` (promoting to `DoubleValue` only if the final
                            //   sum overflows `i64`).
                            //
                            // * Enterprise Edition returns `NULL` on empty numeric sets and
                            //   accumulates sequentially in `i64` before any `DoubleValue`, failing
                            //   with `InvalidArgument` (`"Long overflow"`) on intermediate or final
                            //   `i64` overflow.
                            CompiledAggregationOp::Sum(field_ref) => {
                                let mut values = docs
                                    .iter()
                                    .filter_map(|m| {
                                        query
                                            .effective_numeric_field_value(m, &field_ref)
                                            .filter(|v| v.is_number())
                                            .map(Cow::into_owned)
                                    })
                                    .peekable();
                                if enterprise && values.peek().is_none() {
                                    Value::null()
                                } else {
                                    sum_aggregation_values(enterprise, values)?
                                }
                            }
                            // Average of the values of the requested field.
                            //
                            // * Only numeric values will be aggregated. All non-numeric values
                            //   including `NULL` are skipped.
                            //
                            // * If the aggregated values contain `NaN`, returns `NaN`. Infinity
                            //   math follows IEEE-754 standards.
                            //
                            // * If the aggregated value set is empty, returns `NULL`.
                            //
                            // * Always returns the result as a double (Enterprise Edition applies
                            //   the same sequential `Long overflow` check to its numerator sum).
                            CompiledAggregationOp::Avg(field_ref) => {
                                let values = docs
                                    .iter()
                                    .filter_map(|m| {
                                        query
                                            .effective_numeric_field_value(m, &field_ref)
                                            .filter(|v| v.is_number())
                                            .map(Cow::into_owned)
                                    })
                                    .collect_vec();
                                if values.is_empty() {
                                    Value::null()
                                } else {
                                    let len = values.len();
                                    let sum = sum_aggregation_values(enterprise, values)?;
                                    Value::double(sum.as_double().unwrap_or(0.0) / len as f64)
                                }
                            }
                        };
                        Ok::<_, GenericDatabaseError>((agg.alias, result))
                    })
                    .try_collect()?;
                Ok((read_time, result))
            }
            .await;
            if let Err(ref err) = res {
                debug!(error = %err);
            }
            res
        }
        .instrument(span)
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    };

    use futures_util::poll;
    use googleapis::google::firestore::v1::{
        TransactionOptions,
        structured_query::{self, CollectionSelector},
    };
    use tracing::instrument::WithSubscriber;

    use super::*;
    use crate::{
        FirestoreConfig, FirestoreProject,
        database::lock::{LockPriority, SessionKind},
        reference::{DocumentRef, RootRef},
    };

    fn agg_count(alias: &str, up_to: Option<i64>) -> Aggregation {
        Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Count(aggregation::Count {
                up_to: up_to.map(|value| googleapis::google::protobuf::Int64Value { value }),
            })),
        }
    }

    fn agg_sum(alias: &str, field_path: Option<&str>) -> Aggregation {
        Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Sum(aggregation::Sum {
                field: field_path.map(|fp| structured_query::FieldReference {
                    field_path: fp.to_string(),
                }),
            })),
        }
    }

    fn agg_avg(alias: &str, field_path: Option<&str>) -> Aggregation {
        Aggregation {
            alias:    alias.to_string(),
            operator: Some(aggregation::Operator::Avg(aggregation::Avg {
                field: field_path.map(|fp| structured_query::FieldReference {
                    field_path: fp.to_string(),
                }),
            })),
        }
    }

    #[test]
    fn compile_aggregations_assigns_sequential_auto_aliases_and_rejects_collisions() {
        for enterprise in [false, true] {
            let compiled = compile_aggregations(
                vec![
                    agg_count("", None),
                    agg_sum("", Some("a")),
                    agg_avg("", Some("b")),
                ],
                enterprise,
            )
            .unwrap();
            assert_eq!(
                compiled,
                vec![
                    CompiledAggregation {
                        alias: "field_1".to_string(),
                        op:    CompiledAggregationOp::Count { up_to: None },
                    },
                    CompiledAggregation {
                        alias: "field_2".to_string(),
                        op:    CompiledAggregationOp::Sum(FieldReference::FieldPath(
                            "a".parse().unwrap()
                        )),
                    },
                    CompiledAggregation {
                        alias: "field_3".to_string(),
                        op:    CompiledAggregationOp::Avg(FieldReference::FieldPath(
                            "b".parse().unwrap()
                        )),
                    },
                ]
            );

            let mixed = compile_aggregations(
                vec![
                    agg_count("count_up_to_1", None),
                    agg_count("", None),
                    agg_count("count_up_to_3", None),
                    agg_count("", None),
                ],
                enterprise,
            )
            .unwrap();
            let aliases: Vec<&str> = mixed.iter().map(|c| c.alias.as_str()).collect();
            assert_eq!(
                aliases,
                vec!["count_up_to_1", "field_1", "count_up_to_3", "field_2"]
            );

            for colliding in [
                vec![agg_count("field_1", None), agg_count("", None)],
                vec![agg_count("", None), agg_count("field_1", None)],
                vec![
                    agg_count("", None),
                    agg_count("field_2", None),
                    agg_count("", None),
                ],
                vec![agg_count("dup", None), agg_sum("dup", Some("v"))],
            ] {
                assert!(matches!(
                    compile_aggregations(colliding, enterprise),
                    Err(GenericDatabaseError::InvalidArgument(_))
                ));
            }
        }
    }

    #[test]
    fn compile_aggregations_validates_bounds_operators_aliases_and_fields() {
        // Common checks across both editions: empty, missing operator, alias > 1,500 UTF-8 bytes,
        // and non-__name__ reserved / empty / malformed SUM/AVG fields.
        for enterprise in [false, true] {
            assert!(matches!(
                compile_aggregations(vec![], enterprise),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
            assert!(matches!(
                compile_aggregations(
                    vec![Aggregation {
                        alias:    "x".to_string(),
                        operator: None,
                    }],
                    enterprise,
                ),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));

            let max_ascii = "a".repeat(1_500);
            let max_utf8 = "é".repeat(750);
            assert_eq!(
                compile_aggregations(vec![agg_count(&max_ascii, None)], enterprise).unwrap()[0]
                    .alias,
                max_ascii
            );
            assert_eq!(
                compile_aggregations(vec![agg_count(&max_utf8, None)], enterprise).unwrap()[0]
                    .alias,
                max_utf8
            );

            let over_ascii = "a".repeat(1_501);
            let over_utf8 = "é".repeat(751);
            for over in [over_ascii.as_str(), over_utf8.as_str()] {
                assert!(matches!(
                    compile_aggregations(vec![agg_count(over, None)], enterprise),
                    Err(GenericDatabaseError::InvalidArgument(_))
                ));
            }

            for ok_alias in ["____", "___", "__prefix", "suffix__"] {
                assert_eq!(
                    compile_aggregations(vec![agg_count(ok_alias, None)], enterprise).unwrap()[0]
                        .alias,
                    ok_alias
                );
            }

            for make_agg in [
                agg_sum as fn(&str, Option<&str>) -> Aggregation,
                agg_avg as fn(&str, Option<&str>) -> Aggregation,
            ] {
                assert!(matches!(
                    compile_aggregations(vec![make_agg("f", None)], enterprise),
                    Err(GenericDatabaseError::InvalidArgument(_))
                ));
                for bad_path in [
                    "",
                    "unclosed.`field",
                    "a..b",
                    "__custom__",
                    "a.__custom__.b",
                ] {
                    assert!(matches!(
                        compile_aggregations(vec![make_agg("f", Some(bad_path))], enterprise),
                        Err(GenericDatabaseError::InvalidArgument(_))
                    ));
                }
                assert!(
                    compile_aggregations(vec![make_agg("f", Some("____"))], enterprise).is_ok()
                );
                assert!(
                    compile_aggregations(vec![make_agg("f", Some("nested.`a.b`"))], enterprise)
                        .is_ok()
                );
            }
        }

        // Standard Edition (`enterprise = false`): max 5, Count.up_to >= 0, reserved aliases &
        // `__name__` fields rejected.
        assert_eq!(
            compile_aggregations(
                (0..5).map(|i| agg_count(&format!("a{i}"), None)).collect(),
                false,
            )
            .unwrap()
            .len(),
            5
        );
        assert!(matches!(
            compile_aggregations(
                (0..6).map(|i| agg_count(&format!("a{i}"), None)).collect(),
                false,
            ),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        for bad_up_to in [-1, i64::MIN] {
            assert!(matches!(
                compile_aggregations(vec![agg_count("c", Some(bad_up_to))], false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        assert_eq!(
            compile_aggregations(vec![agg_count("c", Some(0))], false).unwrap(),
            vec![CompiledAggregation {
                alias: "c".to_string(),
                op:    CompiledAggregationOp::Count { up_to: Some(0) },
            }]
        );
        for bad_alias in ["__a__", "__name__", "__id1__", "__reserved__"] {
            assert!(matches!(
                compile_aggregations(vec![agg_count(bad_alias, None)], false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }
        for (make_agg, name_err) in [
            (
                agg_sum as fn(&str, Option<&str>) -> Aggregation,
                "SUM field cannot be __name__",
            ),
            (
                agg_avg as fn(&str, Option<&str>) -> Aggregation,
                "AVG field cannot be __name__",
            ),
        ] {
            for doc_name_path in ["__name__", "`__name__`"] {
                assert_eq!(
                    compile_aggregations(vec![make_agg("f", Some(doc_name_path))], false),
                    Err(GenericDatabaseError::invalid_argument(name_err))
                );
            }
            assert!(matches!(
                compile_aggregations(vec![make_agg("f", Some("a.__name__"))], false),
                Err(GenericDatabaseError::InvalidArgument(_))
            ));
        }

        // Enterprise Edition (`enterprise = true`): max 64, Count.up_to rejected when Some(_),
        // reserved aliases & `__name__` / `a.__name__` fields accepted (duplicate aliases still
        // rejected).
        for count in [6, 10, 64] {
            assert_eq!(
                compile_aggregations(
                    (0..count)
                        .map(|i| agg_count(&format!("a{i}"), None))
                        .collect(),
                    true,
                )
                .unwrap()
                .len(),
                count
            );
        }
        assert!(matches!(
            compile_aggregations(
                (0..65).map(|i| agg_count(&format!("a{i}"), None)).collect(),
                true,
            ),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        for up_to in [-1, 0, 1, 2] {
            assert_eq!(
                compile_aggregations(vec![agg_count("c", Some(up_to))], true),
                Err(GenericDatabaseError::invalid_argument(
                    "The `count_up_to(...)` accumulator is not supported in Enterprise edition."
                ))
            );
        }
        for reserved_alias in ["__a__", "__name__", "__custom__", "__reserved__"] {
            assert_eq!(
                compile_aggregations(vec![agg_count(reserved_alias, None)], true).unwrap()[0].alias,
                reserved_alias
            );
        }
        assert!(matches!(
            compile_aggregations(
                vec![agg_count("__name__", None), agg_count("__name__", None)],
                true,
            ),
            Err(GenericDatabaseError::InvalidArgument(_))
        ));
        for make_agg in [
            agg_sum as fn(&str, Option<&str>) -> Aggregation,
            agg_avg as fn(&str, Option<&str>) -> Aggregation,
        ] {
            for doc_name_path in ["__name__", "`__name__`", "a.__name__"] {
                assert!(
                    compile_aggregations(vec![make_agg("f", Some(doc_name_path))], true).is_ok()
                );
            }
        }
    }

    #[tokio::test]
    async fn run_aggregation_query_enforces_pre_execution_validation_order_and_edition_rules() {
        #[derive(Clone, Debug, PartialEq, Eq)]
        enum SpanFieldValue {
            U64(u64),
            I64(i64),
            Bool(bool),
            Debug(String),
        }

        struct FieldCollector<'a>(&'a mut HashMap<&'static str, SpanFieldValue>);

        impl tracing::field::Visit for FieldCollector<'_> {
            fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                self.0.insert(field.name(), SpanFieldValue::U64(value));
            }

            fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
                self.0.insert(field.name(), SpanFieldValue::I64(value));
            }

            fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
                self.0.insert(field.name(), SpanFieldValue::Bool(value));
            }

            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name(), SpanFieldValue::Debug(format!("{value:?}")));
            }
        }

        #[derive(Clone, Default)]
        struct AggSpanSink {
            next_id: Arc<AtomicU64>,
            spans: Arc<Mutex<Vec<HashMap<&'static str, SpanFieldValue>>>>,
            span_indices: Arc<Mutex<HashMap<u64, usize>>>,
        }

        impl tracing::Subscriber for AggSpanSink {
            fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
                true
            }

            fn new_span(&self, attrs: &tracing::span::Attributes<'_>) -> tracing::span::Id {
                let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
                if attrs.metadata().name() == "run_aggregation_query" {
                    let mut fields = HashMap::new();
                    attrs.record(&mut FieldCollector(&mut fields));
                    let mut spans = self.spans.lock().unwrap();
                    let idx = spans.len();
                    spans.push(fields);
                    self.span_indices.lock().unwrap().insert(id, idx);
                }
                tracing::span::Id::from_u64(id)
            }

            fn record(&self, span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
                if let Some(&idx) = self.span_indices.lock().unwrap().get(&span.into_u64()) {
                    let mut spans = self.spans.lock().unwrap();
                    values.record(&mut FieldCollector(&mut spans[idx]));
                }
            }

            fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {
            }

            fn event(&self, _event: &tracing::Event<'_>) {}

            fn enter(&self, _span: &tracing::span::Id) {}

            fn exit(&self, _span: &tracing::span::Id) {}
        }

        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        let d1: DocumentRef = "projects/p/databases/(default)/documents/items/d1"
            .parse()
            .unwrap();
        let d2: DocumentRef = "projects/p/databases/(default)/documents/items/d2"
            .parse()
            .unwrap();
        db.update_doc(
            &d1,
            HashMap::from([("v".to_string(), Value::integer(10))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();
        db.update_doc(
            &d2,
            HashMap::from([("v".to_string(), Value::integer(20))]),
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let valid_sq = || StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   "items".to_string(),
                all_descendants: false,
            }],
            ..Default::default()
        };

        // (1) ensure_owns_root precedes (2) Query::from_structured.
        let foreign_parent: Ref = "projects/other/databases/(default)".parse().unwrap();
        let invalid_foreign_sq = StructuredQuery {
            from: vec![],
            offset: -1,
            ..Default::default()
        };
        let err1 = db
            .run_aggregation_query(
                foreign_parent,
                invalid_foreign_sq,
                vec![],
                ReadConsistency::Default,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err1, GenericDatabaseError::InvalidArgument(ref m) if m.contains("does not match database")),
            "expected ensure_owns_root error first, got {err1:?}"
        );

        // (2) Query::from_structured (including `select` Projection path checks) precedes
        // (3) compile_aggregations.
        let invalid_sq = StructuredQuery {
            from: vec![],
            offset: -1,
            ..Default::default()
        };
        let err2 = db
            .run_aggregation_query(
                Ref::Root(root.clone()),
                invalid_sq,
                vec![],
                ReadConsistency::Default,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err2, GenericDatabaseError::InvalidArgument(ref m) if m == "offset cannot be negative"),
            "expected Query::from_structured error before compile_aggregations, got {err2:?}"
        );
        for (bad_select_path, expected_msg) in [
            ("__custom__", "the name __custom__ is reserved"),
            ("a..b", "field path cannot contain empty segments"),
        ] {
            let mut sq_bad_select = valid_sq();
            sq_bad_select.select = Some(structured_query::Projection {
                fields: vec![structured_query::FieldReference {
                    field_path: bad_select_path.to_string(),
                }],
            });
            let err_sel = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    sq_bad_select,
                    vec![],
                    ReadConsistency::Default,
                )
                .await
                .unwrap_err();
            assert_eq!(
                err_sel,
                GenericDatabaseError::invalid_argument(expected_msg),
                "expected invalid select path error before empty-aggregations error for \
                 {bad_select_path}"
            );
        }

        // (3) compile_aggregations precedes (4) query.once(self).
        // Rolled-back transactions fail inside `query.once` with
        // `InvalidArgument("invalid transaction ID: ...")`; invalid aggregations must fail earlier
        // with their `compile_aggregations` error message even when a valid `select` is set.
        let stale_txn = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        db.rollback(stale_txn).await.unwrap();
        let mut sq_valid_select = valid_sq();
        sq_valid_select.select = Some(structured_query::Projection {
            fields: vec![structured_query::FieldReference {
                field_path: "other".to_string(),
            }],
        });
        for (bad_aggs, expected_substring) in [
            (
                vec![agg_count("c", Some(-1))],
                "Count.up_to cannot be negative",
            ),
            (
                vec![agg_sum("s", Some("__name__"))],
                "SUM field cannot be __name__",
            ),
        ] {
            let err3 = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    sq_valid_select.clone(),
                    bad_aggs,
                    ReadConsistency::Transaction(stale_txn),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err3,
                    GenericDatabaseError::InvalidArgument(ref m) if m.contains(expected_substring)
                ),
                "expected compile_aggregations InvalidArgument containing {expected_substring:?} \
                 before query.once, got {err3:?}"
            );
        }

        // Held write lock on `items/d1` must not block an invalid aggregation in a live
        // transaction.
        let blocker = db.locks.new_session(
            None,
            SessionKind::SingleWrite,
            LockPriority::NextSeq,
            false,
            false,
        );
        let held_write = blocker.acquire_write(&d1).await.unwrap();
        let active_txn = db.new_txn(TransactionOptions::READ_WRITE).await.unwrap();
        let mut agg_fut = Box::pin(db.run_aggregation_query(
            Ref::Root(root.clone()),
            valid_sq(),
            vec![agg_count("c", Some(-1))],
            ReadConsistency::Transaction(active_txn),
        ));
        let poll_res = poll!(&mut agg_fut);
        assert!(
            matches!(
                poll_res,
                std::task::Poll::Ready(Err(GenericDatabaseError::InvalidArgument(_)))
            ),
            "expected immediate InvalidArgument without lock wait, got {poll_res:?}"
        );
        drop(held_write);
        blocker.close();
        db.rollback(active_txn).await.unwrap();

        // Standard execution: valid `select` (empty and non-empty excluding `v`) is ignored,
        // Count(up_to: 0) -> 0, mixed omitted aliases -> field_1 / field_2, and structured span
        // records agg_count == 3, enterprise == false, matched_docs == 2.
        let std_sink = AggSpanSink::default();
        for select_fields in [
            vec![],
            vec![structured_query::FieldReference {
                field_path: "other".to_string(),
            }],
            vec![structured_query::FieldReference {
                field_path: "__name__".to_string(),
            }],
        ] {
            let mut sq_with_select = valid_sq();
            sq_with_select.select = Some(structured_query::Projection {
                fields: select_fields,
            });
            let (_rt, fields) = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    sq_with_select,
                    vec![
                        agg_count("zero_cap", Some(0)),
                        agg_count("", Some(1)),
                        agg_sum("", Some("v")),
                    ],
                    ReadConsistency::Default,
                )
                .with_subscriber(std_sink.clone())
                .await
                .unwrap();
            assert_eq!(fields.get("zero_cap"), Some(&Value::integer(0)));
            assert_eq!(fields.get("field_1"), Some(&Value::integer(1)));
            assert_eq!(fields.get("field_2"), Some(&Value::integer(30)));
        }
        {
            let spans = std_sink.spans.lock().unwrap();
            assert_eq!(spans.len(), 3);
            for span in spans.iter() {
                assert_eq!(span.get("agg_count"), Some(&SpanFieldValue::U64(3)));
                assert_eq!(span.get("enterprise"), Some(&SpanFieldValue::Bool(false)));
                assert_eq!(span.get("matched_docs"), Some(&SpanFieldValue::U64(2)));
            }
        }

        // Enterprise execution: 6 aggregations (with reserved aliases, valid select, and
        // SUM/AVG on `__name__` / `a.__name__` returning nullValue), while Count.up_to is rejected
        // pre-execution (even on stale txn), and span records agg_count == 6, enterprise == true,
        // matched_docs == 1.
        let ent_root: RootRef = "projects/p/databases/ent".parse().unwrap();
        let ent_db = project.database(&ent_root).await;
        ent_db.set_enterprise_edition(true);
        let ent_d1: DocumentRef = "projects/p/databases/ent/documents/items/d1"
            .parse()
            .unwrap();
        ent_db
            .update_doc(
                &ent_d1,
                HashMap::from([("v".to_string(), Value::integer(7))]),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let ent_stale = ent_db
            .new_txn(TransactionOptions::READ_WRITE)
            .await
            .unwrap();
        ent_db.rollback(ent_stale).await.unwrap();
        assert_eq!(
            ent_db
                .run_aggregation_query(
                    Ref::Root(ent_root.clone()),
                    valid_sq(),
                    vec![agg_count("c", Some(0))],
                    ReadConsistency::Transaction(ent_stale),
                )
                .await
                .unwrap_err(),
            GenericDatabaseError::invalid_argument(
                "The `count_up_to(...)` accumulator is not supported in Enterprise edition."
            )
        );

        let mut ent_sq = valid_sq();
        ent_sq.select = Some(structured_query::Projection {
            fields: vec![structured_query::FieldReference {
                field_path: "other".to_string(),
            }],
        });
        let ent_sink = AggSpanSink::default();
        let (_rt, ent_fields) = ent_db
            .run_aggregation_query(
                Ref::Root(ent_root),
                ent_sq,
                vec![
                    agg_count("__name__", None),
                    agg_sum("__custom__", Some("v")),
                    agg_sum("sum_doc_name", Some("__name__")),
                    agg_avg("avg_doc_name", Some("`__name__`")),
                    agg_sum("sum_nested_doc_name", Some("a.__name__")),
                    agg_avg("", Some("v")),
                ],
                ReadConsistency::Default,
            )
            .with_subscriber(ent_sink.clone())
            .await
            .unwrap();
        assert_eq!(ent_fields.get("__name__"), Some(&Value::integer(1)));
        assert_eq!(ent_fields.get("__custom__"), Some(&Value::integer(7)));
        assert_eq!(ent_fields.get("sum_doc_name"), Some(&Value::null()));
        assert_eq!(ent_fields.get("avg_doc_name"), Some(&Value::null()));
        assert_eq!(ent_fields.get("sum_nested_doc_name"), Some(&Value::null()));
        assert_eq!(ent_fields.get("field_1"), Some(&Value::double(7.0)));
        {
            let spans = ent_sink.spans.lock().unwrap();
            assert_eq!(spans.len(), 1);
            assert_eq!(spans[0].get("agg_count"), Some(&SpanFieldValue::U64(6)));
            assert_eq!(
                spans[0].get("enterprise"),
                Some(&SpanFieldValue::Bool(true))
            );
            assert_eq!(spans[0].get("matched_docs"), Some(&SpanFieldValue::U64(1)));
        }
    }

    #[tokio::test]
    async fn standard_sum_avg_implicitly_require_field_existence_while_enterprise_does_not() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));

        for enterprise in [false, true] {
            let root: RootRef = if enterprise {
                "projects/p/databases/ent"
            } else {
                "projects/p/databases/(default)"
            }
            .parse()
            .unwrap();
            let db = project.database(&root).await;
            db.set_enterprise_edition(enterprise);

            // 1. Mixed collection: {a: "text"}, {a: null}, {other: 42}
            let mixed_sq = || StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   "mixed".to_string(),
                    all_descendants: false,
                }],
                ..Default::default()
            };
            for (id, fields) in [
                (
                    "n1",
                    HashMap::from([("a".to_string(), Value::string("text"))]),
                ),
                ("n2", HashMap::from([("a".to_string(), Value::null())])),
                (
                    "n3",
                    HashMap::from([("other".to_string(), Value::integer(42))]),
                ),
            ] {
                let doc_ref: DocumentRef = format!("{root}/mixed/{id}").parse().unwrap();
                db.update_doc(&doc_ref, fields, None, None, None)
                    .await
                    .unwrap();
            }

            let (_, combined) = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    mixed_sq(),
                    vec![
                        agg_count("c", None),
                        agg_sum("s", Some("a")),
                        agg_avg("avg", Some("a")),
                    ],
                    ReadConsistency::Default,
                )
                .await
                .unwrap();
            if enterprise {
                assert_eq!(combined.get("c"), Some(&Value::integer(3)));
                assert_eq!(combined.get("s"), Some(&Value::null()));
                assert_eq!(combined.get("avg"), Some(&Value::null()));
            } else {
                assert_eq!(combined.get("c"), Some(&Value::integer(2)));
                assert_eq!(combined.get("s"), Some(&Value::integer(0)));
                assert_eq!(combined.get("avg"), Some(&Value::null()));
            }

            let (_, count_only) = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    mixed_sq(),
                    vec![agg_count("c", None)],
                    ReadConsistency::Default,
                )
                .await
                .unwrap();
            assert_eq!(count_only.get("c"), Some(&Value::integer(3)));

            // 2. Nested field path `nested.x`
            let nested_sq = || StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   "nested".to_string(),
                    all_descendants: false,
                }],
                ..Default::default()
            };
            for (id, fields) in [
                (
                    "d1",
                    HashMap::from([(
                        "nested".to_string(),
                        Value::map(HashMap::from([("x".to_string(), Value::integer(10))])),
                    )]),
                ),
                (
                    "d2",
                    HashMap::from([(
                        "nested".to_string(),
                        Value::map(HashMap::from([("x".to_string(), Value::null())])),
                    )]),
                ),
                (
                    "d3",
                    HashMap::from([(
                        "nested".to_string(),
                        Value::map(HashMap::from([("y".to_string(), Value::integer(1))])),
                    )]),
                ),
                (
                    "d4",
                    HashMap::from([("nested".to_string(), Value::integer(42))]),
                ),
            ] {
                let doc_ref: DocumentRef = format!("{root}/nested/{id}").parse().unwrap();
                db.update_doc(&doc_ref, fields, None, None, None)
                    .await
                    .unwrap();
            }

            let (_, nested_res) = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    nested_sq(),
                    vec![agg_count("c", None), agg_sum("s", Some("nested.x"))],
                    ReadConsistency::Default,
                )
                .await
                .unwrap();
            assert_eq!(
                nested_res.get("c"),
                Some(&Value::integer(if enterprise { 4 } else { 2 }))
            );
            assert_eq!(nested_res.get("s"), Some(&Value::integer(10)));

            // 3. `limit` interaction: docs missing 'a' do NOT consume `limit` slots on Standard
            for (id, fields) in [
                (
                    "d1",
                    HashMap::from([("other".to_string(), Value::integer(1))]),
                ),
                ("d2", HashMap::from([("a".to_string(), Value::integer(10))])),
                ("d3", HashMap::from([("a".to_string(), Value::integer(20))])),
            ] {
                let doc_ref: DocumentRef = format!("{root}/limited/{id}").parse().unwrap();
                db.update_doc(&doc_ref, fields, None, None, None)
                    .await
                    .unwrap();
            }
            let limited_sq = StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   "limited".to_string(),
                    all_descendants: false,
                }],
                order_by: if enterprise {
                    vec![structured_query::Order {
                        field:     Some(structured_query::FieldReference {
                            field_path: "__name__".to_string(),
                        }),
                        direction: structured_query::Direction::Ascending as i32,
                    }]
                } else {
                    vec![]
                },
                limit: Some(googleapis::google::protobuf::Int32Value { value: 2 }),
                ..Default::default()
            };
            let (_, limited_res) = db
                .run_aggregation_query(
                    Ref::Root(root.clone()),
                    limited_sq,
                    vec![agg_count("c", None), agg_sum("s", Some("a"))],
                    ReadConsistency::Default,
                )
                .await
                .unwrap();
            assert_eq!(limited_res.get("c"), Some(&Value::integer(2)));
            assert_eq!(
                limited_res.get("s"),
                Some(&Value::integer(if enterprise { 10 } else { 30 }))
            );

            // 4. Multi-field `[count(), sum("a"), avg("b")]` requires both 'a' and 'b' on Standard
            for (id, fields) in [
                (
                    "d1",
                    HashMap::from([
                        ("a".to_string(), Value::integer(10)),
                        ("b".to_string(), Value::integer(20)),
                    ]),
                ),
                ("d2", HashMap::from([("a".to_string(), Value::integer(5))])),
                ("d3", HashMap::from([("b".to_string(), Value::integer(7))])),
                (
                    "d4",
                    HashMap::from([
                        ("a".to_string(), Value::null()),
                        ("b".to_string(), Value::null()),
                    ]),
                ),
            ] {
                let doc_ref: DocumentRef = format!("{root}/multi/{id}").parse().unwrap();
                db.update_doc(&doc_ref, fields, None, None, None)
                    .await
                    .unwrap();
            }
            let multi_sq = StructuredQuery {
                from: vec![CollectionSelector {
                    collection_id:   "multi".to_string(),
                    all_descendants: false,
                }],
                ..Default::default()
            };
            let (_, multi_res) = db
                .run_aggregation_query(
                    Ref::Root(root),
                    multi_sq,
                    vec![
                        agg_count("c", None),
                        agg_sum("s", Some("a")),
                        agg_avg("avg", Some("b")),
                    ],
                    ReadConsistency::Default,
                )
                .await
                .unwrap();
            if enterprise {
                assert_eq!(multi_res.get("c"), Some(&Value::integer(4)));
                assert_eq!(multi_res.get("s"), Some(&Value::integer(15)));
                assert_eq!(multi_res.get("avg"), Some(&Value::double(13.5)));
            } else {
                assert_eq!(multi_res.get("c"), Some(&Value::integer(2)));
                assert_eq!(multi_res.get("s"), Some(&Value::integer(10)));
                assert_eq!(multi_res.get("avg"), Some(&Value::double(20.0)));
            }
        }
    }

    #[tokio::test]
    async fn standard_find_nearest_distance_result_field_not_filtered_as_stored_field() {
        let project: &'static FirestoreProject =
            Box::leak(Box::new(FirestoreProject::new(FirestoreConfig::default())));
        let root: RootRef = "projects/p/databases/(default)".parse().unwrap();
        let db = project.database(&root).await;

        // Seed vector docs WITHOUT stored `"dist"` or `"meta"` fields.
        // Euclidean distances from `[0.0, 0.0]`: v1 = 5.0, v2 = 1.0, v3 = 6.0 (sum = 12.0, avg =
        // 4.0).
        for (id, fields) in [
            (
                "v1",
                HashMap::from([
                    ("embedding".to_string(), Value::vector([3.0, 4.0])),
                    ("other".to_string(), Value::integer(10)),
                ]),
            ),
            (
                "v2",
                HashMap::from([("embedding".to_string(), Value::vector([0.0, 1.0]))]),
            ),
            (
                "v3",
                HashMap::from([
                    ("embedding".to_string(), Value::vector([0.0, 6.0])),
                    ("other".to_string(), Value::integer(30)),
                ]),
            ),
            (
                "v4",
                HashMap::from([("other".to_string(), Value::integer(99))]),
            ),
        ] {
            let doc_ref: DocumentRef = format!("{root}/vec_items/{id}").parse().unwrap();
            db.update_doc(&doc_ref, fields, None, None, None)
                .await
                .unwrap();
        }

        let find_nearest_sq = |distance_result_field: &str, limit: i32| StructuredQuery {
            from: vec![CollectionSelector {
                collection_id:   "vec_items".to_string(),
                all_descendants: false,
            }],
            find_nearest: Some(structured_query::FindNearest {
                vector_field: Some(structured_query::FieldReference {
                    field_path: "embedding".to_string(),
                }),
                query_vector: Some(Value::vector([0.0, 0.0])),
                distance_measure: structured_query::find_nearest::DistanceMeasure::Euclidean as i32,
                limit: Some(googleapis::google::protobuf::Int32Value { value: limit }),
                distance_result_field: distance_result_field.to_string(),
                distance_threshold: None,
            }),
            ..Default::default()
        };

        // 1. Direct `distance_result_field: "dist"` is virtual and must not require stored
        //    `"dist"`.
        let (_, direct) = db
            .run_aggregation_query(
                Ref::Root(root.clone()),
                find_nearest_sq("dist", 10),
                vec![
                    agg_count("c", None),
                    agg_sum("s", Some("dist")),
                    agg_avg("a", Some("dist")),
                ],
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(direct.get("c"), Some(&Value::integer(3)));
        assert!(direct.get("s").unwrap().storage_eq(&Value::double(12.0)));
        assert!(direct.get("a").unwrap().storage_eq(&Value::double(4.0)));

        // 2. Nested `distance_result_field: "meta.dist"` works for `[count(), sum("meta.dist"),
        //    avg("meta.dist")]` without stored `"meta"` or `"meta.dist"`.
        let (_, nested_leaf) = db
            .run_aggregation_query(
                Ref::Root(root.clone()),
                find_nearest_sq("meta.dist", 10),
                vec![
                    agg_count("c", None),
                    agg_sum("s", Some("meta.dist")),
                    agg_avg("a", Some("meta.dist")),
                ],
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(nested_leaf.get("c"), Some(&Value::integer(3)));
        assert!(
            nested_leaf
                .get("s")
                .unwrap()
                .storage_eq(&Value::double(12.0))
        );
        assert!(
            nested_leaf
                .get("a")
                .unwrap()
                .storage_eq(&Value::double(4.0))
        );

        // 3. Ancestor map path `"meta"` synthesized by `distance_result_field: "meta.dist"`:
        //    `count()` retains all matched docs while `sum("meta")` ignores the non-numeric map.
        let (_, nested_parent) = db
            .run_aggregation_query(
                Ref::Root(root.clone()),
                find_nearest_sq("meta.dist", 10),
                vec![
                    agg_count("c", None),
                    agg_sum("s", Some("meta")),
                    agg_avg("a", Some("meta")),
                ],
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(nested_parent.get("c"), Some(&Value::integer(3)));
        assert!(
            nested_parent
                .get("s")
                .unwrap()
                .storage_eq(&Value::integer(0))
        );
        assert_eq!(nested_parent.get("a"), Some(&Value::null()));

        // 4. Non-distance `SUM("other")` still enforces Standard field-existence filtering
        //    alongside `findNearest`, excluding `v2` (missing `"other"`) before `limit: 2`.
        let (_, mixed_other) = db
            .run_aggregation_query(
                Ref::Root(root.clone()),
                find_nearest_sq("dist", 2),
                vec![
                    agg_count("c", None),
                    agg_sum("sum_dist", Some("dist")),
                    agg_avg("avg_dist", Some("dist")),
                    agg_sum("sum_other", Some("other")),
                ],
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(mixed_other.get("c"), Some(&Value::integer(2)));
        assert!(
            mixed_other
                .get("sum_dist")
                .unwrap()
                .storage_eq(&Value::double(11.0))
        );
        assert!(
            mixed_other
                .get("avg_dist")
                .unwrap()
                .storage_eq(&Value::double(5.5))
        );
        assert!(
            mixed_other
                .get("sum_other")
                .unwrap()
                .storage_eq(&Value::integer(40))
        );

        // 5. Sibling non-distance path `"meta.other"` is NOT synthesized by `"meta.dist"`, so
        //    Standard field-existence filtering still excludes docs lacking stored `"meta.other"`.
        let (_, sibling_missing) = db
            .run_aggregation_query(
                Ref::Root(root),
                find_nearest_sq("meta.dist", 10),
                vec![agg_count("c", None), agg_sum("s", Some("meta.other"))],
                ReadConsistency::Default,
            )
            .await
            .unwrap();
        assert_eq!(sibling_missing.get("c"), Some(&Value::integer(0)));
        assert!(
            sibling_missing
                .get("s")
                .unwrap()
                .storage_eq(&Value::integer(0))
        );
    }
}
