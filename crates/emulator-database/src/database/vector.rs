use std::cmp;

use googleapis::{
    VectorClassification, classify_vector_map,
    google::firestore::v1::{
        Value, structured_query::find_nearest::DistanceMeasure as ProtoDistanceMeasure,
        value::ValueType,
    },
};

use crate::{GenericDatabaseError, error::Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceMeasure {
    Euclidean,
    Cosine,
    DotProduct,
}

impl DistanceMeasure {
    pub(crate) const fn fn_name(self) -> &'static str {
        match self {
            Self::Euclidean => "euclidean_distance",
            Self::Cosine => "cosine_distance",
            Self::DotProduct => "dot_product",
        }
    }

    pub(crate) fn from_pipeline_str(s: &str) -> Result<Self> {
        if s.eq_ignore_ascii_case("euclidean") {
            Ok(Self::Euclidean)
        } else if s.eq_ignore_ascii_case("cosine") {
            Ok(Self::Cosine)
        } else if s.eq_ignore_ascii_case("dot_product") {
            Ok(Self::DotProduct)
        } else {
            Err(GenericDatabaseError::invalid_argument(
                "find_nearest distance_measure must be 'euclidean', 'cosine', or 'dot_product'",
            ))
        }
    }

    pub(crate) fn passes_threshold(self, distance: f64, threshold: f64) -> bool {
        match self {
            Self::Euclidean | Self::Cosine => distance <= threshold,
            Self::DotProduct => distance >= threshold,
        }
    }

    pub(crate) fn cmp_distance(self, a: f64, b: f64) -> cmp::Ordering {
        match self {
            Self::Euclidean | Self::Cosine => a.total_cmp(&b),
            Self::DotProduct => b.total_cmp(&a),
        }
    }
}

impl TryFrom<i32> for DistanceMeasure {
    type Error = GenericDatabaseError;

    fn try_from(value: i32) -> Result<Self> {
        match ProtoDistanceMeasure::try_from(value) {
            Ok(ProtoDistanceMeasure::Euclidean) => Ok(Self::Euclidean),
            Ok(ProtoDistanceMeasure::Cosine) => Ok(Self::Cosine),
            Ok(ProtoDistanceMeasure::DotProduct) => Ok(Self::DotProduct),
            Ok(ProtoDistanceMeasure::Unspecified) | Err(_) => {
                Err(GenericDatabaseError::invalid_argument(
                    "invalid structured_query::find_nearest::DistanceMeasure",
                ))
            }
        }
    }
}

pub(crate) fn validate_query_vector(
    value: &Value,
    measure: DistanceMeasure,
    enterprise_edition: bool,
) -> Result<Vec<f64>> {
    let Some(ValueType::MapValue(map)) = value.value_type.as_ref() else {
        return Err(GenericDatabaseError::invalid_argument(
            "query_vector must be a valid VectorValue",
        ));
    };
    let coords: Vec<f64> = match classify_vector_map(&map.fields) {
        VectorClassification::Valid(values) => values
            .iter()
            .map(|v| match v.value_type {
                Some(ValueType::DoubleValue(d)) => d,
                _ => unreachable!("valid vector values only contain DoubleValue"),
            })
            .collect(),
        VectorClassification::Invalid(err) => {
            return Err(GenericDatabaseError::invalid_argument(
                err.as_error_message(),
            ));
        }
        VectorClassification::NotVectorMap => {
            return Err(GenericDatabaseError::invalid_argument(
                "query_vector must be a valid VectorValue",
            ));
        }
    };
    if measure == DistanceMeasure::Cosine {
        compute_query_distance(
            DistanceMeasure::Cosine,
            enterprise_edition,
            coords.iter().copied(),
            coords.iter().copied(),
        )?;
    }
    Ok(coords)
}

fn has_zero_norm(mut iter: impl Iterator<Item = f64>) -> bool {
    let mut norm_sq = 0.0_f64;
    for x in &mut iter {
        if !x.is_finite() {
            return false;
        }
        norm_sq += x * x;
    }
    norm_sq == 0.0
}

pub(crate) fn compute_query_distance<Ia, Ib>(
    measure: DistanceMeasure,
    enterprise_edition: bool,
    a: Ia,
    b: Ib,
) -> Result<f64>
where
    Ia: ExactSizeIterator<Item = f64> + Clone,
    Ib: ExactSizeIterator<Item = f64> + Clone,
{
    if !enterprise_edition
        && measure == DistanceMeasure::Cosine
        && (has_zero_norm(a.clone()) || has_zero_norm(b.clone()))
    {
        return Err(GenericDatabaseError::failed_precondition(
            "Cannot compute cosine distance against a vector with a magnitude of zero.",
        ));
    }
    compute_distance(measure, a, b)
}

pub(crate) fn compute_distance<Ia, Ib>(measure: DistanceMeasure, a: Ia, b: Ib) -> Result<f64>
where
    Ia: ExactSizeIterator<Item = f64> + Clone,
    Ib: ExactSizeIterator<Item = f64> + Clone,
{
    debug_assert_eq!(a.len(), b.len());

    let dist = match measure {
        DistanceMeasure::Euclidean => a
            .zip(b)
            .map(|(x, y)| {
                let d = x - y;
                d * d
            })
            .sum::<f64>()
            .sqrt(),
        DistanceMeasure::Cosine => {
            let mut dot = 0.0_f64;
            let mut norm_a_sq = 0.0_f64;
            let mut norm_b_sq = 0.0_f64;
            for (x, y) in a.zip(b) {
                dot += x * y;
                norm_a_sq += x * x;
                norm_b_sq += y * y;
            }
            let denom = (norm_a_sq * norm_b_sq).sqrt();
            1.0 - (dot / denom)
        }
        DistanceMeasure::DotProduct => a.zip(b).map(|(x, y)| x * y).sum::<f64>(),
    };

    if dist.is_nan() {
        return Err(GenericDatabaseError::invalid_argument(format!(
            "{}(...) result is NaN.",
            measure.fn_name()
        )));
    }
    Ok(if dist == 0.0 { 0.0 } else { dist })
}

pub(crate) fn select_and_sort_nearest<T, F>(
    candidates: &mut Vec<T>,
    limit: Option<usize>,
    mut cmp_fn: F,
) where
    F: FnMut(&T, &T) -> cmp::Ordering,
{
    if let Some(k) = limit {
        if k == 0 {
            candidates.clear();
            return;
        }
        if k < candidates.len() {
            candidates.select_nth_unstable_by(k - 1, &mut cmp_fn);
            candidates.truncate(k);
        }
    }
    candidates.sort_unstable_by(cmp_fn);
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::{VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, cmp_reference_paths};
    use googletest::prelude::*;

    use super::*;
    use crate::database::reference::DocumentRef;

    #[gtest]
    fn self_cosine_and_unscaled_ieee754_behavior() {
        let self_cos = compute_distance(
            DistanceMeasure::Cosine,
            [1.0, 2.0].into_iter(),
            [1.0, 2.0].into_iter(),
        )
        .unwrap();
        assert_eq!(self_cos.to_bits(), 0.0_f64.to_bits());

        assert_that!(
            compute_distance(
                DistanceMeasure::Cosine,
                [1e200, 0.0].into_iter(),
                [1e200, 0.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );

        let e40_self = compute_distance(
            DistanceMeasure::Cosine,
            [1e40, 0.0].into_iter(),
            [1e40, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(e40_self.to_bits(), 0.0_f64.to_bits());

        let ortho_unit = compute_distance(
            DistanceMeasure::Cosine,
            [1.0, 0.0].into_iter(),
            [0.0, 1.0].into_iter(),
        )
        .unwrap();
        assert_eq!(ortho_unit, 1.0);

        let e100_self = compute_distance(
            DistanceMeasure::Cosine,
            [1e100, 0.0].into_iter(),
            [1e100, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(e100_self, 1.0);

        let e_neg100_45 = compute_distance(
            DistanceMeasure::Cosine,
            [1e-100, 1e-100].into_iter(),
            [1e-100, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(e_neg100_45, f64::NEG_INFINITY);

        let e_neg100_opp = compute_distance(
            DistanceMeasure::Cosine,
            [1e-100, 0.0].into_iter(),
            [-1e-100, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(e_neg100_opp, f64::INFINITY);

        assert_that!(
            compute_distance(
                DistanceMeasure::Cosine,
                [1e-200, 0.0].into_iter(),
                [1e-200, 0.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );

        let collinear_overshoot = compute_distance(
            DistanceMeasure::Cosine,
            [1.0000008, 2.0000024, 3.0000056].into_iter(),
            [1.3333344080000062, 2.6666698826666857, 4.000007490666712].into_iter(),
        )
        .unwrap();
        assert_eq!(collinear_overshoot, -2.220446049250313e-16);
    }

    #[gtest]
    fn euclidean_unscaled_and_negative_zero_normalization() {
        let tiny_underflow = compute_distance(
            DistanceMeasure::Euclidean,
            [1e-200, 0.0].into_iter(),
            [0.0, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(tiny_underflow.to_bits(), 0.0_f64.to_bits());

        let finite_1e100 = compute_distance(
            DistanceMeasure::Euclidean,
            [1e100, 0.0].into_iter(),
            [0.0, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(finite_1e100, 1e100);

        let overflow_1e200 = compute_distance(
            DistanceMeasure::Euclidean,
            [1e200, 0.0].into_iter(),
            [0.0, 0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(overflow_1e200, f64::INFINITY);

        let euc_neg_zero = compute_distance(
            DistanceMeasure::Euclidean,
            [-0.0].into_iter(),
            [-0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(euc_neg_zero.to_bits(), 0.0_f64.to_bits());

        let dot_neg_zero = compute_distance(
            DistanceMeasure::DotProduct,
            [1.0].into_iter(),
            [-0.0].into_iter(),
        )
        .unwrap();
        assert_eq!(dot_neg_zero.to_bits(), 0.0_f64.to_bits());
    }

    #[gtest]
    fn infinity_and_nan_semantics() {
        let euc_inf = compute_distance(
            DistanceMeasure::Euclidean,
            [1.0, 2.0].into_iter(),
            [1.0, f64::INFINITY].into_iter(),
        )
        .unwrap();
        assert_eq!(euc_inf, f64::INFINITY);

        assert_that!(
            compute_distance(
                DistanceMeasure::Euclidean,
                [f64::INFINITY].into_iter(),
                [f64::INFINITY].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "euclidean_distance(...) result is NaN."
            )))
        );

        assert_that!(
            compute_distance(
                DistanceMeasure::Cosine,
                [f64::INFINITY, 1.0].into_iter(),
                [1.0, 1.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );

        assert_that!(
            compute_distance(
                DistanceMeasure::DotProduct,
                [f64::INFINITY, f64::INFINITY].into_iter(),
                [1.0, -1.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "dot_product(...) result is NaN."
            )))
        );
    }

    #[gtest]
    fn threshold_inclusivity_and_query_vector_validation() {
        assert!(DistanceMeasure::Euclidean.passes_threshold(2.0, 2.0));
        assert!(!DistanceMeasure::Euclidean.passes_threshold(2.1, 2.0));
        assert!(DistanceMeasure::Cosine.passes_threshold(0.5, 0.5));
        assert!(!DistanceMeasure::Cosine.passes_threshold(0.6, 0.5));
        assert!(DistanceMeasure::DotProduct.passes_threshold(3.0, 3.0));
        assert!(!DistanceMeasure::DotProduct.passes_threshold(2.9, 3.0));

        assert_eq!(
            DistanceMeasure::try_from(ProtoDistanceMeasure::Euclidean as i32).unwrap(),
            DistanceMeasure::Euclidean
        );
        assert_that!(
            DistanceMeasure::try_from(ProtoDistanceMeasure::Unspecified as i32),
            err(eq(&GenericDatabaseError::invalid_argument(
                "invalid structured_query::find_nearest::DistanceMeasure"
            )))
        );
        assert_eq!(
            DistanceMeasure::from_pipeline_str("CoSiNe").unwrap(),
            DistanceMeasure::Cosine
        );
        assert_that!(
            DistanceMeasure::from_pipeline_str("manhattan"),
            err(eq(&GenericDatabaseError::invalid_argument(
                "find_nearest distance_measure must be 'euclidean', 'cosine', or 'dot_product'"
            )))
        );

        assert_that!(
            validate_query_vector(
                &Value::array(vec![Value::double(1.0)]),
                DistanceMeasure::Euclidean,
                false,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "query_vector must be a valid VectorValue"
            )))
        );
        assert_that!(
            validate_query_vector(
                &Value::map(HashMap::from([(
                    VECTOR_MAP_TYPE_KEY.to_string(),
                    Value::string(VECTOR_MAP_TYPE_SENTINEL),
                )])),
                DistanceMeasure::Euclidean,
                false,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "Vector value map is missing 'value' array."
            )))
        );
        assert_that!(
            validate_query_vector(&Value::vector([0.0, 0.0]), DistanceMeasure::Cosine, false),
            err(eq(&GenericDatabaseError::failed_precondition(
                "Cannot compute cosine distance against a vector with a magnitude of zero."
            )))
        );
        assert_that!(
            validate_query_vector(&Value::vector([0.0, 0.0]), DistanceMeasure::Cosine, true),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );
        assert_that!(
            compute_query_distance(
                DistanceMeasure::Cosine,
                false,
                [1.0, 0.0].into_iter(),
                [0.0, 0.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::failed_precondition(
                "Cannot compute cosine distance against a vector with a magnitude of zero."
            )))
        );
        assert_that!(
            compute_query_distance(
                DistanceMeasure::Cosine,
                true,
                [1.0, 0.0].into_iter(),
                [0.0, 0.0].into_iter(),
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );
        assert_that!(
            validate_query_vector(
                &Value::vector([f64::INFINITY, 1.0]),
                DistanceMeasure::Cosine,
                true,
            ),
            err(eq(&GenericDatabaseError::invalid_argument(
                "cosine_distance(...) result is NaN."
            )))
        );
        assert_eq!(
            validate_query_vector(
                &Value::vector([0.0, 0.0]),
                DistanceMeasure::Euclidean,
                false
            )
            .unwrap(),
            vec![0.0, 0.0]
        );
    }

    #[gtest]
    fn select_and_sort_nearest_tie_breaks_by_segment_order() {
        let doc_a: DocumentRef = "projects/p/databases/(default)/documents/parents/doc/collection/\
                                  x"
        .parse()
        .unwrap();
        let doc_b: DocumentRef = "projects/p/databases/(default)/documents/parents/doc-1/\
                                  collection/x"
            .parse()
            .unwrap();
        let doc_c: DocumentRef = "projects/p/databases/(default)/documents/collection/a/\
                                  collection/x"
            .parse()
            .unwrap();
        let doc_d: DocumentRef = "projects/p/databases/(default)/documents/collection/z"
            .parse()
            .unwrap();

        let mut candidates = vec![
            (doc_b.clone(), 1.0_f64),
            (doc_a.clone(), 1.0_f64),
            (doc_d.clone(), 0.5_f64),
            (doc_c.clone(), 0.5_f64),
        ];

        select_and_sort_nearest(&mut candidates, Some(3), |(ra, da), (rb, db)| {
            DistanceMeasure::Euclidean
                .cmp_distance(*da, *db)
                .then_with(|| ra.cmp_firestore_order(rb))
        });

        let ordered_names: Vec<String> = candidates.iter().map(|(r, _)| r.to_string()).collect();
        assert_eq!(
            ordered_names,
            vec![doc_c.to_string(), doc_d.to_string(), doc_a.to_string()]
        );

        assert_eq!(
            cmp_reference_paths(&doc_a.to_string(), &doc_b.to_string()),
            cmp::Ordering::Less
        );
        assert_eq!(
            cmp_reference_paths(&doc_c.to_string(), &doc_d.to_string()),
            cmp::Ordering::Less
        );
    }
}
