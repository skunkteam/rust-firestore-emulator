use std::cmp;

use googleapis::google::firestore::v1::structured_query;

use crate::{
    GenericDatabaseError,
    database::{document::StoredDocumentVersion, field_path::FieldPath},
    error::Result,
};

const MAX_VECTOR_DIMENSION: usize = 2048;
const MAX_FIND_NEAREST_LIMIT: usize = 1000;

/// Distance measure used to compare vectors in a [`FindNearest`] query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceMeasure {
    /// Euclidean (L2) distance. Smaller values indicate higher similarity (sorted ascending,
    /// `distance <= distance_threshold`).
    Euclidean,
    /// Cosine distance (`1.0 - cosine_similarity`). Smaller values indicate higher similarity
    /// (sorted ascending, `distance <= distance_threshold`).
    Cosine,
    /// Dot product similarity. Larger values indicate higher similarity (sorted descending,
    /// `distance >= distance_threshold`).
    DotProduct,
}

impl TryFrom<structured_query::find_nearest::DistanceMeasure> for DistanceMeasure {
    type Error = GenericDatabaseError;

    fn try_from(
        value: structured_query::find_nearest::DistanceMeasure,
    ) -> Result<Self, Self::Error> {
        use structured_query::find_nearest::DistanceMeasure as Other;
        match value {
            Other::Unspecified => Err(GenericDatabaseError::invalid_argument(
                "invalid structured_query::find_nearest::DistanceMeasure",
            )),
            Other::Euclidean => Ok(Self::Euclidean),
            Other::Cosine => Ok(Self::Cosine),
            Other::DotProduct => Ok(Self::DotProduct),
        }
    }
}

/// Nearest-neighbor vector search stage (`StructuredQuery::find_nearest`).
#[derive(Clone, Debug)]
pub struct FindNearest {
    /// Path to the document field containing the target `VectorValue`.
    pub vector_field: FieldPath,
    /// Query vector to compare against candidate documents.
    pub query_vector: Vec<f64>,
    /// Distance metric used to score and order candidate vectors.
    pub distance_measure: DistanceMeasure,
    /// Maximum number of nearest neighbors to return (`1..=1000`).
    pub limit: usize,
    /// Optional document field path to populate with the computed distance in projected results.
    pub distance_result_field: Option<FieldPath>,
    /// Optional threshold on the computed distance/similarity score.
    pub distance_threshold: Option<f64>,
}

impl FindNearest {
    /// Validates `find_nearest` parameters (`vector_field`, `query_vector` dimensionality and
    /// finiteness, `limit` range, non-zero magnitude for `COSINE`, and `distance_threshold`).
    pub(super) fn validate(&self) -> Result<()> {
        if self.vector_field.is_document_name() {
            return Err(GenericDatabaseError::invalid_argument(
                "vector_field cannot be __name__",
            ));
        }
        if self
            .distance_result_field
            .as_ref()
            .is_some_and(FieldPath::is_document_name)
        {
            return Err(GenericDatabaseError::invalid_argument(
                "distance_result_field cannot be __name__",
            ));
        }
        if self.query_vector.is_empty() || self.query_vector.len() > MAX_VECTOR_DIMENSION {
            return Err(GenericDatabaseError::invalid_argument(
                "query_vector dimension must be between 1 and 2048",
            ));
        }
        if !self.query_vector.iter().all(|v| v.is_finite()) {
            return Err(GenericDatabaseError::invalid_argument(
                "query_vector elements must be finite numbers",
            ));
        }
        if self.limit == 0 || self.limit > MAX_FIND_NEAREST_LIMIT {
            return Err(GenericDatabaseError::invalid_argument(
                "find_nearest limit must be between 1 and 1000",
            ));
        }
        if self.distance_measure == DistanceMeasure::Cosine
            && !self.query_vector.iter().any(|&v| v != 0.0)
        {
            return Err(GenericDatabaseError::invalid_argument(
                "cannot compute COSINE distance with a zero-magnitude query_vector",
            ));
        }
        if let Some(threshold) = self.distance_threshold
            && threshold.is_nan()
        {
            return Err(GenericDatabaseError::invalid_argument(
                "distance_threshold cannot be NaN",
            ));
        }
        Ok(())
    }

    /// Evaluates the vector distance for a candidate document if it has a valid vector of matching
    /// dimensionality and satisfies the optional `distance_threshold`.
    pub(super) fn eval_distance(&self, doc: &StoredDocumentVersion) -> Result<Option<f64>> {
        let Some(doc_vector) = self
            .vector_field
            .get_value(&doc.fields)
            .and_then(|v| v.as_vector())
        else {
            return Ok(None);
        };
        if doc_vector.len() != self.query_vector.len() || !doc_vector.iter().all(|v| v.is_finite())
        {
            return Ok(None);
        }

        let distance = match self.distance_measure {
            DistanceMeasure::Euclidean => self
                .query_vector
                .iter()
                .zip(&doc_vector)
                .map(|(q, d)| (q - d).powi(2))
                .sum::<f64>()
                .sqrt(),
            DistanceMeasure::Cosine => {
                // Cloud Firestore aborts the query with `INVALID_ARGUMENT` if a candidate document
                // matching the pre-filters has a zero-magnitude vector (`[0.0, ...]`) under COSINE.
                if !doc_vector.iter().any(|&v| v != 0.0) {
                    return Err(GenericDatabaseError::invalid_argument(
                        "cosine_distance(...) result is NaN.",
                    ));
                }
                let query_norm_sq = dot_product(&self.query_vector, &self.query_vector);
                let doc_norm_sq = dot_product(&doc_vector, &doc_vector);
                let denom = query_norm_sq.sqrt() * doc_norm_sq.sqrt();
                if denom == 0.0 {
                    return Err(GenericDatabaseError::invalid_argument(
                        "cosine_distance(...) result is NaN.",
                    ));
                }
                1.0 - (dot_product(&self.query_vector, &doc_vector) / denom)
            }
            DistanceMeasure::DotProduct => dot_product(&self.query_vector, &doc_vector),
        };

        if !distance.is_finite() {
            return Ok(None);
        }

        if let Some(threshold) = self.distance_threshold {
            let within_threshold = match self.distance_measure {
                DistanceMeasure::Euclidean | DistanceMeasure::Cosine => distance <= threshold,
                DistanceMeasure::DotProduct => distance >= threshold,
            };
            if !within_threshold {
                return Ok(None);
            }
        }

        Ok(Some(distance))
    }

    /// Compares two distances according to this measure's ordering (ascending for
    /// [`DistanceMeasure::Euclidean`] and [`DistanceMeasure::Cosine`]; descending for
    /// [`DistanceMeasure::DotProduct`]).
    #[must_use]
    pub(super) fn cmp_distance(&self, a: f64, b: f64) -> cmp::Ordering {
        match self.distance_measure {
            DistanceMeasure::Euclidean | DistanceMeasure::Cosine => a.total_cmp(&b),
            DistanceMeasure::DotProduct => b.total_cmp(&a),
        }
    }
}

impl TryFrom<structured_query::FindNearest> for FindNearest {
    type Error = GenericDatabaseError;

    fn try_from(value: structured_query::FindNearest) -> Result<Self, Self::Error> {
        let distance_measure: DistanceMeasure = value.distance_measure().try_into()?;
        let vector_field: FieldPath = value
            .vector_field
            .as_ref()
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument("missing vector_field in find_nearest")
            })?
            .field_path
            .parse()?;

        let query_vector = value
            .query_vector
            .as_ref()
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument("missing query_vector in find_nearest")
            })?
            .as_vector()
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument("query_vector must be a valid VectorValue")
            })?;

        let raw_limit = value
            .limit
            .ok_or_else(|| GenericDatabaseError::invalid_argument("missing limit in find_nearest"))?
            .value;
        let limit = usize::try_from(raw_limit).map_err(|_| {
            GenericDatabaseError::invalid_argument("find_nearest limit must be between 1 and 1000")
        })?;

        let distance_result_field = (!value.distance_result_field.is_empty())
            .then(|| value.distance_result_field.parse())
            .transpose()?;

        let distance_threshold = value.distance_threshold.map(|v| v.value);

        Ok(Self {
            vector_field,
            query_vector,
            distance_measure,
            limit,
            distance_result_field,
            distance_threshold,
        })
    }
}

fn dot_product(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b).map(|(x, y)| x * y).sum()
}
