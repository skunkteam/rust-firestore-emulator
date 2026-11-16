use std::{fmt, str::FromStr};

use googleapis::{
    MAX_VECTOR_DIMENSIONS, VectorFormatError, google::firestore::v1::structured_query,
};

use crate::{
    GenericDatabaseError,
    database::{
        field_path::{FieldPath, FieldReference},
        vector::{self, DistanceMeasure},
    },
    error::Result,
};

#[derive(Clone)]
pub struct FindNearest {
    pub vector_field: FieldPath,
    pub query_vector: Vec<f64>,
    pub distance_measure: DistanceMeasure,
    pub limit: usize,
    pub distance_result_field: Option<FieldPath>,
    pub distance_threshold: Option<f64>,
}

impl fmt::Debug for FindNearest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let preview = &self.query_vector[..self.query_vector.len().min(3)];
        f.debug_struct("FindNearest")
            .field("vector_field", &self.vector_field)
            .field("query_vector_dims", &self.query_vector.len())
            .field("query_vector_prefix", &preview)
            .field("distance_measure", &self.distance_measure)
            .field("limit", &self.limit)
            .field("distance_result_field", &self.distance_result_field)
            .field("distance_threshold", &self.distance_threshold)
            .finish()
    }
}

impl FindNearest {
    pub fn new(
        vector_field: FieldPath,
        query_vector: Vec<f64>,
        distance_measure: DistanceMeasure,
        limit: usize,
    ) -> Self {
        Self {
            vector_field,
            query_vector,
            distance_measure,
            limit,
            distance_result_field: None,
            distance_threshold: None,
        }
    }

    pub fn distance_result_field(mut self, field: FieldPath) -> Self {
        self.distance_result_field = Some(field);
        self
    }

    pub fn distance_threshold(mut self, threshold: f64) -> Self {
        self.distance_threshold = Some(threshold);
        self
    }

    pub(crate) fn validate(&self, enterprise_edition: bool) -> Result<()> {
        self.vector_field.validate_non_reserved()?;
        if let Some(dist_field) = &self.distance_result_field {
            dist_field.validate_non_reserved()?;
        }
        if self.query_vector.is_empty() {
            return Err(GenericDatabaseError::invalid_argument(
                VectorFormatError::EmptyVector.as_error_message(),
            ));
        }
        if self.query_vector.len() > MAX_VECTOR_DIMENSIONS {
            return Err(GenericDatabaseError::invalid_argument(
                VectorFormatError::TooManyDimensions.as_error_message(),
            ));
        }
        if self.query_vector.iter().any(|d| d.is_nan()) {
            return Err(GenericDatabaseError::invalid_argument(
                VectorFormatError::NanElement.as_error_message(),
            ));
        }
        if self.distance_measure == DistanceMeasure::Cosine {
            vector::compute_query_distance(
                DistanceMeasure::Cosine,
                enterprise_edition,
                self.query_vector.iter().copied(),
                self.query_vector.iter().copied(),
            )?;
        }
        if !(1..=1000).contains(&self.limit) {
            return Err(GenericDatabaseError::invalid_argument(
                "find_nearest limit must be between 1 and 1000",
            ));
        }
        if self.distance_threshold.is_some_and(f64::is_nan) {
            return Err(GenericDatabaseError::invalid_argument(
                "distance_threshold cannot be NaN",
            ));
        }
        Ok(())
    }

    pub(crate) fn try_from_proto(
        proto: structured_query::FindNearest,
        enterprise_edition: bool,
    ) -> Result<Self> {
        let distance_measure = DistanceMeasure::try_from(proto.distance_measure)?;

        let vector_field_ref = proto.vector_field.ok_or_else(|| {
            GenericDatabaseError::invalid_argument("missing vector_field in find_nearest")
        })?;
        let vector_field = FieldReference::from_str(&vector_field_ref.field_path)?
            .into_non_reserved_field_path(
                "vector_field cannot be __name__ or contain empty or reserved segments",
            )?;

        let distance_result_field = if proto.distance_result_field.is_empty() {
            None
        } else {
            Some(
                FieldReference::from_str(&proto.distance_result_field)?
                    .into_non_reserved_field_path(
                        "distance_result_field cannot be __name__ or contain empty or reserved \
                         segments",
                    )?,
            )
        };

        let query_vector_val = proto.query_vector.ok_or_else(|| {
            GenericDatabaseError::invalid_argument("missing query_vector in find_nearest")
        })?;
        let query_vector =
            vector::validate_query_vector(&query_vector_val, distance_measure, enterprise_edition)?;

        let limit = proto
            .limit
            .filter(|v| (1..=1000).contains(&v.value))
            .map(|v| v.value as usize)
            .ok_or_else(|| {
                GenericDatabaseError::invalid_argument(
                    "find_nearest limit must be between 1 and 1000",
                )
            })?;

        let distance_threshold = match proto.distance_threshold {
            Some(v) if v.value.is_nan() => {
                return Err(GenericDatabaseError::invalid_argument(
                    "distance_threshold cannot be NaN",
                ));
            }
            Some(v) => Some(v.value),
            None => None,
        };

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
