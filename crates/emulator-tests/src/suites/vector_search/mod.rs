use googleapis::google::{
    firestore::v1::{
        Value,
        structured_query::{FieldReference, FindNearest, find_nearest},
    },
    protobuf::{DoubleValue, Int32Value},
};

mod listeners;
mod pipelines;
mod structured_query;
mod transactions;
mod writes;

fn make_find_nearest(
    vector_field: &str,
    query_vector: Value,
    distance_measure: find_nearest::DistanceMeasure,
    limit: i32,
    distance_result_field: Option<&str>,
    distance_threshold: Option<f64>,
) -> FindNearest {
    FindNearest {
        vector_field: Some(FieldReference {
            field_path: vector_field.to_string(),
        }),
        query_vector: Some(query_vector),
        distance_measure: distance_measure as i32,
        limit: Some(Int32Value { value: limit }),
        distance_result_field: distance_result_field.unwrap_or_default().to_string(),
        distance_threshold: distance_threshold.map(|value| DoubleValue { value }),
    }
}
