use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::parse_non_negative_count;
use crate::error::Result;

/// Executes the `offset` pipeline stage.
///
/// This stage skips the first N documents in the pipeline stream and yields the remainder.
pub(super) fn execute(stage: Stage, mut docs: Vec<Document>) -> Result<Vec<Document>> {
    let offset = parse_non_negative_count(&stage, "offset", "Offset")?;

    if offset >= docs.len() {
        docs.clear();
    } else {
        docs.drain(0..offset);
    }

    Ok(docs)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use googleapis::google::firestore::v1::Value;

    use super::*;
    use crate::GenericDatabaseError;

    fn sample_docs() -> Vec<Document> {
        ["d1", "d2", "d3"]
            .into_iter()
            .map(|id| Document {
                name: format!("projects/p/databases/(default)/documents/c/{id}"),
                fields: HashMap::new(),
                create_time: None,
                update_time: None,
            })
            .collect()
    }

    #[test]
    fn offset_accepts_i64_max_and_clears_docs() {
        let out = execute(
            Stage {
                name:    "offset".to_string(),
                args:    vec![Value::integer(i64::MAX)],
                options: HashMap::new(),
            },
            sample_docs(),
        )
        .unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn offset_rejects_negative_and_non_integer() {
        for n in [-1_i64, i64::MIN] {
            let err = execute(
                Stage {
                    name:    "offset".to_string(),
                    args:    vec![Value::integer(n)],
                    options: HashMap::new(),
                },
                sample_docs(),
            )
            .unwrap_err();
            assert_eq!(
                err,
                GenericDatabaseError::invalid_argument("Offset must be >= 0")
            );
        }

        let err = execute(
            Stage {
                name:    "offset".to_string(),
                args:    vec![Value::string("2")],
                options: HashMap::new(),
            },
            sample_docs(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            GenericDatabaseError::invalid_argument("offset stage expects an IntegerValue")
        );
    }
}
