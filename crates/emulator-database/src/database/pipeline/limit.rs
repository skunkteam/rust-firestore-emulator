use googleapis::google::firestore::v1::{Document, pipeline::Stage};

use super::parse_non_negative_count;
use crate::error::Result;

pub(super) fn execute(stage: Stage, mut docs: Vec<Document>) -> Result<Vec<Document>> {
    let limit = parse_non_negative_count(&stage, "limit", "Limit")?;

    docs.truncate(limit);
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
    fn limit_accepts_i64_max_and_zero() {
        let docs = sample_docs();
        let all = execute(
            Stage {
                name:    "limit".to_string(),
                args:    vec![Value::integer(i64::MAX)],
                options: HashMap::new(),
            },
            docs.clone(),
        )
        .unwrap();
        assert_eq!(all.len(), 3);

        let none = execute(
            Stage {
                name:    "limit".to_string(),
                args:    vec![Value::integer(0)],
                options: HashMap::new(),
            },
            docs,
        )
        .unwrap();
        assert!(none.is_empty());
    }

    #[test]
    fn limit_rejects_negative_and_non_integer() {
        for n in [-1_i64, i64::MIN] {
            let err = execute(
                Stage {
                    name:    "limit".to_string(),
                    args:    vec![Value::integer(n)],
                    options: HashMap::new(),
                },
                sample_docs(),
            )
            .unwrap_err();
            assert_eq!(
                err,
                GenericDatabaseError::invalid_argument("Limit must be >= 0")
            );
        }

        let err = execute(
            Stage {
                name:    "limit".to_string(),
                args:    vec![Value::string("2")],
                options: HashMap::new(),
            },
            sample_docs(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            GenericDatabaseError::invalid_argument("limit stage expects an IntegerValue")
        );
    }
}
