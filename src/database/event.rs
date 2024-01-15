use super::document::DocumentVersion;
use prost_types::Timestamp;

#[derive(Clone)]
pub struct DatabaseEvent {
    pub update_time: Timestamp,
    pub updates: Vec<DocumentVersion>,
}
