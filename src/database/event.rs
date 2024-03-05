use std::collections::HashMap;

use prost_types::Timestamp;
use string_cache::DefaultAtom;

use super::document::DocumentVersion;

pub struct DatabaseEvent {
    pub update_time: Timestamp,
    pub updates:     HashMap<DefaultAtom, DocumentVersion>,
}
