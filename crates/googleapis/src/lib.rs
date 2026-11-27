#![allow(rustdoc::invalid_rust_codeblocks)]
#![allow(rustdoc::invalid_html_tags)]

tonic::include_proto!("googleapis");

mod macros;
mod ser;
mod timestamp_ext;
mod transaction_options_ext;
mod value_ext;
pub mod vector_ext;

pub use timestamp_ext::{MAX_TIMESTAMP_SECONDS, MIN_TIMESTAMP_SECONDS};
pub use value_ext::{cmp_reference_paths, cmp_reference_segments};
pub use vector_ext::{
    MAX_VECTOR_DIMENSIONS, VECTOR_MAP_TYPE_KEY, VECTOR_MAP_TYPE_SENTINEL, VECTOR_MAP_VALUE_KEY,
    VectorClassification, VectorFormatError, classify_vector_map,
};
