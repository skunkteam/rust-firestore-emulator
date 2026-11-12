use std::{collections::HashMap, fmt::Debug};

use googleapis::google::{
    firestore::v1::{Document, Value},
    protobuf::Timestamp,
};
use googletest::prelude::*;
use prost::Message;
use tonic::{Code, Status};

use super::builders::doc_path;

/// Asserts that a gRPC `Result<T, Status>` failed with the expected [`tonic::Code`].
pub(crate) fn assert_status_code<T: Debug>(res: Result<T, Status>, expected: Code) -> Status {
    let status = res.expect_err("expected gRPC error Status, got Ok");
    assert_eq!(
        status.code(),
        expected,
        "expected gRPC status {expected:?}, got {:?} (message: {:?})",
        status.code(),
        status.message()
    );
    status
}

/// Asserts that a sequences of timestamps is strictly or non-strictly monotonically increasing and
/// every nanosecond field is within `0..1_000_000_000`.
pub(crate) fn assert_monotonic_timestamps(timestamps: &[Timestamp], strict: bool) {
    for ts in timestamps {
        assert!(
            (0..1_000_000_000).contains(&ts.nanos),
            "timestamp nanos out of valid range: {ts:?}"
        );
    }
    for pair in timestamps.windows(2) {
        let a = (pair[0].seconds, pair[0].nanos);
        let b = (pair[1].seconds, pair[1].nanos);
        if strict {
            assert!(
                a < b,
                "expected strictly increasing timestamps, got {:?} then {:?}",
                pair[0],
                pair[1]
            );
        } else {
            assert!(
                a <= b,
                "expected monotonic non-decreasing timestamps, got {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
    }
}

/// Asserts timestamp-ignoring wire-exact equality (`Value::fields_storage_eq`) on a [`Document`],
/// distinguishing `IntegerValue(1)` from `DoubleValue(1.0)` and `-0.0` from `+0.0` while also
/// verifying valid `create_time`/`update_time` ordering and Protobuf wire roundtrip integrity.
pub(crate) fn assert_doc_wire_eq(
    actual: &Document,
    expected_rel_or_full_name: &str,
    expected_fields: &HashMap<String, Value>,
) {
    let expected_name = doc_path(expected_rel_or_full_name);
    expect_eq!(actual.name, expected_name);
    assert!(
        Value::fields_storage_eq(&actual.fields, expected_fields),
        "wire-exact field mismatch on {}:\n  actual:   {:?}\n  expected: {:?}",
        actual.name,
        actual.fields,
        expected_fields
    );
    if let (Some(ct), Some(ut)) = (actual.create_time, actual.update_time) {
        assert_monotonic_timestamps(&[ct, ut], false);
    }
    // Verify Prost binary wire roundtrip preserves storage representation.
    let encoded = actual.encode_to_vec();
    let decoded = Document::decode(encoded.as_slice()).expect("valid Document protobuf wire bytes");
    assert!(Value::fields_storage_eq(&decoded.fields, expected_fields));
}

/// Opt-in semantic document comparison using Firestore query `PartialEq` (`1 == 1.0`, `-0.0 ==
/// +0.0`).
pub(crate) fn assert_doc_semantic_eq(
    actual: &Document,
    expected_rel_or_full_name: &str,
    expected_fields: &HashMap<String, Value>,
) {
    let expected_name = doc_path(expected_rel_or_full_name);
    expect_eq!(actual.name, expected_name);
    expect_eq!(&actual.fields, expected_fields);
}

pub(crate) fn doc_names(docs: &[Document]) -> Vec<String> {
    docs.iter().map(|d| d.name.clone()).collect()
}

pub(crate) fn short_ids(docs: &[Document]) -> Vec<&str> {
    docs.iter()
        .map(|d| d.name.rsplit('/').next().unwrap_or(d.name.as_str()))
        .collect()
}
