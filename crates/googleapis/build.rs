use std::{
    env, fs,
    path::{Path, PathBuf},
};

/// Strips `prost-build`'s derived `PartialEq` from `Value`, `value::ValueType`, and `LatLng`
/// so `crate::value_ext` can implement Firestore-semantic equality (`1 == 1.0`, `-0.0 == +0.0`,
/// `NaN == NaN`) consistent with `Ord` (`a == b` iff `a.cmp(b) == Equal`).
///
/// This string replacement is coupled to `prost-build`'s exact generated `#[derive(...)]`
/// attribute order and formatting; the `assert_eq!` fails the build immediately if a future
/// `prost-build` upgrade alters that layout (#234).
fn strip_partial_eq_derives(path: &Path, replacements: &[(&str, &str)]) {
    let mut contents = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()));
    for &(target, replacement) in replacements {
        assert_eq!(
            contents.matches(target).count(),
            1,
            "expected exactly one match for `{target}` in {}",
            path.display()
        );
        contents = contents.replace(target, replacement);
    }
    fs::write(path, contents).unwrap_or_else(|e| panic!("failed to write {}: {e}", path.display()));
}

fn main() {
    let serde_camel_default = "#[derive(serde::Serialize, \
                               serde::Deserialize)]\n#[serde(rename_all = \"camelCase\", default)]";
    let deserialize_null_default =
        "#[serde(deserialize_with = \"crate::ser::deserialize_null_default\")]";

    tonic_prost_build::configure()
        .build_client(cfg!(feature = "client"))
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .bytes("bytes_value")
        .type_attribute(".google.protobuf.Timestamp", "#[derive(PartialOrd, Ord)]")
        .type_attribute(".google.firestore.v1.ArrayValue", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.ArrayValue.values",
            deserialize_null_default,
        )
        .type_attribute(".google.firestore.v1.MapValue", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.MapValue.fields",
            "#[serde(serialize_with = \"crate::ser::serialize_sorted_map\", deserialize_with = \
             \"crate::ser::deserialize_null_default\")]",
        )
        .type_attribute(".google.firestore.v1.Document", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.Document.name",
            deserialize_null_default,
        )
        .field_attribute(
            ".google.firestore.v1.Document.fields",
            "#[serde(skip_serializing_if = \"std::collections::HashMap::is_empty\", \
             serialize_with = \"crate::ser::serialize_sorted_map\", deserialize_with = \
             \"crate::ser::deserialize_null_default\")]",
        )
        .field_attribute(
            ".google.firestore.v1.Document.create_time",
            "#[serde(skip_serializing_if = \"Option::is_none\")]",
        )
        .field_attribute(
            ".google.firestore.v1.Document.update_time",
            "#[serde(skip_serializing_if = \"Option::is_none\")]",
        )
        .type_attribute(".google.firestore.v1.DocumentMask", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.DocumentMask.field_paths",
            deserialize_null_default,
        )
        .type_attribute(
            ".google.firestore.v1.ListDocumentsResponse",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListDocumentsResponse.documents",
            deserialize_null_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListDocumentsResponse.next_page_token",
            "#[serde(skip_serializing_if = \"String::is_empty\", deserialize_with = \
             \"crate::ser::deserialize_null_default\")]",
        )
        .type_attribute(
            ".google.firestore.v1.ListCollectionIdsResponse",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListCollectionIdsResponse.collection_ids",
            deserialize_null_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListCollectionIdsResponse.next_page_token",
            "#[serde(skip_serializing_if = \"String::is_empty\", deserialize_with = \
             \"crate::ser::deserialize_null_default\")]",
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.CollectionSelector",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.StructuredQuery.CollectionSelector.collection_id",
            deserialize_null_default,
        )
        .field_attribute(
            ".google.firestore.v1.StructuredQuery.CollectionSelector.all_descendants",
            deserialize_null_default,
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.FieldReference",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.StructuredQuery.FieldReference.field_path",
            deserialize_null_default,
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.Projection",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.StructuredQuery.Projection.fields",
            deserialize_null_default,
        )
        .type_attribute(".google.firestore.v1.Cursor", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.Cursor.values",
            deserialize_null_default,
        )
        .field_attribute(
            ".google.firestore.v1.Cursor.before",
            deserialize_null_default,
        )
        .compile_protos(
            &["include/google/firestore/v1/firestore.proto"],
            &["include"],
        )
        .unwrap();

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR not set"));
    strip_partial_eq_derives(
        &out_dir.join("google.firestore.v1.rs"),
        &[
            (
                "#[derive(Clone, PartialEq, ::prost::Message)]\npub struct Value",
                "#[derive(Clone, ::prost::Message)]\npub struct Value",
            ),
            (
                "    #[derive(Clone, PartialEq, ::prost::Oneof)]\n    pub enum ValueType",
                "    #[derive(Clone, ::prost::Oneof)]\n    pub enum ValueType",
            ),
        ],
    );
    strip_partial_eq_derives(
        &out_dir.join("google.r#type.rs"),
        &[(
            "#[derive(Clone, Copy, PartialEq, ::prost::Message)]\npub struct LatLng",
            "#[derive(Clone, Copy, ::prost::Message)]\npub struct LatLng",
        )],
    );
}
