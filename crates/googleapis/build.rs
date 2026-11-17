use std::{
    env, fs,
    path::{Path, PathBuf},
};

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

    tonic_prost_build::configure()
        .build_client(cfg!(feature = "client"))
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .bytes("bytes_value")
        .type_attribute(".google.protobuf.Timestamp", "#[derive(PartialOrd, Ord)]")
        .type_attribute(".google.firestore.v1.ArrayValue", serde_camel_default)
        .type_attribute(".google.firestore.v1.MapValue", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.MapValue.fields",
            "#[serde(serialize_with = \"crate::ser::serialize_sorted_map\")]",
        )
        .type_attribute(".google.firestore.v1.Document", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.Document.fields",
            "#[serde(skip_serializing_if = \"std::collections::HashMap::is_empty\", \
             serialize_with = \"crate::ser::serialize_sorted_map\")]",
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
        .type_attribute(
            ".google.firestore.v1.ListDocumentsResponse",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListDocumentsResponse.next_page_token",
            "#[serde(skip_serializing_if = \"String::is_empty\")]",
        )
        .type_attribute(
            ".google.firestore.v1.ListCollectionIdsResponse",
            serde_camel_default,
        )
        .field_attribute(
            ".google.firestore.v1.ListCollectionIdsResponse.next_page_token",
            "#[serde(skip_serializing_if = \"String::is_empty\")]",
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.CollectionSelector",
            serde_camel_default,
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.FieldReference",
            serde_camel_default,
        )
        .type_attribute(
            ".google.firestore.v1.StructuredQuery.Projection",
            serde_camel_default,
        )
        .type_attribute(".google.firestore.v1.Cursor", serde_camel_default)
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
