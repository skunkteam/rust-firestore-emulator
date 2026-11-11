use std::{env, fs, path::PathBuf};

fn main() {
    let serde_camel_default = "#[derive(serde::Serialize, \
                               serde::Deserialize)]\n#[serde(rename_all = \"camelCase\", default)]";

    tonic_prost_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .bytes("bytes_value")
        .type_attribute(".google.protobuf.Timestamp", "#[derive(PartialOrd, Ord)]")
        .type_attribute(".google.type.LatLng", serde_camel_default)
        .type_attribute(".google.firestore.v1.ArrayValue", serde_camel_default)
        .type_attribute(".google.firestore.v1.MapValue", serde_camel_default)
        .type_attribute(".google.firestore.v1.Document", serde_camel_default)
        .field_attribute(
            ".google.firestore.v1.Document.fields",
            "#[serde(skip_serializing_if = \"std::collections::HashMap::is_empty\")]",
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
    let firestore_rs = out_dir.join("google.firestore.v1.rs");
    let contents =
        fs::read_to_string(&firestore_rs).expect("failed to read google.firestore.v1.rs");

    const VALUE_STRUCT_TARGET: &str =
        "#[derive(Clone, PartialEq, ::prost::Message)]\npub struct Value";
    const VALUE_STRUCT_REPLACEMENT: &str = "#[derive(Clone, ::prost::Message)]\npub struct Value";

    const VALUE_TYPE_ENUM_TARGET: &str =
        "    #[derive(Clone, PartialEq, ::prost::Oneof)]\n    pub enum ValueType";
    const VALUE_TYPE_ENUM_REPLACEMENT: &str =
        "    #[derive(Clone, ::prost::Oneof)]\n    pub enum ValueType";

    assert_eq!(
        contents.matches(VALUE_STRUCT_TARGET).count(),
        1,
        "expected exactly one match for `pub struct Value` derive in google.firestore.v1.rs"
    );
    assert_eq!(
        contents.matches(VALUE_TYPE_ENUM_TARGET).count(),
        1,
        "expected exactly one match for `pub enum ValueType` derive in google.firestore.v1.rs"
    );

    let updated = contents
        .replace(VALUE_STRUCT_TARGET, VALUE_STRUCT_REPLACEMENT)
        .replace(VALUE_TYPE_ENUM_TARGET, VALUE_TYPE_ENUM_REPLACEMENT);
    fs::write(&firestore_rs, updated).expect("failed to write google.firestore.v1.rs");
}
