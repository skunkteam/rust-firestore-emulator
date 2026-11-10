use std::{env, fs, path::PathBuf};

fn main() {
    tonic_prost_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .bytes("bytes_value")
        .type_attribute(".google.type.LatLng", "#[derive(serde::Serialize)]")
        .type_attribute(
            ".google.protobuf.Timestamp",
            "#[derive(PartialOrd, Ord, serde::Serialize)]",
        )
        .type_attribute(
            ".google.firestore.v1.Document",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".google.firestore.v1.ArrayValue",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".google.firestore.v1.MapValue",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".google.firestore.v1.Function",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(
            ".google.firestore.v1.Pipeline",
            "#[derive(serde::Serialize)]",
        )
        .type_attribute(".google.firestore.v1.Value", "#[derive(serde::Serialize)]")
        .message_attribute(".google.firestore.v1.Value", "#[serde(transparent)]")
        .enum_attribute(".google.firestore.v1.Value", "#[serde(untagged)]")
        .field_attribute(
            "bytes_value",
            r#"#[serde(serialize_with="crate::ser::as_base64")]"#,
        )
        .field_attribute(
            "value_type.null_value",
            r#"#[serde(serialize_with="crate::ser::as_null")]"#,
        )
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
        "#[serde(transparent)]\n#[derive(Clone, PartialEq, ::prost::Message)]\npub struct Value";
    const VALUE_STRUCT_REPLACEMENT: &str =
        "#[serde(transparent)]\n#[derive(Clone, ::prost::Message)]\npub struct Value";

    const VALUE_TYPE_ENUM_TARGET: &str = "    #[serde(untagged)]\n    #[derive(Clone, PartialEq, \
                                          ::prost::Oneof)]\n    pub enum ValueType";
    const VALUE_TYPE_ENUM_REPLACEMENT: &str =
        "    #[serde(untagged)]\n    #[derive(Clone, ::prost::Oneof)]\n    pub enum ValueType";

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
