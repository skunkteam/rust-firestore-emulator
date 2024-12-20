fn main() {
    tonic_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .message_attribute(".google.type.LatLng", "#[derive(serde::Serialize, Copy)]")
        .message_attribute(
            ".google.protobuf.Timestamp",
            "#[derive(Eq, PartialOrd, Ord, serde::Serialize, Copy)]",
        )
        .message_attribute(
            ".google.firestore.v1.Document",
            "#[derive(serde::Serialize)]",
        )
        .message_attribute(".google.firestore.v1.Value", "#[derive(serde::Serialize)]")
        .message_attribute(".google.firestore.v1.Value", "#[serde(transparent)]")
        .enum_attribute(".google.firestore.v1.Value", "#[derive(serde::Serialize)]")
        .enum_attribute(".google.firestore.v1.Value", "#[serde(untagged)]")
        .message_attribute(
            ".google.firestore.v1.ArrayValue",
            "#[derive(serde::Serialize)]",
        )
        .message_attribute(
            ".google.firestore.v1.MapValue",
            "#[derive(serde::Serialize)]",
        )
        .compile(
            &["include/google/firestore/v1/firestore.proto"],
            &["include"],
        )
        .unwrap();
}
