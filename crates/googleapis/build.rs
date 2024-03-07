fn main() {
    tonic_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile_well_known_types(true)
        .message_attribute(
            "google.protobuf.Timestamp",
            "#[derive(Eq, PartialOrd, Ord)]",
        )
        .compile(
            &["include/google/firestore/v1/firestore.proto"],
            &["include"],
        )
        .unwrap();
}
