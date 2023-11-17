fn main() {
    tonic_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile(
            &["googleapis/google/firestore/v1/firestore.proto"],
            &["googleapis"],
        )
        .unwrap()
}
