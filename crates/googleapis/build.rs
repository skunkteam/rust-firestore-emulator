fn main() {
    tonic_build::configure()
        .build_client(false)
        .include_file("googleapis.rs")
        .compile(
            &["include/google/firestore/v1/firestore.proto"],
            &["include"],
        )
        .unwrap()
}
