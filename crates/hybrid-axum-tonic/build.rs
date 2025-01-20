fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["./tests/test.proto"], &["./"])
        .unwrap();
}
