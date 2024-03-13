fn main() {
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile(&["./tests/test.proto"], &["./"])
        .unwrap();
}
