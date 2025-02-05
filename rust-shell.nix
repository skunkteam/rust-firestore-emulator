{
  pkgs,
  naersk,
  shellHook ? "",
  name ? "default",
}:
# Should imported from flake.nix with rust overlay pkgs
let
  rust-toolchain = pkgs.rust-bin.stable.latest.default.override {
    extensions = [
      "rust-src"
      "rust-std"
      "rust-analyzer"
    ];
  };
  builder = pkgs.callPackage naersk {
    cargo = rust-toolchain;
    rustc = rust-toolchain;
  };
in {
  inherit builder;

  shell = pkgs.mkShell {
    inherit name shellHook;

    RUST_BACKTRACE = 1;
    RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src";

    buildInputs = with pkgs; [
      openssl
      pkg-config
      rust-toolchain
    ];
    packages = with pkgs; [
      nixd
      nixfmt-rfc-style
      just
      cargo-tarpaulin
      cargo-nextest
      rust-toolchain
      protobuf
    ];
  };
}
