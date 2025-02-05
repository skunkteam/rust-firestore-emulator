{
  description = "Local, in-memory emulator for Firestore to use for testing.";

  inputs = {
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    naersk.url = "github:nix-community/naersk";
    googleapis = {
      url = "github:googleapis/googleapis";
      flake = false;
    };
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
    naersk,
    googleapis,
    ...
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {inherit system overlays;};
        rust-toolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [
            "rust-src"
            "rust-std"
            "rust-analyzer"
          ];
        };
        rust-builder = pkgs.callPackage naersk {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };
      in {
        devShells.default = pkgs.mkShell {
          name = "rust-shell";

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
        formatter = pkgs.nixfmt-rfc-style;
        defaultPackage = rust-builder.buildPackage {
          name = "firestore-emulator";
          version = "0.1.5";
          src = self;
          root = self;
          gitSubmodules = true;
          gitAllRefs = true;
          # singeStep = true;
          nativeBuildInputs = [pkgs.protobuf];
          compressTarget = false;
          PROTOC = "${pkgs.protobuf}/bin/protoc";
          PROTOC_INCLUDE = "${pkgs.protobuf}/include";
          patchPhase = ''
            sed -i 's:"include:"${googleapis}:g' crates/googleapis/build.rs
          '';
        };
      }
    );
}
