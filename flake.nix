{
  description = "Google Cloud Firestore emulator with focus on stability";

  inputs = {
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    naersk = {
      url = "github:nix-community/naersk"; # For building rust crates as nix derivations.
      inputs.nixpkgs.follows = "nixpkgs";
    };
    googleapis = {
      # The git submodule as flake input. Necessary as workaround to build the emulator, because
      # for some reason the `crates/googleapis/include` submodule folder doesn't get copied into
      # the nix store when building.
      # Use `just update-submodule` to simultaneously update the git submodule and this commit hash.
      url = "github:googleapis/googleapis/3776db131e34e42ec8d287203020cb4282166aa5";
      flake = false;
    };
  };

  outputs = {
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
          # Additional rustup components to include in this toolchain:
          extensions = [
            "rust-src"
            "rust-std"
            "rust-analyzer"
          ];
        };
        # Initialize the naersk builder to use our specific toolchain:
        rust-builder = pkgs.callPackage naersk {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };
      in {
        # Define the development environment with the necessary dependencies and rust toolchain:
        devShells.default = pkgs.mkShell {
          name = "rust-shell";

          # These will be set as environment variables in the shell:
          RUST_BACKTRACE = 1;
          RUST_SRC_PATH = "${rust-toolchain}/lib/rustlib/src";

          # Packages necessary for building the firestore-emulator crate. Made available in the
          # shell's PATH.
          buildInputs = with pkgs; [
            openssl
            pkg-config
            rust-toolchain
            protobuf
          ];
          # Other project dependencies to be made avaible in PATH:
          packages = with pkgs; [
            # Nix language server and formatter.
            nixd
            nixfmt-rfc-style
            # Additional project dependencies.
            just
            cargo-tarpaulin
            cargo-nextest
          ];
        };

        # Nix derivation for building the firestore-emulator using naersk.
        #
        # `nix build` produces a symlink `./result` that points to the nix store directory containing
        # the built firestore-emulator binary under `./result/bin/firestore-emulator`
        #
        # `nix run . -- ARGS` runs the firestore-emulator binary with the provided arguments.
        # For example: `nix run . -- --host-port 127.0.0.1:8080` to start the emulator on localhost.
        defaultPackage = rust-builder.buildPackage {
          src = ./.;
          # naersk can't deal with `version = { workspace = true}` for the root package, so extract it
          # manually:
          version = with builtins; (fromTOML (readFile ./Cargo.toml)).workspace.package.version;
          nativeBuildInputs = with pkgs; [protobuf openssl pkg-config rust-toolchain];
          # Workaround - build.rs refers to git submodule path `./crates/googleapis/include`, but this
          # doesn't get copied over to the nix store for some reason. This patch replaces the reference
          # to local directory `include` by the absolute path to the nix store in the build.rs source
          # code before building.
          patchPhase = ''
            runHook prePatch
            sed -i 's:"include:"${googleapis}:g' crates/googleapis/build.rs
            runHook postPatch
          '';
        };
      }
    );
}
