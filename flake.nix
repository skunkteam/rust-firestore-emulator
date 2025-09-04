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
      url = "github:nix-community/naersk"; # For building rust crates as Nix derivations.
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # `self` refers to the copy of this git repository in the Nix Store. By setting
    # `self.submodules` to `true`, we ensure the `googleapis` git submodule gets copied into the Nix
    # store at the expected path.
    self.submodules = true;
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      rust-overlay,
      naersk,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };
        rust-toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        # Initialize the naersk builder to use our specific toolchain:
        rust-builder = pkgs.callPackage naersk {
          cargo = rust-toolchain;
          rustc = rust-toolchain;
        };
        # Packages necessary for building the firestore-emulator crate.
        buildInputs = with pkgs; [
          openssl
          pkg-config
          protobuf
        ];
      in
      {
        # Define the development environment with the necessary dependencies and rust toolchain:
        devShells.default = pkgs.mkShell {
          # Made available in the shell's PATH.
          inherit buildInputs;
          name = "rust-shell";

          # Other project dependencies to be made avaible in PATH:
          packages = with pkgs; [
            # Nix language server and formatter.
            nixd
            nixfmt-rfc-style
            # Additional project dependencies.
            just
            cargo-tarpaulin
            cargo-nextest
            rustup
            # Misc.
            git
          ];
        };

        # Nix derivation for building the firestore-emulator using Naersk.
        #
        # `nix build` produces a symlink `./result` that points to the Nix store directory
        # containing the built firestore-emulator binary under `./result/bin/firestore-emulator`
        #
        # Locally, `nix run . -- ARGS` runs the firestore-emulator binary with the provided
        # arguments. For example: `nix run . -- --host-port 127.0.0.1:8080` to start the emulator on
        # localhost.
        #
        # You can even run the emulator without manually doing a local checkout of this repository
        # with `nix run github:skunkteam/rust-firestore-emulator -- ARGS`.
        defaultPackage = rust-builder.buildPackage {
          src = self;
          # Naersk can't deal with `version = { workspace = true }` for the root package, so extract
          # it manually:
          version = with builtins; (fromTOML (readFile ./Cargo.toml)).workspace.package.version;
          nativeBuildInputs = buildInputs;
        };
      }
    );
}
