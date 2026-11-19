# Usage:
# Most recipes can re-run automatically on file change by prepending "watch" to the command. For example: `just lint` runs linting once,
# `just watch lint` runs linting on file change.
#
# Available commands:
# just                          - Run all CI checks locally (fmt-check, lint, test, sdk-test)
# just watch                    - Watch and re-run all default checks on file change
# just [watch] test [crate]     - Run tests on workspace (including --no-default-features and doctests) or the specified crate (run `just test` without a crate to include cross-crate black-box suites in `emulator-tests`)
# just [watch] cov [crate]      - Generate line/region coverage summary via cargo-llvm-cov + nextest
# just mutants <crate> <file>   - Run file-scoped mutation testing in ./tmp/mutants using the `mutants` nextest profile
# just [watch] clippy [crate]   - Run Clippy on workspace or the specified crate
# just [watch] lint [crate]     - Run Clippy on workspace or the specified crate (includes `sdk-lint` and `ui-check` when crate is omitted or 'all')
# just [watch] sdk-lint         - Lint and type-check the TypeScript conformance test suite (ESLint, Prettier, tsc)
# just [watch] ui-check         - Type-check the Web UI JavaScript modules via JSDoc/TypeScript
# just [watch] doc [crate]      - Create crate docs for the workspace or the specified crate
# just [watch] fmt [crate]      - Format the code of the entire workspace or the specified crate
# just [watch] fmt-check [crate]- Check Rust formatting without modifying files
# just [watch] clean            - Clean the repo, removing all compiled and generated artifacts
# just [watch] run [args..]     - Run the application, passing the provided args to `cargo run`
# just [watch] build [args..]   - Build the application, passing the provided args to `cargo build`
# just [watch] sdk-test         - Build `--no-default-features` and run the JS SDK conformance suite
#
# All commands, except `run` and `build` can be combined sequentially, which is especially useful when using `watch`. For example:
# `just watch clean doc all test all` or `just watch test emulator-grpc lint emulator-grpc`
# Note that optional arguments must be provided (use `all` to target all crates) when combining sequentially.

set quiet

export FORCE_COLOR := "1"

# Default action runs all CI checks locally: formatting, linting/type-checking, Rust tests, and SDK conformance tests.
default: fmt-check lint test sdk-test

# Clean the repo, removing all compiled and generated artifacts
clean:
    cargo clean

# Test the given crate or the entire workspace if target is omitted
test target="all": && (test-no-default-features target) (doctest target)
    echo Running tests for target '"{{ target }}"'
    cargo nextest run {{ if target == "all" { "--workspace" } else { "--package " + target } }}

[private]
test-no-default-features target:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{ target }}" = "all" ]; then
        echo 'Running tests without default features'
        cargo nextest run --workspace --exclude emulator-tests --no-default-features
    elif [ "{{ target }}" = "firestore-emulator" ]; then
        echo 'Running tests without default features for firestore-emulator'
        cargo nextest run --package firestore-emulator --no-default-features
    fi

[private]
doctest target:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{ target }}" = "all" ]; then
        cargo test --doc --workspace --exclude googleapis
    elif [ "{{ target }}" = "googleapis" ]; then
        echo skipping doctests
    else
        cargo test --doc --package "{{ target }}"
    fi

# Create crate docs for the workspace or the specified crate
doc target="all":
    cargo doc --no-deps {{ if target == "all" { "--workspace" } else { "--package " + target } }}

# Format the code of the entire workspace or the specified crate
fmt target="all":
    cargo +nightly fmt {{ if target == "all" { "--all" } else { "--package " + target } }}

# Check Rust formatting without modifying files (matches CI Rustfmt job)
fmt-check target="all":
    echo Checking Rust formatting for target '"{{ target }}"'
    cargo +nightly fmt {{ if target == "all" { "--all" } else { "--package " + target } }} --check

# Test the given crate with coverage info or the entire workspace if target is omitted
cov target="all":
    mkdir -p ./tmp/cov
    TMPDIR="{{ justfile_directory() }}/tmp/cov" cargo llvm-cov nextest {{ if target == "all" { "--workspace" } else { "--package " + target } }} --ignore-filename-regex '(OUT_DIR|google\.firestore\.v1\.rs|ui_assets\.rs|crates[\\/]emulator-tests[\\/])'

# Run file-scoped mutation testing with scratch artifacts isolated in ./tmp/mutants
mutants crate file *args:
    mkdir -p ./tmp/mutants
    TMPDIR="{{ justfile_directory() }}/tmp/mutants" NEXTEST_PROFILE=mutants cargo mutants --package {{ crate }} --file {{ file }} --output ./tmp/mutants {{ args }}

# Run Clippy on the given crate or the entire workspace if target is omitted (matches CI Clippy job)
clippy target="all":
    echo Running clippy for target '"{{ target }}"'
    cargo clippy {{ if target == "all" { "--workspace" } else { "--package " + target } }} --all-targets --all-features -- -D warnings

# Lint the given crate or the entire workspace if target is omitted (includes test-suite and Web UI checks for 'all')
lint target="all": (clippy target) && (js-lint target)

[private]
js-lint target:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{ target }}" = "all" ]; then
        "{{ just_executable() }}" sdk-lint ui-check
    fi

# Ensure test-suite npm dependencies are installed and up to date with package.json and package-lock.json
[private]
[working-directory: "test-suite"]
sdk-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -f node_modules/.package-lock.json ] || [ package-lock.json -nt node_modules/.package-lock.json ] || [ package.json -nt node_modules/.package-lock.json ]; then
        echo 'Installing test-suite dependencies via npm ci'
        npm ci
    fi

# Lint and type-check the TypeScript conformance test suite (ESLint, Prettier, tsc)
[working-directory: "test-suite"]
sdk-lint: sdk-deps
    echo Running test-suite ESLint, Prettier, and TypeScript checks
    npm run lint
    ./node_modules/.bin/tsc -p tsconfig.json --noEmit

# Type-check the zero-build Web UI JavaScript modules using JSDoc and TypeScript
ui-check: sdk-deps
    echo Running TypeScript JSDoc check for Web UI
    ./test-suite/node_modules/.bin/tsc --noEmit --allowJs --checkJs --strict --target es2022 --module es2022 --lib es2022,dom,dom.iterable ui/app.js

# Run the application, simple alias for cargo run
run *cmd:
    cargo run {{ cmd }}

# Build the application, simple alias for cargo build
build *cmd:
    cargo build {{ cmd }}

# Test the application using the test suite that uses the JavaScript SDK.
[working-directory: "test-suite"]
sdk-test: (build "--no-default-features") sdk-deps
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p ../tmp/jest
    TMPDIR="{{ justfile_directory() }}/tmp/jest" npm run test:rust-emulator -- --maxConcurrency="${JEST_MAX_CONCURRENCY:-3}"

# Watch code and execute just command on change, e.g. `just watch test googleapis`
watch *cmd:
    cargo watch --ignore test-suite --ignore tmp --ignore lcov.info --clear --shell 'just {{ cmd }}'
