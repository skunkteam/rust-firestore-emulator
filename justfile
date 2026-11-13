# Usage:
# Most recipes can re-run automatically on file change by prepending "watch" to the command. For example: `just lint` runs linting once,
# `just watch lint` runs linting on file change.
#
# Available commands:
# just                          - Run test and linting once
# just watch                    - Watch test and lint, re-run on filechange
# just [watch] test [crate]     - Run tests on workspace or the specified crate (run `just test` without a crate to include cross-crate black-box suites in `emulator-tests`)
# just [watch] cov [crate]      - Generate line/region coverage summary via cargo-llvm-cov + nextest
# just mutants <crate> <file>   - Run file-scoped mutation testing in ./tmp/mutants using the `mutants` nextest profile
# just miri                     - Run Miri unsafe aliasing checks on `fifo-rwlock`
# just [watch] lint [crate]     - Run linting on workspace or the specified crate (includes Web UI check when crate is omitted or 'all')
# just [watch] ui-check         - Type-check the Web UI JavaScript modules via JSDoc/TypeScript
# just [watch] doc [crate]      - Create crate docs for the workspace or the specified crate
# just [watch] fmt [crate]      - Format the code of the entire workspace or the specified crate
# just [watch] clean            - Clean the repo, removing all compiled and generated artifacts
# just [watch] run [args..]     - Run the application, passing the provided args to `cargo run`
# just [watch] build [args..]   - Build the application, passing the provided args to `cargo build`
# just [watch] sdk-test       - Test the application using the test suite that uses the JavaScript SDK.
#
# All commands, except `run` and `build` can be combined sequentially, which is especially useful when using `watch`. For example:
# `just watch clean doc all test all` or `just watch test emulator-grpc lint emulator-grpc`
# Note that optional arguments must be provided (use `all` to target all crates) when combining sequentially.

set quiet

export FORCE_COLOR := "1"

# Default action is to test and lint once.
default: test lint sdk-test

# Clean the repo, removing all compiled and generated artifacts
clean:
    cargo clean

# Test the given crate or the entire workspace if target is omitted
test target="all": && (doctest target)
    echo Running tests for target '"{{ target }}"'
    cargo nextest run {{ if target == "all" { "--workspace" } else { "--package " + target } }}

[private]
doctest target: (
    exec if target == "all" {
        "cargo test --doc --workspace --exclude googleapis"
    } else if target == "googleapis" {
        "echo skipping doctests"
    } else {
        "cargo test --doc --package " + target
    }
)

# Trick to be able to use `if` statement for conditional execution above
[private]
exec +cmd:
    {{ cmd }}

# Create crate docs for the workspace or the specified crate
doc target="all":
    cargo doc --no-deps {{ if target == "all" { "--workspace" } else { "--package " + target } }}

# Format the code of the entire workspace or the specified crate
fmt target="all":
    cargo +nightly fmt {{ if target == "all" { "--all" } else { "--package " + target } }}

# Test the given crate with coverage info or the entire workspace if target is omitted
cov target="all":
    mkdir -p ./tmp/cov
    TMPDIR="{{ justfile_directory() }}/tmp/cov" cargo llvm-cov nextest {{ if target == "all" { "--workspace" } else { "--package " + target } }} --ignore-filename-regex '(OUT_DIR|google\.firestore\.v1\.rs|ui_assets\.rs|crates[\\/]emulator-tests[\\/])'

# Run file-scoped mutation testing with scratch artifacts isolated in ./tmp/mutants
mutants crate file *args:
    mkdir -p ./tmp/mutants
    TMPDIR="{{ justfile_directory() }}/tmp/mutants" NEXTEST_PROFILE=mutants cargo mutants --package {{ crate }} --file {{ file }} --output ./tmp/mutants {{ args }}

# Check unsafe aliasing invariants in fifo-rwlock under Miri
miri:
    mkdir -p ./tmp/miri/sysroot-build/.cargo
    printf '[source.crates-io]\nreplace-with = "vendored-sources"\n[source.vendored-sources]\ndirectory = "%s/lib/rustlib/src/rust/library/vendor"\n' "$(rustc +nightly --print sysroot)" > ./tmp/miri/sysroot-build/.cargo/config.toml
    cd ./tmp/miri/sysroot-build && TMPDIR="{{ justfile_directory() }}/tmp/miri/sysroot-build" MIRI_SYSROOT="${MIRI_SYSROOT:-{{ justfile_directory() }}/tmp/miri/sysroot}" cargo +nightly miri setup
    TMPDIR="{{ justfile_directory() }}/tmp/miri" MIRI_SYSROOT="${MIRI_SYSROOT:-{{ justfile_directory() }}/tmp/miri/sysroot}" cargo +nightly miri test --package fifo-rwlock

# Lint the given crate or the entire workspace if target is omitted
lint target="all": && (ui-lint target)
    echo Running lint for target '"{{ target }}"'
    cargo clippy {{ if target == "all" { "--workspace" } else { "--package " + target } }}

[private]
ui-lint target: (
    exec if target == "all" {
        "just ui-check"
    } else {
        "true"
    }
)

# Type-check the zero-build Web UI JavaScript modules using JSDoc and TypeScript
ui-check:
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
sdk-test: (build "--no-default-features")
    mkdir -p ../tmp/jest
    TMPDIR="{{ justfile_directory() }}/tmp/jest" npm run test:rust-emulator

# Watch code and execute just command on change, e.g. `just watch test googleapis`
watch *cmd:
    cargo watch --ignore test-suite --ignore tmp --ignore lcov.info --clear --shell 'just {{ cmd }}'
