# Usage:
# Most recipes can re-run automatically on file change by prepending "watch" to the command. For example: `just lint` runs linting once, `just watch lint` runs linting on file change.
# Some commands:
# just                          - Run test and linting once
# just watch                    - Watch test and lint, re-run on filechange
# just [watch] test [crate]     - Run tests on workspace or the specified crate, e.g. `just test` or `just watch test emulator-tracing`
# just [watch] lint [crate]     - Run linting on workspace or the specified crate
# just [watch] clean            - Clean the repo, removing all compiled and generated artifacts
# just [watch] run [args..]     - Run the application, passing the provided args to `cargo run`
# just [watch] build [args..]   - Build the application, passing the provided args to `cargo build`

set quiet

# Default action is to test and lint once.
default: test lint

# Clean the repo, removing all compiled and generated artifacts
clean:
    cargo clean

# Test the given crate or the entire workspace if target is omitted
test target="workspace": && (doctest target)
    echo Running tests for target '"{{ target }}"'
    cargo nextest run {{ if target == "workspace" { "--workspace" } else { "--package " + target } }}

[private]
doctest target: (
    exec if target == "workspace" {
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

# Test the given crate with coverage info or the entire workspace if target is omitted
cov target="workspace":
    cargo tarpaulin --out lcov --out html {{ if target == "workspace" { "--workspace" } else { "--packages " + target } }}

# Lint the given crate or the entire workspace if target is omitted
lint target="workspace":
    echo Running lint for target '"{{ target }}"'
    cargo clippy {{ if target == "workspace" { "--workspace" } else { "--package " + target } }}

# Run the application, simple alias for cargo run
run *cmd:
    cargo run {{ cmd }}

# Build the application, simple alias for cargo build
build *cmd:
    cargo build {{ cmd }}

# Watch code and execute just command on change, e.g. `just watch test googleapis`
watch *cmd:
    cargo watch --ignore '/*.{info,profraw}' --ignore tarpaulin-report.html --ignore test-suite --clear --shell 'just {{ cmd }}'
