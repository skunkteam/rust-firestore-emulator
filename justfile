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
