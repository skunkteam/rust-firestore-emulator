set quiet

# Default action is to test and lint once.
default: test lint

# Clean the repo, removing all compiled and generated artifacts
clean:
    cargo clean

test target="workspace": && (_doctest target)
    echo Running tests for target '"{{ target }}"'
    cargo nextest run {{ if target == "workspace" { "--workspace" } else { "--package " + target } }}

_doctest target: (
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

cov target="workspace":
    cargo tarpaulin --out lcov --out html {{ if target == "workspace" { "--workspace" } else { "--packages " + target } }}

lint target="workspace":
    echo Running lint for target '"{{ target }}"'
    cargo clippy {{ if target == "workspace" { "--workspace" } else { "--package " + target } }}

# Watch code and execute just command on change
watch +cmd="lint":
    cargo watch --ignore '*.{info,html,profraw}' --clear --shell 'just {{ cmd }}'
