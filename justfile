set quiet := true

[private]
default:
    just --choose --justfile {{ justfile() }}

alias t:= test

test:
    cargo nextest run
    cargo test --doc
