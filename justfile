# Simple project tasks

build:
    cargo build

test:
    cargo test --all

example FILE="examples/wordcount.jsonl":
    cargo run -p pulse-examples -- {{FILE}}

# Format the workspace
format:
    cargo fmt --all

# Lint with clippy and treat warnings as errors
clippy:
    cargo clippy --all-targets -- --deny warnings

# Generate documentation
docs:
    cargo doc --open --workspace --no-deps