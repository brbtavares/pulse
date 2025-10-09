# Simple project tasks

build:
    cargo build

test:
    cargo test --all

run-example FILE="examples/wordcount.jsonl":
    cargo run -p pulse-examples -- {{FILE}}
