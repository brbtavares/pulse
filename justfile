# Simple project tasks

build:
    cargo build

test:
    cargo test --all

# Run an example binary with default input dataset matching the bin name
# Usage:
#   just run wordcount
#   just run sliding_avg
run bin:
    cargo run -p pulse-examples --bin {{bin}} -- pulse-examples/examples/{{bin}}.jsonl

# Run an example binary with a custom input file
# Usage:
#   just run sliding_avg pulse-examples/examples/sliding_avg.jsonl
#   just run wordcount C:/data/my_words.jsonl
run-with-input bin input:
    cargo run -p pulse-examples --bin {{bin}} -- {{input}}

# Run an example and save output to a file using the default dataset
# Usage:
#   just run-to-file sliding_avg out.jsonl
run-to-file bin out:
    cargo run -p pulse-examples --bin {{bin}} -- pulse-examples/examples/{{bin}}.jsonl | Out-File -Encoding utf8 {{out}}

# Run an example with a custom input file and save output to a file
# Usage:
#   just run-to-file-with-input sliding_avg pulse-examples/examples/sliding_avg.jsonl out.jsonl
run-to-file-with-input bin input out:
    cargo run -p pulse-examples --bin {{bin}} -- {{input}} | Out-File -Encoding utf8 {{out}}

# Ensure outputs directory exists (PowerShell)
ensure-outputs:
    pwsh.exe -NoLogo -NoProfile -Command "New-Item -ItemType Directory -Force outputs | Out-Null"

# Run an example with default dataset and write to outputs/{{bin}}.jsonl
# Usage:
#   just run-to-outputs sliding_avg
run-to-outputs bin: ensure-outputs
    pwsh.exe -NoLogo -NoProfile -Command "cargo run -p pulse-examples --bin {{bin}} -- pulse-examples/examples/{{bin}}.jsonl | Out-File -Encoding utf8 outputs/{{bin}}.jsonl"

# Run an example with a custom input file and write to outputs/{{bin}}.jsonl
# Usage:
#   just run-to-outputs-with-input sliding_avg pulse-examples/examples/sliding_avg.jsonl
run-to-outputs-with-input bin input: ensure-outputs
    pwsh.exe -NoLogo -NoProfile -Command "cargo run -p pulse-examples --bin {{bin}} -- {{input}} | Out-File -Encoding utf8 outputs/{{bin}}.jsonl"

# Format the workspace
format:
    cargo fmt --all

# Lint with clippy and treat warnings as errors
clippy:
    cargo clippy --all-targets -- --deny warnings

# Generate documentation
docs:
    cargo doc --open --workspace --no-deps