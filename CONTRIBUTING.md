# Contributing to Pulse

Thanks for considering a contribution!

- Use Rust 2021, `cargo fmt` and `cargo clippy`.
- Async code should use `tokio` and `async-trait`.
- Avoid unnecessary dependencies.
- Small focused commits; descriptive messages.
- Unit tests where it makes sense.

## Style
- `rustfmt.toml` and `clippy.toml` in the repo define basic rules.
- Prefer clear names and documentation via doc comments `//!` and `///`.

## How to run
- Build: `cargo build`
- Test: `cargo test`
- Example: `cargo run -p pulse-examples -- examples/wordcount.jsonl`
