# Pulse

A tiny real-time streaming framework (Flink/Beam-like) written in Rust.

Goals:
- High performance and backpressure-friendly (future)
- Modularity: `pulse-core`, `pulse-ops`, `pulse-state`, `pulse-io`
- Local-first and simple to operate

## Structure
- `pulse-core`: types, traits and basic executor
- `pulse-ops`: standard operators (Map, Filter, KeyBy, Window, Aggregate)
- `pulse-state`: state layer (in-memory, optional RocksDB)
- `pulse-io`: sources/sinks (FileSource, FileSink)
- `pulse-examples`: examples

## Quick DSL
```rust
use pulse_core::Executor;
use pulse_io::{FileSource, FileSink};
use pulse_ops::{Map, MapFn, KeyBy, Aggregate};

# async fn demo() -> anyhow::Result<()> {
let mut exec = Executor::new();
exec
  .source(FileSource::jsonl("examples/wordcount.jsonl", "event_time"))
  .operator(Map::new(MapFn::new(|v| vec![v])))
  .operator(KeyBy::new("my_key"))
  .operator(Aggregate::count_per_window("key", "my_field"))
  .sink(FileSink::stdout());
# Ok(())
# }
```

## Build and run
- Build: `cargo build`
- Run wordcount example:
```
cargo run -p pulse-examples -- examples/wordcount.jsonl
```

Expected input (JSONL):
```
{"event_time":"2024-01-01T00:00:00Z","text":"hello world hello"}
{"event_time":1704067200000,"text":"world"}
```

Output (one JSON per line):
```
{"window_start_ms":1704067200000,"key":"hello","count":1}
{"window_start_ms":1704067200000,"key":"world","count":1}
{"window_start_ms":1704067200000,"key":"hello","count":2}
```

License: MIT

## Optional integrations
Pulse supports optional integrations behind feature flags:

- Kafka (rdkafka) for streaming in/out topics
- Arrow / Parquet for columnar formats (planned minimal sinks/sources)

Enable them per-crate or from the workspace with Cargo features:

```
# Build pulse-io with Kafka support
cargo build -p pulse-io --features kafka

# Build pulse-io with Arrow/Parquet support (APIs are experimental)
cargo build -p pulse-io --features "arrow parquet"
```

Notes (Windows + Kafka): enabling `kafka` builds the native `librdkafka` by default and requires CMake and MSVC Build Tools.

- Install CMake and Visual Studio Build Tools (C++), then rerun the build.
- Alternatively, configure dynamic linking to a preinstalled librdkafka and remove the `cmake-build` feature in `pulse-io/Cargo.toml`.

Status:
- Kafka: basic `KafkaSource` and `KafkaSink` are available behind the `kafka` feature in `pulse-io`.
- Arrow/Parquet: feature flags are wired; concrete sinks/sources will land next.
