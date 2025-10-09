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
