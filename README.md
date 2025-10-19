# Pulse

[![CI](https://github.com/brbtavares/pulse/actions/workflows/ci.yml/badge.svg)](https://github.com/brbtavares/pulse/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/brbtavares/pulse/branch/main/graph/badge.svg)](https://codecov.io/gh/brbtavares/pulse)

[![crates.io - pulse-core](https://img.shields.io/crates/v/pulse-core.svg)](https://crates.io/crates/pulse-core)
[![docs.rs - pulse-core](https://img.shields.io/docsrs/pulse-core)](https://docs.rs/pulse-core)

[![crates.io - pulse-state](https://img.shields.io/crates/v/pulse-state.svg)](https://crates.io/crates/pulse-state)
[![docs.rs - pulse-state](https://img.shields.io/docsrs/pulse-state)](https://docs.rs/pulse-state)

[![crates.io - pulse-ops](https://img.shields.io/crates/v/pulse-ops.svg)](https://crates.io/crates/pulse-ops)
[![docs.rs - pulse-ops](https://img.shields.io/docsrs/pulse-ops)](https://docs.rs/pulse-ops)

[![crates.io - pulse-io](https://img.shields.io/crates/v/pulse-io.svg)](https://crates.io/crates/pulse-io)
[![docs.rs - pulse-io](https://img.shields.io/docsrs/pulse-io)](https://docs.rs/pulse-io)


Pulse is a tiny, modular, event-time streaming framework (Flink/Beam-like) written in Rust. It focuses on clarity, testability, and a local-first workflow. It supports watermarks, windowing, pluggable state, Prometheus metrics, and a single-binary CLI that runs pipelines from a TOML file.

Goals:
- Local-first: zero external services required for the common path
- Stream processing with event-time and watermarks
- Configurable windowing (tumbling/sliding/session APIs, tumbling wired in CLI)
- Pluggable state backends (in-memory and optional RocksDB)
- File and Parquet I/O, plus optional Kafka
- First-class observability (tracing/Prometheus)

## Local-first vision

- Single binary you can run on your laptop or inside a container without standing up a cluster or control plane.
- Files-in/files-out by default; opt into Kafka when needed.
- Deterministic replay support (EOF watermark) so you can iterate quickly and write golden tests.
- Ergonomics first: small, readable codebase; simple config; sensible defaults.

## Workspace structure
- `pulse-core`: core types/traits, `Executor`, `Record`, event-time `Watermark`, timers, metrics, and config loader
- `pulse-ops`: operators (`Map`, `Filter`, `KeyBy`, `Aggregate`, `WindowedAggregate`), event-time window helpers
- `pulse-state`: state backends
  - `InMemoryState` (default)
  - `RocksDbState` (feature `rocksdb`)
- `pulse-io`: sources/sinks
  - `FileSource` (JSONL/CSV) with EOF watermark
  - `FileSink` (JSONL)
  - `ParquetSink` (feature `parquet`) with date partitioning and rotation
  - `KafkaSource`/`KafkaSink` (feature `kafka`)
- `pulse-examples`: runnable examples and sample data
- `pulse-bin`: CLI (`pulse`) and `/metrics` HTTP server

## Core features

### Event-time & watermarks
- `Record { event_time: chrono::DateTime<Utc>, value: serde_json::Value }`
- `Watermark(EventTime)` propagated through the pipeline
- Executor drives `on_watermark` for operators and informs sinks
- Lag metric (`pulse_watermark_lag_ms`) = now - watermark

Semantics overview:
- Event-time is derived from your data (RFC3339 string or epoch ms).
- Sources advance a low watermark to signal “no more records ≤ t expected”.
- Operators emit window results when `watermark >= window.end`.
- FileSource emits a final EOF watermark far in the future to flush all windows in batch-like runs.

### Windowing
- Operators: `WindowedAggregate` supports Tumbling/Sliding/Session (API)
- The CLI currently wires Tumbling windows for count-by-key
- EOF watermark emitted by `FileSource` flushes windows

### State & snapshots
- `KvState` trait: `get/put/delete/iter_prefix/snapshot/restore`
- `pulse-state::InMemoryState` implements full API (snapshots kept in-memory)
- `pulse-state::RocksDbState` (feature `rocksdb`): prefix iteration and checkpoint directory creation via RocksDB Checkpoint
- `WindowOperator` can persist per-window state via a backend and restore after restart (optional hook)

### Guarantees (MVP)

- Single-node at-least-once: each record is processed at least once; exactly-once is not guaranteed.
- FileSource is deterministic (EOF watermark). KafkaSource commits offsets periodically (configurable); on restart, offsets are available in KvState for recovery logic.
- Operator state updates and sink writes are not transactional as a unit (no two-phase commit in MVP).

### Limitations (MVP)

- No cluster/distributed runtime yet (single process, single binary).
- No SQL/DSL planner; define pipelines in Rust or with the minimal TOML config.
- CLI wires tumbling count-by-key today; other window kinds/aggregations available via the Rust API.
- Checkpoint/resume orchestration is minimal: offsets/snapshots exist, but full recovery wiring is a follow-up.
- Kafka is optional and depends on native `librdkafka`.

### I/O
- `FileSource` (JSONL/CSV): parses `event_time` from RFC3339 or epoch ms; final watermark at EOF
- `FileSink`: writes JSON lines to stdout/file
- `ParquetSink` (feature `parquet`):
  - Schema: `event_time: timestamp(ms)`, `payload: utf8` (full JSON)
  - Partitioning: `out_dir/dt=YYYY-MM-DD/part-*.parquet`
  - Rotation: by row-count and time
  - Tested: writes files then read back via Arrow reader, asserting row counts
- `KafkaSource`/`KafkaSink` (feature `kafka`): basic integration using `rdkafka`

### Observability
- Tracing spans on operators (receive/emit) using `tracing`
- Prometheus metrics (via `pulse-core::metrics`):
  - `pulse_operator_records_total{operator,stage=receive|emit}`
  - `pulse_watermark_lag_ms` (gauge)
  - `pulse_bytes_written_total{sink}`
  - `pulse_state_size{operator}`
- `/metrics` HTTP endpoint served by `pulse-bin` (axum 0.7)

## CLI: pulse

Binary crate: `pulse-bin`. Subcommands:

- `pulse serve --port 9898`
  - Serves `/metrics` in Prometheus format.

- `pulse run --config pipeline.toml [--http-port 9898]`
  - Loads a TOML config, validates it, builds the pipeline, and runs until EOF (or Ctrl-C if you wire a streaming source).
  - If `--http-port` is provided, starts `/metrics` on that port.

### Config format (`pulse-core::config`)

```toml
[source]
kind = "file"
path = "pulse-examples/examples/sliding_avg.jsonl"
time_field = "event_time"

[time]
allowed_lateness = "10s"

[window]
# supported in API: tumbling|sliding|session; CLI wires tumbling now
type = "tumbling"
size = "60s"

[ops]
# simple aggregation list; currently: count_by
count_by = "word"

[sink]
kind = "parquet"
out_dir = "outputs"
```

Validation rules:
- `source.kind` must be `file` (or `kafka`)
- `sink.kind` must be `parquet`/`file` (or `kafka`)
- `ops.count_by` must be present

### Example: run from config

```powershell
# Build
cargo build

# Run the pipeline and export metrics
cargo run -p pulse-bin -- run --config examples/pipeline.toml --http-port 9898

# Scrape metrics
curl http://127.0.0.1:9898/metrics
```

Expected output:
- Parquet files created under `outputs/dt=YYYY-MM-DD/part-*.parquet`.

### Example: CSV → Parquet

CLI currently expects JSONL for file inputs. CSV is supported at the library level. Two options:

1) Convert CSV to JSONL (quickest for CLI):
   - Ensure your CSV has `event_time` column as epoch ms.
   - Convert to JSONL (one JSON object per line) and use the file example above.

2) Use the Rust API to run CSV directly:

```rust
use pulse_core::Executor;
use pulse_io::{FileSource, ParquetSink, ParquetSinkConfig, PartitionSpec};
use pulse_ops::{KeyBy, WindowedAggregate};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // CSV source: header must include event_time (epoch ms)
  let src = FileSource { path: "input.csv".into(), format: pulse_io::FileFormat::Csv, event_time_field: "event_time".into(), text_field: None };

  let mut exec = Executor::new();
  exec.source(src)
    .operator(KeyBy::new("word"))
    .operator(WindowedAggregate::tumbling_count("key", 60_000))
    .sink(ParquetSink::new(ParquetSinkConfig {
      out_dir: "outputs".into(),
      partition_by: PartitionSpec::ByDate { field: "event_time".into(), fmt: "%Y-%m-%d".into() },
      max_rows: 1_000_000,
      max_age: std::time::Duration::from_secs(300),
    }));
  exec.run().await?;
  Ok(())
}
```

### Examples crate
- Data examples under `pulse-examples/examples/`
- Sample dataset: `sliding_avg.jsonl` (generated earlier) works with the file source
- You can also wire other examples in `pulse-examples` using the operators from `pulse-ops`

## Optional integrations

Enable per crate features:

```powershell
# Kafka
cargo build -p pulse-io --features kafka

# Arrow/Parquet
cargo build -p pulse-io --features parquet
```

Windows + Kafka notes: enabling `kafka` builds the native `librdkafka` by default and requires CMake and MSVC Build Tools.

- Install CMake and Visual Studio Build Tools (C++), then rerun the build
- Or link against a preinstalled librdkafka and remove `cmake-build` feature

## Why not Flink / Arroyo / Fluvio / Materialize?

Pulse takes a pragmatic, local-first approach for single-node pipelines. A comparison at a glance:

| System        | Install/runtime footprint     | Local-first UX | Event-time/windowing | SQL | Cluster | Primary niche |
|---------------|-------------------------------|----------------|----------------------|-----|---------|---------------|
| Flink         | Heavy (cluster/services)      | No (dev spins cluster) | Yes (rich)            | Yes | Yes     | Large-scale distributed streaming |
| Arroyo        | Moderate (service + workers)  | Partial        | Yes                  | Yes | Yes     | Cloud-native streaming w/ SQL     |
| Fluvio        | Broker + clients              | Partial        | Limited              | No  | Yes     | Distributed streaming data plane  |
| Materialize   | Service + storage             | Partial        | Incremental views    | Yes | Yes     | Streaming SQL materialized views  |
| Pulse         | Single binary                 | Yes            | Yes (MVP focused)    | No  | No      | Local-first pipelines & testing   |

If you need distributed scale, multi-tenant scheduling, or a SQL-first experience, those systems are a better fit. Pulse aims to be the simplest way to iterate on event-time pipelines locally and ship small, self-contained jobs.
