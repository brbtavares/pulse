//! Sliding average per device using a 5-minute window that slides every 1 minute.
//! Run:
//!   cargo run -p pulse-examples --bin sliding_avg -- pulse-examples/examples/sliding_avg.jsonl
//! Input JSONL fields: event_time (RFC3339 or epoch ms), device (string), value (number)

use anyhow::Result;
use pulse_core::Executor;
use pulse_io::{FileSink, FileSource};
use pulse_ops::WindowedAggregate;

#[tokio::main]
async fn main() -> Result<()> {
    let input = std::env::args().nth(1).unwrap_or_else(|| {
        let p = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("sliding_avg.jsonl");
        p.to_string_lossy().to_string()
    });

    let mut exec = Executor::new();

    // Sliding average: size=5 minutes, slide=1 minute over field `value` grouped by `device`.
    let op = WindowedAggregate::sliding_avg("device", 5 * 60_000, 60_000, "value");

    exec
        .source(FileSource::jsonl(input, "event_time"))
        .operator(op)
        .sink(FileSink::stdout());

    exec.run().await?;
    Ok(())
}
