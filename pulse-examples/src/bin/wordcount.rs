use anyhow::Result;
use pulse_core::Executor;
use pulse_io::{FileSink, FileSource};
use pulse_ops::{Aggregate, KeyBy, Map, MapFn};

#[tokio::main]
async fn main() -> Result<()> {
    // Expect a JSONL file with fields: event_time (RFC3339 or epoch ms), and text
    let arg = std::env::args().nth(1);
    let input = resolve_input_path(arg.as_deref());

    let mut exec = Executor::new();

    // Map: explode text into words
    let mapper = Map::new(MapFn::new(|v: serde_json::Value| {
        let text = v.get("text").and_then(|x| x.as_str()).unwrap_or("");
        text.split_whitespace()
            .map(|w| serde_json::json!({"word": w.to_lowercase()}))
            .collect::<Vec<_>>()
    }));

    // KeyBy: set key = word
    let key_by = KeyBy::new("word");

    // Aggregate: count per minute per key
    let agg = Aggregate::count_per_window("key", "word");

    exec.source(FileSource::jsonl(input, "event_time"))
        .operator(mapper)
        .operator(key_by)
        .operator(agg)
        .sink(FileSink::stdout());

    exec.run().await?;
    Ok(())
}

fn resolve_input_path(arg: Option<&str>) -> String {
    use std::path::PathBuf;
    // Default to crate-local examples/wordcount.jsonl
    let default = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("examples")
        .join("wordcount.jsonl");
    if let Some(p) = arg {
        let candidate = PathBuf::from(p);
        if candidate.exists() {
            return candidate.to_string_lossy().to_string();
        }
        let candidate2 = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(p);
        if candidate2.exists() {
            return candidate2.to_string_lossy().to_string();
        }
    }
    default.to_string_lossy().to_string()
}
