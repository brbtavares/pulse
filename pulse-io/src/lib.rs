//! pulse-io: simple I/O sources and sinks.
//! - FileSource: reads JSONL or CSV
//! - FileSink: writes JSONL (stdout or file)

use async_trait::async_trait;
use pulse_core::{Context, Record, Result, Source, Sink, EventTime};
use tokio::io::AsyncBufReadExt;

#[derive(Clone)]
pub enum FileFormat { Jsonl, Csv }

pub struct FileSource { pub path: String, pub format: FileFormat, pub event_time_field: String, pub text_field: Option<String> }
impl FileSource {
    pub fn jsonl(path: impl Into<String>, event_time_field: impl Into<String>) -> Self {
        Self { path: path.into(), format: FileFormat::Jsonl, event_time_field: event_time_field.into(), text_field: None }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn run(&mut self, ctx: &mut dyn Context) -> Result<()> {
        match self.format {
            FileFormat::Jsonl => {
                let mut lines = tokio::io::BufReader::new(tokio::fs::File::open(&self.path).await?).lines();
                while let Some(line) = lines.next_line().await? {
                    let v: serde_json::Value = serde_json::from_str(&line)?;
                    let ts = v.get(&self.event_time_field).cloned().unwrap_or(serde_json::Value::Null);
                    let ts = match ts {
                        serde_json::Value::Number(n) => n.as_i64().unwrap_or(0) as i128 * 1_000_000, // assume ms, convert to ns
                        serde_json::Value::String(s) => {
                            // try parse RFC3339
                            time::OffsetDateTime::parse(&s, &time::format_description::well_known::Rfc3339)
                                .map(|t| t.unix_timestamp_nanos())
                                .unwrap_or(0)
                        }
                        _ => 0,
                    };
                    ctx.collect(Record { event_time: EventTime(ts), value: v });
                }
            }
            FileFormat::Csv => {
                let file = tokio::fs::read_to_string(&self.path).await?;
                let mut rdr = csv::ReaderBuilder::new().has_headers(true).from_reader(file.as_bytes());
                let headers = rdr.headers()?.clone();
                for row in rdr.records() {
                    let row = row?;
                    // build json object
                    let mut obj = serde_json::Map::new();
                    for (h, v) in headers.iter().zip(row.iter()) { obj.insert(h.to_string(), serde_json::json!(v)); }
                    let v = serde_json::Value::Object(obj);
                    let ts = v.get(&self.event_time_field).and_then(|x| x.as_str()).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0) as i128 * 1_000_000;
                    ctx.collect(Record { event_time: EventTime(ts), value: v });
                }
            }
        }
        Ok(())
    }
}

pub struct FileSink { pub path: Option<String> }
impl FileSink { pub fn stdout() -> Self { Self { path: None } } }

#[async_trait]
impl Sink for FileSink {
    async fn on_element(&mut self, record: Record) -> Result<()> {
        let line = serde_json::to_string(&record.value)?;
        if let Some(p) = &self.path {
            use tokio::io::AsyncWriteExt;
            let mut f = tokio::fs::OpenOptions::new().create(true).append(true).open(p).await?;
            f.write_all(line.as_bytes()).await?;
            f.write_all(b"\n").await?;
        } else { println!("{}", line); }
        Ok(())
    }
}
