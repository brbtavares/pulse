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

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{Context, EventTime, KvState, Record, Result, Timers, Watermark};
    use std::sync::Arc;

    struct TestState;
    #[async_trait]
    impl KvState for TestState {
        async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> { Ok(None) }
        async fn put(&self, _key: &[u8], _value: Vec<u8>) -> Result<()> { Ok(()) }
        async fn delete(&self, _key: &[u8]) -> Result<()> { Ok(()) }
    }

    struct TestTimers;
    #[async_trait]
    impl Timers for TestTimers {
        async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> { Ok(()) }
    }

    struct TestCtx {
        pub out: Vec<Record>,
        kv: Arc<dyn KvState>,
        timers: Arc<dyn Timers>,
    }

    #[async_trait]
    impl Context for TestCtx {
        fn collect(&mut self, record: Record) { self.out.push(record); }
        fn watermark(&mut self, _wm: Watermark) {}
        fn kv(&self) -> Arc<dyn KvState> { self.kv.clone() }
        fn timers(&self) -> Arc<dyn Timers> { self.timers.clone() }
    }

    fn tmp_file(name: &str) -> String {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
        p.push(format!("pulse_test_{}_{}.tmp", name, nanos));
        p.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn file_source_jsonl_reads_lines() {
        let path = tmp_file("jsonl");
        let content = "{\"event_time\":1704067200000,\"text\":\"hello\"}\n{\"event_time\":\"2024-01-01T00:00:00Z\",\"text\":\"world\"}\n";
        tokio::fs::write(&path, content).await.unwrap();

        let mut src = FileSource::jsonl(&path, "event_time");
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        src.run(&mut ctx).await.unwrap();
        assert_eq!(ctx.out.len(), 2);
        assert_eq!(ctx.out[0].value["text"], serde_json::json!("hello"));
        assert_eq!(ctx.out[1].value["text"], serde_json::json!("world"));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_source_csv_reads_rows() {
        let path = tmp_file("csv");
        let csv_data = "event_time,text\n1704067200000,hello\n1704067260000,world\n";
        tokio::fs::write(&path, csv_data).await.unwrap();

        let mut src = FileSource { path: path.clone(), format: FileFormat::Csv, event_time_field: "event_time".into(), text_field: None };
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        src.run(&mut ctx).await.unwrap();
        assert_eq!(ctx.out.len(), 2);
        assert_eq!(ctx.out[0].value["text"], serde_json::json!("hello"));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_appends_to_file() {
        let path = tmp_file("sink");
        let mut sink = FileSink { path: Some(path.clone()) };
        sink.on_element(Record { event_time: EventTime::now(), value: serde_json::json!({"a":1}) }).await.unwrap();
        sink.on_element(Record { event_time: EventTime::now(), value: serde_json::json!({"b":2}) }).await.unwrap();

        let data = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = data.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"a\":1"));
        assert!(lines[1].contains("\"b\":2"));

        let _ = tokio::fs::remove_file(&path).await;
    }
}
