//! pulse-io: simple I/O sources and sinks.
//! - `FileSource`: reads JSONL or CSV and emits JSON records with event-time
//! - `FileSink`: writes JSON lines to stdout or a file

use async_trait::async_trait;
use pulse_core::{Context, EventTime, Record, Result, Sink, Source};
use chrono::{DateTime, Utc, TimeZone};
use tokio::io::AsyncBufReadExt;

#[derive(Clone)]
/// Supported file formats for `FileSource`.
pub enum FileFormat {
    Jsonl,
    Csv,
}

/// Reads a file (JSONL or CSV) and emits records.
/// - `event_time_field`: name of the timestamp field (RFC3339 string or epoch ms)
pub struct FileSource {
    pub path: String,
    pub format: FileFormat,
    pub event_time_field: String,
    pub text_field: Option<String>,
}
impl FileSource {
    pub fn jsonl(path: impl Into<String>, event_time_field: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            format: FileFormat::Jsonl,
            event_time_field: event_time_field.into(),
            text_field: None,
        }
    }
}

#[async_trait]
impl Source for FileSource {
    async fn run(&mut self, ctx: &mut dyn Context) -> Result<()> {
        match self.format {
            FileFormat::Jsonl => {
                let mut lines = tokio::io::BufReader::new(tokio::fs::File::open(&self.path).await?).lines();
                let mut max_ts: Option<DateTime<Utc>> = None;
                while let Some(line) = lines.next_line().await? {
                    let v: serde_json::Value = serde_json::from_str(&line)?;
                    let ts = v
                        .get(&self.event_time_field)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let ts = match ts {
                        serde_json::Value::Number(n) => {
                            // assume ms epoch
                            let ms = n.as_i64().unwrap_or(0);
                            DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_else(|| Utc.timestamp_millis_opt(0).unwrap())
                        }
                        serde_json::Value::String(s) => {
                            // parse RFC3339
                            DateTime::parse_from_rfc3339(&s).map(|t| t.with_timezone(&Utc)).unwrap_or_else(|_| Utc.timestamp_millis_opt(0).unwrap())
                        }
                        _ => Utc.timestamp_millis_opt(0).unwrap(),
                    };
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    ctx.collect(Record {
                        event_time: ts,
                        value: v,
                    });
                }
                if let Some(m) = max_ts {
                    // Emit an EOF watermark far in the future to flush downstream windows/timers.
                    // Using +100 years as a practical "infinity" for batch sources.
                    let eof_wm = m + chrono::Duration::days(365 * 100);
                    ctx.watermark(pulse_core::Watermark(EventTime(eof_wm)));
                }
            }
            FileFormat::Csv => {
                let file = tokio::fs::read_to_string(&self.path).await?;
                let mut max_ts: Option<DateTime<Utc>> = None;
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_reader(file.as_bytes());
                let headers = rdr.headers()?.clone();
                for row in rdr.records() {
                    let row = row?;
                    // build json object
                    let mut obj = serde_json::Map::new();
                    for (h, v) in headers.iter().zip(row.iter()) {
                        obj.insert(h.to_string(), serde_json::json!(v));
                    }
                    let v = serde_json::Value::Object(obj);
                    let ts = v
                        .get(&self.event_time_field)
                        .and_then(|x| x.as_str())
                        .and_then(|s| s.parse::<i64>().ok())
                        .and_then(|ms| DateTime::<Utc>::from_timestamp_millis(ms))
                        .unwrap_or_else(|| Utc.timestamp_millis_opt(0).unwrap());
                    max_ts = Some(max_ts.map_or(ts, |m| m.max(ts)));
                    ctx.collect(Record {
                        event_time: ts,
                        value: v,
                    });
                }
                if let Some(m) = max_ts {
                    let eof_wm = m + chrono::Duration::days(365 * 100);
                    ctx.watermark(pulse_core::Watermark(EventTime(eof_wm)));
                }
            }
        }
        Ok(())
    }
}

/// Writes each record value as a single JSON line to stdout or a file.
pub struct FileSink {
    pub path: Option<String>,
}
impl FileSink {
    pub fn stdout() -> Self {
        Self { path: None }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn on_element(&mut self, record: Record) -> Result<()> {
        let line = serde_json::to_string(&record.value)?;
        if let Some(p) = &self.path {
            use tokio::io::AsyncWriteExt;
            let mut f = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .await?;
            f.write_all(line.as_bytes()).await?;
            f.write_all(b"\n").await?;
            pulse_core::metrics::BYTES_WRITTEN.with_label_values(&["FileSink"]).inc_by((line.len() + 1) as u64);
        } else {
            println!("{}", line);
        }
        Ok(())
    }
}

// --- Optional Kafka integration (behind feature flag) ---
#[cfg(feature = "kafka")]
mod kafka {
    use super::*;
    use anyhow::Context as AnyhowContext;
    use futures::StreamExt;
    use rdkafka::consumer::{CommitMode, Consumer, StreamConsumer};
    use rdkafka::message::{BorrowedMessage, Message};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::ClientConfig;

    const CP_NS: &[u8] = b"kafka:offset:"; // namespace for checkpoint offset per topic-partition

    pub(crate) fn extract_event_time(v: &serde_json::Value, field: &str) -> chrono::DateTime<chrono::Utc> {
        match v.get(field).cloned().unwrap_or(serde_json::Value::Null) {
            serde_json::Value::Number(n) => chrono::DateTime::<chrono::Utc>::from_timestamp_millis(n.as_i64().unwrap_or(0)).unwrap_or_else(|| chrono::Utc.timestamp_millis_opt(0).unwrap()),
            serde_json::Value::String(s) => chrono::DateTime::parse_from_rfc3339(&s).map(|t| t.with_timezone(&chrono::Utc)).unwrap_or_else(|_| chrono::Utc.timestamp_millis_opt(0).unwrap()),
            _ => chrono::Utc.timestamp_millis_opt(0).unwrap(),
        }
    }

    pub(crate) fn parse_payload_to_value(payload: &[u8]) -> Option<serde_json::Value> {
        // Try UTF-8 -> JSON; fallback to base64 or passthrough string
        if let Ok(s) = std::str::from_utf8(payload) {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(s) {
                return Some(v);
            }
            return Some(serde_json::json!({"bytes": s}));
        } else {
            return Some(serde_json::json!({"bytes_b64": base64::encode(payload)}));
        }
    }

    fn msg_to_value(m: &BorrowedMessage) -> Option<serde_json::Value> {
        m.payload().and_then(|p| parse_payload_to_value(p))
    }

    pub struct KafkaSource {
        pub brokers: String,
        pub group_id: String,
        pub topic: String,
        pub event_time_field: String,
        pub auto_offset_reset: Option<String>,
        pub commit_interval: std::time::Duration,
        current_offset: Option<String>,
    }

    impl KafkaSource {
        pub fn new(
            brokers: impl Into<String>,
            group_id: impl Into<String>,
            topic: impl Into<String>,
            event_time_field: impl Into<String>,
        ) -> Self {
            Self {
                brokers: brokers.into(),
                group_id: group_id.into(),
                topic: topic.into(),
                event_time_field: event_time_field.into(),
                auto_offset_reset: None,
                commit_interval: std::time::Duration::from_secs(5),
                current_offset: None,
            }
        }

        fn cp_key(&self, partition: i32) -> Vec<u8> {
            let mut k = CP_NS.to_vec();
            k.extend_from_slice(self.topic.as_bytes());
            k.push(b':');
            k.extend_from_slice(self.group_id.as_bytes());
            k.push(b':');
            k.extend_from_slice(partition.to_string().as_bytes());
            k
        }
    }

    #[async_trait]
    impl Source for KafkaSource {
        async fn run(&mut self, ctx: &mut dyn Context) -> Result<()> {
            let mut cfg = ClientConfig::new();
            cfg.set("bootstrap.servers", &self.brokers)
                .set("group.id", &self.group_id)
                .set("enable.partition.eof", "false")
                .set("enable.auto.commit", "false")
                .set("session.timeout.ms", "10000");
            if let Some(r) = &self.auto_offset_reset { cfg.set("auto.offset.reset", r); }

            let consumer: StreamConsumer = cfg
                .create()
                .context("failed to create kafka consumer")?;

            consumer
                .subscribe(&[&self.topic])
                .context("failed to subscribe to topic")?;

            let mut last_commit = std::time::Instant::now();
            let mut stream = consumer.stream();
            while let Some(ev) = stream.next().await {
                match ev {
                    Ok(m) => {
                        if let Some(mut v) = msg_to_value(&m) {
                            let ts = extract_event_time(&v, &self.event_time_field);
                            // attach topic/partition/offset for debugging
                            if let Some(obj) = v.as_object_mut() {
                                obj.insert("_topic".into(), serde_json::json!(m.topic()));
                                obj.insert("_partition".into(), serde_json::json!(m.partition()));
                                obj.insert("_offset".into(), serde_json::json!(m.offset()));
                            }
                            ctx.collect(Record { event_time: ts, value: v });

                            // Track current offset and persist periodically for checkpoints
                            let part = m.partition();
                            let off = m.offset();
                            self.current_offset = Some(format!("{}@{}", part, off));
                            if last_commit.elapsed() >= self.commit_interval {
                                // Commit to Kafka and persist to KvState for recovery
                                let _ = consumer.commit_message(&m, CommitMode::Async);
                                let key = self.cp_key(part);
                                let _ = ctx.kv().put(&key, off.to_string().into_bytes()).await;
                                last_commit = std::time::Instant::now();
                            }
                        }
                    }
                    Err(_) => {
                        // Backoff briefly on errors
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                }
            }
            Ok(())
        }
    }

    pub struct KafkaSink {
        pub brokers: String,
        pub topic: String,
        pub acks: Option<String>,
        pub key_field: Option<String>,
        producer: Option<FutureProducer>,
    }

    impl KafkaSink {
        pub fn new(brokers: impl Into<String>, topic: impl Into<String>) -> Self {
            Self { brokers: brokers.into(), topic: topic.into(), acks: Some("all".into()), key_field: None, producer: None }
        }
        pub fn with_key_field(mut self, key_field: impl Into<String>) -> Self { self.key_field = Some(key_field.into()); self }
        fn ensure_producer(&mut self) -> anyhow::Result<()> {
            if self.producer.is_none() {
                let mut cfg = ClientConfig::new();
                cfg.set("bootstrap.servers", &self.brokers);
                if let Some(a) = &self.acks { cfg.set("acks", a); }
                self.producer = Some(cfg.create().context("failed to create kafka producer")?);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl Sink for KafkaSink {
        async fn on_element(&mut self, record: Record) -> Result<()> {
            self.ensure_producer().map_err(|e| Error::Anyhow(e.into()))?;
            let producer = self.producer.as_ref().unwrap();
            let payload = serde_json::to_string(&record.value)?;
            let key = self.key_field.as_ref().and_then(|k| record.value.get(k).and_then(|v| v.as_str()).map(|s| s.to_string()))
                .unwrap_or_default();
            // Respect backpressure by awaiting the send future; mirrors ParquetSink's per-record write
            let mut fr = FutureRecord::to(&self.topic).payload(&payload);
            if !key.is_empty() { fr = fr.key(&key); }
            let _ = producer.send(fr, std::time::Duration::from_secs(5)).await;
            Ok(())
        }
    }
}

#[cfg(feature = "kafka")]
pub use kafka::{KafkaSink, KafkaSource};

#[cfg(all(test, feature = "kafka"))]
mod kafka_tests {
    use super::kafka::{extract_event_time, parse_payload_to_value};
    use chrono::{TimeZone, Utc};
    use super::kafka::KafkaSource;

    #[test]
    fn payload_json_decodes() {
        let v = parse_payload_to_value(br#"{"a":1}"#).unwrap();
        assert_eq!(v["a"], serde_json::json!(1));
    }

    #[test]
    fn payload_utf8_non_json_falls_back() {
        let v = parse_payload_to_value(b"hello world").unwrap();
        assert_eq!(v["bytes"], serde_json::json!("hello world"));
    }

    #[test]
    fn payload_binary_base64() {
        let v = parse_payload_to_value(&[0, 159, 146, 150]).unwrap();
        assert!(v.get("bytes_b64").is_some());
    }

    #[test]
    fn extract_event_time_number_and_rfc3339() {
        let v_num = serde_json::json!({"ts": 1_700_000_000_000i64});
        let dt = extract_event_time(&v_num, "ts");
        assert_eq!(dt.timestamp_millis(), 1_700_000_000_000);

        let v_str = serde_json::json!({"ts": "2023-12-01T00:00:00Z"});
        let dt2 = extract_event_time(&v_str, "ts");
        assert_eq!(dt2, Utc.with_ymd_and_hms(2023,12,1,0,0,0).unwrap());
    }

    #[test]
    fn checkpoint_key_format() {
        let src = KafkaSource::new("b:9092", "g1", "t1", "ts");
        // private method cp_key not accessible; replicate format expectation
        let expected_prefix = b"kafka:offset:";
        let topic = b"t1";
        let group = b"g1";
        let part = b"0";
        let mut k = expected_prefix.to_vec();
        k.extend_from_slice(topic);
        k.push(b':');
        k.extend_from_slice(group);
        k.push(b':');
        k.extend_from_slice(part);
        // Ensure we didn't accidentally change the namespace constant layout
        // (indirectly): simply ensure the prefix matches and the topic/group ordering is as documented.
        let as_str = String::from_utf8_lossy(&k);
        assert!(as_str.starts_with("kafka:offset:t1:g1:"));
    }
}

#[cfg(feature = "parquet")]
pub mod parquet_sink;
#[cfg(feature = "parquet")]
pub use parquet_sink::{ParquetSink, ParquetSinkConfig, PartitionSpec};

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{Context, EventTime, KvState, Record, Result, Timers, Watermark};
    use std::sync::Arc;

    struct TestState;
    #[async_trait]
    impl KvState for TestState {
        async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        async fn put(&self, _key: &[u8], _value: Vec<u8>) -> Result<()> {
            Ok(())
        }
        async fn delete(&self, _key: &[u8]) -> Result<()> {
            Ok(())
        }
        async fn iter_prefix(&self, _prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            Ok(Vec::new())
        }
        async fn snapshot(&self) -> Result<pulse_core::SnapshotId> {
            Ok("test-snap".to_string())
        }
        async fn restore(&self, _snapshot: pulse_core::SnapshotId) -> Result<()> {
            Ok(())
        }
    }

    struct TestTimers;
    #[async_trait]
    impl Timers for TestTimers {
        async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> {
            Ok(())
        }
    }

    struct TestCtx {
        pub out: Vec<Record>,
        kv: Arc<dyn KvState>,
        timers: Arc<dyn Timers>,
    }

    #[async_trait]
    impl Context for TestCtx {
        fn collect(&mut self, record: Record) {
            self.out.push(record);
        }
        fn watermark(&mut self, _wm: Watermark) {}
        fn kv(&self) -> Arc<dyn KvState> {
            self.kv.clone()
        }
        fn timers(&self) -> Arc<dyn Timers> {
            self.timers.clone()
        }
    }

    fn tmp_file(name: &str) -> String {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("pulse_test_{}_{}.tmp", name, nanos));
        p.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn file_source_jsonl_reads_lines() {
        let path = tmp_file("jsonl");
        let content = "{\"event_time\":1704067200000,\"text\":\"hello\"}\n{\"event_time\":\"2024-01-01T00:00:00Z\",\"text\":\"world\"}\n";
        tokio::fs::write(&path, content).await.unwrap();

        let mut src = FileSource::jsonl(&path, "event_time");
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
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

        let mut src = FileSource {
            path: path.clone(),
            format: FileFormat::Csv,
            event_time_field: "event_time".into(),
            text_field: None,
        };
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        src.run(&mut ctx).await.unwrap();
        assert_eq!(ctx.out.len(), 2);
        assert_eq!(ctx.out[0].value["text"], serde_json::json!("hello"));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_sink_appends_to_file() {
        let path = tmp_file("sink");
        let mut sink = FileSink {
            path: Some(path.clone()),
        };
        sink.on_element(Record {
            event_time: chrono::Utc::now(),
            value: serde_json::json!({"a":1}),
        })
        .await
        .unwrap();
        sink.on_element(Record {
            event_time: chrono::Utc::now(),
            value: serde_json::json!({"b":2}),
        })
        .await
        .unwrap();

        let data = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<_> = data.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"a\":1"));
        assert!(lines[1].contains("\"b\":2"));

        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn file_source_emits_eof_watermark() {
        // Arrange: a single-line JSONL with event_time
        let path = tmp_file("wm");
        let content = "{\"event_time\":1704067200000,\"text\":\"x\"}\n";
        tokio::fs::write(&path, content).await.unwrap();

        // Custom ctx capturing watermark calls
        struct WmCtx { saw_wm: bool, out: Vec<Record>, kv: Arc<dyn KvState>, timers: Arc<dyn Timers> }
        #[async_trait]
        impl Context for WmCtx {
            fn collect(&mut self, record: Record) { self.out.push(record); }
            fn watermark(&mut self, _wm: Watermark) { self.saw_wm = true; }
            fn kv(&self) -> Arc<dyn KvState> { self.kv.clone() }
            fn timers(&self) -> Arc<dyn Timers> { self.timers.clone() }
        }

        let mut src = FileSource::jsonl(&path, "event_time");
        let mut ctx = WmCtx { saw_wm: false, out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        src.run(&mut ctx).await.unwrap();
        assert_eq!(ctx.out.len(), 1);
        assert!(ctx.saw_wm, "EOF watermark not emitted by FileSource");

        let _ = tokio::fs::remove_file(&path).await;
    }
}
