#![cfg(feature = "parquet")]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use arrow::array::{ArrayRef, StringBuilder, TimestampMillisecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
#[cfg(test)]
use chrono::{DateTime, Utc};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use pulse_core::{Record, Result, Sink};
use tokio::fs;

#[derive(Clone, Debug)]
pub enum PartitionSpec {
    ByDate { field: String, fmt: String },
    ByField { field: String },
}

#[derive(Clone, Debug)]
pub struct ParquetSinkConfig {
    pub out_dir: PathBuf,
    pub partition_by: PartitionSpec,
    /// Rotate a file when reaching this many rows (approx size). Default: 100_000.
    pub max_rows: usize,
    /// Rotate a file when this time elapses without a rotation. Default: 60s.
    pub max_age: std::time::Duration,
    /// Optional compression: none|snappy|zstd (default: snappy)
    pub compression: Option<String>,
    /// Optional approximate max bytes per file for rotation (in addition to rows/age).
    pub max_bytes: Option<usize>,
}

impl Default for ParquetSinkConfig {
    fn default() -> Self {
        Self {
            out_dir: PathBuf::from("./out"),
            partition_by: PartitionSpec::ByDate {
                field: "event_time".into(),
                fmt: "%Y-%m-%d".into(),
            },
            max_rows: 100_000,
            max_age: Duration::from_secs(60),
            compression: Some("snappy".into()),
            max_bytes: None,
        }
    }
}

struct ActiveFile {
    writer: ArrowWriter<std::fs::File>, // sync writer is fine; we buffer in memory batch
    started: Instant,
    rows: usize,
    path: PathBuf,
    approx_bytes: usize,
}

pub struct ParquetSink {
    cfg: ParquetSinkConfig,
    current_part: Option<String>,
    active: Option<ActiveFile>,
    // cached schema: event_time (timestamp ms) + dynamic fields as strings
    schema: std::sync::Arc<Schema>,
}

impl ParquetSink {
    pub fn new(cfg: ParquetSinkConfig) -> Self {
        let fields = vec![
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("payload", DataType::Utf8, false),
        ];
        let schema = std::sync::Arc::new(Schema::new(fields));
        Self {
            cfg,
            current_part: None,
            active: None,
            schema,
        }
    }

    fn partition_value(&self, rec: &Record) -> String {
        match &self.cfg.partition_by {
            PartitionSpec::ByDate { field, fmt } => {
                // event_time comes from Record.event_time
                if field == "event_time" {
                    rec.event_time.format(fmt).to_string()
                } else {
                    // fallback: parse string field
                    let s = rec.value.get(field).and_then(|v| v.as_str()).unwrap_or("");
                    s.to_string()
                }
            }
            PartitionSpec::ByField { field } => {
                if let Some(v) = rec.value.get(field) {
                    match v {
                        serde_json::Value::String(s) => s.clone(),
                        other => other.to_string(),
                    }
                } else {
                    String::new()
                }
            }
        }
    }

    async fn ensure_dir(path: &Path) -> Result<()> {
        fs::create_dir_all(path).await?;
        Ok(())
    }

    fn build_batch(&self, records: &[Record]) -> Result<RecordBatch> {
        let mut tsb = TimestampMillisecondBuilder::new();
        let mut payload = StringBuilder::new();
        for r in records {
            tsb.append_value(r.event_time.timestamp_millis());
            payload.append_value(serde_json::to_string(&r.value)?);
        }
        let arrays: Vec<ArrayRef> = vec![
            std::sync::Arc::new(tsb.finish()),
            std::sync::Arc::new(payload.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), arrays).context("record batch")?;
        Ok(batch)
    }

    fn rotate_needed(&self) -> bool {
        if let Some(active) = &self.active {
            let by_rows = active.rows >= self.cfg.max_rows;
            let by_age = active.started.elapsed() >= self.cfg.max_age;
            let by_bytes = self
                .cfg
                .max_bytes
                .map(|b| active.approx_bytes >= b)
                .unwrap_or(false);
            by_rows || by_age || by_bytes
        } else {
            true
        }
    }

    fn new_writer(
        path: &Path,
        schema: std::sync::Arc<Schema>,
        compression: Option<&str>,
    ) -> Result<ActiveFile> {
        let file = std::fs::File::create(path)?;
        let mut builder = WriterProperties::builder();
        match compression.unwrap_or("snappy").to_lowercase().as_str() {
            "snappy" => {
                builder = builder.set_compression(parquet::basic::Compression::SNAPPY);
            }
            "zstd" => {
                builder = builder.set_compression(parquet::basic::Compression::ZSTD(
                    parquet::basic::ZstdLevel::default(),
                ));
            }
            _ => {
                builder = builder.set_compression(parquet::basic::Compression::UNCOMPRESSED);
            }
        }
        let props = builder.build();
        let writer = ArrowWriter::try_new(file, schema, Some(props)).map_err(|e| anyhow::anyhow!(e))?;
        Ok(ActiveFile {
            writer,
            started: Instant::now(),
            rows: 0,
            path: path.to_path_buf(),
            approx_bytes: 0,
        })
    }

    fn open_partition(&mut self, part: &str) -> Result<()> {
        if self.current_part.as_deref() != Some(part) {
            // close previous
            if let Some(active) = self.active.take() {
                let _ = active.writer.close();
            }
            // open new file
            let dir = self.cfg.out_dir.join(format!("dt={}", part));
            std::fs::create_dir_all(&dir)?;
            let fname = format!("part-{}.parquet", chrono::Utc::now().timestamp_millis());
            let path = dir.join(fname);
            self.active = Some(Self::new_writer(
                &path,
                self.schema.clone(),
                self.cfg.compression.as_deref(),
            )?);
            self.current_part = Some(part.to_string());
        }
        Ok(())
    }

    fn close_active(&mut self) {
        if let Some(active) = self.active.take() {
            let _ = active.writer.close();
        }
    }
}

#[async_trait]
impl Sink for ParquetSink {
    async fn on_element(&mut self, record: Record) -> Result<()> {
        let part = self.partition_value(&record);
        self.open_partition(&part)?;
        if self.rotate_needed() {
            if let Some(active) = self.active.take() {
                let _ = active.writer.close();
            }
            // reopen in same partition
            if let Some(p) = &self.current_part {
                let dir = self.cfg.out_dir.join(format!("dt={}", p));
                let fname = format!("part-{}.parquet", chrono::Utc::now().timestamp_millis());
                let path = dir.join(fname);
                self.active = Some(Self::new_writer(
                    &path,
                    self.schema.clone(),
                    self.cfg.compression.as_deref(),
                )?);
            }
        }
        // For simplicity, write record-by-record as single-row batches.
        let batch = self.build_batch(std::slice::from_ref(&record))?;
        if let Some(active) = &mut self.active {
            active.writer.write(&batch).map_err(|e| anyhow::anyhow!(e))?;
            active.rows += 1;
            // Roughly estimate bytes as payload string + fixed overhead; for simplicity, use JSON length
            if let Some(s) = record.value.as_str() {
                active.approx_bytes += s.len();
            } else {
                active.approx_bytes += serde_json::to_string(&record.value)?.len();
            }
        }
        Ok(())
    }

    async fn on_watermark(&mut self, _wm: pulse_core::Watermark) -> Result<()> {
        // Finalize any open file so readers can see a valid footer
        self.close_active();
        Ok(())
    }
}

impl Drop for ParquetSink {
    fn drop(&mut self) {
        self.close_active();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn tmp_dir(prefix: &str) -> PathBuf {
        let mut d = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        d.push(format!("pulse_parquet_test_{}_{}", prefix, nanos));
        d
    }

    #[tokio::test]
    async fn writes_and_reads_back() {
        let out_dir = tmp_dir("parquet");
        let cfg = ParquetSinkConfig {
            out_dir: out_dir.clone(),
            partition_by: PartitionSpec::ByDate {
                field: "event_time".into(),
                fmt: "%Y-%m-%d".into(),
            },
            max_rows: 10,
            max_age: Duration::from_secs(60),
            compression: Some("snappy".into()),
            max_bytes: None,
        };
        let mut sink = ParquetSink::new(cfg);

        // Write 3 records on same day
        let ts = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        for i in 0..3 {
            let rec = Record {
                event_time: ts,
                value: serde_json::json!({"n": i, "s": format!("v{}", i)}),
            };
            sink.on_element(rec).await.unwrap();
        }
        // Close current writer explicitly by simulating rotation
        if let Some(active) = sink.active.take() {
            let _ = active.writer.close();
        }

        // Read back
        let part_dir = out_dir.join(format!("dt={}", ts.format("%Y-%m-%d")));
        let mut total = 0usize;
        if let Ok(read_dir) = std::fs::read_dir(&part_dir) {
            for e in read_dir.flatten() {
                if e.path().extension().map(|s| s == "parquet").unwrap_or(false) {
                    let file = std::fs::File::open(e.path()).unwrap();
                    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
                    let mut reader = builder.build().unwrap();
                    while let Some(batch) = reader.next() {
                        let batch = batch.unwrap();
                        total += batch.num_rows();
                    }
                }
            }
        }
        assert_eq!(total, 3);

        // cleanup
        let _ = std::fs::remove_dir_all(out_dir);
    }
}
