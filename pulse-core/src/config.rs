use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SourceConfig {
    pub kind: String,           // "file"
    pub path: PathBuf,          // path to file
    pub time_field: String,     // e.g., "event_time" or "ts"
    // Kafka-specific (used when kind=="kafka")
    #[serde(default)]
    pub bootstrap_servers: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub group_id: Option<String>,
    #[serde(default)]
    pub auto_offset_reset: Option<String>, // "earliest" | "latest"
    #[serde(default)]
    pub commit_interval_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct TimeConfig {
    pub allowed_lateness: String, // e.g., "10s"
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct WindowConfig {
    #[serde(rename = "type")]
    pub kind: String, // tumbling|sliding|session
    pub size: String, // e.g., "60s"
    #[serde(default)]
    pub slide: Option<String>,
    #[serde(default)]
    pub gap: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct OpsConfig {
    #[serde(default)]
    pub count_by: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct SinkConfig {
    pub kind: String,     // "parquet" | "file" | "kafka"
    #[serde(default)]
    pub out_dir: PathBuf, // for parquet or file path for file sink
    // Kafka-specific (when kind=="kafka")
    #[serde(default)]
    pub bootstrap_servers: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub acks: Option<String>, // "all" | "1" | "0"
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PipelineConfig {
    pub source: SourceConfig,
    pub time: TimeConfig,
    pub window: WindowConfig,
    pub ops: OpsConfig,
    pub sink: SinkConfig,
}

impl PipelineConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        match self.source.kind.as_str() {
            "file" => {
                if self.source.path.as_os_str().is_empty() {
                    anyhow::bail!("source.path must be set for file source");
                }
            }
            "kafka" => {
                if self.source.bootstrap_servers.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("source.bootstrap_servers must be set for kafka source");
                }
                if self.source.topic.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("source.topic must be set for kafka source");
                }
                if self.source.group_id.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("source.group_id must be set for kafka source");
                }
            }
            other => anyhow::bail!("unsupported source kind: {}", other),
        }
        match self.sink.kind.as_str() {
            "parquet" | "file" => {}
            "kafka" => {
                if self.sink.bootstrap_servers.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("sink.bootstrap_servers must be set for kafka sink");
                }
                if self.sink.topic.as_deref().unwrap_or("").is_empty() {
                    anyhow::bail!("sink.topic must be set for kafka sink");
                }
            }
            other => anyhow::bail!("unsupported sink kind: {}", other),
        }
        if self.ops.count_by.is_none() {
            anyhow::bail!("ops.count_by must be set (e.g., word field)");
        }
        Ok(())
    }
}

pub fn parse_duration_ms(s: &str) -> anyhow::Result<i64> {
    // very small parser for values like "10s", "500ms", "2m"
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        return Ok(num.parse::<i64>()?);
    }
    if let Some(num) = s.strip_suffix('s') {
        return Ok(num.parse::<i64>()? * 1_000);
    }
    if let Some(num) = s.strip_suffix('m') {
        return Ok(num.parse::<i64>()? * 60_000);
    }
    if let Some(num) = s.strip_suffix('h') {
        return Ok(num.parse::<i64>()? * 3_600_000);
    }
    // default assume seconds
    Ok(s.parse::<i64>()? * 1_000)
}
