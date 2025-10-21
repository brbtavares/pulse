use axum::{routing::get, Router};
use std::net::SocketAddr;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "pulse", version, about = "Pulse CLI", disable_help_subcommand = false)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a pipeline from a pipeline.toml
    Run { #[arg(short, long)] config: std::path::PathBuf, #[arg(long, default_value_t = 0)] http_port: u16 },
    /// Serve only /metrics
    Serve { #[arg(long, default_value_t = 9898)] port: u16 },
}

fn app() -> Router {
    Router::new().route("/metrics", get(metrics))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Serve { port } => {
            let app = app();
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            tracing::info!("serving /metrics on {}", addr);
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
            Ok(())
        }
        Commands::Run { config, http_port } => {
            if http_port != 0 {
                let app = app();
                let addr: SocketAddr = format!("127.0.0.1:{}", http_port).parse().unwrap();
                let listener = tokio::net::TcpListener::bind(addr).await?;
                tokio::spawn(async move { let _ = axum::serve(listener, app).await; });
            }
            run_pipeline(config).await?;
            Ok(())
        }
    }
}

async fn metrics() -> String {
    pulse_core::metrics::render_prometheus()
}

async fn run_pipeline(path: std::path::PathBuf) -> anyhow::Result<()> {
    // Read and parse
    let text = tokio::fs::read_to_string(&path).await?;
    let cfg: pulse_core::config::PipelineConfig = toml::from_str(&text)?;
    cfg.validate()?;

    // Build components
    let allowed_lateness_ms = pulse_core::config::parse_duration_ms(&cfg.time.allowed_lateness)?;
    let win_size_ms = pulse_core::config::parse_duration_ms(&cfg.window.size)?;
    let op_window = match cfg.window.kind.as_str() {
        "tumbling" => pulse_ops::WindowedAggregate::tumbling_count(cfg.ops.count_by.clone().unwrap(), win_size_ms),
        "sliding" => {
            let slide_ms = pulse_core::config::parse_duration_ms(cfg.window.slide.as_deref().ok_or_else(|| anyhow::anyhow!("slide must be set for sliding window"))?)?;
            pulse_ops::WindowedAggregate::sliding_count(cfg.ops.count_by.clone().unwrap(), win_size_ms, slide_ms)
        }
        "session" => {
            let gap_ms = pulse_core::config::parse_duration_ms(cfg.window.gap.as_deref().ok_or_else(|| anyhow::anyhow!("gap must be set for session window"))?)?;
            pulse_ops::WindowedAggregate::session_count(cfg.ops.count_by.clone().unwrap(), gap_ms)
        }
        other => return Err(anyhow::anyhow!(format!("unsupported window kind: {}", other))),
    }.with_allowed_lateness(allowed_lateness_ms);

    // Build source based on kind
    #[allow(unused_mut)]
    let src: Box<dyn pulse_core::Source> = match cfg.source.kind.as_str() {
        "file" => Box::new(pulse_io::FileSource::jsonl(cfg.source.path.to_string_lossy(), cfg.source.time_field.clone())),
    #[cfg(feature = "kafka")]
        "kafka" => {
            let mut s = pulse_io::KafkaSource::new(
                cfg.source.bootstrap_servers.clone().unwrap(),
                cfg.source.group_id.clone().unwrap(),
                cfg.source.topic.clone().unwrap(),
                cfg.source.time_field.clone(),
            );
            s.auto_offset_reset = cfg.source.auto_offset_reset.clone();
            if let Some(ms) = cfg.source.commit_interval_ms { s.commit_interval = std::time::Duration::from_millis(ms); }
            Box::new(s)
        }
        other => return Err(anyhow::anyhow!(format!("unsupported source kind: {}", other))),
    };

    let mut exec = pulse_core::Executor::new();
    exec.source(src)
        .operator(pulse_ops::KeyBy::new(cfg.ops.count_by.clone().unwrap()))
        .operator(op_window)
        .sink(match cfg.sink.kind.as_str() {
            "parquet" => {
                let partition = if let Some(f) = &cfg.sink.partition_field {
                    pulse_io::PartitionSpec::ByField { field: f.clone() }
                } else {
                    let fmt = cfg.sink.partition_format.clone().unwrap_or_else(|| "%Y-%m-%d".into());
                    pulse_io::PartitionSpec::ByDate { field: "event_time".into(), fmt }
                };
                let mut conf = pulse_io::ParquetSinkConfig {
                    out_dir: cfg.sink.out_dir.clone(),
                    partition_by: partition,
                    max_rows: 1_000_000,
                    max_age: std::time::Duration::from_secs(300),
                    compression: cfg.sink.compression.clone(),
                    max_bytes: cfg.sink.max_bytes.map(|b| b as usize),
                };
                pulse_io::ParquetSink::new(conf)
            }
            #[cfg(feature = "kafka")]
            "kafka" => {
                let mut ks = pulse_io::KafkaSink::new(
                    cfg.sink.bootstrap_servers.clone().unwrap(),
                    cfg.sink.topic.clone().unwrap(),
                );
                ks.acks = cfg.sink.acks.clone();
                ks
            }
            _ => {
                // default to FileSink stdout for now
                let _ = allowed_lateness_ms; // not yet wired
                return Err(anyhow::anyhow!("unsupported sink kind"));
            }
        });
    exec.run().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{self, Body};
    use axum::http::Request;
    use tower::util::ServiceExt;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::path::PathBuf;

    #[tokio::test]
    async fn metrics_endpoint_returns_text() {
        let app = app();
        // Touch a metric
        pulse_core::metrics::OP_THROUGHPUT.with_label_values(&["Test", "emit"]).inc();
        let res = app
            .oneshot(Request::builder().uri("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert!(res.status().is_success());
    let body = body::to_bytes(res.into_body(), 1_048_576).await.unwrap();
        let text = String::from_utf8(body.to_vec()).unwrap();
        assert!(text.contains("pulse_operator_records_total"));
    }

    fn remove_dir_all_quiet(p: &PathBuf) {
        let _ = std::fs::remove_dir_all(p);
    }

    #[tokio::test]
    async fn golden_simple_pipeline_parquet_rowcount() {
        // Use the fixture pipeline and input; override out_dir to a unique temp folder to avoid pollution.
        let mut cfg_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        cfg_path.pop(); // go to workspace root from pulse-bin
        let pipeline_toml = cfg_path.join("pulse-tests/fixtures/simple/pipeline.toml");
        assert!(pipeline_toml.exists(), "fixture pipeline missing: {:?}", pipeline_toml);

        // Read and modify out_dir to a temp dir
    let text = tokio::fs::read_to_string(&pipeline_toml).await.unwrap();
    let mut cfg: pulse_core::config::PipelineConfig = toml::from_str(&text).unwrap();
    // Ensure source.path is absolute so it works regardless of test cwd
    let abs_input = cfg_path.join("pulse-tests/fixtures/simple/input.jsonl");
    cfg.source.path = abs_input;
        let tmp_out = std::env::temp_dir().join(format!("pulse_golden_out_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        cfg.sink.out_dir = tmp_out.clone();
        // Write a temp config file
        let tmp_cfg = tmp_out.with_file_name("pipeline.temp.toml");
        if let Some(parent) = tmp_cfg.parent() { let _ = std::fs::create_dir_all(parent); }
        tokio::fs::write(&tmp_cfg, toml::to_string(&cfg).unwrap()).await.unwrap();

        // Ensure clean output dir
        remove_dir_all_quiet(&tmp_out);

        // Run pipeline
        run_pipeline(tmp_cfg.clone()).await.unwrap();

        // Scan parquet files and count rows
        let mut total_rows = 0usize;
        if let Ok(rd) = std::fs::read_dir(&tmp_out) {
            for part in rd.flatten() {
                if part.path().is_dir() {
                    for file in std::fs::read_dir(part.path()).unwrap().flatten() {
                        let p = file.path();
                        if p.extension().map(|e| e == "parquet").unwrap_or(false) {
                            let f = std::fs::File::open(&p).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
                            let mut reader = builder.build().unwrap();
                            while let Some(batch) = reader.next() {
                                total_rows += batch.unwrap().num_rows();
                            }
                        }
                    }
                }
            }
        }

        // For the fixture input of 3 events within the same 60s window for keys a,a,b,
        // WordCount tumbling count should emit 2 rows (keys a and b) when flushed by EOF watermark.
        assert_eq!(total_rows, 2, "unexpected parquet total rows");

        // Cleanup
        remove_dir_all_quiet(&tmp_out);
    }

    #[tokio::test]
    async fn golden_session_pipeline_parquet_rowcount() {
        // Fixture base path
        let mut cfg_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        cfg_path.pop(); // workspace root
        let pipeline_toml = cfg_path.join("pulse-tests/fixtures/simple/pipeline.toml");
        assert!(pipeline_toml.exists(), "fixture pipeline missing: {:?}", pipeline_toml);

        // Load and modify to session window with 30s gap
        let text = tokio::fs::read_to_string(&pipeline_toml).await.unwrap();
        let mut cfg: pulse_core::config::PipelineConfig = toml::from_str(&text).unwrap();
        // Ensure absolute path for input
        let abs_input = cfg_path.join("pulse-tests/fixtures/simple/input.jsonl");
        cfg.source.path = abs_input;
        // Switch window to session; keep size (unused) to satisfy parsing
        cfg.window.kind = "session".into();
        cfg.window.gap = Some("30s".into());
        cfg.window.slide = None;

        // Temp output dir and config path
        let tmp_out = std::env::temp_dir().join(format!("pulse_golden_out_session_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
        cfg.sink.out_dir = tmp_out.clone();
        let tmp_cfg = tmp_out.with_file_name("pipeline.session.temp.toml");
        if let Some(parent) = tmp_cfg.parent() { let _ = std::fs::create_dir_all(parent); }
        tokio::fs::write(&tmp_cfg, toml::to_string(&cfg).unwrap()).await.unwrap();

        // Ensure clean output dir
        remove_dir_all_quiet(&tmp_out);

        // Run pipeline
        run_pipeline(tmp_cfg.clone()).await.unwrap();

        // Count Parquet rows
        let mut total_rows = 0usize;
        if let Ok(rd) = std::fs::read_dir(&tmp_out) {
            for part in rd.flatten() {
                if part.path().is_dir() {
                    for file in std::fs::read_dir(part.path()).unwrap().flatten() {
                        let p = file.path();
                        if p.extension().map(|e| e == "parquet").unwrap_or(false) {
                            let f = std::fs::File::open(&p).unwrap();
                            let builder = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
                            let mut reader = builder.build().unwrap();
                            while let Some(batch) = reader.next() {
                                total_rows += batch.unwrap().num_rows();
                            }
                        }
                    }
                }
            }
        }

        // Para o input com 3 eventos (a, a, b) e gap=30s, devem resultar 2 sessões (a e b)
        assert_eq!(total_rows, 2, "unexpected parquet total rows for session window");

        // Cleanup
        remove_dir_all_quiet(&tmp_out);
    }
}
