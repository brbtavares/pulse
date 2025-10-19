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

    let mut src = pulse_io::FileSource::jsonl(cfg.source.path.to_string_lossy(), cfg.source.time_field);

    let mut exec = pulse_core::Executor::new();
    exec.source(src)
        .operator(pulse_ops::KeyBy::new(cfg.ops.count_by.clone().unwrap()))
        .operator(pulse_ops::WindowedAggregate::tumbling_count("key", win_size_ms))
        .sink(match cfg.sink.kind.as_str() {
            "parquet" => {
                let ps = pulse_io::ParquetSink::new(pulse_io::ParquetSinkConfig {
                    out_dir: cfg.sink.out_dir.clone(),
                    partition_by: pulse_io::PartitionSpec::ByDate { field: "event_time".into(), fmt: "%Y-%m-%d".into() },
                    max_rows: 1_000_000,
                    max_age: std::time::Duration::from_secs(300),
                });
                ps
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
}
