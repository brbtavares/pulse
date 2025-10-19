use axum::{routing::get, Router};
use std::net::SocketAddr;

fn app() -> Router {
    Router::new().route("/metrics", get(metrics))
}

#[tokio::main]
async fn main() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into());
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let app = app();
    let addr: SocketAddr = "127.0.0.1:9898".parse().unwrap();
    tracing::info!("serving /metrics on {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn metrics() -> String {
    pulse_core::metrics::render_prometheus()
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
