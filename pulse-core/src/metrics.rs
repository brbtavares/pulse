use once_cell::sync::Lazy;
use prometheus::{
    Encoder, Histogram, HistogramOpts, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry, TextEncoder,
};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);

pub static OP_THROUGHPUT: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        Opts::new("pulse_operator_records_total", "Records processed by operator"),
        &["operator", "stage"],
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static LAG_WATERMARK_MS: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "pulse_watermark_lag_ms",
        "Lag between now and current watermark in ms",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub static BYTES_WRITTEN: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        Opts::new("pulse_bytes_written_total", "Total bytes written by sink"),
        &["sink"],
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub static STATE_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    let g = IntGaugeVec::new(
        Opts::new("pulse_state_size", "State size per operator"),
        &["operator"],
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub static OP_PROC_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(
        HistogramOpts::new(
            "pulse_operator_process_latency_ms",
            "Operator on_element processing latency (ms)",
        )
        .buckets(vec![
            0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
        ]),
    )
    .unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

pub static SINK_PROC_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(
        HistogramOpts::new(
            "pulse_sink_process_latency_ms",
            "Sink on_element processing latency (ms)",
        )
        .buckets(vec![
            0.1, 0.5, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0,
        ]),
    )
    .unwrap();
    REGISTRY.register(Box::new(h.clone())).unwrap();
    h
});

pub static QUEUE_DEPTH: Lazy<IntGauge> = Lazy::new(|| {
    let g = IntGauge::new(
        "pulse_queue_depth",
        "Current in-flight queue depth between source and operators",
    )
    .unwrap();
    REGISTRY.register(Box::new(g.clone())).unwrap();
    g
});

pub static DROPPED_RECORDS: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(
        Opts::new(
            "pulse_dropped_records_total",
            "Total records dropped due to backpressure",
        ),
        &["reason"],
    )
    .unwrap();
    REGISTRY.register(Box::new(c.clone())).unwrap();
    c
});

pub fn render_prometheus() -> String {
    let mut buffer = Vec::new();
    let encoder = TextEncoder::new();
    encoder.encode(&REGISTRY.gather(), &mut buffer).ok();
    String::from_utf8(buffer).unwrap_or_default()
}
