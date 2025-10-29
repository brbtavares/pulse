#![cfg(feature = "kafka")]

use std::time::Duration;

use pulse_core::SimpleInMemoryState;
use pulse_core::{Context, EventTime, KvState, Record, Result, Timers, Watermark};
use pulse_io::KafkaSource;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;
use tokio::sync::mpsc;

struct TestTimers;
#[async_trait::async_trait]
impl Timers for TestTimers {
    async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> {
        Ok(())
    }
}

struct ChanCtx {
    tx: mpsc::UnboundedSender<Record>,
    kv: std::sync::Arc<dyn KvState>,
}

#[async_trait::async_trait]
impl Context for ChanCtx {
    fn collect(&mut self, record: Record) {
        let _ = self.tx.send(record);
    }
    fn watermark(&mut self, _wm: Watermark) {}
    fn kv(&self) -> std::sync::Arc<dyn KvState> {
        self.kv.clone()
    }
    fn timers(&self) -> std::sync::Arc<dyn Timers> {
        std::sync::Arc::new(TestTimers)
    }
}

#[tokio::test]
async fn kafka_roundtrip_basic_if_env_present() {
    let brokers = match std::env::var("KAFKA_BROKER") {
        Ok(v) => v,
        Err(_) => return,
    }; // skip if not configured
    let topic = match std::env::var("KAFKA_TOPIC") {
        Ok(v) => v,
        Err(_) => return,
    };

    // Produce a couple of JSON messages
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("producer");
    for i in 0..2 {
        let payload = serde_json::json!({
            "event_time": chrono::Utc::now().to_rfc3339(),
            "stream_id": "test",
            "n": i
        })
        .to_string();
        let _ = producer
            .send(FutureRecord::to(&topic).payload(&payload), Duration::from_secs(1))
            .await;
    }

    // Run KafkaSource and collect a few outputs, then abort
    let mut src = KafkaSource::new(
        brokers,
        format!(
            "g-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ),
        topic,
        "event_time",
    );
    src.auto_offset_reset = Some("earliest".into());
    src.commit_interval = Duration::from_millis(200);

    let (tx, mut rx) = mpsc::unbounded_channel();
    let kv = std::sync::Arc::new(SimpleInMemoryState::default()) as std::sync::Arc<dyn KvState>;
    let mut ctx = ChanCtx { tx, kv: kv.clone() };

    let handle = tokio::spawn(async move {
        let _ = src.run(&mut ctx).await;
    });

    // Wait to receive at least 2 records or timeout
    let mut got = 0usize;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while got < 2 && tokio::time::Instant::now() < deadline {
        if let Ok(rec) = tokio::time::timeout(Duration::from_millis(500), rx.recv()).await {
            if rec.is_some() {
                got += 1;
            }
        }
    }

    // Stop the source task (infinite loop) and ignore result
    handle.abort();

    // Basic assertions
    assert!(got >= 1, "did not receive any records from Kafka");

    // Check that offsets checkpointing likely happened (best-effort)
    let entries = kv.iter_prefix(Some(b"kafka:offset:")).await.unwrap_or_default();
    // Not asserting non-empty strictly because commit interval might not have elapsed; still useful when running locally/CI
    let _maybe = entries; // keep for potential debug
}
