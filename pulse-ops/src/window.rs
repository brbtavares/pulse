use std::collections::HashMap;

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use pulse_core::KvState;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum WindowAssigner {
    Tumbling { size: Duration },
    Sliding { size: Duration, slide: Duration },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Window {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl WindowAssigner {
    fn assign(&self, ts: DateTime<Utc>) -> Vec<Window> {
        match *self {
            WindowAssigner::Tumbling { size } => {
                let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
                let since = ts - epoch;
                let buckets = since.num_milliseconds() / size.num_milliseconds();
                let start = epoch + Duration::milliseconds(buckets * size.num_milliseconds());
                let end = start + size;
                vec![Window { start, end }]
            }
            WindowAssigner::Sliding { size, slide } => {
                let epoch = DateTime::<Utc>::from_timestamp(0, 0).unwrap();
                let since = ts - epoch;
                let k = (size.num_milliseconds() / slide.num_milliseconds()) as i64;
                let anchor_ms = (since.num_milliseconds() / slide.num_milliseconds()) * slide.num_milliseconds();
                let mut out = Vec::new();
                for j in 0..k {
                    let start = epoch
                        + Duration::milliseconds(anchor_ms - j * slide.num_milliseconds());
                    let end = start + size;
                    if start <= ts && ts < end {
                        out.push(Window { start, end });
                    }
                }
                out
            }
        }
    }
}

/// A simple window operator keyed only by window time (no grouping key here, for clarity).
/// Maintains state per window and emits results when watermark >= window.end.
pub struct WindowOperator<S> {
    assigner: WindowAssigner,
    state: HashMap<Window, S>,
    reduce: Box<dyn Fn(&mut S, &serde_json::Value) + Send + Sync>,
    init: Box<dyn Fn() -> S + Send + Sync>,
    /// Optional external state backend used for persistence across restarts.
    backend: Option<std::sync::Arc<dyn KvState>>, // serialized as JSON with key prefix
    ns_prefix: Vec<u8>,
}

impl<S> WindowOperator<S>
where
    S: Clone + Default + Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static,
{
    pub fn new<Init, Red>(assigner: WindowAssigner, init: Init, reduce: Red) -> Self
    where
        Init: Fn() -> S + Send + Sync + 'static,
        Red: Fn(&mut S, &serde_json::Value) + Send + Sync + 'static,
    {
        Self {
            assigner,
            state: HashMap::new(),
            init: Box::new(init),
            reduce: Box::new(reduce),
            backend: None,
            ns_prefix: b"window:".to_vec(),
        }
    }

    /// Configure an external KeyValueState backend for durable window state and a namespace prefix.
    pub fn with_backend(mut self, backend: std::sync::Arc<dyn KvState>, ns_prefix: impl AsRef<[u8]>) -> Self {
        self.backend = Some(backend);
        self.ns_prefix = ns_prefix.as_ref().to_vec();
        self
    }

    fn make_key(&self, w: &Window) -> Vec<u8> {
        // key: ns|start_ms|end_ms
        let start = w.start.timestamp_millis();
        let end = w.end.timestamp_millis();
        let mut k = self.ns_prefix.clone();
        k.extend_from_slice(start.to_string().as_bytes());
        k.push(b'|');
        k.extend_from_slice(end.to_string().as_bytes());
        k
    }

    pub fn on_element(&mut self, ts: DateTime<Utc>, value: &serde_json::Value) {
        for w in self.assigner.assign(ts) {
            let entry = self.state.entry(w.clone()).or_insert_with(|| (self.init)());
            (self.reduce)(entry, value);
            // persist if backend configured
            if let Some(backend) = &self.backend {
                if let Ok(bytes) = serde_json::to_vec(entry) {
                    let key = self.make_key(&w);
                    // best-effort
                    let _ = futures::executor::block_on(backend.put(&key, bytes));
                }
            }
        }
    }

    pub fn on_watermark(&mut self, watermark: DateTime<Utc>) -> Vec<(Window, S)> {
        let mut to_emit = Vec::new();
        let keys: Vec<_> = self.state.keys().cloned().collect();
        for w in keys {
            if watermark >= w.end {
                if let Some(s) = self.state.remove(&w) {
                    to_emit.push((w.clone(), s));
                }
                // cleanup backend if configured
                if let Some(backend) = &self.backend {
                    let key = self.make_key(&w);
                    let _ = futures::executor::block_on(backend.delete(&key));
                }
            }
        }
        to_emit
    }

    /// Restore in-memory window states from the backend for this namespace.
    pub async fn restore_from_backend(&mut self) -> pulse_core::Result<()>
    where
        S: for<'de> Deserialize<'de>,
    {
        if let Some(backend) = &self.backend {
            let entries = backend.iter_prefix(Some(&self.ns_prefix)).await?;
            for (k, v) in entries {
                if let Ok(state) = serde_json::from_slice::<S>(&v) {
                    // parse key into window
                    let s = String::from_utf8_lossy(&k[self.ns_prefix.len()..]);
                    if let Some((a, b)) = s.split_once('|') {
                        if let (Ok(start_ms), Ok(end_ms)) = (a.parse::<i64>(), b.parse::<i64>()) {
                            let w = Window {
                                start: DateTime::<Utc>::from_timestamp_millis(start_ms).unwrap(),
                                end: DateTime::<Utc>::from_timestamp_millis(end_ms).unwrap(),
                            };
                            self.state.insert(w, state);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod persist_tests {
    use super::*;
    use crate::window::WindowAssigner;
    use chrono::Duration;
    use pulse_state::InMemoryState;
    use std::sync::Arc;

    #[tokio::test]
    async fn window_state_persists_and_restores() {
        let assigner = WindowAssigner::Tumbling { size: Duration::seconds(60) };
        let backend = Arc::new(InMemoryState::default());
        // instance 1 processes some data
        let mut op1 = WindowOperator::new(assigner, || 0i64, |s, v| *s += v["n"].as_i64().unwrap_or(0))
            .with_backend(backend.clone(), b"win:count:");
        let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        op1.on_element(t0, &serde_json::json!({"n": 2}));
        // simulate checkpoint: persist already done per element; take backend snapshot
        let _snap = backend.snapshot().await.unwrap();

        // instance 2 restores
        let mut op2 = WindowOperator::new(assigner, || 0i64, |s, v| *s += v["n"].as_i64().unwrap_or(0))
            .with_backend(backend.clone(), b"win:count:");
        op2.restore_from_backend().await.unwrap();
        // advancing watermark closes window and emits preserved state
        let wm = t0 + Duration::seconds(60);
        let out = op2.on_watermark(wm);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].1, 2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tumbling_emits_on_watermark_with_lateness() {
        let assigner = WindowAssigner::Tumbling { size: Duration::seconds(60) };
        let mut op = WindowOperator::new(assigner, || 0i64, |s, v| *s += v["n"].as_i64().unwrap_or(0));

        let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = t0 + Duration::seconds(10);
        let t2 = t0 + Duration::seconds(70); // next window

        op.on_element(t0, &serde_json::json!({"n": 1}));
        op.on_element(t1, &serde_json::json!({"n": 2}));
        // watermark at end of first window should emit first window only
    let wm1 = t0 + Duration::seconds(60);
        let out1 = op.on_watermark(wm1);
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].1, 3);

        op.on_element(t2, &serde_json::json!({"n": 5}));
        let wm2 = t2 + Duration::seconds(60);
        let out2 = op.on_watermark(wm2);
        assert_eq!(out2.len(), 1);
        assert_eq!(out2[0].1, 5);
    }

    #[test]
    fn sliding_emits_multiple_overlaps() {
        let assigner = WindowAssigner::Sliding { size: Duration::seconds(60), slide: Duration::seconds(15) };
        let mut op = WindowOperator::new(assigner, || 0i64, |s, v| *s += v["n"].as_i64().unwrap_or(0));

        let base = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t = base + Duration::seconds(30);
        op.on_element(t, &serde_json::json!({"n": 1}));
        // Compute windows from assigner and set WM to the max end among them
        let wins = assigner.assign(t);
        let max_end = wins.iter().map(|w| w.end).max().unwrap();
        let out = op.on_watermark(max_end);
        assert_eq!(out.len(), wins.len());
        let sums: Vec<i64> = out.iter().map(|(_, s)| *s).collect();
        assert!(sums.iter().all(|&x| x == 1));
    }

    #[test]
    fn out_of_order_data_waits_until_watermark() {
        let assigner = WindowAssigner::Tumbling { size: Duration::seconds(60) };
        let mut op = WindowOperator::new(assigner, || 0i64, |s, v| *s += v["n"].as_i64().unwrap_or(0));
        let base = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let late = base + Duration::seconds(10);
        // First advance to a later timestamp
        op.on_element(base + Duration::seconds(75), &serde_json::json!({"n": 7}));
        // Then an out-of-order event for the previous window
        op.on_element(late, &serde_json::json!({"n": 3}));
        // Not emitted until watermark reaches the end of that window; compute that end via assigner
        let wins_for_late = assigner.assign(late);
        let end_of_late = wins_for_late.iter().map(|w| w.end).max().unwrap();
        let out0 = op.on_watermark(end_of_late - Duration::seconds(1));
        assert!(out0.is_empty());
        let out1 = op.on_watermark(end_of_late);
        assert_eq!(out1.len(), 1);
        assert_eq!(out1[0].1, 3);
    }
}
