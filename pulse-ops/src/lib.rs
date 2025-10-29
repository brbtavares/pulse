//! pulse-ops: standard operators built on top of pulse-core.
//!
//! Included operators:
//! - `Map`: one-to-many mapping of JSON payloads
//! - `Filter`: predicate-based filtering
//! - `KeyBy`: materialize a `key` field from an existing field
//! - `Aggregate` (simplified): per-minute running count updates
//! - `WindowedAggregate`: configurable windows (tumbling/sliding/session) with count/sum/avg/distinct

use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use pulse_core::{Context, EventTime, Operator, Record, Result, Watermark};
use tracing::{info_span, instrument};
pub mod time;
pub mod window;
pub use time::{WatermarkClock, WatermarkPolicy};
pub use window::{Window, WindowAssigner, WindowOperator};

#[async_trait]
pub trait FnMap: Send + Sync {
    async fn call(&self, value: serde_json::Value) -> Result<Vec<serde_json::Value>>;
}

pub struct MapFn<F>(pub F);
impl<F> MapFn<F> {
    pub fn new(f: F) -> Self {
        Self(f)
    }
}
#[async_trait]
impl<F> FnMap for MapFn<F>
where
    F: Fn(serde_json::Value) -> Vec<serde_json::Value> + Send + Sync,
{
    async fn call(&self, value: serde_json::Value) -> Result<Vec<serde_json::Value>> {
        Ok((self.0)(value))
    }
}

/// Map operator: applies a user function that returns zero or more outputs per input.
/// Map operator: applies a user function that returns zero or more outputs per input.
///
/// Example
/// ```no_run
/// use pulse_ops::{Map, MapFn};
/// let map = Map::new(MapFn::new(|v: serde_json::Value| vec![v]));
/// # let _ = map;
/// ```
pub struct Map<F> {
    func: F,
}
impl<F> Map<F> {
    pub fn new(func: F) -> Self {
        Self { func }
    }
}

#[async_trait]
impl<F> Operator for Map<F>
where
    F: FnMap + Send + Sync + 'static,
{
    #[instrument(name = "map_on_element", skip_all)]
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        let outs = self.func.call(rec.value).await?;
        pulse_core::metrics::OP_THROUGHPUT
            .with_label_values(&["Map", "receive"])
            .inc();
        for v in outs {
            ctx.collect(Record {
                event_time: rec.event_time,
                value: v.clone(),
            });
            pulse_core::metrics::OP_THROUGHPUT
                .with_label_values(&["Map", "emit"])
                .inc();
        }
        Ok(())
    }
}

#[async_trait]
pub trait FnFilter: Send + Sync {
    async fn call(&self, value: &serde_json::Value) -> Result<bool>;
}

pub struct FilterFn<F>(pub F);
impl<F> FilterFn<F> {
    pub fn new(f: F) -> Self {
        Self(f)
    }
}
#[async_trait]
impl<F> FnFilter for FilterFn<F>
where
    F: Fn(&serde_json::Value) -> bool + Send + Sync,
{
    async fn call(&self, value: &serde_json::Value) -> Result<bool> {
        Ok((self.0)(value))
    }
}

/// Filter operator: keeps inputs that satisfy the predicate.
///
/// Example
/// ```no_run
/// use pulse_ops::{Filter, FilterFn};
/// let filter = Filter::new(FilterFn::new(|v: &serde_json::Value| v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false)));
/// # let _ = filter;
/// ```
pub struct Filter<F> {
    pred: F,
}
impl<F> Filter<F> {
    pub fn new(pred: F) -> Self {
        Self { pred }
    }
}

#[async_trait]
impl<F> Operator for Filter<F>
where
    F: FnFilter + Send + Sync + 'static,
{
    #[instrument(name = "filter_on_element", skip_all)]
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        pulse_core::metrics::OP_THROUGHPUT
            .with_label_values(&["Filter", "receive"])
            .inc();
        if self.pred.call(&rec.value).await? {
            ctx.collect(rec);
            pulse_core::metrics::OP_THROUGHPUT
                .with_label_values(&["Filter", "emit"])
                .inc();
        }
        Ok(())
    }
}

/// KeyBy operator: copies an existing field into a canonical `key` field.
///
/// Example
/// ```no_run
/// use pulse_ops::KeyBy;
/// let key_by = KeyBy::new("word");
/// # let _ = key_by;
/// ```
pub struct KeyBy {
    field: String,
}
impl KeyBy {
    pub fn new(field: impl Into<String>) -> Self {
        Self { field: field.into() }
    }
}

#[async_trait]
impl Operator for KeyBy {
    #[instrument(name = "keyby_on_element", skip_all)]
    async fn on_element(&mut self, ctx: &mut dyn Context, mut rec: Record) -> Result<()> {
        pulse_core::metrics::OP_THROUGHPUT
            .with_label_values(&["KeyBy", "receive"])
            .inc();
        let key = rec
            .value
            .get(&self.field)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let mut obj = match rec.value {
            serde_json::Value::Object(o) => o,
            _ => serde_json::Map::new(),
        };
        obj.insert("key".to_string(), key);
        rec.value = serde_json::Value::Object(obj);
        ctx.collect(rec);
        pulse_core::metrics::OP_THROUGHPUT
            .with_label_values(&["KeyBy", "emit"])
            .inc();
        Ok(())
    }
}

/// Fixed-size tumbling window helper (legacy from the simple Aggregate).
#[derive(Clone, Copy)]
pub struct WindowTumbling {
    pub size_ms: i64,
}
impl WindowTumbling {
    pub fn minutes(m: i64) -> Self {
        Self { size_ms: m * 60_000 }
    }
}

/// Simple aggregate that maintains a per-minute count per `key_field`.
/// Simple aggregate that maintains a per-minute count per `key_field`.
///
/// Example
/// ```no_run
/// use pulse_ops::Aggregate;
/// let agg = Aggregate::count_per_window("key", "word");
/// # let _ = agg;
/// ```
pub struct Aggregate {
    pub key_field: String,
    pub value_field: String,
    pub op: AggregationKind,
    windows: HashMap<(i128, serde_json::Value), i64>, // (window_start, key) -> count
}

/// Supported aggregation kinds for the simple `Aggregate`.
#[derive(Clone, Copy)]
pub enum AggregationKind {
    Count,
}

impl Aggregate {
    pub fn count_per_window(key_field: impl Into<String>, value_field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            value_field: value_field.into(),
            op: AggregationKind::Count,
            windows: HashMap::new(),
        }
    }
}

#[async_trait]
impl Operator for Aggregate {
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        let minute_ms = 60_000_i128;
        let ts_ms = rec.event_time.timestamp_millis() as i128; // ms
        let win_start_ms = (ts_ms / minute_ms) * minute_ms;
        let key = rec
            .value
            .get(&self.key_field)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        let entry = self.windows.entry((win_start_ms, key.clone())).or_insert(0);
        *entry += 1;
        // Emit current count as an update
        let mut out = serde_json::Map::new();
        out.insert("window_start_ms".into(), serde_json::json!(win_start_ms));
        out.insert("key".into(), key);
        out.insert("count".into(), serde_json::json!(*entry));
        ctx.collect(Record {
            event_time: rec.event_time,
            value: serde_json::Value::Object(out),
        });
        Ok(())
    }
    async fn on_watermark(&mut self, _ctx: &mut dyn Context, _wm: Watermark) -> Result<()> {
        Ok(())
    }
}

pub mod prelude {
    pub use super::{
        AggKind, Aggregate, AggregationKind, Filter, FnFilter, FnMap, KeyBy, Map, WindowKind, WindowTumbling,
        WindowedAggregate,
    };
}

// ===== Windowed, configurable aggregations =====

/// Kinds of windows supported by `WindowedAggregate`.
#[derive(Clone, Debug)]
pub enum WindowKind {
    Tumbling { size_ms: i64 },
    Sliding { size_ms: i64, slide_ms: i64 },
    Session { gap_ms: i64 },
}

/// Supported aggregation kinds for `WindowedAggregate`.
#[derive(Clone, Debug)]
pub enum AggKind {
    Count,
    Sum { field: String },
    Avg { field: String },
    Distinct { field: String },
}

#[derive(Clone, Debug, Default)]
enum AggState {
    #[default]
    Empty,
    Count(i64),
    Sum {
        sum: f64,
        count: i64,
    }, // count is reused for avg
    Distinct(std::collections::HashSet<String>),
}

fn as_f64(v: &serde_json::Value) -> f64 {
    match v {
        serde_json::Value::Number(n) => n.as_f64().unwrap_or(0.0),
        serde_json::Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn stringify(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// A stateful windowed aggregation operator supporting different windows & aggregations.
/// A stateful windowed aggregation operator supporting different windows & aggregations.
///
/// Examples
/// ```no_run
/// use pulse_ops::WindowedAggregate;
/// // Tumbling count of words per 60s window
/// let op = WindowedAggregate::tumbling_count("word", 60_000);
/// # let _ = op;
/// ```
pub struct WindowedAggregate {
    pub key_field: String,
    pub win: WindowKind,
    pub agg: AggKind,
    // For tumbling/sliding: (end_ms, key) -> state, and track start_ms via map
    by_window: HashMap<(i128, serde_json::Value), (i128 /*start_ms*/, AggState)>,
    // For session: key -> (start_ms, last_seen_ms, state)
    sessions: HashMap<serde_json::Value, (i128, i128, AggState)>,
    // Allowed lateness in milliseconds: postpone closing windows until wm - lateness >= end
    allowed_lateness_ms: i64,
    // Last observed watermark in ms to evaluate late events
    last_wm_ms: Option<i128>,
    late_policy: LateDataPolicy,
}

#[derive(Clone, Debug)]
enum LateDataPolicy {
    Drop,
}

impl WindowedAggregate {
    pub fn tumbling_count(key_field: impl Into<String>, size_ms: i64) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Tumbling { size_ms },
            agg: AggKind::Count,
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn tumbling_sum(key_field: impl Into<String>, size_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Tumbling { size_ms },
            agg: AggKind::Sum { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn tumbling_avg(key_field: impl Into<String>, size_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Tumbling { size_ms },
            agg: AggKind::Avg { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn tumbling_distinct(key_field: impl Into<String>, size_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Tumbling { size_ms },
            agg: AggKind::Distinct { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }

    pub fn sliding_count(key_field: impl Into<String>, size_ms: i64, slide_ms: i64) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Sliding { size_ms, slide_ms },
            agg: AggKind::Count,
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn sliding_sum(
        key_field: impl Into<String>,
        size_ms: i64,
        slide_ms: i64,
        field: impl Into<String>,
    ) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Sliding { size_ms, slide_ms },
            agg: AggKind::Sum { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn sliding_avg(
        key_field: impl Into<String>,
        size_ms: i64,
        slide_ms: i64,
        field: impl Into<String>,
    ) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Sliding { size_ms, slide_ms },
            agg: AggKind::Avg { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn sliding_distinct(
        key_field: impl Into<String>,
        size_ms: i64,
        slide_ms: i64,
        field: impl Into<String>,
    ) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Sliding { size_ms, slide_ms },
            agg: AggKind::Distinct { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }

    pub fn session_count(key_field: impl Into<String>, gap_ms: i64) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Session { gap_ms },
            agg: AggKind::Count,
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn session_sum(key_field: impl Into<String>, gap_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Session { gap_ms },
            agg: AggKind::Sum { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn session_avg(key_field: impl Into<String>, gap_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Session { gap_ms },
            agg: AggKind::Avg { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }
    pub fn session_distinct(key_field: impl Into<String>, gap_ms: i64, field: impl Into<String>) -> Self {
        Self {
            key_field: key_field.into(),
            win: WindowKind::Session { gap_ms },
            agg: AggKind::Distinct { field: field.into() },
            by_window: HashMap::new(),
            sessions: HashMap::new(),
            allowed_lateness_ms: 0,
            last_wm_ms: None,
            late_policy: LateDataPolicy::Drop,
        }
    }

    pub fn with_allowed_lateness(mut self, ms: i64) -> Self {
        self.allowed_lateness_ms = ms.max(0);
        self
    }
}

fn update_state(state: &mut AggState, agg: &AggKind, value: &serde_json::Value) {
    match agg {
        AggKind::Count => {
            *state = match std::mem::take(state) {
                AggState::Empty => AggState::Count(1),
                AggState::Count(c) => AggState::Count(c + 1),
                other => other,
            };
        }
        AggKind::Sum { field } => {
            let x = as_f64(value.get(field).unwrap_or(&serde_json::Value::Null));
            *state = match std::mem::take(state) {
                AggState::Empty => AggState::Sum { sum: x, count: 1 },
                AggState::Sum { sum, count } => AggState::Sum {
                    sum: sum + x,
                    count: count + 1,
                },
                other => other,
            };
        }
        AggKind::Avg { field } => {
            let x = as_f64(value.get(field).unwrap_or(&serde_json::Value::Null));
            *state = match std::mem::take(state) {
                AggState::Empty => AggState::Sum { sum: x, count: 1 },
                AggState::Sum { sum, count } => AggState::Sum {
                    sum: sum + x,
                    count: count + 1,
                },
                other => other,
            };
        }
        AggKind::Distinct { field } => {
            let s = stringify(value.get(field).unwrap_or(&serde_json::Value::Null));
            *state = match std::mem::take(state) {
                AggState::Empty => {
                    let mut set = std::collections::HashSet::new();
                    set.insert(s);
                    AggState::Distinct(set)
                }
                AggState::Distinct(mut set) => {
                    set.insert(s);
                    AggState::Distinct(set)
                }
                other => other,
            };
        }
    }
}

fn finalize_value(state: &AggState, agg: &AggKind) -> serde_json::Value {
    match (state, agg) {
        (AggState::Count(c), _) => serde_json::json!(*c),
        (AggState::Sum { sum, .. }, AggKind::Sum { .. }) => serde_json::json!(sum),
        (AggState::Sum { sum, count }, AggKind::Avg { .. }) => {
            let avg = if *count > 0 { *sum / (*count as f64) } else { 0.0 };
            serde_json::json!(avg)
        }
        (AggState::Distinct(set), AggKind::Distinct { .. }) => serde_json::json!(set.len() as i64),
        _ => serde_json::json!(null),
    }
}

#[async_trait]
impl Operator for WindowedAggregate {
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        let ts_ms = rec.event_time.timestamp_millis() as i128; // ms
                                                               // Late data handling: if we have a watermark and this event is older than (wm - allowed_lateness), drop
        if let Some(wm) = self.last_wm_ms {
            if ts_ms < (wm - (self.allowed_lateness_ms as i128)) {
                match self.late_policy {
                    LateDataPolicy::Drop => return Ok(()),
                }
            }
        }
        let key = rec
            .value
            .get(&self.key_field)
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        match self.win {
            WindowKind::Tumbling { size_ms } => {
                let start = (ts_ms / (size_ms as i128)) * (size_ms as i128);
                let end = start + (size_ms as i128);
                let entry = self
                    .by_window
                    .entry((end, key.clone()))
                    .or_insert((start, AggState::Empty));
                update_state(&mut entry.1, &self.agg, &rec.value);
                // Optional: schedule a timer at end
                let _ = ctx
                    .timers()
                    .register_event_time_timer(
                        pulse_core::EventTime(Utc.timestamp_millis_opt(end as i64).unwrap()),
                        None,
                    )
                    .await;
            }
            WindowKind::Sliding { size_ms, slide_ms } => {
                let k = (size_ms / slide_ms) as i128;
                let anchor = (ts_ms / (slide_ms as i128)) * (slide_ms as i128);
                for j in 0..k {
                    let start = anchor - (j * (slide_ms as i128));
                    let end = start + (size_ms as i128);
                    if start <= ts_ms && end > ts_ms {
                        let entry = self
                            .by_window
                            .entry((end, key.clone()))
                            .or_insert((start, AggState::Empty));
                        update_state(&mut entry.1, &self.agg, &rec.value);
                        let _ = ctx
                            .timers()
                            .register_event_time_timer(
                                pulse_core::EventTime(Utc.timestamp_millis_opt(end as i64).unwrap()),
                                None,
                            )
                            .await;
                    }
                }
            }
            WindowKind::Session { gap_ms } => {
                let e = self
                    .sessions
                    .entry(key.clone())
                    .or_insert((ts_ms, ts_ms, AggState::Empty));
                let (start, last_seen, state) = e;
                if ts_ms - *last_seen <= (gap_ms as i128) {
                    *last_seen = ts_ms;
                    update_state(state, &self.agg, &rec.value);
                } else {
                    // close previous session
                    let mut out = serde_json::Map::new();
                    out.insert("window_start_ms".into(), serde_json::json!(*start));
                    out.insert(
                        "window_end_ms".into(),
                        serde_json::json!(*last_seen + (gap_ms as i128)),
                    );
                    out.insert("key".into(), key.clone());
                    let val = finalize_value(state, &self.agg);
                    match self.agg {
                        AggKind::Count => {
                            out.insert("count".into(), val);
                        }
                        AggKind::Sum { .. } => {
                            out.insert("sum".into(), val);
                        }
                        AggKind::Avg { .. } => {
                            out.insert("avg".into(), val);
                        }
                        AggKind::Distinct { .. } => {
                            out.insert("distinct_count".into(), val);
                        }
                    }
                    ctx.collect(Record {
                        event_time: rec.event_time,
                        value: serde_json::Value::Object(out),
                    });
                    // start new
                    *start = ts_ms;
                    *last_seen = ts_ms;
                    *state = AggState::Empty;
                    update_state(state, &self.agg, &rec.value);
                }
                // schedule close timer
                let end = ts_ms + (gap_ms as i128);
                let _ = ctx
                    .timers()
                    .register_event_time_timer(
                        pulse_core::EventTime(Utc.timestamp_millis_opt(end as i64).unwrap()),
                        None,
                    )
                    .await;
            }
        }
        Ok(())
    }

    async fn on_watermark(&mut self, ctx: &mut dyn Context, wm: Watermark) -> Result<()> {
        let wm_ms_raw = wm.0 .0.timestamp_millis() as i128;
        let wm_ms = wm_ms_raw - (self.allowed_lateness_ms as i128);
        self.last_wm_ms = Some(wm_ms_raw);

        match self.win {
            WindowKind::Tumbling { .. } | WindowKind::Sliding { .. } => {
                // Emit and clear all windows with end <= wm
                let mut to_emit: Vec<((i128, serde_json::Value), (i128, AggState))> = self
                    .by_window
                    .iter()
                    .filter(|((end, _), _)| *end <= wm_ms)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for ((end, key), (start, state)) in to_emit.drain(..) {
                    let mut out = serde_json::Map::new();
                    out.insert("window_start_ms".into(), serde_json::json!(start));
                    out.insert("window_end_ms".into(), serde_json::json!(end));
                    out.insert("key".into(), key.clone());
                    let val = finalize_value(&state, &self.agg);
                    match self.agg {
                        AggKind::Count => {
                            out.insert("count".into(), val);
                        }
                        AggKind::Sum { .. } => {
                            out.insert("sum".into(), val);
                        }
                        AggKind::Avg { .. } => {
                            out.insert("avg".into(), val);
                        }
                        AggKind::Distinct { .. } => {
                            out.insert("distinct_count".into(), val);
                        }
                    }
                    ctx.collect(Record {
                        event_time: wm.0 .0,
                        value: serde_json::Value::Object(out),
                    });
                    self.by_window.remove(&(end, key));
                }
            }
            WindowKind::Session { gap_ms } => {
                // Close sessions whose inactivity + gap <= wm
                let keys: Vec<_> = self.sessions.keys().cloned().collect();
                for key in keys {
                    if let Some((start, last_seen, state)) = self.sessions.get(&key).cloned() {
                        if last_seen + (gap_ms as i128) <= wm_ms {
                            let mut out = serde_json::Map::new();
                            out.insert("window_start_ms".into(), serde_json::json!(start));
                            out.insert(
                                "window_end_ms".into(),
                                serde_json::json!(last_seen + (gap_ms as i128)),
                            );
                            out.insert("key".into(), key.clone());
                            let val = finalize_value(&state, &self.agg);
                            match self.agg {
                                AggKind::Count => {
                                    out.insert("count".into(), val);
                                }
                                AggKind::Sum { .. } => {
                                    out.insert("sum".into(), val);
                                }
                                AggKind::Avg { .. } => {
                                    out.insert("avg".into(), val);
                                }
                                AggKind::Distinct { .. } => {
                                    out.insert("distinct_count".into(), val);
                                }
                            }
                            ctx.collect(Record {
                                event_time: wm.0 .0,
                                value: serde_json::Value::Object(out),
                            });
                            self.sessions.remove(&key);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    async fn on_timer(
        &mut self,
        ctx: &mut dyn Context,
        when: EventTime,
        _key: Option<Vec<u8>>,
    ) -> Result<()> {
        // Treat timers same as watermarks for emission, but apply allowed lateness shift.
        let when_ms = when.0.timestamp_millis() as i128 - (self.allowed_lateness_ms as i128);

        match self.win {
            WindowKind::Tumbling { .. } | WindowKind::Sliding { .. } => {
                let mut to_emit: Vec<((i128, serde_json::Value), (i128, AggState))> = self
                    .by_window
                    .iter()
                    .filter(|((end, _), _)| *end <= when_ms)
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for ((end, key), (start, state)) in to_emit.drain(..) {
                    let mut out = serde_json::Map::new();
                    out.insert("window_start_ms".into(), serde_json::json!(start));
                    out.insert("window_end_ms".into(), serde_json::json!(end));
                    out.insert("key".into(), key.clone());
                    let val = finalize_value(&state, &self.agg);
                    match self.agg {
                        AggKind::Count => {
                            out.insert("count".into(), val);
                        }
                        AggKind::Sum { .. } => {
                            out.insert("sum".into(), val);
                        }
                        AggKind::Avg { .. } => {
                            out.insert("avg".into(), val);
                        }
                        AggKind::Distinct { .. } => {
                            out.insert("distinct_count".into(), val);
                        }
                    }
                    ctx.collect(Record {
                        event_time: when.0,
                        value: serde_json::Value::Object(out),
                    });
                    self.by_window.remove(&(end, key));
                }
            }
            WindowKind::Session { gap_ms } => {
                let keys: Vec<_> = self.sessions.keys().cloned().collect();
                for key in keys {
                    if let Some((start, last_seen, state)) = self.sessions.get(&key).cloned() {
                        if last_seen + (gap_ms as i128) <= when_ms {
                            let mut out = serde_json::Map::new();
                            out.insert("window_start_ms".into(), serde_json::json!(start));
                            out.insert(
                                "window_end_ms".into(),
                                serde_json::json!(last_seen + (gap_ms as i128)),
                            );
                            out.insert("key".into(), key.clone());
                            let val = finalize_value(&state, &self.agg);
                            match self.agg {
                                AggKind::Count => {
                                    out.insert("count".into(), val);
                                }
                                AggKind::Sum { .. } => {
                                    out.insert("sum".into(), val);
                                }
                                AggKind::Avg { .. } => {
                                    out.insert("avg".into(), val);
                                }
                                AggKind::Distinct { .. } => {
                                    out.insert("distinct_count".into(), val);
                                }
                            }
                            ctx.collect(Record {
                                event_time: when.0,
                                value: serde_json::Value::Object(out),
                            });
                            self.sessions.remove(&key);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod window_tests {
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
        out: Vec<Record>,
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

    fn record_with(ts_ms: i128, key: &str) -> Record {
        Record {
            event_time: Utc.timestamp_millis_opt(ts_ms as i64).unwrap(),
            value: serde_json::json!({"word": key}),
        }
    }

    #[tokio::test]
    async fn tumbling_count_emits_on_watermark() {
        let mut op = WindowedAggregate::tumbling_count("word", 60_000);
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        op.on_element(&mut ctx, record_with(1_000, "a")).await.unwrap();
        op.on_element(&mut ctx, record_with(1_010, "a")).await.unwrap();
        // Watermark after end of window 0..60000
        op.on_watermark(
            &mut ctx,
            Watermark(EventTime(Utc.timestamp_millis_opt(60_000).unwrap())),
        )
        .await
        .unwrap();
        assert_eq!(ctx.out.len(), 1);
        assert_eq!(ctx.out[0].value["count"], serde_json::json!(2));
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{Context, EventTime, KvState, Record, Result, Timers};
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
        out: Vec<Record>,
        kv: Arc<dyn KvState>,
        timers: Arc<dyn Timers>,
    }

    #[async_trait]
    impl Context for TestCtx {
        fn collect(&mut self, record: Record) {
            self.out.push(record);
        }
        fn watermark(&mut self, _wm: pulse_core::Watermark) {}
        fn kv(&self) -> Arc<dyn KvState> {
            self.kv.clone()
        }
        fn timers(&self) -> Arc<dyn Timers> {
            self.timers.clone()
        }
    }

    fn rec(v: serde_json::Value) -> Record {
        Record {
            event_time: Utc::now(),
            value: v,
        }
    }

    #[tokio::test]
    async fn test_map() {
        let mut op = Map::new(MapFn::new(|v| vec![v]));
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        op.on_element(&mut ctx, rec(serde_json::json!({"a":1})))
            .await
            .unwrap();
        assert_eq!(ctx.out.len(), 1);
    }

    #[tokio::test]
    async fn test_filter() {
        let mut op = Filter::new(FilterFn::new(|v: &serde_json::Value| {
            v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false)
        }));
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        op.on_element(&mut ctx, rec(serde_json::json!({"ok":false})))
            .await
            .unwrap();
        op.on_element(&mut ctx, rec(serde_json::json!({"ok":true})))
            .await
            .unwrap();
        assert_eq!(ctx.out.len(), 1);
    }

    #[tokio::test]
    async fn test_keyby() {
        let mut op = KeyBy::new("word");
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        op.on_element(&mut ctx, rec(serde_json::json!({"word":"hi"})))
            .await
            .unwrap();
        assert_eq!(ctx.out.len(), 1);
        assert_eq!(ctx.out[0].value["key"], serde_json::json!("hi"));
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let mut op = Aggregate::count_per_window("key", "word");
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        op.on_element(&mut ctx, rec(serde_json::json!({"key":"hello"})))
            .await
            .unwrap();
        op.on_element(&mut ctx, rec(serde_json::json!({"key":"hello"})))
            .await
            .unwrap();
        assert_eq!(ctx.out.len(), 2);
        assert_eq!(ctx.out[1].value["count"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn windowed_allowed_lateness_defers_emission() {
        let mut op = WindowedAggregate::tumbling_count("word", 60_000).with_allowed_lateness(30_000);
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        // Two events in first minute window
        op.on_element(&mut ctx, rec(serde_json::json!({"word":"a"})))
            .await
            .unwrap();
        op.on_element(&mut ctx, rec(serde_json::json!({"word":"a"})))
            .await
            .unwrap();
        // Watermark at window end should NOT emit due to allowed lateness of 30s
        let base = Utc::now();
        let end_ms = ((base.timestamp_millis() / 60_000) * 60_000 + 60_000) as i64;
        op.on_watermark(
            &mut ctx,
            Watermark(EventTime(Utc.timestamp_millis_opt(end_ms).unwrap())),
        )
        .await
        .unwrap();
        assert!(ctx.out.is_empty());
        // After lateness passes, emission should occur
        op.on_watermark(
            &mut ctx,
            Watermark(EventTime(Utc.timestamp_millis_opt(end_ms + 30_000).unwrap())),
        )
        .await
        .unwrap();
        assert!(!ctx.out.is_empty());
    }

    #[tokio::test]
    async fn windowed_agg_avg_and_distinct() {
        let mut avg_op = WindowedAggregate::tumbling_avg("key", 60_000, "x");
        let mut distinct_op = WindowedAggregate::tumbling_distinct("key", 60_000, "s");
        let mut ctx = TestCtx {
            out: vec![],
            kv: Arc::new(TestState),
            timers: Arc::new(TestTimers),
        };
        // feed two records in same window
        avg_op
            .on_element(&mut ctx, rec(serde_json::json!({"key":"k","x": 1})))
            .await
            .unwrap();
        avg_op
            .on_element(&mut ctx, rec(serde_json::json!({"key":"k","x": 3})))
            .await
            .unwrap();
        // watermark end of window
        let wm = pulse_core::Watermark(pulse_core::EventTime(
            Utc.timestamp_millis_opt(((Utc::now().timestamp_millis() / 60_000) * 60_000 + 60_000) as i64)
                .unwrap(),
        ));
        avg_op.on_watermark(&mut ctx, wm).await.unwrap();
        // Expect one output with avg=2.0
        assert!(ctx.out.iter().any(|r| r.value.get("avg").is_some()));
        // Reset output for distinct
        ctx.out.clear();
        distinct_op
            .on_element(&mut ctx, rec(serde_json::json!({"key":"k","s":"a"})))
            .await
            .unwrap();
        distinct_op
            .on_element(&mut ctx, rec(serde_json::json!({"key":"k","s":"a"})))
            .await
            .unwrap();
        distinct_op
            .on_element(&mut ctx, rec(serde_json::json!({"key":"k","s":"b"})))
            .await
            .unwrap();
        distinct_op.on_watermark(&mut ctx, wm).await.unwrap();
        // Expect distinct_count = 2
        assert!(ctx.out.iter().any(|r| r
            .value
            .get("distinct_count")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            == 2));
    }
}
