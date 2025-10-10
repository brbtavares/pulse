//! pulse-ops: standard operators built on top of pulse-core.
//! Operators: Map, Filter, KeyBy, WindowTumbling, Aggregate (simplified).

use std::collections::HashMap;

use async_trait::async_trait;
use pulse_core::{Context, Operator, Record, Result, Watermark};

#[async_trait]
pub trait FnMap: Send + Sync {
    async fn call(&self, value: serde_json::Value) -> Result<Vec<serde_json::Value>>;
}

pub struct MapFn<F>(pub F);
impl<F> MapFn<F> { pub fn new(f: F) -> Self { Self(f) } }
#[async_trait]
impl<F> FnMap for MapFn<F>
where
    F: Fn(serde_json::Value) -> Vec<serde_json::Value> + Send + Sync,
{
    async fn call(&self, value: serde_json::Value) -> Result<Vec<serde_json::Value>> { Ok((self.0)(value)) }
}

pub struct Map<F> { func: F }
impl<F> Map<F> { pub fn new(func: F) -> Self { Self { func } } }

#[async_trait]
impl<F> Operator for Map<F>
where
    F: FnMap + Send + Sync + 'static,
{
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        let outs = self.func.call(rec.value).await?;
        for v in outs { ctx.collect(Record { event_time: rec.event_time, value: v.clone() }); }
        Ok(())
    }
}

#[async_trait]
pub trait FnFilter: Send + Sync { async fn call(&self, value: &serde_json::Value) -> Result<bool>; }

pub struct FilterFn<F>(pub F);
impl<F> FilterFn<F> { pub fn new(f: F) -> Self { Self(f) } }
#[async_trait]
impl<F> FnFilter for FilterFn<F>
where
    F: Fn(&serde_json::Value) -> bool + Send + Sync,
{
    async fn call(&self, value: &serde_json::Value) -> Result<bool> { Ok((self.0)(value)) }
}

pub struct Filter<F> { pred: F }
impl<F> Filter<F> { pub fn new(pred: F) -> Self { Self { pred } } }

#[async_trait]
impl<F> Operator for Filter<F>
where
    F: FnFilter + Send + Sync + 'static,
{
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        if self.pred.call(&rec.value).await? { ctx.collect(rec); }
        Ok(())
    }
}

pub struct KeyBy { field: String }
impl KeyBy { pub fn new(field: impl Into<String>) -> Self { Self { field: field.into() } } }

#[async_trait]
impl Operator for KeyBy {
    async fn on_element(&mut self, ctx: &mut dyn Context, mut rec: Record) -> Result<()> {
        let key = rec.value.get(&self.field).cloned().unwrap_or(serde_json::Value::Null);
        let mut obj = match rec.value {
            serde_json::Value::Object(o) => o,
            _ => serde_json::Map::new(),
        };
        obj.insert("key".to_string(), key);
        rec.value = serde_json::Value::Object(obj);
        ctx.collect(rec);
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct WindowTumbling { pub size_ms: i64 }
impl WindowTumbling { pub fn minutes(m: i64) -> Self { Self { size_ms: m * 60_000 } } }

pub struct Aggregate {
    pub key_field: String,
    pub value_field: String,
    pub op: AggregationKind,
    windows: HashMap<(i128, serde_json::Value), i64>, // (window_start, key) -> count
}

#[derive(Clone, Copy)]
pub enum AggregationKind { Count }

impl Aggregate {
    pub fn count_per_window(key_field: impl Into<String>, value_field: impl Into<String>) -> Self {
        Self { key_field: key_field.into(), value_field: value_field.into(), op: AggregationKind::Count, windows: HashMap::new() }
    }
}

#[async_trait]
impl Operator for Aggregate {
    async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
        let ts = rec.event_time.0; // nanos
        let minute_ms = 60_000_i128;
        let ts_ms = ts / 1_000_000; // to ms
        let win_start_ms = (ts_ms / minute_ms) * minute_ms;
        let key = rec.value.get(&self.key_field).cloned().unwrap_or(serde_json::Value::Null);
        let entry = self.windows.entry((win_start_ms, key.clone())).or_insert(0);
        *entry += 1;
        // Emit current count as an update
        let mut out = serde_json::Map::new();
        out.insert("window_start_ms".into(), serde_json::json!(win_start_ms));
        out.insert("key".into(), key);
        out.insert("count".into(), serde_json::json!(*entry));
        ctx.collect(Record { event_time: rec.event_time, value: serde_json::Value::Object(out) });
        Ok(())
    }
    async fn on_watermark(&mut self, _ctx: &mut dyn Context, _wm: Watermark) -> Result<()> { Ok(()) }
}

pub mod prelude {
    pub use super::{Aggregate, AggregationKind, Filter, FnFilter, FnMap, KeyBy, Map, WindowTumbling};
}

#[cfg(test)]
mod tests {
    use super::*;
    use pulse_core::{Context, EventTime, KvState, Record, Result, Timers};
    use std::sync::Arc;

    struct TestState;
    #[async_trait]
    impl KvState for TestState {
        async fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> { Ok(None) }
        async fn put(&self, _key: &[u8], _value: Vec<u8>) -> Result<()> { Ok(()) }
        async fn delete(&self, _key: &[u8]) -> Result<()> { Ok(()) }
    }

    struct TestTimers;
    #[async_trait]
    impl Timers for TestTimers {
        async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> { Ok(()) }
    }

    struct TestCtx {
        out: Vec<Record>,
        kv: Arc<dyn KvState>,
        timers: Arc<dyn Timers>,
    }

    #[async_trait]
    impl Context for TestCtx {
        fn collect(&mut self, record: Record) { self.out.push(record); }
        fn watermark(&mut self, _wm: pulse_core::Watermark) {}
        fn kv(&self) -> Arc<dyn KvState> { self.kv.clone() }
        fn timers(&self) -> Arc<dyn Timers> { self.timers.clone() }
    }

    fn rec(v: serde_json::Value) -> Record { Record { event_time: EventTime::now(), value: v } }

    #[tokio::test]
    async fn test_map() {
        let mut op = Map::new(MapFn::new(|v| vec![v]));
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        op.on_element(&mut ctx, rec(serde_json::json!({"a":1}))).await.unwrap();
        assert_eq!(ctx.out.len(), 1);
    }

    #[tokio::test]
    async fn test_filter() {
    let mut op = Filter::new(FilterFn::new(|v: &serde_json::Value| v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false)));
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        op.on_element(&mut ctx, rec(serde_json::json!({"ok":false}))).await.unwrap();
        op.on_element(&mut ctx, rec(serde_json::json!({"ok":true}))).await.unwrap();
        assert_eq!(ctx.out.len(), 1);
    }

    #[tokio::test]
    async fn test_keyby() {
        let mut op = KeyBy::new("word");
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        op.on_element(&mut ctx, rec(serde_json::json!({"word":"hi"}))).await.unwrap();
        assert_eq!(ctx.out.len(), 1);
        assert_eq!(ctx.out[0].value["key"], serde_json::json!("hi"));
    }

    #[tokio::test]
    async fn test_aggregate_count() {
        let mut op = Aggregate::count_per_window("key", "word");
        let mut ctx = TestCtx { out: vec![], kv: Arc::new(TestState), timers: Arc::new(TestTimers) };
        op.on_element(&mut ctx, rec(serde_json::json!({"key":"hello"}))).await.unwrap();
        op.on_element(&mut ctx, rec(serde_json::json!({"key":"hello"}))).await.unwrap();
        assert_eq!(ctx.out.len(), 2);
        assert_eq!(ctx.out[1].value["count"], serde_json::json!(2));
    }
}
