//! pulse-core: Fundamental types, traits and a basic executor.
//!
//! Goal: provide the essential abstractions for a streaming pipeline:
//! - Record, EventTime, Watermark
//! - Traits: Source, Operator, Sink, Context, KvState, Timers
//! - Simple tokio-based executor
//!
//! Quick example:
//! ```no_run
//! use pulse_core::{Context, Source, Sink, Operator, Record};
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! struct MySource;
//! #[async_trait::async_trait]
//! impl Source for MySource {
//!     async fn run(&mut self, ctx: &mut dyn Context) -> anyhow::Result<()> {
//!         ctx.collect(Record::from_value("hello"));
//!         Ok(())
//!     }
//! }
//! 
//! struct MyOp;
//! #[async_trait::async_trait]
//! impl Operator for MyOp {
//!     async fn on_element(&mut self, _ctx: &mut dyn Context, _rec: Record) -> anyhow::Result<()> { Ok(()) }
//! }
//! 
//! struct MySink;
//! #[async_trait::async_trait]
//! impl Sink for MySink {
//!     async fn on_element(&mut self, _rec: Record) -> anyhow::Result<()> { Ok(()) }
//! }
//! 
//! let mut exec = pulse_core::Executor::new();
//! exec.source(MySource).operator(MyOp).sink(MySink);
//! exec.run().await?;
//! # Ok(()) }
//! ```

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventTime(pub i128); // epoch nanos

impl EventTime {
    pub fn now() -> Self {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        EventTime(now)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub event_time: EventTime,
    pub value: serde_json::Value,
}

impl Record {
    pub fn new(event_time: EventTime, value: serde_json::Value) -> Self { Self { event_time, value } }
    pub fn from_value<V: Into<serde_json::Value>>(v: V) -> Self { Self { event_time: EventTime::now(), value: v.into() } }
}

#[derive(Debug, Clone, Copy)]
pub struct Watermark(pub EventTime);

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Anyhow(#[from] anyhow::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Csv(#[from] csv::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[async_trait::async_trait]
pub trait KvState: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
}

#[async_trait::async_trait]
pub trait Timers: Send + Sync {
    async fn register_event_time_timer(&self, when: EventTime, key: Option<Vec<u8>>) -> Result<()>;
}

#[async_trait::async_trait]
pub trait Context: Send {
    fn collect(&mut self, record: Record);
    fn watermark(&mut self, wm: Watermark);
    fn kv(&self) -> Arc<dyn KvState>;
    fn timers(&self) -> Arc<dyn Timers>;
}

#[async_trait::async_trait]
pub trait Source: Send {
    async fn run(&mut self, ctx: &mut dyn Context) -> Result<()>;
}

#[async_trait::async_trait]
pub trait Operator: Send {
    async fn on_element(&mut self, ctx: &mut dyn Context, record: Record) -> Result<()>;
    async fn on_watermark(&mut self, _ctx: &mut dyn Context, _wm: Watermark) -> Result<()> { Ok(()) }
}

#[async_trait::async_trait]
pub trait Sink: Send {
    async fn on_element(&mut self, record: Record) -> Result<()>;
    async fn on_watermark(&mut self, _wm: Watermark) -> Result<()> { Ok(()) }
}

#[derive(Default)]
struct SimpleStateInner {
    map: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub struct SimpleInMemoryState(Arc<Mutex<SimpleStateInner>>);

impl Default for SimpleInMemoryState {
    fn default() -> Self { Self(Arc::new(Mutex::new(SimpleStateInner::default()))) }
}

#[async_trait::async_trait]
impl KvState for SimpleInMemoryState {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.0.lock().map.get(key).cloned())
    }
    async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.0.lock().map.insert(key.to_vec(), value);
        Ok(())
    }
    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.0.lock().map.remove(key);
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct SimpleTimers;

#[async_trait::async_trait]
impl Timers for SimpleTimers {
    async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> {
        // No-op basic impl; real executors would drive timer callbacks.
        Ok(())
    }
}

pub struct Executor {
    source: Option<Box<dyn Source>>,
    operators: Vec<Box<dyn Operator>>,
    sink: Option<Box<dyn Sink>>,
    kv: Arc<dyn KvState>,
    timers: Arc<dyn Timers>,
}

impl Executor {
    pub fn new() -> Self {
        Self {
            source: None,
            operators: Vec::new(),
            sink: None,
            kv: Arc::new(SimpleInMemoryState::default()),
            timers: Arc::new(SimpleTimers::default()),
        }
    }

    pub fn source<S: Source + 'static>(&mut self, s: S) -> &mut Self {
        self.source = Some(Box::new(s));
        self
    }

    pub fn operator<O: Operator + 'static>(&mut self, o: O) -> &mut Self {
        self.operators.push(Box::new(o));
        self
    }

    pub fn sink<K: Sink + 'static>(&mut self, s: K) -> &mut Self {
        self.sink = Some(Box::new(s));
        self
    }

    pub async fn run(&mut self) -> Result<()> {
    let kv = self.kv.clone();
    let timers = self.timers.clone();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Record>();

        struct ExecCtx {
            tx: tokio::sync::mpsc::UnboundedSender<Record>,
            kv: Arc<dyn KvState>,
            timers: Arc<dyn Timers>,
        }
        
        #[async_trait::async_trait]
        impl Context for ExecCtx {
            fn collect(&mut self, record: Record) { let _ = self.tx.send(record); }
            fn watermark(&mut self, _wm: Watermark) { /* no-op in simple executor */ }
            fn kv(&self) -> Arc<dyn KvState> { self.kv.clone() }
            fn timers(&self) -> Arc<dyn Timers> { self.timers.clone() }
        }

        let mut source = self.source.take().ok_or_else(|| anyhow::anyhow!("no source"))?;
        let mut ops = std::mem::take(&mut self.operators);
        let mut sink = self.sink.take().ok_or_else(|| anyhow::anyhow!("no sink"))?;

        // Source task
    let mut sctx = ExecCtx { tx: tx.clone(), kv: kv.clone(), timers: timers.clone() };
    let src_handle = tokio::spawn(async move { source.run(&mut sctx).await });
    // Drop our sender so that when the source is done, channel closes and op task can finish
    drop(tx);

        // Operator chain processing task
        let op_handle = tokio::spawn(async move {
            while let Some(rec) = rx.recv().await {
                // Pipe record through the operator chain collecting outputs at each step
                let mut batch = vec![rec];
                for op in ops.iter_mut() {
                    let mut next = Vec::new();
                    struct LocalCtx<'a> {
                        out: &'a mut Vec<Record>,
                        kv: Arc<dyn KvState>,
                        timers: Arc<dyn Timers>,
                    }
                    #[async_trait::async_trait]
                    impl<'a> Context for LocalCtx<'a> {
                        fn collect(&mut self, record: Record) { self.out.push(record); }
                        fn watermark(&mut self, _wm: Watermark) {}
                        fn kv(&self) -> Arc<dyn KvState> { self.kv.clone() }
                        fn timers(&self) -> Arc<dyn Timers> { self.timers.clone() }
                    }
                    for item in batch.drain(..) {
                        let mut lctx = LocalCtx { out: &mut next, kv: kv.clone(), timers: timers.clone() };
                        op.on_element(&mut lctx, item).await?;
                    }
                    batch = next;
                    if batch.is_empty() { break; }
                }
                for out in batch.into_iter() {
                    sink.on_element(out).await?;
                }
            }
            Ok::<_, Error>(())
        });

        // Await tasks
        src_handle.await.map_err(|e| Error::Anyhow(anyhow::anyhow!(e)))??;
        op_handle.await.map_err(|e| Error::Anyhow(anyhow::anyhow!(e)))??;
        Ok(())
    }
}

pub mod prelude {
    pub use super::{Context, EventTime, Executor, KvState, Operator, Record, Result, Sink, Source, Watermark};
}
