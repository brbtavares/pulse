//! pulse-core: Fundamental types, traits and a basic executor.
//!
//! Goal: provide the essential abstractions for a streaming pipeline:
//! - Record, EventTime, Watermark
//! - Traits: Source, Operator, Sink, Context, KvState, Timers
//! - Simple tokio-based executor
//!
//! Quick example:
//! ```no_run
//! use async_trait::async_trait;
//! use pulse_core::prelude::*;
//!
//! struct MySource;
//! #[async_trait]
//! impl Source for MySource {
//!     async fn run(&mut self, ctx: &mut dyn Context) -> Result<()> {
//!         ctx.collect(Record::from_value("hello"));
//!         Ok(())
//!     }
//! }
//!
//! struct MyOp;
//! #[async_trait]
//! impl Operator for MyOp {
//!     async fn on_element(&mut self, ctx: &mut dyn Context, rec: Record) -> Result<()> {
//!         // pass-through
//!         ctx.collect(rec);
//!         Ok(())
//!     }
//! }
//!
//! struct MySink;
//! #[async_trait]
//! impl Sink for MySink {
//!     async fn on_element(&mut self, rec: Record) -> Result<()> {
//!         println!("{}", rec.value);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let mut exec = Executor::new();
//!     exec.source(MySource).operator(MyOp).sink(MySink);
//!     exec.run().await?;
//!     Ok(())
//! }
//! ```

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Logical event-time represented as Unix epoch nanoseconds.
/// Use [`EventTime::now`] for wall-clock timestamps when needed.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventTime(pub i128); // epoch nanos

impl EventTime {
    pub fn now() -> Self {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        EventTime(now)
    }
}

/// A data record flowing through the pipeline.
/// - `event_time` drives windowing and watermark/timer semantics
/// - `value` is an arbitrary JSON payload in this PoC
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub event_time: EventTime,
    pub value: serde_json::Value,
}

impl Record {
    pub fn new(event_time: EventTime, value: serde_json::Value) -> Self {
        Self { event_time, value }
    }
    pub fn from_value<V: Into<serde_json::Value>>(v: V) -> Self {
        Self {
            event_time: EventTime::now(),
            value: v.into(),
        }
    }
}

/// A low-watermark indicating no future records <= this event-time are expected.
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

/// Key-Value state abstraction for stateful operators.
#[async_trait::async_trait]
pub trait KvState: Send + Sync {
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
}

/// Timer service for event-time callbacks requested by operators.
#[async_trait::async_trait]
pub trait Timers: Send + Sync {
    async fn register_event_time_timer(&self, when: EventTime, key: Option<Vec<u8>>) -> Result<()>;
}

/// Execution context visible to Sources and Operators.
#[async_trait::async_trait]
pub trait Context: Send {
    fn collect(&mut self, record: Record);
    fn watermark(&mut self, wm: Watermark);
    fn kv(&self) -> Arc<dyn KvState>;
    fn timers(&self) -> Arc<dyn Timers>;
}

/// A data source that pushes records into the pipeline.
#[async_trait::async_trait]
pub trait Source: Send {
    async fn run(&mut self, ctx: &mut dyn Context) -> Result<()>;
}

/// Core operator interface. Override `on_watermark`/`on_timer` if needed.
#[async_trait::async_trait]
pub trait Operator: Send {
    async fn on_element(&mut self, ctx: &mut dyn Context, record: Record) -> Result<()>;
    async fn on_watermark(&mut self, _ctx: &mut dyn Context, _wm: Watermark) -> Result<()> {
        Ok(())
    }
    async fn on_timer(
        &mut self,
        _ctx: &mut dyn Context,
        _when: EventTime,
        _key: Option<Vec<u8>>,
    ) -> Result<()> {
        Ok(())
    }
}

/// A terminal sink that receives records (and optional watermarks).
#[async_trait::async_trait]
pub trait Sink: Send {
    async fn on_element(&mut self, record: Record) -> Result<()>;
    async fn on_watermark(&mut self, _wm: Watermark) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
struct SimpleStateInner {
    map: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub struct SimpleInMemoryState(Arc<Mutex<SimpleStateInner>>);

impl Default for SimpleInMemoryState {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(SimpleStateInner::default())))
    }
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

/// Minimal in-memory timer service used by the demo executor.
#[derive(Clone, Default)]
pub struct SimpleTimers;

#[async_trait::async_trait]
impl Timers for SimpleTimers {
    async fn register_event_time_timer(&self, _when: EventTime, _key: Option<Vec<u8>>) -> Result<()> {
        // No-op basic impl; real executors would drive timer callbacks.
        Ok(())
    }
}

/// A tiny, single-pipeline executor.
/// Wires: Source -> Operators -> Sink. Drives watermarks and event-time timers.
pub struct Executor {
    source: Option<Box<dyn Source>>,
    operators: Vec<Box<dyn Operator>>,
    sink: Option<Box<dyn Sink>>,
    kv: Arc<dyn KvState>,
    timers: Arc<dyn Timers>,
}

impl Executor {
    /// Create a new empty executor.
    pub fn new() -> Self {
        Self {
            source: None,
            operators: Vec::new(),
            sink: None,
            kv: Arc::new(SimpleInMemoryState::default()),
            timers: Arc::new(SimpleTimers::default()),
        }
    }

    /// Set the pipeline source.
    pub fn source<S: Source + 'static>(&mut self, s: S) -> &mut Self {
        self.source = Some(Box::new(s));
        self
    }

    /// Append an operator to the pipeline.
    pub fn operator<O: Operator + 'static>(&mut self, o: O) -> &mut Self {
        self.operators.push(Box::new(o));
        self
    }

    /// Set the pipeline sink.
    pub fn sink<K: Sink + 'static>(&mut self, s: K) -> &mut Self {
        self.sink = Some(Box::new(s));
        self
    }

    /// Run the pipeline to completion. The loop exits when the source finishes
    /// and the internal channel closes. Watermarks are propagated and due timers fired.
    pub async fn run(&mut self) -> Result<()> {
        let kv = self.kv.clone();
        let timers = self.timers.clone();

        // Shared timer queue used to schedule per-operator event-time timers
        #[derive(Clone)]
        struct TimerEntry {
            op_idx: usize,
            when: EventTime,
            key: Option<Vec<u8>>,
        }
        #[derive(Clone, Default)]
        struct SharedTimers(Arc<Mutex<Vec<TimerEntry>>>);
        impl SharedTimers {
            fn add(&self, op_idx: usize, when: EventTime, key: Option<Vec<u8>>) {
                self.0.lock().push(TimerEntry { op_idx, when, key });
            }
            fn drain_due(&self, wm: EventTime) -> Vec<TimerEntry> {
                let mut guard = self.0.lock();
                let mut fired = Vec::new();
                let mut i = 0;
                while i < guard.len() {
                    if guard[i].when.0 <= wm.0 {
                        fired.push(guard.remove(i));
                    } else {
                        i += 1;
                    }
                }
                fired
            }
        }

        enum EventMsg {
            Data(Record),
            Wm(Watermark),
        }
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<EventMsg>();

        struct ExecCtx {
            tx: tokio::sync::mpsc::UnboundedSender<EventMsg>,
            kv: Arc<dyn KvState>,
            timers: Arc<dyn Timers>,
        }

        #[async_trait::async_trait]
        impl Context for ExecCtx {
            fn collect(&mut self, record: Record) {
                let _ = self.tx.send(EventMsg::Data(record));
            }
            fn watermark(&mut self, wm: Watermark) {
                let _ = self.tx.send(EventMsg::Wm(wm));
            }
            fn kv(&self) -> Arc<dyn KvState> {
                self.kv.clone()
            }
            fn timers(&self) -> Arc<dyn Timers> {
                self.timers.clone()
            }
        }

        let mut source = self.source.take().ok_or_else(|| anyhow::anyhow!("no source"))?;
        let mut ops = std::mem::take(&mut self.operators);
        let mut sink = self.sink.take().ok_or_else(|| anyhow::anyhow!("no sink"))?;

        // Shared timers queue
        let shared_timers = SharedTimers::default();

        // Source task
        let mut sctx = ExecCtx {
            tx: tx.clone(),
            kv: kv.clone(),
            timers: timers.clone(),
        };
        let src_handle = tokio::spawn(async move { source.run(&mut sctx).await });
        // Drop our local sender so the channel closes once the source finishes (its clone will drop then)
        drop(tx);

        // Operator chain processing task
        let op_handle = tokio::spawn(async move {
            // Local Timers wrapper capturing operator index
            struct LocalTimers {
                op_idx: usize,
                shared: SharedTimers,
            }
            #[async_trait::async_trait]
            impl Timers for LocalTimers {
                async fn register_event_time_timer(
                    &self,
                    when: EventTime,
                    key: Option<Vec<u8>>,
                ) -> Result<()> {
                    self.shared.add(self.op_idx, when, key);
                    Ok(())
                }
            }

            // Local Context used for operators; collects into a Vec to be forwarded
            struct LocalCtx<'a> {
                out: &'a mut Vec<Record>,
                kv: Arc<dyn KvState>,
                timers: Arc<dyn Timers>,
            }
            #[async_trait::async_trait]
            impl<'a> Context for LocalCtx<'a> {
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

            while let Some(msg) = rx.recv().await {
                match msg {
                    EventMsg::Data(rec) => {
                        // Pipe record through the operator chain collecting outputs at each step
                        let mut batch = vec![rec];
                        for (i, op) in ops.iter_mut().enumerate() {
                            let mut next = Vec::new();
                            let timers = Arc::new(LocalTimers {
                                op_idx: i,
                                shared: shared_timers.clone(),
                            });
                            for item in batch.drain(..) {
                                let mut lctx = LocalCtx {
                                    out: &mut next,
                                    kv: kv.clone(),
                                    timers: timers.clone(),
                                };
                                op.on_element(&mut lctx, item).await?;
                            }
                            batch = next;
                            if batch.is_empty() {
                                break;
                            }
                        }
                        for out in batch.into_iter() {
                            sink.on_element(out).await?;
                        }
                    }
                    EventMsg::Wm(wm) => {
                        // Propagate watermark to operators in order, allowing them to emit
                        let mut emitted = Vec::new();
                        for (i, op) in ops.iter_mut().enumerate() {
                            let timers = Arc::new(LocalTimers {
                                op_idx: i,
                                shared: shared_timers.clone(),
                            });
                            let mut lctx = LocalCtx {
                                out: &mut emitted,
                                kv: kv.clone(),
                                timers: timers.clone(),
                            };
                            op.on_watermark(&mut lctx, wm).await?;
                        }
                        // Fire any timers due at this watermark
                        let due = shared_timers.drain_due(wm.0);
                        for t in due.into_iter() {
                            if let Some(op) = ops.get_mut(t.op_idx) {
                                let timers = Arc::new(LocalTimers {
                                    op_idx: t.op_idx,
                                    shared: shared_timers.clone(),
                                });
                                let mut lctx = LocalCtx {
                                    out: &mut emitted,
                                    kv: kv.clone(),
                                    timers: timers.clone(),
                                };
                                op.on_timer(&mut lctx, t.when, t.key.clone()).await?;
                            }
                        }
                        // Emit produced records to sink
                        for out in emitted.into_iter() {
                            sink.on_element(out).await?;
                        }
                        // Inform sink about watermark
                        sink.on_watermark(wm).await?;
                    }
                }
            }
            Ok::<_, Error>(())
        });

        // Await tasks
        src_handle
            .await
            .map_err(|e| Error::Anyhow(anyhow::anyhow!(e)))??;
        op_handle.await.map_err(|e| Error::Anyhow(anyhow::anyhow!(e)))??;
        Ok(())
    }
}

pub mod prelude {
    pub use super::{
        Context, EventTime, Executor, KvState, Operator, Record, Result, Sink, Source, Watermark,
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestSource;
    #[async_trait::async_trait]
    impl Source for TestSource {
        async fn run(&mut self, ctx: &mut dyn Context) -> Result<()> {
            ctx.collect(Record::from_value(serde_json::json!({"n":1})));
            Ok(())
        }
    }

    struct TestOp;
    #[async_trait::async_trait]
    impl Operator for TestOp {
        async fn on_element(&mut self, ctx: &mut dyn Context, mut record: Record) -> Result<()> {
            record.value["n"] = serde_json::json!(record.value["n"].as_i64().unwrap() + 1);
            ctx.collect(record);
            Ok(())
        }
    }

    struct TestSink(pub std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>);
    #[async_trait::async_trait]
    impl Sink for TestSink {
        async fn on_element(&mut self, record: Record) -> Result<()> {
            self.0.lock().unwrap().push(record.value);
            Ok(())
        }
    }

    #[tokio::test]
    async fn executor_wires_stages() {
        let mut exec = Executor::new();
        let out = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        exec.source(TestSource)
            .operator(TestOp)
            .sink(TestSink(out.clone()));
        exec.run().await.unwrap();
        let got = out.lock().unwrap().clone();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["n"], serde_json::json!(2));
    }
}
