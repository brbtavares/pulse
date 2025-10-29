//! pulse-state: state layer
//! Provides `InMemoryState` and an optional `RocksDbState` backend behind the `rocksdb` feature.

pub mod mem;
#[cfg(feature = "rocksdb")]
pub mod rocks;

pub use mem::InMemoryState;
#[cfg(feature = "rocksdb")]
pub use rocks::RocksDbState;
