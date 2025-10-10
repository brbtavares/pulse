//! pulse-state: state layer
//! Provides InMemoryState and optional RocksDB backend.

use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use pulse_core::{KvState, Result};

#[derive(Default)]
struct Inner {
    map: std::collections::HashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Clone)]
pub struct InMemoryState(Arc<Mutex<Inner>>);

impl Default for InMemoryState {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(Inner::default())))
    }
}

#[async_trait]
impl KvState for InMemoryState {
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

#[cfg(feature = "rocksdb")]
pub mod rocks {
    use super::*;
    use pulse_core::Result;
    pub struct RocksDbState {
        db: rocksdb::DB,
    }
    #[async_trait]
    impl KvState for RocksDbState {
        async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(self.db.get(key)?)
        }
        async fn put(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
            self.db.put(key, value)?;
            Ok(())
        }
        async fn delete(&self, key: &[u8]) -> Result<()> {
            self.db.delete(key)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_state_put_get_delete() {
        let state = InMemoryState::default();
        let key = b"k1";
        assert!(state.get(key).await.unwrap().is_none());
        state.put(key, b"v1".to_vec()).await.unwrap();
        assert_eq!(state.get(key).await.unwrap().unwrap(), b"v1".to_vec());
        state.delete(key).await.unwrap();
        assert!(state.get(key).await.unwrap().is_none());
    }
}
