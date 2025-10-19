use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use pulse_core::{KvState, Result, SnapshotId};

#[derive(Default)]
struct Inner {
    map: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    snapshots: std::collections::HashMap<SnapshotId, std::collections::HashMap<Vec<u8>, Vec<u8>>>,
}

#[derive(Clone)]
/// A simple in-memory `KvState` implementation backed by a `HashMap` with snapshot/restore.
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
    async fn iter_prefix(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let guard = self.0.lock();
        let mut out = Vec::new();
        if let Some(p) = prefix {
            for (k, v) in guard.map.iter() {
                if k.starts_with(p) {
                    out.push((k.clone(), v.clone()));
                }
            }
        } else {
            out.extend(guard.map.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        Ok(out)
    }
    async fn snapshot(&self) -> Result<SnapshotId> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let mut guard = self.0.lock();
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let id: SnapshotId = format!("mem-{}", ts);
        let current = guard.map.clone();
        guard.snapshots.insert(id.clone(), current);
        Ok(id)
    }
    async fn restore(&self, snapshot: SnapshotId) -> Result<()> {
        let mut guard = self.0.lock();
        if let Some(m) = guard.snapshots.get(&snapshot) {
            guard.map = m.clone();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_state_put_get_delete_iter_snapshot_restore() {
        let state = InMemoryState::default();
        let key = b"k1";
        assert!(state.get(key).await.unwrap().is_none());
        state.put(key, b"v1".to_vec()).await.unwrap();
        assert_eq!(state.get(key).await.unwrap().unwrap(), b"v1".to_vec());
        // iter
        let all = state.iter_prefix(None).await.unwrap();
        assert_eq!(all.len(), 1);
        // snapshot
        let snap = state.snapshot().await.unwrap();
        state.put(b"k2", b"v2".to_vec()).await.unwrap();
        // restore
        state.restore(snap).await.unwrap();
        assert!(state.get(b"k2").await.unwrap().is_none());
        assert_eq!(state.get(b"k1").await.unwrap().unwrap(), b"v1".to_vec());
        // delete
        state.delete(key).await.unwrap();
        assert!(state.get(key).await.unwrap().is_none());
    }
}
