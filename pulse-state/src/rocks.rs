#![cfg(feature = "rocksdb")]
use async_trait::async_trait;
use pulse_core::{KvState, Result, SnapshotId};

/// A RocksDB-backed `KvState` (enable with `--features rocksdb`).
pub struct RocksDbState {
    db: rocksdb::DB,
}

impl RocksDbState {
    pub fn open(path: &str) -> Result<Self> {
        let db = rocksdb::DB::open_default(path)?;
        Ok(Self { db })
    }
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
    async fn iter_prefix(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        use rocksdb::DBIterator;
        let mut out = Vec::new();
        match prefix {
            Some(p) => {
                let mode = rocksdb::IteratorMode::From(p, rocksdb::Direction::Forward);
                let iter: DBIterator<'_> = self.db.iterator(mode);
                for item in iter {
                    let (k, v) = item?;
                    if k.starts_with(p) {
                        out.push((k.to_vec(), v.to_vec()));
                    } else {
                        break;
                    }
                }
            }
            None => {
                for item in self.db.iterator(rocksdb::IteratorMode::Start) {
                    let (k, v) = item?;
                    out.push((k.to_vec(), v.to_vec()));
                }
            }
        }
        Ok(out)
    }
    async fn snapshot(&self) -> Result<SnapshotId> {
        // For simplicity: create a checkpoint directory name and return it; caller responsible to persist mapping
        // Real-world would use rocksdb::checkpoint::Checkpoint
        use std::time::{SystemTime, UNIX_EPOCH};
        let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis();
        let id = format!("rocks-{}", ts);
        let cp = rocksdb::checkpoint::Checkpoint::new(&self.db)?;
        let dir = format!("_cp_{}", &id);
        cp.create_checkpoint(&dir)?;
        Ok(id)
    }
    async fn restore(&self, _snapshot: SnapshotId) -> Result<()> {
        // Not supported in-place for an open DB; in practice, one would reopen from checkpoint dir.
        // We no-op here and document the limitation for this demo.
        Ok(())
    }
}
