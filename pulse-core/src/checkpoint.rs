use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Identifier returned by state backends to refer to a point-in-time snapshot.
pub type SnapshotId = String;

/// Minimal checkpoint metadata for resuming processing after restart.
/// Includes a source offset (opaque), current watermark, and the state snapshot id.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CheckpointMeta {
    /// Opaque source offset (e.g., file byte offset, Kafka partition@offset, etc.)
    pub source_offset: String,
    /// Last committed watermark at the time of checkpoint.
    pub current_watermark: DateTime<Utc>,
    /// Snapshot identifier returned by the KeyValueState backend.
    pub snapshot_id: SnapshotId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_meta_roundtrip() {
        let meta = CheckpointMeta {
            source_offset: "file://path:12345".to_string(),
            current_watermark: DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            snapshot_id: "mem-123".to_string(),
        };
        let s = serde_json::to_string(&meta).unwrap();
        let back: CheckpointMeta = serde_json::from_str(&s).unwrap();
        assert_eq!(meta, back);
    }
}
