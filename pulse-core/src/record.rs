use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Core data record with explicit event-time (event-time semantics).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub event_time: DateTime<Utc>,
    pub value: serde_json::Value,
}

impl Record {
    pub fn new(event_time: DateTime<Utc>, value: serde_json::Value) -> Self {
        Self { event_time, value }
    }
    pub fn from_value<V: Into<serde_json::Value>>(v: V) -> Self {
        Self {
            event_time: chrono::Utc::now(),
            value: v.into(),
        }
    }
}
