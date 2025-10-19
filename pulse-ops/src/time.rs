use chrono::{DateTime, Duration, Utc};

/// Policy that defines how to compute watermarks.
/// watermark = max_observed_event_time - allowed_lateness
#[derive(Debug, Clone, Copy)]
pub struct WatermarkPolicy {
    pub allowed_lateness: Duration,
}

impl WatermarkPolicy {
    pub fn new(allowed_lateness: Duration) -> Self {
        Self { allowed_lateness }
    }
}

#[derive(Debug, Clone)]
pub struct WatermarkClock {
    policy: WatermarkPolicy,
    max_observed: Option<DateTime<Utc>>,
}

impl WatermarkClock {
    pub fn new(policy: WatermarkPolicy) -> Self {
        Self {
            policy,
            max_observed: None,
        }
    }

    pub fn observe(&mut self, ts: DateTime<Utc>) {
        self.max_observed = match self.max_observed {
            Some(max) if ts > max => Some(ts),
            None => Some(ts),
            Some(max) => Some(max),
        };
    }

    pub fn watermark(&self) -> Option<DateTime<Utc>> {
        self.max_observed
            .map(|t| t - self.policy.allowed_lateness)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clock_advances_with_observations_and_lateness() {
        let pol = WatermarkPolicy::new(Duration::seconds(10));
        let mut clk = WatermarkClock::new(pol);
        let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
        let t1 = DateTime::<Utc>::from_timestamp(1_700_000_030, 0).unwrap();
        clk.observe(t0);
        assert_eq!(clk.watermark(), Some(t0 - Duration::seconds(10)));
        clk.observe(t1);
        assert_eq!(clk.watermark(), Some(t1 - Duration::seconds(10)));
        // Out of order earlier than max shouldn't decrease WM
        clk.observe(t0 - Duration::seconds(100));
        assert_eq!(clk.watermark(), Some(t1 - Duration::seconds(10)));
    }
}
