//! Process-wide counters, logged periodically so a run can be audited.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};

#[derive(Debug, Default)]
pub struct Metrics {
    pub frames: AtomicU64,
    pub snapshots: AtomicU64,
    pub deltas: AtomicU64,
    pub sessions_started: AtomicU64,
    pub session_failures: AtomicU64,
    pub sequence_gaps: AtomicU64,
    pub parse_errors: AtomicU64,
    pub exchange_time_missing: AtomicU64,
    pub command_errors: AtomicU64,
    pub resync_requests: AtomicU64,
    /// Max of (local receive − Kalshi `sending_ts_ms`) since the last summary:
    /// time between Kalshi queueing a message and us reading it.
    pub max_send_lag_ms: AtomicI64,
    /// Max of (local receive − delta `ts_ms`) since the last summary: time
    /// between the book change and us reading it.
    pub max_change_lag_ms: AtomicI64,
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Relaxed);
    }

    pub fn observe_max(gauge: &AtomicI64, value: i64) {
        gauge.fetch_max(value, Relaxed);
    }

    /// Counters are cumulative; lag gauges reset on each call.
    pub fn summary(&self) -> String {
        let f = |c: &AtomicU64| c.load(Relaxed);
        format!(
            "frames={} snapshots={} deltas={} sessions={} session_failures={} seq_gaps={} \
             parse_errors={} exchange_time_missing={} command_errors={} resyncs={} \
             max_send_lag_ms={} max_change_lag_ms={}",
            f(&self.frames),
            f(&self.snapshots),
            f(&self.deltas),
            f(&self.sessions_started),
            f(&self.session_failures),
            f(&self.sequence_gaps),
            f(&self.parse_errors),
            f(&self.exchange_time_missing),
            f(&self.command_errors),
            f(&self.resync_requests),
            self.max_send_lag_ms.swap(i64::MIN, Relaxed),
            self.max_change_lag_ms.swap(i64::MIN, Relaxed),
        )
    }
}
