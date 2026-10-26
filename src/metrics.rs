//! Process-wide counters, logged periodically so a run can be audited.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

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
}

impl Metrics {
    pub fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Relaxed);
    }

    pub fn summary(&self) -> String {
        let f = |c: &AtomicU64| c.load(Relaxed);
        format!(
            "frames={} snapshots={} deltas={} sessions={} session_failures={} seq_gaps={} \
             parse_errors={} exchange_time_missing={} command_errors={} resyncs={}",
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
        )
    }
}
