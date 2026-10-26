//! Subscription management: which markets to carry, and on which connection.
//!
//! [`Planner`] is the synchronous decision-maker (unit-tested); [`Manager`]
//! runs it against discovery updates and executes its [`Action`]s by sending
//! [`Event`]s to the collector and [`Command`]s to connections.
//!
//! Rules:
//!
//! * A market is *live* while discovery reports it `active` and its
//!   `close_time` has not passed. Kalshi does not announce the implicit
//!   close at `close_time`, so we check it locally every second.
//! * A market missing from a completed full scan, and not reported active by
//!   any later update, is no longer trading.
//! * Updates carry the time their request was sent; an update older than the
//!   newest one already applied to a market is ignored, so a slow full-scan
//!   page cannot resurrect a market that an incremental poll saw close.
//! * New markets go to the least-loaded connection with room; a new
//!   connection is opened when all are full, up to `max_connections`.
//!   Markets that do not fit wait until capacity frees up.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};
use tokio_util::sync::CancellationToken;

use super::connection::{Command, Connection, ConnectionConfig};
use super::discovery::{DiscoveryUpdate, MarketInfo};
use crate::collector::{Event, ResyncRequest, SourceId};
use crate::metrics::Metrics;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Open connection number `conn` (0-based; `SourceId` is `conn + 1`).
    Spawn {
        conn: usize,
    },
    Add {
        conn: usize,
        tickers: Vec<String>,
    },
    Remove {
        conn: usize,
        tickers: Vec<String>,
    },
}

pub fn source_id(conn: usize) -> SourceId {
    conn as SourceId + 1
}

#[derive(Debug, Clone)]
struct Tracked {
    live: bool,
    conn: Option<usize>,
    close_time: Option<DateTime<Utc>>,
    last_seen_active: DateTime<Utc>,
    last_observed: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlannerStats {
    pub live: usize,
    pub assigned: usize,
    pub unassigned: usize,
    pub connections: usize,
}

pub struct Planner {
    per_connection: usize,
    max_connections: usize,
    /// Optional cap on live markets (for smoke tests).
    max_markets: Option<usize>,
    markets: HashMap<String, Tracked>,
    loads: Vec<usize>,
    unassigned: BTreeSet<String>,
    live: usize,
}

impl Planner {
    pub fn new(per_connection: usize, max_connections: usize, max_markets: Option<usize>) -> Self {
        assert!(per_connection > 0 && max_connections > 0);
        Self {
            per_connection,
            max_connections,
            max_markets,
            markets: HashMap::new(),
            loads: Vec::new(),
            unassigned: BTreeSet::new(),
            live: 0,
        }
    }

    pub fn owner(&self, ticker: &str) -> Option<usize> {
        self.markets.get(ticker).filter(|t| t.live)?.conn
    }

    pub fn stats(&self) -> PlannerStats {
        PlannerStats {
            live: self.live,
            assigned: self.live - self.unassigned.len(),
            unassigned: self.unassigned.len(),
            connections: self.loads.len(),
        }
    }

    pub fn apply_markets(
        &mut self,
        observed_at: DateTime<Utc>,
        markets: Vec<MarketInfo>,
        now: DateTime<Utc>,
    ) -> Vec<Action> {
        let mut added = Vec::new();
        let mut removed = Vec::new();
        for m in markets {
            let is_live = m.active && m.close_time.is_none_or(|c| c > now);
            match self.markets.get_mut(&m.ticker) {
                Some(t) if observed_at < t.last_observed => continue,
                Some(t) => {
                    t.last_observed = observed_at;
                    t.close_time = m.close_time;
                    if is_live {
                        t.last_seen_active = observed_at;
                        if !t.live {
                            if self.max_markets.is_some_and(|max| self.live >= max) {
                                continue;
                            }
                            t.live = true;
                            self.live += 1;
                            added.push(m.ticker);
                        }
                    } else if t.live {
                        removed.push(m.ticker);
                    }
                }
                None if is_live => {
                    if self.max_markets.is_some_and(|max| self.live >= max) {
                        continue;
                    }
                    self.markets.insert(
                        m.ticker.clone(),
                        Tracked {
                            live: true,
                            conn: None,
                            close_time: m.close_time,
                            last_seen_active: observed_at,
                            last_observed: observed_at,
                        },
                    );
                    self.live += 1;
                    added.push(m.ticker);
                }
                None => {
                    // Remember non-live markets so stale pages can't add them.
                    self.markets.insert(
                        m.ticker,
                        Tracked {
                            live: false,
                            conn: None,
                            close_time: m.close_time,
                            last_seen_active: DateTime::<Utc>::MIN_UTC,
                            last_observed: observed_at,
                        },
                    );
                }
            }
        }
        let mut actions = self.retire(removed);
        actions.extend(self.assign(added));
        actions
    }

    pub fn full_scan_complete(&mut self, started_at: DateTime<Utc>) -> Vec<Action> {
        let gone: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, t)| t.live && t.last_seen_active < started_at)
            .map(|(k, _)| k.clone())
            .collect();
        self.retire(gone)
    }

    /// Retires markets past `close_time`, places waiting markets, and forgets
    /// old non-live entries.
    pub fn sweep(&mut self, now: DateTime<Utc>) -> Vec<Action> {
        let closed: Vec<String> = self
            .markets
            .iter()
            .filter(|(_, t)| t.live && t.close_time.is_some_and(|c| c <= now))
            .map(|(k, _)| k.clone())
            .collect();
        let mut actions = self.retire(closed);
        let waiting: Vec<String> = std::mem::take(&mut self.unassigned).into_iter().collect();
        actions.extend(self.assign(waiting));
        let horizon = now - chrono::Duration::hours(1);
        self.markets
            .retain(|_, t| t.live || t.last_observed > horizon);
        actions
    }

    fn retire(&mut self, tickers: Vec<String>) -> Vec<Action> {
        let mut by_conn: HashMap<usize, Vec<String>> = HashMap::new();
        for ticker in tickers {
            let Some(t) = self.markets.get_mut(&ticker) else {
                continue;
            };
            if !t.live {
                continue;
            }
            t.live = false;
            self.live -= 1;
            match t.conn.take() {
                Some(conn) => {
                    self.loads[conn] -= 1;
                    by_conn.entry(conn).or_default().push(ticker);
                }
                None => {
                    self.unassigned.remove(&ticker);
                }
            }
        }
        let mut actions: Vec<Action> = by_conn
            .into_iter()
            .map(|(conn, tickers)| Action::Remove { conn, tickers })
            .collect();
        actions.sort_by_key(|a| match a {
            Action::Remove { conn, .. } => *conn,
            _ => unreachable!(),
        });
        actions
    }

    fn assign(&mut self, tickers: Vec<String>) -> Vec<Action> {
        let mut actions = Vec::new();
        let mut by_conn: Vec<Vec<String>> = vec![Vec::new(); self.loads.len()];
        for ticker in tickers {
            let target = self
                .loads
                .iter()
                .enumerate()
                .filter(|(_, load)| **load < self.per_connection)
                .min_by_key(|(_, load)| **load)
                .map(|(i, _)| i);
            let conn = match target {
                Some(conn) => conn,
                None if self.loads.len() < self.max_connections => {
                    let conn = self.loads.len();
                    self.loads.push(0);
                    by_conn.push(Vec::new());
                    actions.push(Action::Spawn { conn });
                    conn
                }
                None => {
                    self.unassigned.insert(ticker);
                    continue;
                }
            };
            self.loads[conn] += 1;
            self.markets.get_mut(&ticker).expect("tracked").conn = Some(conn);
            by_conn[conn].push(ticker);
        }
        for (conn, tickers) in by_conn.into_iter().enumerate() {
            if !tickers.is_empty() {
                actions.push(Action::Add { conn, tickers });
            }
        }
        actions
    }
}

struct ConnHandle {
    commands: UnboundedSender<Command>,
    task: tokio::task::JoinHandle<()>,
}

pub struct Manager {
    planner: Planner,
    conns: Vec<ConnHandle>,
    conn_config: Arc<ConnectionConfig>,
    events: Sender<Event>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
    stats_interval: Duration,
}

impl Manager {
    pub fn new(
        planner: Planner,
        conn_config: Arc<ConnectionConfig>,
        events: Sender<Event>,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
        stats_interval: Duration,
    ) -> Self {
        Self {
            planner,
            conns: Vec::new(),
            conn_config,
            events,
            metrics,
            cancel,
            stats_interval,
        }
    }

    pub async fn run(
        mut self,
        mut updates: UnboundedReceiver<DiscoveryUpdate>,
        mut resyncs: UnboundedReceiver<ResyncRequest>,
    ) {
        let mut sweep = tokio::time::interval(Duration::from_secs(1));
        let mut stats = tokio::time::interval(self.stats_interval);
        let outcome: anyhow::Result<()> = async {
            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => return Ok(()),
                    update = updates.recv() => {
                        let Some(update) = update else { return Ok(()) };
                        let actions = match update {
                            DiscoveryUpdate::Markets { observed_at, markets } => {
                                self.planner.apply_markets(observed_at, markets, Utc::now())
                            }
                            DiscoveryUpdate::FullScanComplete { started_at } => {
                                self.planner.full_scan_complete(started_at)
                            }
                        };
                        self.execute(actions)?;
                    }
                    Some(request) = resyncs.recv() => {
                        Metrics::inc(&self.metrics.resync_requests);
                        if let Some(conn) = self.planner.owner(&request.symbol)
                            && source_id(conn) == request.source
                        {
                            let _ = self.conns[conn].commands.send(Command::Resync(vec![request.symbol]));
                        }
                    }
                    _ = sweep.tick() => {
                        let actions = self.planner.sweep(Utc::now());
                        self.execute(actions)?;
                    }
                    _ = stats.tick() => {
                        let s = self.planner.stats();
                        tracing::info!(
                            live = s.live, assigned = s.assigned, unassigned = s.unassigned,
                            connections = s.connections, metrics = %self.metrics.summary(),
                            "subscription stats"
                        );
                    }
                }
            }
        }
        .await;
        if let Err(error) = outcome {
            tracing::error!(%error, "manager stopping");
            self.cancel.cancel();
        }

        // Let every connection send its Close frame before returning.
        let tasks: Vec<_> = self.conns.drain(..).map(|c| c.task).collect();
        let _ = tokio::time::timeout(
            Duration::from_secs(5),
            futures_util::future::join_all(tasks),
        )
        .await;
    }

    fn execute(&mut self, actions: Vec<Action>) -> anyhow::Result<()> {
        for action in actions {
            match action {
                Action::Spawn { conn } => {
                    debug_assert_eq!(conn, self.conns.len());
                    let (commands, commands_rx) = unbounded_channel();
                    let connection = Connection::new(
                        source_id(conn),
                        self.conn_config.clone(),
                        commands_rx,
                        self.events.clone(),
                        self.metrics.clone(),
                        self.cancel.clone(),
                    );
                    let task = tokio::spawn(connection.run());
                    self.conns.push(ConnHandle { commands, task });
                    tracing::info!(conn = source_id(conn), "opened connection");
                }
                Action::Add { conn, tickers } => {
                    // Ownership must reach the collector before the
                    // connection can deliver anything for these markets.
                    for symbol in &tickers {
                        self.send_event(Event::Assign {
                            symbol: symbol.clone(),
                            source: source_id(conn),
                        })?;
                    }
                    let _ = self.conns[conn].commands.send(Command::Add(tickers));
                }
                Action::Remove { conn, tickers } => {
                    for symbol in &tickers {
                        self.send_event(Event::Unassign {
                            symbol: symbol.clone(),
                        })?;
                    }
                    let _ = self.conns[conn].commands.send(Command::Remove(tickers));
                }
            }
        }
        Ok(())
    }

    fn send_event(&self, event: Event) -> anyhow::Result<()> {
        self.events
            .send(event)
            .map_err(|_| anyhow::anyhow!("collector stopped"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    fn market(ticker: &str, active: bool, close: Option<i64>) -> MarketInfo {
        MarketInfo {
            ticker: ticker.into(),
            active,
            close_time: close.map(t),
        }
    }

    fn names(n: usize) -> Vec<MarketInfo> {
        (0..n)
            .map(|i| market(&format!("M{i}"), true, None))
            .collect()
    }

    #[test]
    fn spreads_markets_over_connections() {
        let mut p = Planner::new(2, 10, None);
        let actions = p.apply_markets(t(0), names(5), t(0));
        assert_eq!(
            actions,
            [
                Action::Spawn { conn: 0 },
                Action::Spawn { conn: 1 },
                Action::Spawn { conn: 2 },
                Action::Add {
                    conn: 0,
                    tickers: vec!["M0".into(), "M1".into()]
                },
                Action::Add {
                    conn: 1,
                    tickers: vec!["M2".into(), "M3".into()]
                },
                Action::Add {
                    conn: 2,
                    tickers: vec!["M4".into()]
                },
            ]
        );
        assert_eq!(p.owner("M4"), Some(2));
        // Duplicates are not re-added.
        assert!(p.apply_markets(t(1), names(5), t(1)).is_empty());
    }

    #[test]
    fn respects_connection_limit_and_fills_freed_capacity() {
        let mut p = Planner::new(1, 2, None);
        p.apply_markets(t(0), names(3), t(0));
        assert_eq!(
            p.stats(),
            PlannerStats {
                live: 3,
                assigned: 2,
                unassigned: 1,
                connections: 2
            }
        );
        let actions = p.apply_markets(t(1), vec![market("M0", false, None)], t(1));
        assert_eq!(
            actions,
            [Action::Remove {
                conn: 0,
                tickers: vec!["M0".into()]
            }]
        );
        assert_eq!(
            p.sweep(t(2)),
            [Action::Add {
                conn: 0,
                tickers: vec!["M2".into()]
            }]
        );
        assert_eq!(p.stats().unassigned, 0);
    }

    #[test]
    fn close_time_retires_market_locally() {
        let mut p = Planner::new(10, 10, None);
        p.apply_markets(t(0), vec![market("A", true, Some(100))], t(0));
        assert!(p.sweep(t(99)).is_empty());
        assert_eq!(
            p.sweep(t(100)),
            [Action::Remove {
                conn: 0,
                tickers: vec!["A".into()]
            }]
        );
        // Already past close: never subscribed.
        assert!(
            p.apply_markets(t(101), vec![market("B", true, Some(50))], t(101))
                .is_empty()
        );
    }

    #[test]
    fn full_scan_retires_markets_not_seen_since_it_started() {
        let mut p = Planner::new(10, 10, None);
        p.apply_markets(t(0), names(3), t(0));
        // Scan starts at 10; M0 seen by the scan, M1 by an incremental poll.
        p.apply_markets(t(10), vec![market("M0", true, None)], t(10));
        p.apply_markets(t(12), vec![market("M1", true, None)], t(12));
        assert_eq!(
            p.full_scan_complete(t(10)),
            [Action::Remove {
                conn: 0,
                tickers: vec!["M2".into()]
            }]
        );
    }

    #[test]
    fn stale_updates_are_ignored() {
        let mut p = Planner::new(10, 10, None);
        p.apply_markets(t(0), vec![market("A", true, None)], t(0));
        p.apply_markets(t(20), vec![market("A", false, None)], t(20));
        // A full-scan page requested at t=10 arrives late, still saying active.
        assert!(
            p.apply_markets(t(10), vec![market("A", true, None)], t(21))
                .is_empty()
        );
        assert_eq!(p.owner("A"), None);
        // A genuinely newer update reopens it.
        assert_eq!(
            p.apply_markets(t(30), vec![market("A", true, None)], t(30)),
            [Action::Add {
                conn: 0,
                tickers: vec!["A".into()]
            }]
        );
    }

    #[test]
    fn market_cap_limits_live_markets() {
        let mut p = Planner::new(10, 10, Some(2));
        p.apply_markets(t(0), names(5), t(0));
        assert_eq!(p.stats().live, 2);
    }
}
