//! The collector core: owns every order book and the output writer.
//!
//! Exchange adapters turn wire messages into [`Event`]s (already converted to
//! fixed-point, with sides mapped to [`Side`]) and send them over one channel.
//! A single thread runs [`run_writer_thread`], which applies each event to its
//! book and writes the result with the template's [`AlignedSbeWriter`]:
//!
//! * exchange snapshot -> replace the book, write a `Snapshot`;
//! * exchange delta    -> apply it, write a `PriceLevelUpdate` carrying the
//!   level's new *absolute* quantity;
//! * every snapshot interval -> write a `Snapshot` of every live book.
//!
//! Keeping all book state and all writes on one thread means no locks and a
//! total order in the output file that matches the order events were applied.
//!
//! ## Source ownership
//!
//! Each symbol is owned by exactly one source (WebSocket connection) at a
//! time, set with [`Event::Assign`] before that source subscribes. Events from
//! any other source are ignored. This protects a book from stale messages that
//! were still in flight on an old connection when a symbol moved, and is the
//! hook for switching between redundant connections.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossbeam::channel::{Receiver, select, tick};

use crate::Side;
use crate::book::{BookError, OrderBook};
use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Identifies one upstream connection. Never reused within a process.
pub type SourceId = u64;

#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// `source` becomes the only accepted source for `symbol`; any existing
    /// book is discarded until that source delivers a snapshot.
    Assign { symbol: String, source: SourceId },
    /// Stop tracking `symbol` (it stopped trading or was unsubscribed).
    Unassign { symbol: String },
    /// `source` lost continuity (disconnect, sequence gap). Every book it
    /// owns is discarded until it delivers fresh snapshots.
    SourceReset { source: SourceId },
    /// Full book from the exchange. Levels are `(price, absolute quantity)`.
    Snapshot {
        source: SourceId,
        symbol: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
    /// A quantity *change* at one level.
    Delta {
        source: SourceId,
        symbol: String,
        side: Side,
        price: i64,
        delta: i64,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    },
}

/// Ask the owning source to send a fresh snapshot for `symbol`, because our
/// copy of the book was found to be inconsistent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResyncRequest {
    pub source: SourceId,
    pub symbol: String,
}

/// Where the collector writes. Implemented for the template's
/// [`AlignedSbeWriter`]; tests can substitute an in-memory sink.
pub trait OrderbookSink {
    fn write_snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()>;
    fn write_update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()>;
    fn flush(&mut self) -> anyhow::Result<()>;
}

impl OrderbookSink for AlignedSbeWriter {
    fn write_snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()> {
        self.write_msg(msg)
    }

    fn write_update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
        self.write_msg(msg)
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        AlignedSbeWriter::flush(self)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Stats {
    pub snapshots_written: u64,
    pub periodic_snapshots_written: u64,
    pub updates_written: u64,
    /// Events from a source that does not own the symbol (or unknown symbol).
    pub ignored_foreign: u64,
    /// Deltas for an owned symbol whose snapshot has not arrived yet.
    pub ignored_awaiting_snapshot: u64,
    /// Snapshots or deltas that contradicted our book; a resync was requested.
    pub inconsistencies: u64,
    /// Writes that left the book crossed (best bid >= best ask).
    pub crossed_after_write: u64,
    /// Deltas whose exchange time is later than their local receive time.
    /// A message can't arrive before it was sent, so a rising count means the
    /// local clock is behind the exchange's (check NTP).
    pub exchange_after_receive: u64,
}

struct SymbolState {
    owner: SourceId,
    book: Option<LiveBook>,
}

struct LiveBook {
    book: OrderBook,
    /// Exchange time of the newest change reflected in `book`.
    exchange_timestamp: DateTime<Utc>,
}

pub struct Collector<S: OrderbookSink> {
    sink: S,
    symbols: HashMap<String, SymbolState>,
    /// Newest `received_at` among processed events. Every book reflects every
    /// event this collector has processed, so this is the local time at which
    /// the books' contents are known to be current.
    processed_through: Option<DateTime<Utc>>,
    stats: Stats,
}

impl<S: OrderbookSink> Collector<S> {
    pub fn new(sink: S) -> Self {
        Self {
            sink,
            symbols: HashMap::new(),
            processed_through: None,
            stats: Stats::default(),
        }
    }

    pub fn stats(&self) -> &Stats {
        &self.stats
    }

    pub fn tracked_symbols(&self) -> usize {
        self.symbols.len()
    }

    pub fn live_books(&self) -> usize {
        self.symbols.values().filter(|s| s.book.is_some()).count()
    }

    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.symbols.get(symbol)?.book.as_ref().map(|b| &b.book)
    }

    /// Applies one event. Returns a resync request if the event revealed that
    /// our copy of a book is wrong. Errors only on write failure.
    pub fn handle(&mut self, event: Event) -> anyhow::Result<Option<ResyncRequest>> {
        match event {
            Event::Assign { symbol, source } => {
                self.symbols.insert(
                    symbol,
                    SymbolState {
                        owner: source,
                        book: None,
                    },
                );
            }
            Event::Unassign { symbol } => {
                self.symbols.remove(&symbol);
            }
            Event::SourceReset { source } => {
                for state in self.symbols.values_mut() {
                    if state.owner == source {
                        state.book = None;
                    }
                }
            }
            Event::Snapshot {
                source,
                symbol,
                bids,
                asks,
                received_at,
                exchange_timestamp,
            } => {
                self.observe(received_at);
                let Some(state) = self.symbols.get_mut(&symbol).filter(|s| s.owner == source)
                else {
                    self.stats.ignored_foreign += 1;
                    return Ok(None);
                };
                match OrderBook::from_levels(bids, asks) {
                    Ok(book) => {
                        let msg = book.to_snapshot(&symbol, received_at, exchange_timestamp);
                        if book.is_crossed() {
                            self.stats.crossed_after_write += 1;
                        }
                        state.book = Some(LiveBook {
                            book,
                            exchange_timestamp,
                        });
                        self.sink.write_snapshot(&msg)?;
                        self.stats.snapshots_written += 1;
                    }
                    Err(error) => {
                        tracing::warn!(%symbol, %error, "rejected exchange snapshot");
                        state.book = None;
                        self.stats.inconsistencies += 1;
                        return Ok(Some(ResyncRequest { source, symbol }));
                    }
                }
            }
            Event::Delta {
                source,
                symbol,
                side,
                price,
                delta,
                received_at,
                exchange_timestamp,
            } => {
                self.observe(received_at);
                let Some(state) = self.symbols.get_mut(&symbol).filter(|s| s.owner == source)
                else {
                    self.stats.ignored_foreign += 1;
                    return Ok(None);
                };
                let Some(live) = state.book.as_mut() else {
                    self.stats.ignored_awaiting_snapshot += 1;
                    return Ok(None);
                };
                match live.book.apply_delta(side, price, delta) {
                    Ok(quantity) => {
                        live.exchange_timestamp = exchange_timestamp;
                        if exchange_timestamp > received_at {
                            self.stats.exchange_after_receive += 1;
                        }
                        if live.book.is_crossed() {
                            self.stats.crossed_after_write += 1;
                        }
                        self.sink.write_update(&PriceLevelUpdateDomain {
                            backtest_timestamp: received_at,
                            exchange_timestamp,
                            price_level: price,
                            quantity,
                            side,
                            symbol,
                        })?;
                        self.stats.updates_written += 1;
                    }
                    Err(
                        error @ (BookError::NegativeQuantity { .. } | BookError::Overflow { .. }),
                    ) => {
                        tracing::warn!(%symbol, %error, "delta inconsistent with book; resyncing");
                        state.book = None;
                        self.stats.inconsistencies += 1;
                        return Ok(Some(ResyncRequest { source, symbol }));
                    }
                    Err(error) => unreachable!("apply_delta cannot return {error}"),
                }
            }
        }
        Ok(None)
    }

    /// Writes a snapshot of every live book.
    ///
    /// `received_at` is the newest receive time among processed events: the
    /// local time as of which these books are known to be complete.
    /// `exchange_timestamp` is the exchange time of the newest change in that
    /// particular book.
    pub fn write_periodic_snapshots(&mut self) -> anyhow::Result<usize> {
        let Some(received_at) = self.processed_through else {
            return Ok(0);
        };
        let mut written = 0;
        for (symbol, state) in &self.symbols {
            if let Some(live) = &state.book {
                let msg = live
                    .book
                    .to_snapshot(symbol, received_at, live.exchange_timestamp);
                self.sink.write_snapshot(&msg)?;
                written += 1;
            }
        }
        self.stats.periodic_snapshots_written += written as u64;
        Ok(written)
    }

    pub fn flush(&mut self) -> anyhow::Result<()> {
        self.sink.flush()
    }

    pub fn into_sink(self) -> S {
        self.sink
    }

    fn observe(&mut self, received_at: DateTime<Utc>) {
        if self.processed_through.is_none_or(|t| received_at > t) {
            self.processed_through = Some(received_at);
        }
    }
}

/// Runs the collector until every sender of `events` is dropped, then drains
/// the channel, flushes and returns the final stats.
///
/// A snapshot of every live book is written every `snapshot_interval`.
/// `on_resync` is called for each [`ResyncRequest`]; it must not block.
pub fn run_writer_thread<S: OrderbookSink>(
    events: Receiver<Event>,
    sink: S,
    snapshot_interval: Duration,
    stats_interval: Duration,
    mut on_resync: impl FnMut(ResyncRequest),
) -> anyhow::Result<Stats> {
    let mut collector = Collector::new(sink);
    let snapshot_ticker = tick(snapshot_interval);
    let stats_ticker = tick(stats_interval);

    let result = (|| -> anyhow::Result<()> {
        loop {
            select! {
                recv(events) -> event => match event {
                    Ok(event) => {
                        if let Some(request) = collector.handle(event)? {
                            on_resync(request);
                        }
                    }
                    // All senders dropped and the channel is empty.
                    Err(_) => return Ok(()),
                },
                recv(snapshot_ticker) -> _ => {
                    let started = std::time::Instant::now();
                    let written = collector.write_periodic_snapshots()?;
                    tracing::info!(
                        books = written,
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        backlog = events.len(),
                        "wrote periodic snapshots"
                    );
                }
                recv(stats_ticker) -> _ => {
                    tracing::info!(
                        tracked = collector.tracked_symbols(),
                        live = collector.live_books(),
                        backlog = events.len(),
                        stats = ?collector.stats(),
                        "collector stats"
                    );
                }
            }
        }
    })();

    // Flush whatever was written even if a later write failed, so the file
    // keeps every complete message.
    let flushed = collector.flush();
    result?;
    flushed?;
    Ok(collector.stats().clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct VecSink {
        snapshots: Vec<SnapshotDomain>,
        updates: Vec<PriceLevelUpdateDomain>,
        flushes: usize,
    }

    impl OrderbookSink for VecSink {
        fn write_snapshot(&mut self, msg: &SnapshotDomain) -> anyhow::Result<()> {
            self.snapshots.push(msg.clone());
            Ok(())
        }
        fn write_update(&mut self, msg: &PriceLevelUpdateDomain) -> anyhow::Result<()> {
            self.updates.push(msg.clone());
            Ok(())
        }
        fn flush(&mut self) -> anyhow::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    fn ts(micros: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000 + micros).unwrap()
    }

    fn assign(symbol: &str, source: SourceId) -> Event {
        Event::Assign {
            symbol: symbol.into(),
            source,
        }
    }

    fn snapshot(symbol: &str, source: SourceId, at: i64) -> Event {
        Event::Snapshot {
            source,
            symbol: symbol.into(),
            bids: vec![(400_000, 10_000_000)],
            asks: vec![(600_000, 5_000_000)],
            received_at: ts(at),
            exchange_timestamp: ts(at - 1),
        }
    }

    fn delta(symbol: &str, source: SourceId, side: Side, price: i64, delta: i64, at: i64) -> Event {
        Event::Delta {
            source,
            symbol: symbol.into(),
            side,
            price,
            delta,
            received_at: ts(at),
            exchange_timestamp: ts(at - 1),
        }
    }

    #[test]
    fn snapshot_then_deltas_write_absolute_quantities() {
        let mut c = Collector::new(VecSink::default());
        c.handle(assign("A", 1)).unwrap();
        c.handle(snapshot("A", 1, 10)).unwrap();
        c.handle(delta("A", 1, Side::Buy, 400_000, -3_000_000, 20))
            .unwrap();
        c.handle(delta("A", 1, Side::Sell, 600_000, -5_000_000, 30))
            .unwrap();

        let sink = c.into_sink();
        assert_eq!(sink.snapshots.len(), 1);
        assert_eq!(sink.snapshots[0].received_at, ts(10));
        assert_eq!(sink.snapshots[0].exchange_timestamp, ts(9));
        let updates: Vec<_> = sink
            .updates
            .iter()
            .map(|u| {
                (
                    u.side,
                    u.price_level,
                    u.quantity,
                    u.backtest_timestamp,
                    u.exchange_timestamp,
                )
            })
            .collect();
        assert_eq!(
            updates,
            [
                (Side::Buy, 400_000, 7_000_000, ts(20), ts(19)),
                (Side::Sell, 600_000, 0, ts(30), ts(29)),
            ]
        );
    }

    #[test]
    fn ignores_unassigned_and_foreign_sources() {
        let mut c = Collector::new(VecSink::default());
        c.handle(snapshot("A", 1, 1)).unwrap();
        c.handle(assign("A", 2)).unwrap();
        c.handle(snapshot("A", 1, 2)).unwrap();
        c.handle(delta("A", 1, Side::Buy, 1, 1, 3)).unwrap();
        assert_eq!(c.stats().ignored_foreign, 3);
        assert!(c.into_sink().snapshots.is_empty());
    }

    #[test]
    fn deltas_before_snapshot_are_dropped() {
        let mut c = Collector::new(VecSink::default());
        c.handle(assign("A", 1)).unwrap();
        c.handle(delta("A", 1, Side::Buy, 1, 1, 1)).unwrap();
        assert_eq!(c.stats().ignored_awaiting_snapshot, 1);
        assert!(c.into_sink().updates.is_empty());
    }

    #[test]
    fn inconsistent_delta_drops_book_and_requests_resync() {
        let mut c = Collector::new(VecSink::default());
        c.handle(assign("A", 7)).unwrap();
        c.handle(snapshot("A", 7, 1)).unwrap();
        let resync = c
            .handle(delta("A", 7, Side::Buy, 400_000, -11_000_000, 2))
            .unwrap();
        assert_eq!(
            resync,
            Some(ResyncRequest {
                source: 7,
                symbol: "A".into()
            })
        );
        assert!(c.book("A").is_none());
        // Further deltas wait for the new snapshot.
        c.handle(delta("A", 7, Side::Buy, 400_000, 1, 3)).unwrap();
        assert_eq!(c.stats().ignored_awaiting_snapshot, 1);
        c.handle(snapshot("A", 7, 4)).unwrap();
        assert!(c.book("A").is_some());
        assert!(c.into_sink().updates.is_empty());
    }

    #[test]
    fn source_reset_only_drops_that_sources_books() {
        let mut c = Collector::new(VecSink::default());
        for (symbol, source) in [("A", 1), ("B", 2)] {
            c.handle(assign(symbol, source)).unwrap();
            c.handle(snapshot(symbol, source, 1)).unwrap();
        }
        c.handle(Event::SourceReset { source: 1 }).unwrap();
        assert!(c.book("A").is_none());
        assert!(c.book("B").is_some());
        assert_eq!(c.tracked_symbols(), 2);
    }

    #[test]
    fn unassign_stops_tracking() {
        let mut c = Collector::new(VecSink::default());
        c.handle(assign("A", 1)).unwrap();
        c.handle(snapshot("A", 1, 1)).unwrap();
        c.handle(Event::Unassign { symbol: "A".into() }).unwrap();
        assert_eq!(c.tracked_symbols(), 0);
        assert_eq!(c.write_periodic_snapshots().unwrap(), 0);
    }

    #[test]
    fn periodic_snapshots_use_processed_watermark_and_book_exchange_time() {
        let mut c = Collector::new(VecSink::default());
        assert_eq!(c.write_periodic_snapshots().unwrap(), 0);
        c.handle(assign("A", 1)).unwrap();
        c.handle(assign("B", 1)).unwrap();
        c.handle(assign("C", 1)).unwrap(); // never gets a snapshot
        c.handle(snapshot("A", 1, 10)).unwrap();
        c.handle(snapshot("B", 1, 20)).unwrap();
        c.handle(delta("A", 1, Side::Buy, 400_000, 1, 30)).unwrap();

        assert_eq!(c.write_periodic_snapshots().unwrap(), 2);
        let sink = c.into_sink();
        let mut periodic: Vec<_> = sink.snapshots[2..]
            .iter()
            .map(|s| {
                (
                    s.symbol.as_str(),
                    s.received_at,
                    s.exchange_timestamp,
                    s.bids[0].quantity,
                )
            })
            .collect();
        periodic.sort();
        assert_eq!(
            periodic,
            [
                ("A", ts(30), ts(29), 10_000_001),
                ("B", ts(30), ts(19), 10_000_000)
            ]
        );
    }

    #[test]
    fn writer_thread_drains_and_flushes_on_disconnect() {
        let (tx, rx) = crossbeam::channel::unbounded();
        tx.send(assign("A", 1)).unwrap();
        tx.send(snapshot("A", 1, 1)).unwrap();
        tx.send(delta("A", 1, Side::Buy, 400_000, 1, 2)).unwrap();
        drop(tx);
        let stats = run_writer_thread(
            rx,
            VecSink::default(),
            Duration::from_secs(3600),
            Duration::from_secs(3600),
            |_| panic!("no resync expected"),
        )
        .unwrap();
        assert_eq!(stats.snapshots_written, 1);
        assert_eq!(stats.updates_written, 1);
    }
}
