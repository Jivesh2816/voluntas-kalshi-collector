//! Decodes a collector output file with the template's `AlignedSbeReader`,
//! replays every book, and prints consistency checks.
//!
//! ```bash
//! cargo run --release --example inspect -- kalshi.sbe
//! ```

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Default)]
struct Book {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    snapshots: u64,
    updates: u64,
    last_update: Option<DateTime<Utc>>,
}

impl Book {
    fn crossed(&self) -> bool {
        matches!(
            (self.bids.keys().next_back(), self.asks.keys().next()),
            (Some(b), Some(a)) if b >= a
        )
    }
}

#[derive(Default)]
struct Checks {
    snapshots: u64,
    updates: u64,
    update_before_snapshot: u64,
    non_positive_snapshot_levels: u64,
    unsorted_snapshots: u64,
    out_of_range_prices: u64,
    negative_update_quantity: u64,
    crossed_after_message: u64,
    /// Snapshot that disagreed with the replayed book (only meaningful when
    /// no gap/resync happened in between).
    snapshot_mismatch: u64,
    exchange_after_local: u64,
    latency_ms: Vec<i64>,
    first: Option<DateTime<Utc>>,
    last: Option<DateTime<Utc>>,
}

fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("usage: inspect <file.sbe> [--dump-json <out.json>]"))?;
    let dump = args
        .iter()
        .position(|a| a == "--dump-json")
        .and_then(|i| args.get(i + 1).cloned());
    // Every book state, keyed by symbol, as (local time, bids, asks): lets an
    // external check ask "did the stream ever hold this exact book?".
    let history_out = args
        .iter()
        .position(|a| a == "--history-json")
        .and_then(|i| args.get(i + 1).cloned());
    let mut history: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    // Per-delta timing rows: local receive µs, exchange µs, symbol.
    let mut latency_tsv = args
        .iter()
        .position(|a| a == "--latency-tsv")
        .and_then(|i| args.get(i + 1))
        .map(std::fs::File::create)
        .transpose()?
        .map(std::io::BufWriter::new);
    let mut record = |symbol: &str, at: DateTime<Utc>, book: &Book| {
        if history_out.is_some() {
            history
                .entry(symbol.to_owned())
                .or_default()
                .push(serde_json::json!([
                    at.to_rfc3339(),
                    book.bids.iter().collect::<Vec<_>>(),
                    book.asks.iter().collect::<Vec<_>>(),
                ]));
        }
    };
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut books: HashMap<String, Book> = HashMap::new();
    let mut c = Checks::default();
    let mut samples: Vec<String> = Vec::new();

    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(d) => {
                let s = SnapshotDomain::try_from_decoder(d)?;
                c.snapshots += 1;
                track_span(&mut c, s.received_at);
                let bids: Vec<_> = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                let asks: Vec<_> = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
                if bids.windows(2).any(|w| w[0].0 <= w[1].0)
                    || asks.windows(2).any(|w| w[0].0 >= w[1].0)
                {
                    c.unsorted_snapshots += 1;
                }
                for &(p, q) in bids.iter().chain(&asks) {
                    if q <= 0 {
                        c.non_positive_snapshot_levels += 1;
                    }
                    if !(0..=1_000_000).contains(&p) {
                        c.out_of_range_prices += 1;
                    }
                }
                let book = books.entry(s.symbol.clone()).or_default();
                let new_bids: BTreeMap<_, _> = bids.into_iter().collect();
                let new_asks: BTreeMap<_, _> = asks.into_iter().collect();
                if book.snapshots > 0 && (book.bids != new_bids || book.asks != new_asks) {
                    c.snapshot_mismatch += 1;
                    eprintln!(
                        "info: snapshot replaced a different book: {} at {}",
                        s.symbol, s.received_at
                    );
                }
                book.bids = new_bids;
                book.asks = new_asks;
                book.snapshots += 1;
                if book.crossed() {
                    c.crossed_after_message += 1;
                }
                record(&s.symbol, s.received_at, book);
                if samples.len() < 3 && (!s.bids.is_empty() || !s.asks.is_empty()) {
                    samples.push(format!(
                        "Snapshot {} recv={} exch={} best_bid={:?} best_ask={:?} levels={}/{}",
                        s.symbol,
                        s.received_at,
                        s.exchange_timestamp,
                        s.bids.first().map(|l| (l.price_level, l.quantity)),
                        s.asks.first().map(|l| (l.price_level, l.quantity)),
                        s.bids.len(),
                        s.asks.len()
                    ));
                }
            }
            AnyMessage::PriceLevelUpdate(d) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(d)?;
                c.updates += 1;
                track_span(&mut c, u.backtest_timestamp);
                track_latency(&mut c, u.backtest_timestamp, u.exchange_timestamp);
                if let Some(out) = latency_tsv.as_mut() {
                    use std::io::Write;
                    writeln!(
                        out,
                        "{}	{}	{}",
                        u.backtest_timestamp.timestamp_micros(),
                        u.exchange_timestamp.timestamp_micros(),
                        u.symbol
                    )?;
                }
                if !(0..=1_000_000).contains(&u.price_level) {
                    c.out_of_range_prices += 1;
                }
                if u.quantity < 0 {
                    c.negative_update_quantity += 1;
                }
                let Some(book) = books.get_mut(&u.symbol).filter(|b| b.snapshots > 0) else {
                    c.update_before_snapshot += 1;
                    continue;
                };
                let side = if u.side == Side::Buy {
                    &mut book.bids
                } else {
                    &mut book.asks
                };
                if u.quantity == 0 {
                    side.remove(&u.price_level);
                } else {
                    side.insert(u.price_level, u.quantity);
                }
                book.updates += 1;
                book.last_update = Some(u.backtest_timestamp);
                if book.crossed() {
                    c.crossed_after_message += 1;
                }
                record(&u.symbol, u.backtest_timestamp, book);
                if samples.len() < 6 {
                    samples.push(format!(
                        "Update {} {:?} price={} qty={} recv={} exch={}",
                        u.symbol,
                        u.side,
                        u.price_level,
                        u.quantity,
                        u.backtest_timestamp,
                        u.exchange_timestamp
                    ));
                }
            }
            _ => anyhow::bail!("unknown message type"),
        }
    }

    drop(record);
    if let Some(out) = &history_out {
        std::fs::write(out, serde_json::to_string(&history)?)?;
    }
    c.latency_ms.sort_unstable();
    let pct = |p: f64| -> Option<i64> {
        (!c.latency_ms.is_empty())
            .then(|| c.latency_ms[((c.latency_ms.len() - 1) as f64 * p) as usize])
    };
    let with_updates = books.values().filter(|b| b.updates > 0).count();
    let crossed_now = books.values().filter(|b| b.crossed()).count();
    let mut busiest: Vec<_> = books.iter().map(|(s, b)| (b.updates, s.as_str())).collect();
    busiest.sort_unstable_by(|a, b| b.cmp(a));

    println!("file: {path}");
    println!("messages: {} snapshots, {} updates", c.snapshots, c.updates);
    println!(
        "symbols: {} ({} with updates); time span {:?} .. {:?}",
        books.len(),
        with_updates,
        c.first,
        c.last
    );
    println!("busiest: {:?}", &busiest[..busiest.len().min(5)]);
    println!(
        "delta recv - exchange ms: p50={:?} p90={:?} p99={:?} max={:?}",
        pct(0.5),
        pct(0.9),
        pct(0.99),
        c.latency_ms.last()
    );
    println!("checks (all should be 0):");
    println!(
        "  update_before_snapshot       {}",
        c.update_before_snapshot
    );
    println!(
        "  non_positive_snapshot_levels {}",
        c.non_positive_snapshot_levels
    );
    println!("  unsorted_snapshots           {}", c.unsorted_snapshots);
    println!("  out_of_range_prices          {}", c.out_of_range_prices);
    println!(
        "  negative_update_quantity     {}",
        c.negative_update_quantity
    );
    println!("  crossed_after_message        {}", c.crossed_after_message);
    println!("  crossed_books_at_end         {crossed_now}");
    println!(
        "info: {} exchange snapshot(s) replaced a different replayed book (e.g. mass cancel at close)",
        c.snapshot_mismatch
    );
    println!(
        "  delta_exchange_after_local   {} (clock skew if > 0)",
        c.exchange_after_local
    );
    if let Some(dump) = dump {
        let json: serde_json::Map<String, serde_json::Value> = books
            .iter()
            .map(|(symbol, b)| {
                (
                    symbol.clone(),
                    serde_json::json!({
                        "bids": b.bids.iter().collect::<Vec<_>>(),
                        "asks": b.asks.iter().collect::<Vec<_>>(),
                        "last_update": b.last_update.map(|t| t.to_rfc3339()),
                    }),
                )
            })
            .collect();
        std::fs::write(&dump, serde_json::to_string(&json)?)?;
        println!("final books written to {dump}");
    }
    println!("samples:");
    for s in samples {
        println!("  {s}");
    }
    Ok(())
}

fn track_span(c: &mut Checks, local: DateTime<Utc>) {
    c.first = Some(c.first.map_or(local, |f| f.min(local)));
    c.last = Some(c.last.map_or(local, |l| l.max(local)));
}

/// Local receive time minus exchange time, for deltas. Negative values mean
/// the local clock is behind the exchange's.
fn track_latency(c: &mut Checks, local: DateTime<Utc>, exchange: DateTime<Utc>) {
    let latency = (local - exchange).num_milliseconds();
    if latency < 0 {
        c.exchange_after_local += 1;
    }
    c.latency_ms.push(latency);
}
