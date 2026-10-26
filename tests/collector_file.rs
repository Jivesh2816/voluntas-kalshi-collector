//! Drives the collector thread with the template's real `AlignedSbeWriter`,
//! then reads the file back with `AlignedSbeReader` to check the bytes on disk.

use std::time::Duration;

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::collector::{Event, run_writer_thread};
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

const CHUNK: usize = 64 << 20;

fn ts(micros: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_micros(1_700_000_000_000_000 + micros).unwrap()
}

#[derive(Debug, PartialEq)]
enum Msg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

fn read_all(path: &std::path::Path) -> Vec<Msg> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
    let mut out = Vec::new();
    while let Some(msg) = reader.read_msg().unwrap() {
        out.push(match msg {
            AnyMessage::Snapshot(d) => Msg::Snapshot(SnapshotDomain::try_from_decoder(d).unwrap()),
            AnyMessage::PriceLevelUpdate(d) => {
                Msg::Update(PriceLevelUpdateDomain::try_from_decoder(d).unwrap())
            }
            _ => panic!("unexpected message"),
        });
    }
    out
}

#[test]
fn events_round_trip_through_the_template_file_format() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = AlignedSbeWriter::create(file.path(), CHUNK).unwrap();
    let (tx, rx) = crossbeam::channel::unbounded();

    tx.send(Event::Assign {
        symbol: "KX-A".into(),
        source: 1,
    })
    .unwrap();
    tx.send(Event::Snapshot {
        source: 1,
        symbol: "KX-A".into(),
        bids: vec![(80_000, 300_000_000), (220_000, 333_000_000)],
        asks: vec![(460_000, 146_000_000)],
        received_at: ts(100),
        exchange_timestamp: ts(50),
    })
    .unwrap();
    tx.send(Event::Delta {
        source: 1,
        symbol: "KX-A".into(),
        side: Side::Buy,
        price: 220_000,
        delta: -54_000_000,
        received_at: ts(200),
        exchange_timestamp: ts(150),
    })
    .unwrap();
    drop(tx);

    let stats = run_writer_thread(
        rx,
        writer,
        Duration::from_secs(3600),
        Duration::from_secs(3600),
        |_| {},
    )
    .unwrap();
    assert_eq!((stats.snapshots_written, stats.updates_written), (1, 1));

    let msgs = read_all(file.path());
    assert_eq!(msgs.len(), 2);
    let Msg::Snapshot(snap) = &msgs[0] else {
        panic!("expected snapshot first")
    };
    assert_eq!(snap.symbol, "KX-A");
    assert_eq!(
        (snap.received_at, snap.exchange_timestamp),
        (ts(100), ts(50))
    );
    let bids: Vec<_> = snap
        .bids
        .iter()
        .map(|l| (l.price_level, l.quantity))
        .collect();
    assert_eq!(bids, [(220_000, 333_000_000), (80_000, 300_000_000)]);
    assert_eq!(
        msgs[1],
        Msg::Update(PriceLevelUpdateDomain {
            backtest_timestamp: ts(200),
            exchange_timestamp: ts(150),
            price_level: 220_000,
            quantity: 279_000_000,
            side: Side::Buy,
            symbol: "KX-A".into(),
        })
    );
}

#[test]
fn periodic_snapshots_are_written_while_running() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = AlignedSbeWriter::create(file.path(), CHUNK).unwrap();
    let (tx, rx) = crossbeam::channel::unbounded();
    let handle = std::thread::spawn(move || {
        run_writer_thread(
            rx,
            writer,
            Duration::from_millis(100),
            Duration::from_secs(3600),
            |_| {},
        )
    });

    tx.send(Event::Assign {
        symbol: "KX-B".into(),
        source: 3,
    })
    .unwrap();
    tx.send(Event::Snapshot {
        source: 3,
        symbol: "KX-B".into(),
        bids: vec![],
        asks: vec![(990_000, 1_000_000)],
        received_at: ts(1),
        exchange_timestamp: ts(0),
    })
    .unwrap();
    std::thread::sleep(Duration::from_millis(350));
    drop(tx);
    let stats = handle.join().unwrap().unwrap();
    assert!(
        stats.periodic_snapshots_written >= 2,
        "expected repeated periodic snapshots, got {stats:?}"
    );

    let msgs = read_all(file.path());
    assert_eq!(msgs.len() as u64, 1 + stats.periodic_snapshots_written);
    for msg in &msgs[1..] {
        let Msg::Snapshot(s) = msg else {
            panic!("periodic output must be snapshots")
        };
        assert_eq!(s.symbol, "KX-B");
        assert!(s.bids.is_empty());
        assert_eq!(s.asks.len(), 1);
        // Watermark of processed events, and the book's own exchange time.
        assert_eq!((s.received_at, s.exchange_timestamp), (ts(1), ts(0)));
    }
}

/// Scale check (run with `cargo test --release -- --ignored`): one periodic
/// snapshot pass over 150k books of 20 levels each.
#[test]
#[ignore]
fn periodic_snapshot_scale() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let writer = AlignedSbeWriter::create(file.path(), CHUNK).unwrap();
    let mut collector = engineering_challenge::collector::Collector::new(writer);
    for i in 0..150_000u64 {
        let symbol = format!("KXSCALE-26OCT27-T{i}");
        collector
            .handle(Event::Assign {
                symbol: symbol.clone(),
                source: 1,
            })
            .unwrap();
        collector
            .handle(Event::Snapshot {
                source: 1,
                symbol,
                bids: (1..=10).map(|p| (p * 10_000, 1_000_000)).collect(),
                asks: (60..=69).map(|p| (p * 10_000, 1_000_000)).collect(),
                received_at: ts(i as i64),
                exchange_timestamp: ts(i as i64),
            })
            .unwrap();
    }
    let started = std::time::Instant::now();
    let written = collector.write_periodic_snapshots().unwrap();
    let elapsed = started.elapsed();
    collector.flush().unwrap();
    let bytes = std::fs::metadata(file.path()).unwrap().len();
    eprintln!(
        "periodic pass: {written} books in {elapsed:?}; file {} MiB",
        bytes >> 20
    );
    assert_eq!(written, 150_000);
    assert!(elapsed < Duration::from_secs(10));
}
