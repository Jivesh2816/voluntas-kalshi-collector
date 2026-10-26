//! Runs the real binary against mock Kalshi REST + WebSocket servers, stops it
//! with SIGINT, and checks the output file with the template's reader:
//!
//! * only live, non-MVE markets are subscribed, across several connections;
//! * replaying snapshots + absolute updates reproduces the exchange's books;
//! * periodic snapshots are written;
//! * a market that stops trading is unsubscribed and no longer snapshotted;
//! * SIGINT closes every WebSocket and exits cleanly with a readable file.

mod support;

use std::collections::{BTreeMap, HashMap};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use engineering_challenge::Side;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};
use support::mock_kalshi::{Inject, MockBook, MockKalshi};
use support::mock_rest::{MockRest, market};

/// Mock prices are in cents and sizes in hundredths of a contract; the
/// schema's fixed point is 1e6 per dollar / contract.
const CENT: i64 = 10_000;

#[derive(Debug)]
enum Msg {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

impl Msg {
    fn symbol(&self) -> &str {
        match self {
            Msg::Snapshot(s) => &s.symbol,
            Msg::Update(u) => &u.symbol,
        }
    }
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
            _ => panic!("unexpected message type"),
        });
    }
    out
}

type Levels = BTreeMap<i64, i64>;

/// Rebuilds each book the way a backtester would.
fn replay(msgs: &[Msg]) -> HashMap<String, (Levels, Levels)> {
    let mut books: HashMap<String, (Levels, Levels)> = HashMap::new();
    for msg in msgs {
        match msg {
            Msg::Snapshot(s) => {
                let bids = s.bids.iter().map(|l| (l.price_level, l.quantity)).collect();
                let asks = s.asks.iter().map(|l| (l.price_level, l.quantity)).collect();
                books.insert(s.symbol.clone(), (bids, asks));
            }
            Msg::Update(u) => {
                let book = books.get_mut(&u.symbol).expect("update before snapshot");
                let side = if u.side == Side::Buy {
                    &mut book.0
                } else {
                    &mut book.1
                };
                if u.quantity == 0 {
                    side.remove(&u.price_level);
                } else {
                    side.insert(u.price_level, u.quantity);
                }
            }
        }
    }
    books
}

fn expected(book: &MockBook) -> (Levels, Levels) {
    let conv = |m: &BTreeMap<i64, i64>| m.iter().map(|(p, q)| (p * CENT, q * CENT)).collect();
    (conv(&book.yes), conv(&book.no))
}

fn wait_for(what: &str, timeout: Duration, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < timeout, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn subscribed_tickers(ws: &MockKalshi) -> Vec<String> {
    let mut out = Vec::new();
    for cmd in ws.commands() {
        let is_add = cmd["cmd"] == "subscribe" || cmd["params"]["action"] == "add_markets";
        if is_add {
            for t in cmd["params"]["market_tickers"].as_array().unwrap() {
                out.push(t.as_str().unwrap().to_owned());
            }
        }
    }
    out.sort();
    out
}

#[test]
fn collects_books_and_shuts_down_cleanly_on_sigint() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let book = |yes: &[(i64, i64)], no: &[(i64, i64)]| MockBook {
        yes: yes.iter().copied().collect(),
        no: no.iter().copied().collect(),
    };
    let ws = runtime.block_on(MockKalshi::start(HashMap::from([
        (
            "KX-A".to_owned(),
            book(&[(40, 1000), (42, 500)], &[(60, 700)]),
        ),
        ("KX-B".to_owned(), book(&[(10, 100)], &[])),
    ])));
    let rest = runtime.block_on(MockRest::start(vec![
        market("KX-A", "active", None),
        market("KX-B", "active", None),
        market("KXMVE-C", "active", None),
        market("KX-D", "closed", None),
        market("KX-E", "active", Some("KXMVECOLLECTION")),
    ]));

    let dir = tempfile::tempdir().unwrap();
    let output = dir.path().join("out.sbe");
    let started_at = Utc::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_engineering-challenge"))
        .args([
            "--allow-unauthenticated",
            "--ws-url",
            &ws.url(),
            "--rest-url",
            &rest.url(),
        ])
        .args([
            "--markets-per-connection",
            "1",
            "--snapshot-interval-secs",
            "1",
        ])
        .args([
            "--incremental-interval-secs",
            "1",
            "--full-scan-interval-secs",
            "3600",
        ])
        .arg("--output")
        .arg(&output)
        .env_remove("KALSHI_API_KEY_ID")
        .env_remove("KALSHI_PRIVATE_KEY_PATH")
        .env("RUST_LOG", "info")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();

    // Two live regular markets, one per connection.
    wait_for("both subscriptions", Duration::from_secs(20), || {
        ws.state.lock().unwrap().handshakes.len() == 2 && subscribed_tickers(&ws).len() == 2
    });
    assert_eq!(
        subscribed_tickers(&ws),
        ["KX-A", "KX-B"],
        "MVEs and closed markets skipped"
    );
    std::thread::sleep(Duration::from_millis(300)); // snapshots delivered

    for (ticker, side, price_cents, delta_centi) in [
        ("KX-A", "yes", 40, -300),
        ("KX-A", "no", 61, 200),
        ("KX-A", "yes", 43, 50),
        ("KX-A", "yes", 42, -500),
        ("KX-B", "yes", 10, -40),
    ] {
        ws.inject(Inject::Delta {
            ticker: ticker.into(),
            side,
            price_cents,
            delta_centi,
        });
    }
    std::thread::sleep(Duration::from_millis(1500)); // at least one periodic snapshot

    // KX-B stops trading: the next incremental poll reports it closed.
    rest.set_markets(vec![
        market("KX-A", "active", None),
        market("KX-B", "closed", None),
    ]);
    wait_for("KX-B unsubscribed", Duration::from_secs(10), || {
        ws.commands().iter().any(|c| c["cmd"] == "unsubscribe")
    });
    std::thread::sleep(Duration::from_millis(2500)); // periodic snapshots without KX-B
    ws.inject(Inject::Delta {
        ticker: "KX-A".into(),
        side: "no",
        price_cents: 60,
        delta_centi: -100,
    });
    std::thread::sleep(Duration::from_millis(300));

    let status = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(status.success());
    let exit = {
        let start = Instant::now();
        loop {
            if let Some(exit) = child.try_wait().unwrap() {
                break exit;
            }
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "collector did not exit"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    };
    assert!(exit.success(), "collector exited with {exit:?}");
    // KX-B's connection had unsubscribed but stays open; both get closed.
    assert_eq!(
        ws.state.lock().unwrap().closes_received,
        2,
        "every WebSocket closed"
    );
    let finished_at = Utc::now();

    // ---- The file ----
    let msgs = read_all(&output);
    assert!(!msgs.is_empty());
    for symbol in ["KX-A", "KX-B"] {
        let first = msgs.iter().find(|m| m.symbol() == symbol).unwrap();
        assert!(
            matches!(first, Msg::Snapshot(_)),
            "{symbol} starts with a snapshot"
        );
    }
    assert!(
        msgs.iter()
            .all(|m| m.symbol() == "KX-A" || m.symbol() == "KX-B")
    );

    // Replayed books equal the exchange's books (B as of its removal).
    let books = replay(&msgs);
    let st = ws.state.lock().unwrap();
    assert_eq!(books["KX-A"], expected(&st.books["KX-A"]));
    assert_eq!(books["KX-B"], expected(&st.books["KX-B"]));
    drop(st);

    // Updates carry absolute quantities: the level removed by -500 is 0.
    let removed = msgs.iter().find_map(|m| match m {
        Msg::Update(u) if u.symbol == "KX-A" && u.price_level == 42 * CENT => Some(u.quantity),
        _ => None,
    });
    assert_eq!(removed, Some(0));

    // Periodic snapshots: several of A; none of B after it stopped trading.
    let snapshots_of = |s: &str| {
        msgs.iter()
            .filter(|m| matches!(m, Msg::Snapshot(x) if x.symbol == s))
            .count()
    };
    assert!(snapshots_of("KX-A") >= 4, "A snapshotted every second");
    let last_b = msgs.iter().rposition(|m| m.symbol() == "KX-B").unwrap();
    let a_snapshots_after_b = msgs[last_b..]
        .iter()
        .filter(|m| matches!(m, Msg::Snapshot(x) if x.symbol == "KX-A"))
        .count();
    assert!(a_snapshots_after_b >= 2, "snapshots continue for A only");

    // Timestamps are real and plausible.
    let window =
        |t: DateTime<Utc>| t >= started_at - chrono::Duration::seconds(1) && t <= finished_at;
    for m in &msgs {
        match m {
            Msg::Snapshot(s) => assert!(window(s.received_at) && window(s.exchange_timestamp)),
            Msg::Update(u) => {
                assert!(window(u.backtest_timestamp) && window(u.exchange_timestamp));
                // Mock stamps ts_ms just before sending; we receive after.
                assert!(u.exchange_timestamp <= u.backtest_timestamp);
            }
        }
    }
}
