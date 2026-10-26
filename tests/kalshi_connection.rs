//! Exercises one `Connection` against the in-process mock Kalshi server.

mod support;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use crossbeam::channel::Receiver;
use engineering_challenge::Side;
use engineering_challenge::collector::Event;
use engineering_challenge::kalshi::auth::Signer;
use engineering_challenge::kalshi::connection::{Command, Connection, ConnectionConfig};
use engineering_challenge::metrics::Metrics;
use ring::signature::{ED25519, Ed25519KeyPair, KeyPair, UnparsedPublicKey};
use support::mock_kalshi::{Inject, MockBook, MockKalshi};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

struct Harness {
    mock: MockKalshi,
    events: Receiver<Event>,
    commands: UnboundedSender<Command>,
    cancel: CancellationToken,
    task: tokio::task::JoinHandle<()>,
    public_key: Vec<u8>,
    metrics: Arc<Metrics>,
}

fn book(yes: &[(i64, i64)], no: &[(i64, i64)]) -> MockBook {
    MockBook {
        yes: yes.iter().copied().collect(),
        no: no.iter().copied().collect(),
    }
}

async fn start(books: HashMap<String, MockBook>) -> Harness {
    let mock = MockKalshi::start(books).await;
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&ring::rand::SystemRandom::new()).unwrap();
    let public_key = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref())
        .unwrap()
        .public_key()
        .as_ref()
        .to_vec();
    let pem = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        base64::engine::general_purpose::STANDARD.encode(pkcs8.as_ref())
    );
    let signer = Arc::new(Signer::from_pem("test-key-id", &pem).unwrap());
    let mut config = ConnectionConfig::new(mock.url(), Some(signer));
    config.initial_backoff = Duration::from_millis(20);
    config.max_backoff = Duration::from_millis(100);
    config.snapshot_timeout = Duration::from_millis(1500);

    let (events_tx, events) = crossbeam::channel::unbounded();
    let (commands, commands_rx) = tokio::sync::mpsc::unbounded_channel();
    let cancel = CancellationToken::new();
    let metrics = Arc::new(Metrics::default());
    let conn = Connection::new(
        7,
        Arc::new(config),
        commands_rx,
        events_tx,
        metrics.clone(),
        cancel.clone(),
    );
    let task = tokio::spawn(conn.run());
    Harness {
        mock,
        events,
        commands,
        cancel,
        task,
        public_key,
        metrics,
    }
}

fn next(events: &Receiver<Event>) -> Event {
    events
        .recv_timeout(Duration::from_secs(5))
        .expect("timed out waiting for event")
}

fn snapshot_symbol(event: &Event) -> &str {
    match event {
        Event::Snapshot { symbol, .. } => symbol,
        other => panic!("expected snapshot, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signed_handshake_snapshots_and_deltas() {
    let h = start(HashMap::from([(
        "MKT-A".to_owned(),
        book(&[(8, 30_000), (22, 33_300)], &[(54, 2_000)]),
    )]))
    .await;
    h.commands.send(Command::Add(vec!["MKT-A".into()])).unwrap();

    let Event::Snapshot {
        source,
        symbol,
        bids,
        asks,
        ..
    } = next(&h.events)
    else {
        panic!("expected snapshot")
    };
    assert_eq!((source, symbol.as_str()), (7, "MKT-A"));
    assert_eq!(bids, [(80_000, 300_000_000), (220_000, 333_000_000)]);
    assert_eq!(asks, [(540_000, 20_000_000)]);

    // Handshake was signed over timestamp + GET + path.
    let headers = h.mock.state.lock().unwrap().handshakes[0].clone();
    assert_eq!(headers["KALSHI-ACCESS-KEY"], "test-key-id");
    let message = format!("{}GET/trade-api/ws/v2", headers["KALSHI-ACCESS-TIMESTAMP"]);
    let signature = base64::engine::general_purpose::STANDARD
        .decode(&headers["KALSHI-ACCESS-SIGNATURE"])
        .unwrap();
    UnparsedPublicKey::new(&ED25519, &h.public_key)
        .verify(message.as_bytes(), &signature)
        .expect("handshake signature verifies");

    h.mock.inject(Inject::Delta {
        ticker: "MKT-A".into(),
        side: "no",
        price_cents: 54,
        delta_centi: -500,
    });
    let Event::Delta {
        side,
        price,
        delta,
        received_at,
        exchange_timestamp,
        ..
    } = next(&h.events)
    else {
        panic!("expected delta")
    };
    assert_eq!((side, price, delta), (Side::Sell, 540_000, -5_000_000));
    assert!(exchange_timestamp <= received_at + chrono::Duration::seconds(1));

    h.cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), h.task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        h.mock.state.lock().unwrap().closes_received,
        1,
        "sent a Close frame"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_gap_resets_and_resubscribes() {
    let h = start(HashMap::from([("G".to_owned(), book(&[(40, 100)], &[]))])).await;
    h.commands.send(Command::Add(vec!["G".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "G");

    h.mock.inject(Inject::Gap);
    h.mock.inject(Inject::Delta {
        ticker: "G".into(),
        side: "yes",
        price_cents: 40,
        delta_centi: 100,
    });
    // The delta after the gap must not be forwarded; the book is reset and
    // re-snapshotted (with the delta already included by the exchange).
    assert_eq!(next(&h.events), Event::SourceReset { source: 7 });
    let Event::Snapshot { bids, .. } = next(&h.events) else {
        panic!("expected fresh snapshot")
    };
    assert_eq!(bids, [(400_000, 2_000_000)]);
    assert_eq!(
        h.metrics
            .sequence_gaps
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    h.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn disconnect_reconnects_with_fresh_signature() {
    let h = start(HashMap::new()).await;
    h.commands.send(Command::Add(vec!["D".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "D");
    h.mock.inject(Inject::Disconnect);
    assert_eq!(next(&h.events), Event::SourceReset { source: 7 });
    assert_eq!(snapshot_symbol(&next(&h.events)), "D");
    assert_eq!(h.mock.state.lock().unwrap().handshakes.len(), 2);
    h.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_snapshot_is_requested_again() {
    let h = start(HashMap::new()).await;
    h.commands.send(Command::Add(vec!["A".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "A");
    h.mock.state.lock().unwrap().suppress_add_snapshots = true;
    h.commands.send(Command::Add(vec!["B".into()])).unwrap();
    // No snapshot from add_markets; the timeout triggers get_snapshot.
    assert_eq!(snapshot_symbol(&next(&h.events)), "B");
    let last = h.mock.commands().last().cloned().unwrap();
    assert_eq!(last["params"]["action"], "get_snapshot");
    assert_eq!(last["params"]["market_tickers"], serde_json::json!(["B"]));
    h.cancel.cancel();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_remove_and_resync_use_update_subscription() {
    let h = start(HashMap::new()).await;
    h.commands.send(Command::Add(vec!["A".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "A");

    h.commands.send(Command::Add(vec!["B".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "B");

    h.commands.send(Command::Resync(vec!["A".into()])).unwrap();
    assert_eq!(snapshot_symbol(&next(&h.events)), "A");

    h.commands.send(Command::Remove(vec!["A".into()])).unwrap();
    // Deltas for a removed market are no longer delivered.
    tokio::time::sleep(Duration::from_millis(200)).await;
    h.mock.inject(Inject::Delta {
        ticker: "A".into(),
        side: "yes",
        price_cents: 10,
        delta_centi: 100,
    });
    h.mock.inject(Inject::Delta {
        ticker: "B".into(),
        side: "yes",
        price_cents: 10,
        delta_centi: 100,
    });
    let Event::Delta { symbol, .. } = next(&h.events) else {
        panic!("expected delta")
    };
    assert_eq!(symbol, "B");

    // Removing the last market drops the subscription entirely.
    h.commands.send(Command::Remove(vec!["B".into()])).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let actions: Vec<String> = h
        .mock
        .commands()
        .iter()
        .map(|c| match c["cmd"].as_str().unwrap() {
            "update_subscription" => c["params"]["action"].as_str().unwrap().to_owned(),
            other => other.to_owned(),
        })
        .collect();
    assert_eq!(
        actions,
        [
            "subscribe",
            "add_markets",
            "get_snapshot",
            "delete_markets",
            "unsubscribe"
        ]
    );
    h.cancel.cancel();
}
