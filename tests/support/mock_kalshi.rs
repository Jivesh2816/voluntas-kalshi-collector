//! A small in-process imitation of Kalshi's `orderbook_delta` WebSocket,
//! following the documented message shapes. It keeps a real book per market
//! (YES-leg prices, like `use_yes_price: true`), numbers every message on a
//! subscription with `seq`, and can be told to inject deltas, sequence gaps
//! and disconnects.

#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

/// Prices and sizes in hundredths (cents / centi-contracts) to keep the mock
/// simple; formatted as Kalshi's 4-dp price and 2-dp count strings.
#[derive(Default, Clone, Debug)]
pub struct MockBook {
    pub yes: BTreeMap<i64, i64>,
    pub no: BTreeMap<i64, i64>,
}

#[derive(Clone, Debug)]
pub enum Inject {
    /// side is "yes" or "no"; price in cents; delta in centi-contracts.
    Delta {
        ticker: String,
        side: &'static str,
        price_cents: i64,
        delta_centi: i64,
    },
    /// Skip one `seq` number on every live subscription.
    Gap,
    /// Drop every connection without a close handshake.
    Disconnect,
}

#[derive(Default)]
pub struct MockState {
    pub books: HashMap<String, MockBook>,
    /// Handshake headers seen, one map per connection.
    pub handshakes: Vec<HashMap<String, String>>,
    /// Every client command, in arrival order.
    pub commands: Vec<Value>,
    pub closes_received: usize,
    /// Misbehave: don't send snapshots for markets added with add_markets.
    pub suppress_add_snapshots: bool,
    next_sid: u64,
}

pub struct MockKalshi {
    pub addr: SocketAddr,
    pub state: Arc<Mutex<MockState>>,
    inject: broadcast::Sender<Inject>,
}

impl MockKalshi {
    pub async fn start(books: HashMap<String, MockBook>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(MockState {
            books,
            ..Default::default()
        }));
        let (inject, _) = broadcast::channel(1024);
        let server_state = state.clone();
        let server_inject = inject.clone();
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let state = server_state.clone();
                let rx = server_inject.subscribe();
                tokio::spawn(handle(tcp, state, rx));
            }
        });
        Self {
            addr,
            state,
            inject,
        }
    }

    pub fn url(&self) -> String {
        format!("ws://{}/trade-api/ws/v2", self.addr)
    }

    pub fn inject(&self, event: Inject) {
        // Apply deltas to the canonical book first so later snapshots
        // include them, exactly like the exchange.
        if let Inject::Delta {
            ticker,
            side,
            price_cents,
            delta_centi,
        } = &event
        {
            let mut st = self.state.lock().unwrap();
            let book = st.books.entry(ticker.clone()).or_default();
            let ladder = if *side == "yes" {
                &mut book.yes
            } else {
                &mut book.no
            };
            let q = ladder.entry(*price_cents).or_insert(0);
            *q += delta_centi;
            assert!(*q >= 0, "mock delta made a level negative");
            if *q == 0 {
                ladder.remove(price_cents);
            }
        }
        let _ = self.inject.send(event);
    }

    pub fn commands(&self) -> Vec<Value> {
        self.state.lock().unwrap().commands.clone()
    }
}

fn price(cents: i64) -> String {
    format!("{}.{:02}00", cents / 100, cents % 100)
}

fn count(centi: i64) -> String {
    format!("{}.{:02}", centi / 100, centi % 100)
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn snapshot_msg(state: &MockState, sid: u64, seq: u64, ticker: &str) -> String {
    let book = state.books.get(ticker).cloned().unwrap_or_default();
    let mut msg = json!({ "market_ticker": ticker, "market_id": "00000000-0000-0000-0000-000000000000", "clock": 1 });
    let levels = |m: &BTreeMap<i64, i64>| -> Vec<[String; 2]> {
        m.iter().map(|(p, q)| [price(*p), count(*q)]).collect()
    };
    if !book.yes.is_empty() {
        msg["yes_dollars_fp"] = json!(levels(&book.yes));
    }
    if !book.no.is_empty() {
        msg["no_dollars_fp"] = json!(levels(&book.no));
    }
    json!({ "type": "orderbook_snapshot", "sid": sid, "seq": seq, "sending_ts_ms": now_ms(), "msg": msg })
        .to_string()
}

struct Sub {
    sid: u64,
    seq: u64,
    tickers: HashSet<String>,
}

impl Sub {
    fn next(&mut self) -> u64 {
        self.seq += 1;
        self.seq
    }
}

// The handshake callback's error type is fixed by tungstenite's API.
#[allow(clippy::result_large_err)]
async fn handle(
    tcp: tokio::net::TcpStream,
    state: Arc<Mutex<MockState>>,
    mut inject: broadcast::Receiver<Inject>,
) {
    let hs_state = state.clone();
    let callback = move |req: &Request, resp: Response| {
        let headers = req
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    k.as_str().to_ascii_uppercase(),
                    v.to_str().unwrap_or("").to_owned(),
                )
            })
            .collect();
        hs_state.lock().unwrap().handshakes.push(headers);
        Ok(resp)
    };
    let Ok(ws) = tokio_tungstenite::accept_hdr_async(tcp, callback).await else {
        return;
    };
    let (mut tx, mut rx) = ws.split();
    let mut sub: Option<Sub> = None;

    loop {
        tokio::select! {
            frame = rx.next() => {
                let Some(Ok(frame)) = frame else { return };
                let text = match frame {
                    Message::Text(t) => t.to_string(),
                    Message::Close(_) => {
                        state.lock().unwrap().closes_received += 1;
                        let _ = tx.send(Message::Close(None)).await;
                        return;
                    }
                    _ => continue,
                };
                let cmd: Value = serde_json::from_str(&text).unwrap();
                state.lock().unwrap().commands.push(cmd.clone());
                let id = cmd["id"].as_u64().unwrap_or(0);
                let mut out = Vec::new();
                match cmd["cmd"].as_str().unwrap() {
                    "subscribe" => {
                        assert_eq!(cmd["params"]["use_yes_price"], json!(true));
                        let tickers: Vec<String> = serde_json::from_value(cmd["params"]["market_tickers"].clone()).unwrap();
                        let sid = { let mut st = state.lock().unwrap(); st.next_sid += 1; st.next_sid };
                        let mut s = Sub { sid, seq: 0, tickers: tickers.iter().cloned().collect() };
                        out.push(json!({"id": id, "type": "subscribed", "msg": {"channel": "orderbook_delta", "sid": sid}}).to_string());
                        let st = state.lock().unwrap();
                        for t in &tickers {
                            let seq = s.next();
                            out.push(snapshot_msg(&st, sid, seq, t));
                        }
                        sub = Some(s);
                    }
                    "update_subscription" => {
                        let s = sub.as_mut().expect("update before subscribe");
                        assert_eq!(cmd["params"]["sid"].as_u64(), Some(s.sid));
                        let tickers: Vec<String> = serde_json::from_value(cmd["params"]["market_tickers"].clone()).unwrap();
                        let seq = s.next();
                        out.push(json!({"id": id, "sid": s.sid, "seq": seq, "type": "ok", "msg": {"market_tickers": tickers}}).to_string());
                        let st = state.lock().unwrap();
                        match cmd["params"]["action"].as_str().unwrap() {
                            "add_markets" => for t in tickers {
                                if !st.suppress_add_snapshots {
                                    let seq = s.next();
                                    out.push(snapshot_msg(&st, s.sid, seq, &t));
                                }
                                s.tickers.insert(t);
                            },
                            "delete_markets" => for t in &tickers { s.tickers.remove(t); },
                            "get_snapshot" => for t in tickers {
                                let seq = s.next();
                                out.push(snapshot_msg(&st, s.sid, seq, &t));
                            },
                            other => panic!("unexpected action {other}"),
                        }
                    }
                    "unsubscribe" => {
                        if let Some(mut s) = sub.take() {
                            let seq = s.next();
                            out.push(json!({"id": id, "sid": s.sid, "seq": seq, "type": "unsubscribed"}).to_string());
                        }
                    }
                    other => panic!("unexpected command {other}"),
                }
                for m in out {
                    if tx.send(Message::text(m)).await.is_err() { return; }
                }
            }
            event = inject.recv() => {
                let Ok(event) = event else { return };
                let Some(s) = sub.as_mut() else { continue };
                let text = match event {
                    Inject::Delta { ticker, side, price_cents, delta_centi } => {
                        if !s.tickers.contains(&ticker) { continue; }
                        let seq = s.next();
                        let ts = now_ms();
                        json!({"type": "orderbook_delta", "sid": s.sid, "seq": seq, "sending_ts_ms": ts + 1, "msg": {
                            "market_ticker": ticker, "market_id": "x", "clock": seq,
                            "price_dollars": price(price_cents), "delta_fp": if delta_centi < 0 { format!("-{}", count(-delta_centi)) } else { count(delta_centi) },
                            "side": side, "ts_ms": ts }}).to_string()
                    }
                    Inject::Gap => { s.next(); continue; }
                    Inject::Disconnect => return,
                };
                if tx.send(Message::text(text)).await.is_err() { return; }
            }
        }
    }
}
