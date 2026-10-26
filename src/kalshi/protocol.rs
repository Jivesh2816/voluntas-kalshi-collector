//! Kalshi WebSocket wire format (`orderbook_delta` channel) and its mapping to
//! collector [`Event`]s. Pure functions only, so it is fully unit-testable.
//!
//! ## Book mapping
//!
//! A Kalshi book has two bid ladders: YES bids and NO bids. Buying NO at `p`
//! is economically selling YES at `1 - p`. We subscribe with
//! `use_yes_price: true`, which makes Kalshi report NO levels already in
//! YES-leg prices, so:
//!
//! * `side: "yes"` -> [`Side::Buy`] (bid) at `price_dollars`
//! * `side: "no"`  -> [`Side::Sell`] (ask) at `price_dollars`
//!
//! Setting the flag explicitly also keeps us correct when Kalshi flips its
//! default to `true` and later removes the flag.
//!
//! ## Quantities
//!
//! `orderbook_snapshot` levels are absolute `[price_dollars, count_fp]`.
//! `orderbook_delta.delta_fp` is a signed *change*. Both are converted to
//! fixed-point here; turning deltas into absolute quantities happens in the
//! book.
//!
//! ## Timestamps
//!
//! * `received_at` / `backtest_timestamp`: local wall-clock time captured as
//!   soon as the WebSocket frame is read, before parsing.
//! * delta `exchange_timestamp`: `msg.ts_ms` (when Kalshi recorded the
//!   change); if absent, the envelope's `sending_ts_ms` (when Kalshi queued
//!   the message).
//! * snapshot `exchange_timestamp`: `sending_ts_ms`, the only exchange time a
//!   snapshot carries.
//!
//! If no exchange time is present, `received_at` is used and the event is
//! flagged so it can be counted; these fields are documented as optional.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::Side;
use crate::collector::{Event, SourceId};
use crate::fixed_point::{ParseFixedError, parse_fixed};

pub const CHANNEL: &str = "orderbook_delta";

/// Error code Kalshi sends when a subscription's outbound buffer overflowed,
/// i.e. messages were dropped.
pub const ERR_BUFFER_OVERFLOW: i64 = 25;

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Inbound {
    Subscribed {
        id: Option<u64>,
        msg: SubscribedMsg,
    },
    Ok {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
    },
    Unsubscribed {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
    },
    Error {
        id: Option<u64>,
        msg: ErrorMsg,
    },
    OrderbookSnapshot {
        sid: u64,
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: SnapshotMsg,
    },
    OrderbookDelta {
        sid: u64,
        seq: u64,
        sending_ts_ms: Option<i64>,
        msg: DeltaMsg,
    },
    #[serde(other)]
    Other,
}

impl Inbound {
    /// The `(sid, seq)` pair, for messages that are part of a subscription's
    /// sequence. Kalshi numbers *all* messages on a subscription, including
    /// `ok`/`unsubscribed` replies to commands on it, not just book data.
    pub fn sequence(&self) -> Option<(u64, u64)> {
        match *self {
            Self::OrderbookSnapshot { sid, seq, .. } | Self::OrderbookDelta { sid, seq, .. } => {
                Some((sid, seq))
            }
            Self::Ok {
                sid: Some(sid),
                seq: Some(seq),
                ..
            }
            | Self::Unsubscribed {
                sid: Some(sid),
                seq: Some(seq),
                ..
            } => Some((sid, seq)),
            _ => None,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SubscribedMsg {
    pub channel: String,
    pub sid: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct ErrorMsg {
    pub code: i64,
    #[serde(default)]
    pub msg: String,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct SnapshotMsg {
    pub market_ticker: String,
    #[serde(default)]
    pub yes_dollars_fp: Vec<[String; 2]>,
    #[serde(default)]
    pub no_dollars_fp: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum KalshiSide {
    Yes,
    No,
}

impl KalshiSide {
    /// Valid only for subscriptions made with `use_yes_price: true`.
    pub fn book_side(self) -> Side {
        match self {
            Self::Yes => Side::Buy,
            Self::No => Side::Sell,
        }
    }
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct DeltaMsg {
    pub market_ticker: String,
    pub price_dollars: String,
    pub delta_fp: String,
    pub side: KalshiSide,
    pub ts_ms: Option<i64>,
}

pub fn parse(text: &str) -> serde_json::Result<Inbound> {
    serde_json::from_str(text)
}

/// A converted event plus whether its exchange timestamp had to fall back to
/// the local receive time.
#[derive(Debug, PartialEq)]
pub struct Converted {
    pub event: Event,
    pub exchange_time_missing: bool,
}

fn exchange_time(candidates: &[Option<i64>], received_at: DateTime<Utc>) -> (DateTime<Utc>, bool) {
    candidates
        .iter()
        .flatten()
        .find_map(|&ms| DateTime::from_timestamp_millis(ms))
        .map_or((received_at, true), |t| (t, false))
}

fn levels(raw: Vec<[String; 2]>) -> Result<Vec<(i64, i64)>, ParseFixedError> {
    raw.into_iter()
        .map(|[price, qty]| Ok((parse_fixed(&price)?, parse_fixed(&qty)?)))
        .collect()
}

pub fn snapshot_event(
    source: SourceId,
    received_at: DateTime<Utc>,
    sending_ts_ms: Option<i64>,
    msg: SnapshotMsg,
) -> Result<Converted, ParseFixedError> {
    let (exchange_timestamp, missing) = exchange_time(&[sending_ts_ms], received_at);
    Ok(Converted {
        event: Event::Snapshot {
            source,
            symbol: msg.market_ticker,
            bids: levels(msg.yes_dollars_fp)?,
            asks: levels(msg.no_dollars_fp)?,
            received_at,
            exchange_timestamp,
        },
        exchange_time_missing: missing,
    })
}

pub fn delta_event(
    source: SourceId,
    received_at: DateTime<Utc>,
    sending_ts_ms: Option<i64>,
    msg: DeltaMsg,
) -> Result<Converted, ParseFixedError> {
    let (exchange_timestamp, missing) = exchange_time(&[msg.ts_ms, sending_ts_ms], received_at);
    Ok(Converted {
        event: Event::Delta {
            source,
            symbol: msg.market_ticker,
            side: msg.side.book_side(),
            price: parse_fixed(&msg.price_dollars)?,
            delta: parse_fixed(&msg.delta_fp)?,
            received_at,
            exchange_timestamp,
        },
        exchange_time_missing: missing,
    })
}

pub fn subscribe_cmd(id: u64, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "subscribe",
        "params": {
            "channels": [CHANNEL],
            "market_tickers": tickers,
            "use_yes_price": true,
        }
    })
    .to_string()
}

/// `action` is `add_markets`, `delete_markets` or `get_snapshot`.
pub fn update_subscription_cmd(id: u64, sid: u64, action: &str, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sid": sid, "market_tickers": tickers, "action": action }
    })
    .to_string()
}

pub fn unsubscribe_cmd(id: u64, sid: u64) -> String {
    json!({ "id": id, "cmd": "unsubscribe", "params": { "sids": [sid] } }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_669_149_841_500_123).unwrap()
    }

    // Examples copied from Kalshi's AsyncAPI documentation.
    const SNAPSHOT: &str = r#"{"type":"orderbook_snapshot","sending_ts_ms":1669149841234,"sid":2,"seq":2,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1","clock":4182057,"yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],"no_dollars_fp":[["0.5400","20.00"],["0.5600","146.00"]]}}"#;
    const DELTA: &str = r#"{"type":"orderbook_delta","sending_ts_ms":1669149841123,"sid":2,"seq":3,"msg":{"market_ticker":"FED-23DEC-T3.00","market_id":"9b0f6b43-5b68-4f9f-9f02-9a2d1b8ac1a1","clock":4182061,"price_dollars":"0.9600","delta_fp":"-54.00","side":"yes","ts":"2022-11-22T20:44:01Z","ts_ms":1669149841000}}"#;

    #[test]
    fn parses_documented_snapshot() {
        let Inbound::OrderbookSnapshot {
            sid,
            seq,
            sending_ts_ms,
            msg,
        } = parse(SNAPSHOT).unwrap()
        else {
            panic!("wrong variant")
        };
        assert_eq!((sid, seq, sending_ts_ms), (2, 2, Some(1_669_149_841_234)));
        let converted = snapshot_event(9, now(), sending_ts_ms, msg).unwrap();
        assert!(!converted.exchange_time_missing);
        assert_eq!(
            converted.event,
            Event::Snapshot {
                source: 9,
                symbol: "FED-23DEC-T3.00".into(),
                bids: vec![(80_000, 300_000_000), (220_000, 333_000_000)],
                asks: vec![(540_000, 20_000_000), (560_000, 146_000_000)],
                received_at: now(),
                exchange_timestamp: DateTime::from_timestamp_millis(1_669_149_841_234).unwrap(),
            }
        );
    }

    #[test]
    fn parses_documented_delta_using_ts_ms() {
        let Inbound::OrderbookDelta {
            sending_ts_ms, msg, ..
        } = parse(DELTA).unwrap()
        else {
            panic!("wrong variant")
        };
        let converted = delta_event(9, now(), sending_ts_ms, msg).unwrap();
        assert_eq!(
            converted.event,
            Event::Delta {
                source: 9,
                symbol: "FED-23DEC-T3.00".into(),
                side: Side::Buy,
                price: 960_000,
                delta: -54_000_000,
                received_at: now(),
                exchange_timestamp: DateTime::from_timestamp_millis(1_669_149_841_000).unwrap(),
            }
        );
    }

    #[test]
    fn no_side_maps_to_ask_and_timestamp_fallbacks() {
        let delta = |ts_ms| DeltaMsg {
            market_ticker: "M".into(),
            price_dollars: "0.7000".into(),
            delta_fp: "5.00".into(),
            side: KalshiSide::No,
            ts_ms,
        };
        let c = delta_event(1, now(), Some(2_000), delta(None)).unwrap();
        let Event::Delta {
            side,
            exchange_timestamp,
            ..
        } = c.event
        else {
            panic!()
        };
        assert_eq!(side, Side::Sell);
        assert_eq!(
            exchange_timestamp,
            DateTime::from_timestamp_millis(2_000).unwrap()
        );
        assert!(!c.exchange_time_missing);

        let c = delta_event(1, now(), None, delta(None)).unwrap();
        assert!(c.exchange_time_missing);
        let Event::Delta {
            exchange_timestamp, ..
        } = c.event
        else {
            panic!()
        };
        assert_eq!(exchange_timestamp, now());
    }

    #[test]
    fn empty_sides_are_omitted_keys() {
        let text = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,"msg":{"market_ticker":"E","market_id":"x","clock":0}}"#;
        let Inbound::OrderbookSnapshot { msg, .. } = parse(text).unwrap() else {
            panic!()
        };
        let c = snapshot_event(1, now(), None, msg).unwrap();
        let Event::Snapshot { bids, asks, .. } = c.event else {
            panic!()
        };
        assert!(bids.is_empty() && asks.is_empty());
        assert!(c.exchange_time_missing);
    }

    #[test]
    fn control_messages_and_sequence() {
        let ok = r#"{"id":123,"sid":456,"seq":222,"type":"ok","sending_ts_ms":1,"msg":{"market_tickers":["A"]}}"#;
        assert_eq!(parse(ok).unwrap().sequence(), Some((456, 222)));
        let list = r#"{"id":3,"type":"ok","msg":[{"channel":"orderbook_delta","sid":1}]}"#;
        assert_eq!(parse(list).unwrap().sequence(), None);
        let unsub = r#"{"id":102,"sid":2,"seq":7,"type":"unsubscribed","sending_ts_ms":1}"#;
        assert_eq!(parse(unsub).unwrap().sequence(), Some((2, 7)));
        let subscribed =
            r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":1}}"#;
        assert_eq!(
            parse(subscribed).unwrap(),
            Inbound::Subscribed {
                id: Some(1),
                msg: SubscribedMsg {
                    channel: "orderbook_delta".into(),
                    sid: 1
                }
            }
        );
        let err = r#"{"id":123,"type":"error","msg":{"code":7,"msg":"Unknown subscription ID"}}"#;
        assert!(matches!(
            parse(err).unwrap(),
            Inbound::Error {
                msg: ErrorMsg { code: 7, .. },
                ..
            }
        ));
        assert_eq!(
            parse(r#"{"type":"ticker","msg":{}}"#).unwrap(),
            Inbound::Other
        );
    }

    #[test]
    fn bad_numbers_are_errors() {
        let msg = DeltaMsg {
            market_ticker: "M".into(),
            price_dollars: "0.5".into(),
            delta_fp: "1e3".into(),
            side: KalshiSide::Yes,
            ts_ms: None,
        };
        assert!(delta_event(1, now(), None, msg).is_err());
    }

    #[test]
    fn commands_match_documented_shapes() {
        let sub: serde_json::Value =
            serde_json::from_str(&subscribe_cmd(1, &["A".into(), "B".into()])).unwrap();
        assert_eq!(
            sub,
            json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":["A","B"],"use_yes_price":true}})
        );
        let upd: serde_json::Value = serde_json::from_str(&update_subscription_cmd(
            5,
            456,
            "add_markets",
            &["C".into()],
        ))
        .unwrap();
        assert_eq!(
            upd,
            json!({"id":5,"cmd":"update_subscription","params":{"sid":456,"market_tickers":["C"],"action":"add_markets"}})
        );
        let unsub: serde_json::Value = serde_json::from_str(&unsubscribe_cmd(6, 2)).unwrap();
        assert_eq!(
            unsub,
            json!({"id":6,"cmd":"unsubscribe","params":{"sids":[2]}})
        );
    }
}
