//! One Kalshi WebSocket connection carrying one `orderbook_delta`
//! subscription for a set of markets.
//!
//! The manager tells a connection which markets it should carry
//! ([`Command::Add`] / [`Command::Remove`]); the connection keeps the live
//! subscription in line with that set across reconnects.
//!
//! ## Continuity
//!
//! Kalshi's `seq` counts every message on one *subscription* (`sid`), across
//! all of its markets, including `ok` replies to commands on it. A skipped
//! number therefore means *some* message for *some* market on this
//! connection was lost, and we can't tell which. The only safe recovery is to
//! drop every book this connection owns ([`Event::SourceReset`]) and
//! resubscribe, which delivers fresh snapshots. The same applies to
//! disconnects, silent connections, and Kalshi's buffer-overflow error.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use crossbeam::channel::Sender;
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_util::sync::CancellationToken;

use super::auth::Signer;
use super::protocol::{self, CHANNEL, ERR_BUFFER_OVERFLOW, Inbound};
use crate::collector::{Event, SourceId};
use crate::metrics::Metrics;

/// Max tickers per `update_subscription` command.
const COMMAND_BATCH: usize = 500;

#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    pub url: String,
    /// Kalshi requires authentication; `None` is only useful for local mocks.
    pub signer: Option<Arc<Signer>>,
    /// Reconnect if no frame (data or ping) arrives for this long. Kalshi
    /// pings every 10 seconds.
    pub stale_after: Duration,
    pub connect_timeout: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    /// Re-request a snapshot for a subscribed market that has not received
    /// one within this long. Kalshi documents a snapshot as the first message
    /// for every subscribed market; this is a safety net.
    pub snapshot_timeout: Duration,
}

impl ConnectionConfig {
    pub fn new(url: impl Into<String>, signer: Option<Arc<Signer>>) -> Self {
        Self {
            url: url.into(),
            signer,
            stale_after: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(15),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(60),
            snapshot_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Add(Vec<String>),
    Remove(Vec<String>),
    /// Ask Kalshi for fresh snapshots of these markets.
    Resync(Vec<String>),
}

#[derive(Debug)]
enum SessionEnd {
    Shutdown,
    /// The collector is gone; nothing left to do.
    WriterGone,
    Failed(String),
}

enum Pending {
    Subscribe,
    Add(Vec<String>),
    Other,
}

#[derive(Default)]
struct Session {
    sid: Option<u64>,
    last_seq: Option<u64>,
    subscribe_in_flight: bool,
    /// Markets included in a subscribe/add_markets on this session.
    requested: HashSet<String>,
    /// Markets Kalshi refused to add on this session; retried next session.
    refused: HashSet<String>,
    /// Requested markets still waiting for their snapshot, and since when.
    awaiting_snapshot: HashMap<String, Instant>,
    pending: HashMap<u64, Pending>,
    next_id: u64,
}

impl Session {
    fn id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    fn expect_snapshots<'a>(&mut self, tickers: impl IntoIterator<Item = &'a String>) {
        let now = Instant::now();
        for t in tickers {
            self.awaiting_snapshot.insert(t.clone(), now);
        }
    }
}

pub struct Connection {
    id: SourceId,
    config: Arc<ConnectionConfig>,
    desired: HashSet<String>,
    commands: UnboundedReceiver<Command>,
    events: Sender<Event>,
    metrics: Arc<Metrics>,
    cancel: CancellationToken,
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
type WsSink = futures_util::stream::SplitSink<Ws, Message>;

impl Connection {
    pub fn new(
        id: SourceId,
        config: Arc<ConnectionConfig>,
        commands: UnboundedReceiver<Command>,
        events: Sender<Event>,
        metrics: Arc<Metrics>,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            id,
            config,
            desired: HashSet::new(),
            commands,
            events,
            metrics,
            cancel,
        }
    }

    pub async fn run(mut self) {
        let mut backoff = self.config.initial_backoff;
        loop {
            let started = Instant::now();
            match self.session().await {
                SessionEnd::Shutdown | SessionEnd::WriterGone => return,
                SessionEnd::Failed(reason) => {
                    Metrics::inc(&self.metrics.session_failures);
                    tracing::warn!(conn = self.id, %reason, markets = self.desired.len(), "session ended");
                }
            }
            // Continuity is lost: every book from this connection is stale
            // until the next session re-snapshots it.
            if self
                .events
                .send(Event::SourceReset { source: self.id })
                .is_err()
            {
                return;
            }
            if started.elapsed() > Duration::from_secs(60) {
                backoff = self.config.initial_backoff;
            }
            // Spread reconnects of many connections over time.
            let jitter = Duration::from_millis(self.id.wrapping_mul(7919) % 1000);
            let wait = sleep(backoff + jitter);
            tokio::pin!(wait);
            loop {
                tokio::select! {
                    _ = self.cancel.cancelled() => return,
                    _ = &mut wait => break,
                    cmd = self.commands.recv() => match cmd {
                        Some(cmd) => self.apply(&cmd),
                        None => return,
                    },
                }
            }
            backoff = (backoff * 2).min(self.config.max_backoff);
        }
    }

    fn apply(&mut self, cmd: &Command) {
        match cmd {
            Command::Add(tickers) => self.desired.extend(tickers.iter().cloned()),
            Command::Remove(tickers) => {
                for t in tickers {
                    self.desired.remove(t);
                }
            }
            Command::Resync(_) => {}
        }
    }

    async fn connect(&self) -> anyhow::Result<Ws> {
        let mut request = self.config.url.as_str().into_client_request()?;
        if let Some(signer) = &self.config.signer {
            let path = request.uri().path().to_owned();
            for (name, value) in signer.headers(Utc::now().timestamp_millis(), "GET", &path)? {
                request
                    .headers_mut()
                    .insert(name, HeaderValue::from_str(&value)?);
            }
        }
        let (ws, _response) =
            tokio_tungstenite::connect_async_with_config(request, None, true).await?;
        Ok(ws)
    }

    async fn session(&mut self) -> SessionEnd {
        let connected = tokio::select! {
            _ = self.cancel.cancelled() => return SessionEnd::Shutdown,
            result = timeout(self.config.connect_timeout, self.connect()) => result,
        };
        let ws = match connected {
            Ok(Ok(ws)) => ws,
            Ok(Err(error)) => return SessionEnd::Failed(format!("connect failed: {error:#}")),
            Err(_) => return SessionEnd::Failed("connect timed out".into()),
        };
        Metrics::inc(&self.metrics.sessions_started);
        tracing::info!(conn = self.id, markets = self.desired.len(), "connected");

        let (mut sink, mut stream) = ws.split();
        let mut session = Session::default();
        let mut last_frame = Instant::now();
        let mut health = interval(Duration::from_secs(1));
        health.set_missed_tick_behavior(MissedTickBehavior::Delay);

        let result: Result<(), SessionEnd> = async {
            self.reconcile(&mut session, &mut sink).await?;
            loop {
                tokio::select! {
                    biased;
                    _ = self.cancel.cancelled() => return Err(SessionEnd::Shutdown),
                    frame = stream.next() => {
                        // Stamp before any parsing.
                        let received_at = Utc::now();
                        match frame {
                            None => return Err(SessionEnd::Failed("stream ended".into())),
                            Some(Err(error)) => return Err(SessionEnd::Failed(format!("read error: {error}"))),
                            Some(Ok(message)) => {
                                last_frame = Instant::now();
                                Metrics::inc(&self.metrics.frames);
                                match message {
                                    Message::Text(text) => {
                                        self.on_text(&mut session, &mut sink, text.as_str(), received_at).await?;
                                    }
                                    Message::Close(frame) => {
                                        return Err(SessionEnd::Failed(format!("server closed: {frame:?}")));
                                    }
                                    // tungstenite answers pings itself.
                                    _ => {}
                                }
                            }
                        }
                    }
                    cmd = self.commands.recv() => match cmd {
                        Some(cmd) => {
                            self.apply(&cmd);
                            if let Command::Resync(tickers) = &cmd {
                                self.request_snapshots(&mut session, &mut sink, tickers).await?;
                            }
                            self.reconcile(&mut session, &mut sink).await?;
                        }
                        None => return Err(SessionEnd::Shutdown),
                    },
                    _ = health.tick() => {
                        if last_frame.elapsed() > self.config.stale_after {
                            return Err(SessionEnd::Failed("no frames received; connection stale".into()));
                        }
                        let overdue: Vec<String> = session
                            .awaiting_snapshot
                            .iter()
                            .filter(|(_, since)| since.elapsed() > self.config.snapshot_timeout)
                            .map(|(t, _)| t.clone())
                            .collect();
                        if !overdue.is_empty() && session.sid.is_some() {
                            tracing::warn!(conn = self.id, count = overdue.len(), "snapshots overdue; requesting");
                            self.request_snapshots(&mut session, &mut sink, &overdue).await?;
                        }
                    }
                }
            }
        }
        .await;
        let end = result.expect_err("session loop only exits with an end reason");

        if matches!(end, SessionEnd::Shutdown) {
            // Polite close: send Close and wait briefly for the server's.
            let _ = sink.send(Message::Close(None)).await;
            let _ = timeout(Duration::from_secs(2), async {
                while let Some(Ok(frame)) = stream.next().await {
                    if frame.is_close() {
                        break;
                    }
                }
            })
            .await;
            tracing::info!(conn = self.id, "closed");
        }
        end
    }

    async fn send(&self, sink: &mut WsSink, text: String) -> Result<(), SessionEnd> {
        sink.send(Message::text(text))
            .await
            .map_err(|e| SessionEnd::Failed(format!("send failed: {e}")))
    }

    /// Brings the session's subscription in line with `desired`.
    async fn reconcile(&self, s: &mut Session, sink: &mut WsSink) -> Result<(), SessionEnd> {
        let Some(sid) = s.sid else {
            if s.subscribe_in_flight {
                return Ok(()); // reconciled again once `subscribed` arrives
            }
            let tickers: Vec<String> = self.desired.difference(&s.refused).cloned().collect();
            if tickers.is_empty() {
                return Ok(());
            }
            let id = s.id();
            s.pending.insert(id, Pending::Subscribe);
            s.subscribe_in_flight = true;
            s.requested = tickers.iter().cloned().collect();
            s.awaiting_snapshot.clear();
            s.expect_snapshots(&tickers);
            return self.send(sink, protocol::subscribe_cmd(id, &tickers)).await;
        };

        if self.desired.is_empty() {
            // A subscription can't be empty; drop it and start over later.
            let id = s.id();
            s.pending.insert(id, Pending::Other);
            s.sid = None;
            s.last_seq = None;
            s.requested.clear();
            s.awaiting_snapshot.clear();
            return self.send(sink, protocol::unsubscribe_cmd(id, sid)).await;
        }

        let to_remove: Vec<String> = s.requested.difference(&self.desired).cloned().collect();
        for batch in to_remove.chunks(COMMAND_BATCH) {
            for t in batch {
                s.requested.remove(t);
                s.awaiting_snapshot.remove(t);
            }
            let id = s.id();
            s.pending.insert(id, Pending::Other);
            self.send(
                sink,
                protocol::update_subscription_cmd(id, sid, "delete_markets", batch),
            )
            .await?;
        }

        let to_add: Vec<String> = self
            .desired
            .iter()
            .filter(|t| !s.requested.contains(*t) && !s.refused.contains(*t))
            .cloned()
            .collect();
        for batch in to_add.chunks(COMMAND_BATCH) {
            s.requested.extend(batch.iter().cloned());
            s.expect_snapshots(batch);
            let id = s.id();
            s.pending.insert(id, Pending::Add(batch.to_vec()));
            self.send(
                sink,
                protocol::update_subscription_cmd(id, sid, "add_markets", batch),
            )
            .await?;
        }
        Ok(())
    }

    async fn request_snapshots(
        &self,
        s: &mut Session,
        sink: &mut WsSink,
        tickers: &[String],
    ) -> Result<(), SessionEnd> {
        let Some(sid) = s.sid else { return Ok(()) };
        let (tickers, gone): (Vec<String>, Vec<String>) = tickers
            .iter()
            .cloned()
            .partition(|t| s.requested.contains(t));
        for t in &gone {
            s.awaiting_snapshot.remove(t);
        }
        s.expect_snapshots(&tickers);
        for batch in tickers.chunks(COMMAND_BATCH) {
            let id = s.id();
            s.pending.insert(id, Pending::Other);
            self.send(
                sink,
                protocol::update_subscription_cmd(id, sid, "get_snapshot", batch),
            )
            .await?;
        }
        Ok(())
    }

    async fn on_text(
        &self,
        s: &mut Session,
        sink: &mut WsSink,
        text: &str,
        received_at: chrono::DateTime<Utc>,
    ) -> Result<(), SessionEnd> {
        let message = match protocol::parse(text) {
            Ok(message) => message,
            Err(error) => {
                Metrics::inc(&self.metrics.parse_errors);
                let preview: String = text.chars().take(200).collect();
                // An unparseable book message would silently corrupt a book.
                if preview.contains("orderbook_") {
                    return Err(SessionEnd::Failed(format!(
                        "unparseable book message: {error}"
                    )));
                }
                tracing::warn!(conn = self.id, %error, %preview, "ignoring unparseable message");
                return Ok(());
            }
        };

        if let Some((sid, seq)) = message.sequence() {
            if s.sid.is_none() && s.subscribe_in_flight {
                // Data raced ahead of the `subscribed` reply.
                s.sid = Some(sid);
            }
            if s.sid != Some(sid) {
                return Ok(()); // a subscription we already dropped
            }
            if let Some(last) = s.last_seq
                && seq != last + 1
            {
                Metrics::inc(&self.metrics.sequence_gaps);
                return Err(SessionEnd::Failed(format!(
                    "sequence gap on sid {sid}: expected {}, got {seq}",
                    last + 1
                )));
            }
            s.last_seq = Some(seq);
        }

        match message {
            Inbound::Subscribed { id, msg } => {
                if msg.channel != CHANNEL {
                    return Ok(());
                }
                if let Some(id) = id {
                    s.pending.remove(&id);
                }
                if s.sid.is_some_and(|sid| sid != msg.sid) {
                    return Err(SessionEnd::Failed(format!(
                        "subscribed sid {} differs from data sid {:?}",
                        msg.sid, s.sid
                    )));
                }
                s.sid = Some(msg.sid);
                s.subscribe_in_flight = false;
                tracing::debug!(conn = self.id, sid = msg.sid, "subscribed");
                self.reconcile(s, sink).await?;
            }
            Inbound::Ok { id, .. } | Inbound::Unsubscribed { id, .. } => {
                if let Some(id) = id {
                    s.pending.remove(&id);
                }
            }
            Inbound::Error { id, msg } => {
                Metrics::inc(&self.metrics.command_errors);
                if msg.code == ERR_BUFFER_OVERFLOW {
                    return Err(SessionEnd::Failed(format!(
                        "subscription buffer overflow: {}",
                        msg.msg
                    )));
                }
                match id.and_then(|id| s.pending.remove(&id)) {
                    Some(Pending::Subscribe) => {
                        return Err(SessionEnd::Failed(format!(
                            "subscribe rejected: code {} {}",
                            msg.code, msg.msg
                        )));
                    }
                    Some(Pending::Add(tickers)) => {
                        tracing::warn!(conn = self.id, code = msg.code, error = %msg.msg, count = tickers.len(), "add_markets rejected");
                        for t in tickers {
                            s.requested.remove(&t);
                            s.awaiting_snapshot.remove(&t);
                            s.refused.insert(t);
                        }
                    }
                    _ => {
                        tracing::warn!(conn = self.id, code = msg.code, error = %msg.msg, "command error")
                    }
                }
            }
            Inbound::OrderbookSnapshot {
                sending_ts_ms, msg, ..
            } => {
                Metrics::inc(&self.metrics.snapshots);
                s.awaiting_snapshot.remove(&msg.market_ticker);
                let converted = protocol::snapshot_event(self.id, received_at, sending_ts_ms, msg)
                    .map_err(|e| SessionEnd::Failed(format!("bad snapshot: {e}")))?;
                self.forward(converted)?;
            }
            Inbound::OrderbookDelta {
                sending_ts_ms, msg, ..
            } => {
                Metrics::inc(&self.metrics.deltas);
                let converted = protocol::delta_event(self.id, received_at, sending_ts_ms, msg)
                    .map_err(|e| SessionEnd::Failed(format!("bad delta: {e}")))?;
                self.forward(converted)?;
            }
            Inbound::Other => {}
        }
        Ok(())
    }

    fn forward(&self, converted: protocol::Converted) -> Result<(), SessionEnd> {
        if converted.exchange_time_missing {
            Metrics::inc(&self.metrics.exchange_time_missing);
        }
        self.events
            .send(converted.event)
            .map_err(|_| SessionEnd::WriterGone)
    }
}
