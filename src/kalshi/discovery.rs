//! Market discovery over Kalshi's public REST API.
//!
//! Two loops feed the manager:
//!
//! * **Full scan** (`status=open&mve_filter=exclude`, paginated): the complete
//!   set of tradeable regular markets. Run at startup and periodically to
//!   reconcile. A full scan of ~150k markets takes a few minutes, so pages are
//!   forwarded as they arrive instead of waiting for the end.
//! * **Incremental** (`min_updated_ts=<since>&mve_filter=exclude`): every
//!   market whose metadata changed since the last poll, in *any* status. This
//!   is how we see markets open, close, pause and resume within seconds.
//!   Kalshi bumps `updated_time` when a market opens, closes or is created.
//!
//! MVEs (multivariate event combos) are excluded server-side with
//! `mve_filter=exclude`, and again client-side: a market is skipped if it has
//! an `mve_collection_ticker` or a `KXMVE` ticker prefix, which is how Kalshi
//! partitions its lifecycle channels.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

pub const PAGE_LIMIT: u32 = 1000;

#[derive(Debug, Clone)]
pub struct DiscoveryConfig {
    pub rest_url: String,
    pub full_scan_interval: Duration,
    pub incremental_interval: Duration,
    /// How far back each incremental query reaches before the previous
    /// poll's start, to cover clock skew and `updated_time` granularity.
    pub incremental_overlap: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketInfo {
    pub ticker: String,
    /// True if the market is open for trading (`status == "active"`).
    pub active: bool,
    pub close_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryUpdate {
    /// Market states as of `observed_at` (when the request was sent).
    Markets {
        observed_at: DateTime<Utc>,
        markets: Vec<MarketInfo>,
    },
    /// A full scan that started at `started_at` completed. Any market not
    /// seen active since then has stopped trading.
    FullScanComplete { started_at: DateTime<Utc> },
}

#[derive(Debug, Deserialize)]
struct MarketsPage {
    markets: Vec<RawMarket>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawMarket {
    ticker: String,
    status: String,
    #[serde(default)]
    close_time: Option<String>,
    #[serde(default)]
    mve_collection_ticker: Option<String>,
}

pub fn is_mve(ticker: &str, mve_collection_ticker: Option<&str>) -> bool {
    mve_collection_ticker.is_some_and(|c| !c.is_empty()) || ticker.starts_with("KXMVE")
}

fn convert(raw: Vec<RawMarket>) -> Vec<MarketInfo> {
    raw.into_iter()
        .filter(|m| !is_mve(&m.ticker, m.mve_collection_ticker.as_deref()))
        .map(|m| MarketInfo {
            active: m.status == "active",
            close_time: m
                .close_time
                .as_deref()
                .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
                .map(|t| t.with_timezone(&Utc)),
            ticker: m.ticker,
        })
        .collect()
}

pub struct Discovery {
    config: DiscoveryConfig,
    client: reqwest::Client,
    updates: UnboundedSender<DiscoveryUpdate>,
    cancel: CancellationToken,
}

impl Discovery {
    pub fn new(
        config: DiscoveryConfig,
        updates: UnboundedSender<DiscoveryUpdate>,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .user_agent(concat!(
                "voluntas-kalshi-collector/",
                env!("CARGO_PKG_VERSION")
            ))
            .build()?;
        Ok(Self {
            config,
            client,
            updates,
            cancel,
        })
    }

    pub async fn run(self) {
        tokio::join!(self.full_scan_loop(), self.incremental_loop());
    }

    async fn full_scan_loop(&self) {
        loop {
            let started_at = Utc::now();
            let begun = std::time::Instant::now();
            let mut wait = self.config.full_scan_interval;
            match self
                .paginate(&[("status", "open".into()), ("mve_filter", "exclude".into())])
                .await
            {
                Ok(count) => {
                    tracing::info!(
                        markets = count,
                        secs = begun.elapsed().as_secs(),
                        "full market scan complete"
                    );
                    if self
                        .updates
                        .send(DiscoveryUpdate::FullScanComplete { started_at })
                        .is_err()
                    {
                        return;
                    }
                }
                Err(_) if self.cancel.is_cancelled() => return,
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), "full market scan failed");
                    wait = wait.min(Duration::from_secs(30));
                }
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(wait) => {}
            }
        }
    }

    async fn incremental_loop(&self) {
        // The first full scan covers everything before startup.
        let mut since = Utc::now();
        loop {
            tokio::select! {
                _ = self.cancel.cancelled() => return,
                _ = tokio::time::sleep(self.config.incremental_interval) => {}
            }
            let poll_started = Utc::now();
            let overlap =
                chrono::Duration::from_std(self.config.incremental_overlap).unwrap_or_default();
            let min_updated_ts = (since - overlap).timestamp();
            match self
                .paginate(&[
                    ("min_updated_ts", min_updated_ts.to_string()),
                    ("mve_filter", "exclude".into()),
                ])
                .await
            {
                Ok(count) => {
                    tracing::debug!(markets = count, "incremental market poll");
                    since = poll_started;
                }
                Err(_) if self.cancel.is_cancelled() => return,
                Err(error) => {
                    tracing::warn!(error = %format!("{error:#}"), "incremental market poll failed");
                }
            }
        }
    }

    /// Fetches every page of `GET /markets` with `query`, forwarding each page
    /// as it arrives. Returns the number of markets forwarded.
    async fn paginate(&self, query: &[(&str, String)]) -> anyhow::Result<usize> {
        let mut cursor = String::new();
        let mut total = 0;
        loop {
            let observed_at = Utc::now();
            let page = self.fetch_page(query, &cursor).await?;
            let markets = convert(page.markets);
            total += markets.len();
            if !markets.is_empty()
                && self
                    .updates
                    .send(DiscoveryUpdate::Markets {
                        observed_at,
                        markets,
                    })
                    .is_err()
            {
                anyhow::bail!("manager stopped");
            }
            match page.cursor {
                Some(next) if !next.is_empty() => cursor = next,
                _ => return Ok(total),
            }
        }
    }

    async fn fetch_page(
        &self,
        query: &[(&str, String)],
        cursor: &str,
    ) -> anyhow::Result<MarketsPage> {
        let url = format!("{}/markets", self.config.rest_url);
        let mut attempt = 0u32;
        loop {
            if self.cancel.is_cancelled() {
                anyhow::bail!("shutting down");
            }
            let mut request = self
                .client
                .get(&url)
                .query(query)
                .query(&[("limit", PAGE_LIMIT.to_string())]);
            if !cursor.is_empty() {
                request = request.query(&[("cursor", cursor)]);
            }
            let fetch = async {
                let response = request.send().await?;
                let status = response.status();
                if !status.is_success() {
                    anyhow::bail!("HTTP {status}");
                }
                Ok(response.json::<MarketsPage>().await?)
            };
            let result = tokio::select! {
                _ = self.cancel.cancelled() => anyhow::bail!("shutting down"),
                result = fetch => result,
            };
            match result {
                Ok(page) => return Ok(page),
                Err(error) if attempt < 5 => {
                    attempt += 1;
                    let wait = Duration::from_millis(500 * 2u64.pow(attempt));
                    tracing::debug!(%error, attempt, "GET /markets failed; retrying");
                    tokio::select! {
                        _ = self.cancel.cancelled() => anyhow::bail!("shutting down"),
                        _ = tokio::time::sleep(wait) => {}
                    }
                }
                Err(error) => return Err(error.context("GET /markets")),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mve_filter() {
        assert!(is_mve("KXMVESPORTS-ABC", None));
        assert!(is_mve("ANY", Some("KXMVECOLLECTION")));
        assert!(!is_mve("KXECULPSPREAD-26OCT26TECMUR-TEC5", None));
        assert!(!is_mve("FED-23DEC-T3.00", Some("")));
    }

    #[test]
    fn converts_page_and_drops_mves() {
        let page: MarketsPage = serde_json::from_str(
            r#"{"cursor":"abc","markets":[
                {"ticker":"A","status":"active","close_time":"2026-10-28T20:30:00Z","mve_collection_ticker":null},
                {"ticker":"B","status":"closed","close_time":"2026-10-20T00:00:00Z"},
                {"ticker":"KXMVEX-1","status":"active"},
                {"ticker":"C","status":"active","mve_collection_ticker":"KXMVECOLL"}
            ]}"#,
        )
        .unwrap();
        assert_eq!(page.cursor.as_deref(), Some("abc"));
        let markets = convert(page.markets);
        assert_eq!(
            markets,
            [
                MarketInfo {
                    ticker: "A".into(),
                    active: true,
                    close_time: Some("2026-10-28T20:30:00Z".parse().unwrap()),
                },
                MarketInfo {
                    ticker: "B".into(),
                    active: false,
                    close_time: Some("2026-10-20T00:00:00Z".parse().unwrap()),
                },
            ]
        );
    }
}
