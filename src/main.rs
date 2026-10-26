//! Kalshi order book collector.
//!
//! Discovers every live regular (non-MVE) Kalshi market, subscribes to their
//! order books across several WebSocket connections, and writes every delta
//! plus a full snapshot of every book each minute to an SBE file using the
//! template's `AlignedSbeWriter`. SIGINT/SIGTERM closes the WebSockets and
//! flushes the file.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, bail};
use clap::Parser;
use engineering_challenge::collector::{ResyncRequest, run_writer_thread};
use engineering_challenge::kalshi::auth::{KEY_ID_ENV, KEY_PATH_ENV, Signer};
use engineering_challenge::kalshi::connection::ConnectionConfig;
use engineering_challenge::kalshi::discovery::{Discovery, DiscoveryConfig};
use engineering_challenge::kalshi::manager::{Manager, Planner};
use engineering_challenge::kalshi::{PROD_REST_URL, PROD_WS_URL};
use engineering_challenge::metrics::Metrics;
use engineering_challenge::util::AlignedSbeWriter;
use tokio_util::sync::CancellationToken;

/// The template's `AlignedSbeReader::open` reads 64 MiB chunks, so files must
/// be written with the same chunk size to be readable.
const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[derive(Debug, Parser)]
#[command(about = "Collect Kalshi order books into a FIX SBE file")]
struct Args {
    /// Output file (FIX SBE, 64 MiB aligned chunks).
    #[arg(
        short,
        long,
        env = "COLLECTOR_OUTPUT",
        default_value = "kalshi_orderbooks.sbe"
    )]
    output: PathBuf,
    /// Replace the output file if it already exists.
    #[arg(long)]
    overwrite: bool,

    #[arg(long, env = "KALSHI_WS_URL", default_value = PROD_WS_URL)]
    ws_url: String,
    #[arg(long, env = "KALSHI_REST_URL", default_value = PROD_REST_URL)]
    rest_url: String,

    /// Markets carried by one WebSocket connection.
    #[arg(long, default_value_t = 1000)]
    markets_per_connection: usize,
    /// Upper bound on WebSocket connections (Kalshi's Basic tier allows 200
    /// concurrent connections per user).
    #[arg(long, default_value_t = 190)]
    max_connections: usize,
    /// Only subscribe to this many markets (for smoke tests).
    #[arg(long)]
    max_markets: Option<usize>,

    /// Seconds between full snapshots of every book.
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,
    /// Seconds between full market scans (reconciliation).
    #[arg(long, default_value_t = 1800)]
    full_scan_interval_secs: u64,
    /// Seconds between incremental market polls.
    #[arg(long, default_value_t = 5)]
    incremental_interval_secs: u64,
    /// Seconds between stats log lines.
    #[arg(long, default_value_t = 60)]
    stats_interval_secs: u64,

    /// Connect without API credentials. Kalshi rejects this; only useful
    /// against a local mock server.
    #[arg(long, hide = true)]
    allow_unauthenticated: bool,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::IsTerminal::is_terminal(&std::io::stderr()))
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install TLS crypto provider"))?;

    let args = Args::parse();
    anyhow::ensure!(
        args.markets_per_connection > 0,
        "--markets-per-connection must be > 0"
    );
    anyhow::ensure!(args.max_connections > 0, "--max-connections must be > 0");
    anyhow::ensure!(
        args.snapshot_interval_secs > 0,
        "--snapshot-interval-secs must be > 0"
    );

    let signer = Signer::from_env()?.map(Arc::new);
    match &signer {
        Some(s) => tracing::info!(
            key_id = s.key_id(),
            kind = s.kind(),
            "loaded Kalshi API key"
        ),
        None if args.allow_unauthenticated => tracing::warn!("running without API credentials"),
        None => bail!(
            "Kalshi WebSockets require an API key. Set {KEY_ID_ENV} and {KEY_PATH_ENV} \
             (see README.md)."
        ),
    }

    if args.output.exists() && !args.overwrite {
        bail!(
            "{} already exists; pass --overwrite to replace it",
            args.output.display()
        );
    }
    let writer = AlignedSbeWriter::create(&args.output, CHUNK_SIZE_64_MEBIBYTES)
        .with_context(|| format!("creating {}", args.output.display()))?;
    tracing::info!(output = %args.output.display(), "writing order books");

    let (events_tx, events_rx) = crossbeam::channel::unbounded();
    let (resync_tx, resync_rx) = tokio::sync::mpsc::unbounded_channel::<ResyncRequest>();
    let cancel = CancellationToken::new();

    let snapshot_interval = Duration::from_secs(args.snapshot_interval_secs);
    let stats_interval = Duration::from_secs(args.stats_interval_secs);
    let writer_cancel = cancel.clone();
    let writer_thread = std::thread::Builder::new()
        .name("collector".into())
        .spawn(move || {
            let result =
                run_writer_thread(events_rx, writer, snapshot_interval, stats_interval, |r| {
                    let _ = resync_tx.send(r);
                });
            // If the writer stops on its own (e.g. disk full), stop everything.
            writer_cancel.cancel();
            result
        })?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let metrics = Arc::new(Metrics::default());
        let conn_config = Arc::new(ConnectionConfig::new(args.ws_url.clone(), signer));
        let (updates_tx, updates_rx) = tokio::sync::mpsc::unbounded_channel();

        let discovery = Discovery::new(
            DiscoveryConfig {
                rest_url: args.rest_url.trim_end_matches('/').to_owned(),
                full_scan_interval: Duration::from_secs(args.full_scan_interval_secs),
                incremental_interval: Duration::from_secs(args.incremental_interval_secs),
                incremental_overlap: Duration::from_secs(60),
            },
            updates_tx,
            cancel.clone(),
        )?;
        let manager = Manager::new(
            Planner::new(
                args.markets_per_connection,
                args.max_connections,
                args.max_markets,
            ),
            conn_config,
            events_tx,
            metrics.clone(),
            cancel.clone(),
            stats_interval,
        );

        tokio::spawn(shutdown_on_signal(cancel.clone()));
        let discovery_task = tokio::spawn(discovery.run());
        // The manager owns the last event sender; returning drops it, which
        // lets the writer thread drain and finish.
        manager.run(updates_rx, resync_rx).await;
        cancel.cancel();
        let _ = discovery_task.await;
        tracing::info!(metrics = %metrics.summary(), "network tasks stopped");
        anyhow::Ok(())
    })?;
    runtime.shutdown_timeout(Duration::from_secs(1));

    let stats = writer_thread
        .join()
        .map_err(|_| anyhow::anyhow!("collector thread panicked"))??;
    tracing::info!(?stats, output = %args.output.display(), "output flushed and closed");
    Ok(())
}

/// First SIGINT/SIGTERM starts a graceful shutdown; a second one exits
/// immediately (losing any unflushed data).
async fn shutdown_on_signal(cancel: CancellationToken) {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => tracing::info!("SIGINT received; shutting down"),
        _ = sigterm.recv() => tracing::info!("SIGTERM received; shutting down"),
    }
    cancel.cancel();
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = sigterm.recv() => {}
    }
    tracing::warn!("second signal; exiting without flushing");
    std::process::exit(130);
}
