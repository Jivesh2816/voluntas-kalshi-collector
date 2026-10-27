# Kalshi order book collector

A Rust service that discovers every live regular (non-MVE) Kalshi market,
keeps an order book for each one across several WebSocket connections, and
writes every book change plus a full snapshot of every book each minute to a
FIX SBE file. It uses the template's schema and `AlignedSbeWriter`
unchanged. SIGINT (or SIGTERM) closes the WebSockets and flushes and closes
the file.

- [Setup](#setup)
- [Running](#running)
- [Architecture](#architecture)
- [Exchange semantics](#exchange-semantics-and-how-they-map-to-the-schema)
- [Testing](#testing)
- [What has and has not been verified live](#what-has-and-has-not-been-verified-live)
- [Known limitations](#known-limitations)
- [Template background](#template-background)

## Setup

### Toolchain

The template's `AlignedSbeReader` uses `std::os::unix::fs::FileExt`, so the
crate builds on Linux/macOS only. On Windows, use WSL:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --component rustfmt,clippy
source ~/.cargo/env
```

A C compiler (`cc`) is needed by `ring`. Ubuntu usually has one; if not,
run `sudo apt install build-essential`. TLS uses rustls with bundled roots,
so OpenSSL is not needed.

### Kalshi API credentials (required)

Kalshi rejects unauthenticated WebSocket connections (`401
token_authentication_failure`), including for market data. Market discovery
uses the public REST API and needs no key.

1. Log in to kalshi.com, open **Account / Profile settings → API Keys**
   (<https://kalshi.com/account/profile>), and click **Create New API Key**.
   Ed25519 is the default and is supported, and so is RSA. If you're offered
   permission scopes, read-only is enough.
2. Kalshi shows the private key once and downloads it as a file. Move it
   somewhere outside the repository and restrict it:

   ```bash
   mkdir -p ~/.kalshi && mv ~/Downloads/<key-file> ~/.kalshi/kalshi-key.pem
   chmod 600 ~/.kalshi/kalshi-key.pem
   ```

3. Give the collector the key ID and the key file path through environment
   variables. The private key itself never goes into an environment variable,
   a command line, or the repository:

   ```bash
   export KALSHI_API_KEY_ID=<key id shown by Kalshi>
   export KALSHI_PRIVATE_KEY_PATH=$HOME/.kalshi/kalshi-key.pem
   ```

   You can also put those two lines in a `.env` file in the repository root,
   which is gitignored, and load it with `set -a; source .env; set +a`.

The collector logs only the key ID and the key type, never key material.

## Running

```bash
# Smoke test: 50 markets, 2 minutes, then Ctrl-C.
cargo run --release -- --output smoke.sbe --max-markets 50

# Everything (≈150k markets on 26 Oct 2026).
cargo run --release -- --output kalshi.sbe
```

Press Ctrl-C once for a graceful shutdown: every WebSocket gets a Close frame
and the file is flushed. A second Ctrl-C exits immediately and loses unflushed
data. The collector refuses to overwrite an existing output file unless you
pass `--overwrite`.

| Option | Default | Meaning |
| - | - | - |
| `-o, --output` | `kalshi_orderbooks.sbe` | Output file (also `COLLECTOR_OUTPUT`) |
| `--markets-per-connection` | 1000 | Markets carried by one WebSocket |
| `--max-connections` | 190 | Cap on WebSockets. Kalshi's Basic tier allows 200 per user |
| `--max-markets` | none | Only subscribe to this many markets (smoke tests) |
| `--snapshot-interval-secs` | 60 | Full snapshot of every book |
| `--incremental-interval-secs` | 5 | Market change poll |
| `--full-scan-interval-secs` | 1800 | Full market reconciliation |
| `--ws-url`, `--rest-url` | production | Also `KALSHI_WS_URL`, `KALSHI_REST_URL` (demo: `wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2`, `https://external-api.demo.kalshi.co/trade-api/v2`) |

Logs go to stderr. Control verbosity with `RUST_LOG`; for example,
`RUST_LOG=info,engineering_challenge=debug`. Every minute the collector logs
the market, connection and book counts plus these counters: sequence gaps,
reconnects, resyncs, missing exchange timestamps, and crossed books.

## Architecture

```
                 ┌────────────────────────── tokio ───────────────────────────┐
 Kalshi REST ───►│ Discovery ──updates──► Manager (Planner) ──Add/Remove──► Connection 1 ─┼─◄─ WS
                 │  full scan + incremental      │                         Connection 2 ─┼─◄─ WS
                 │                               │ Assign/Unassign          …            │
                 └───────────────────────────────┼──────────────────────────────┬─────────┘
                                                 ▼                              │ Snapshot/Delta/
                                    crossbeam channel (one, FIFO) ◄──────────────┘ SourceReset
                                                 ▼
                       collector thread: books + AlignedSbeWriter + 60 s ticker
```

| Module | Role |
| - | - |
| `fixed_point` | Exact decimal-string → fixed-point (1e6) conversion, never via floats |
| `book` | Exchange-agnostic book. `apply_delta` returns the level's new **absolute** quantity |
| `collector` | Owns every book and the writer on one thread. Writes snapshots, updates and periodic snapshots. Enforces per-symbol source ownership |
| `kalshi::auth` | Ed25519 / RSA-PSS request signing |
| `kalshi::protocol` | Wire types and Kalshi → `Event` mapping (pure, unit-tested) |
| `kalshi::connection` | One WebSocket: subscription upkeep, `seq` checks, staleness, reconnect with backoff, polite close |
| `kalshi::discovery` | REST market discovery |
| `kalshi::manager` | `Planner` decides which market goes on which connection; `Manager` carries those decisions out |

### Key decisions

- **One writer thread with all book state.** It needs no locks, and the order
  of records in the file is exactly the order events were applied. A 150k-book
  periodic snapshot pass takes about 0.3 s in a release build.
- **Exchange logic stays out of the core.** `collector` and `book` know
  nothing about Kalshi. Adapters send `Event`s that are already converted to
  fixed point and to `Side`.
- **Source ownership.** The manager sends `Assign` to the collector before it
  tells a connection to subscribe. The collector ignores events from any other
  source. This keeps late messages from an old connection out of a book that
  has moved, and it's the hook for redundant connections.
- **Discovery** combines three things:
  - **Full scan:** `status=open&mve_filter=exclude`, paginated. Pages are
    applied as they arrive.
  - **Incremental poll:** every 5 s, `min_updated_ts=<last poll − 60 s>&mve_filter=exclude`.
    This returns markets in any status whose metadata changed. Kalshi bumps
    `updated_time` when a market is created, opens, closes, pauses or settles.
  - **Local `close_time` sweep:** every second. Kalshi sends no event for the
    implicit close at `close_time`.

  Each update carries the time its request was sent. Older information never
  overrides newer information, so a slow full-scan page can't bring back a
  market that already closed.
- **Unsubscribing.** A market is dropped when an update reports it non-active,
  when its `close_time` passes, or when a completed full scan didn't contain it
  and nothing newer reported it active.
- **Sharding.** New markets go to the least-loaded connection that has room. A
  new connection is opened when all are full, up to `--max-connections`. If a
  connection's last market is removed, its subscription is dropped
  (`unsubscribe`); the connection stays open for future markets.

## Exchange semantics and how they map to the schema

All of this comes from Kalshi's current docs at <https://docs.kalshi.com>.

**Book sides.** A Kalshi book is two bid ladders, YES and NO. Buying NO at `p`
is selling YES at `1 − p`. We subscribe with `use_yes_price: true`, so Kalshi
reports NO levels already in YES prices:

- YES → `Side::Buy` / bids
- NO → `Side::Sell` / asks

Setting the flag explicitly also stays correct when Kalshi flips the default
and later removes the flag. Any crossed book is counted as a sanity check.

**Quantities.**
- Snapshots (`yes_dollars_fp`, `no_dollars_fp`) list absolute
  `[price, count]` pairs.
- `orderbook_delta.delta_fp` is a signed *change*.
- The book applies each change and writes the resulting absolute quantity,
  which is what `PriceLevelUpdate.quantity` holds. A quantity of 0 means the
  level was removed.
- Prices (up to 4 decimal places) and counts (up to 2 decimal places) convert
  exactly to the schema's 1e6 fixed point.

**Sequence numbers.**
- `seq` counts every message on one subscription (`sid`), across all of its
  markets. That includes `ok` replies to `update_subscription`.
- `clock` is per market and is only for matching across connections.
- A gap therefore means some message for some market on that connection was
  lost, and we can't tell which. Recovery: drop every book owned by that
  connection (`SourceReset`), reconnect, and resubscribe, which delivers fresh
  snapshots. Disconnects, stale connections (no frame for 30 s; Kalshi pings
  every 10 s) and error 25 (buffer overflow) are handled the same way.
- A delta that would make a level negative causes that one book to be dropped
  and re-requested with `get_snapshot`.
- A market that gets no snapshot within 30 s of subscribing is re-requested.

**Timestamps.**

| Field | Source |
| - | - |
| `Snapshot.received_at`, `PriceLevelUpdate.backtest_timestamp` (exchange messages) | Local UTC wall clock, read immediately after the WebSocket frame arrives and before parsing (µs) |
| `PriceLevelUpdate.exchange_timestamp` | `msg.ts_ms`: when Kalshi recorded the change (ms). If absent, the envelope's `sending_ts_ms` |
| `Snapshot.exchange_timestamp` (exchange snapshot) | `sending_ts_ms`: when Kalshi queued the message. A snapshot carries no other time |
| Periodic snapshot `received_at` | The newest `received_at` the collector has processed. Every book reflects every event up to that local time, and the timestamp is never later than what was actually received |
| Periodic snapshot `exchange_timestamp` | Exchange time of the newest change in that book |

If Kalshi omits every exchange timestamp, `received_at` is used and the
`exchange_time_missing` counter goes up. All of those fields are documented as
optional.

**Regular markets vs MVEs.** `mve_filter=exclude` filters on the server. On
the client, we also skip anything with an `mve_collection_ticker` or a
`KXMVE` ticker prefix. That prefix is how Kalshi separates MVEs into their own
lifecycle channel.

## Testing

```bash
cargo fmt --check && cargo check --all-targets && cargo clippy --all-targets && cargo test
cargo test --release --test collector_file -- --ignored --nocapture   # 150k-book scale check
```

To check a real output file, run `examples/inspect.rs`. It decodes the file
with the template's `AlignedSbeReader`, replays every book, and reports
structural checks and receive latency:

```bash
cargo run --release --example inspect -- kalshi.sbe [--dump-json books.json] [--history-json states.json] [--latency-tsv deltas.tsv]
```

There are 48 tests, and none of them need credentials or the network:

- **Unit tests (40):**
  - fixed-point parsing: exactness, malformed input, overflow
  - book operations
  - the collector's ownership, resync and watermark logic
  - Kalshi message parsing, using the doc examples verbatim
  - Ed25519 and RSA-PSS signatures, checked against public keys (RSA needs `openssl`)
  - discovery parsing and the MVE filter
  - the planner's sharding, closing and stale-update rules
- **`tests/collector_file.rs`:**
  - events → the template's `AlignedSbeWriter` → back through the template's `AlignedSbeReader`
  - periodic snapshots
- **`tests/kalshi_connection.rs`:** one connection against an in-process
  mock of Kalshi's WebSocket, which follows the documented message shapes and
  numbers `seq` per subscription. It checks:
  - the signed handshake (the mock verifies the signature)
  - snapshots and deltas
  - recovery from a sequence gap
  - reconnecting after a disconnect
  - `add_markets` / `delete_markets` / `get_snapshot` / `unsubscribe`
  - the missing-snapshot safety net
  - the Close frame on shutdown
- **`tests/end_to_end.rs`:** runs the **real binary** against the mock REST
  and WebSocket servers with two connections. It injects deltas, closes one
  market through discovery, then sends a real SIGINT. Afterwards it checks:
  - every WebSocket was closed and the process exited 0
  - only live non-MVE markets were subscribed
  - replaying the file with the template reader reproduces the exchange's books exactly
  - periodic snapshots were written, and stopped for the closed market
  - timestamps are plausible

## What has and has not been verified live

All of these runs were on 26–27 Oct 2026, from a home connection through WSL2,
using an authenticated Ed25519 key.

**Discovery (public REST):**
- `status=open&mve_filter=exclude` returned 147,085–147,642 markets, all
  `active`, with zero MVEs leaking through. A full scan takes 3.5–7 minutes.
- 94% of those markets have at least one resting order, and 16% traded in the
  last 24 hours.
- The `min_updated_ts` incremental query returns markets as they open, close,
  pause and settle.
- Unauthenticated WebSocket upgrades return `401`.

**Authenticated WebSocket runs:**

| Run | Markets / connections | Deltas | Gaps, crossed, invalid, command errors | REST cross-check |
| - | - | - | - | - |
| Smoke 1 & 2 | 50 / 1 | 1,209 and 972 | all 0 | 38 of 45 quiet markets match when fetched *after* stopping (timing) |
| In-flight check | 50 / 1 | 1,736 | all 0 | **50 of 50** match |
| Multi-socket | 3,000 / 6 × 500 | 124,982 | all 0 | **150 of 150** match |
| Capacity | 2,000 / 2 × 1000 | 106,791 | all 0; 1 reconnect (see below) | 54 of 59 match; the 5 others lagged behind (see below) |

- **REST cross-check method.** While the collector runs, a script fetches
  Kalshi's public `GET /markets/{ticker}/orderbook` and records the local
  request time. That endpoint reports No bids in No-leg prices. We then ask
  whether the book rebuilt from our file (with `inspect --history-json`) held
  *exactly* that book within ±3 s of the fetch. This independently confirms:
  - delta → absolute-quantity conversion,
  - the `use_yes_price` side mapping (our ask at P equals the REST No bid at 1 − P),
  - that the SBE encoding round-trips.
- **Things the docs left open, now settled:**
  - `ok` replies are part of the `seq` sequence: zero false gaps across every
    `add_markets`.
  - `add_markets` delivers snapshots.
  - 1,000 markets per subscription is accepted, with no error 26.
  - Kalshi sends an unsolicited `orderbook_snapshot` when it cancels a book
    en masse, for example an empty book when an NHL period market closes. The
    collector treats every snapshot as authoritative, which is correct.
- **SIGINT.** It closed every WebSocket (Close frame sent) and flushed a file
  the template reader decodes, in every run.

**Throughput findings (capacity run):**
- At about 1,300 deltas/s during live NFL and NHL games, receive lag rose
  steadily to around 60 s. Kalshi then reset that connection. The collector
  reconnected, resubscribed and re-snapshotted, as designed.
- **The collector was not the bottleneck:**
  - socket `Recv-Q` was always 0,
  - CPU was ≤ 26% of one core,
  - the writer's queue backlog was about 0,
  - receive − `sending_ts_ms` equalled receive − `ts_ms` (the
    `max_send_lag_ms` / `max_change_lag_ms` gauges), so the delay was after
    Kalshi queued each message, on the way to us.
- **The host link measured 0.5–1.2 MB/s,** about the size of that stream.
- **Timestamps stay honest under lag.** `received_at` / `backtest_timestamp`
  are the actual arrival times, so the file records the lag rather than
  hiding it.
- **Compression is available.** Kalshi negotiates `permessage-deflate`, and
  enabling it is the obvious next step (Extension 4). It needs a WebSocket
  library other than tungstenite 0.30, which has no compression support.

**Not yet run:** a full 147k-market collection. On paper it fits the limits:
148 connections at 1,000 per connection, under the default cap of 190 and
Kalshi's Basic limit of 200. But all of Kalshi's traffic is far more than this
link carried. Run it from a host with real bandwidth, close to Kalshi
(US-East), or after adding compression.

## Known limitations

- **No periodic flush.** The template's reader only reads 64 MiB chunks, and
  each flush pads the file to a full chunk. So the writer flushes only when a
  chunk fills and at shutdown. A crash or SIGKILL (but not SIGINT or SIGTERM)
  loses up to 64 MiB of buffered output.
- **No marker for an invalid book.** While a book is being resynced after a
  gap or disconnect, the file can't say so, because the schema has no such
  message. A reader keeps the last state until the next `Snapshot` replaces it.
- **Slow initial coverage.** At startup, markets are subscribed as discovery
  pages arrive. Covering all ~150k takes one full scan, a few minutes at the
  REST latency we saw.
- **Paused markets** (`inactive`) are treated as not trading and unsubscribed.
  They're resubscribed, with a fresh snapshot, when they reopen.
- **Disk usage.** The minute snapshots of ~150k books come to roughly
  50–100 MB a minute, plus deltas.
- **Clock accuracy.** Local timestamps are only as good as the host clock.
  On the test machine, Windows Time had never synced ("Local CMOS Clock"), and
  WSL inherits that clock. NTP showed it about 684 ms behind, so most deltas
  looked like they arrived before Kalshi sent them. The
  `exchange_after_receive` counter flags this. Enable NTP before collecting:
  run `w32tm /resync` as admin on Windows, or use chrony or systemd-timesyncd
  on Linux. Kalshi's exchange times have millisecond resolution.
- **Bandwidth.** Receive lag grows if the link can't carry the feed (see
  above). Watch `max_send_lag_ms` in the stats line.
- **Unbounded channel.** The channel from the connections to the writer has
  no limit, so a writer that falls behind uses memory rather than stalling the
  sockets. The backlog is logged.
- **Not implemented:** the optional extensions — redundant connections,
  subscription health monitoring, and latency work. The exchange-agnostic
  `collector` and `book` modules, plus `SourceId` ownership, are the intended
  starting points for redundancy and generic adapters.

---

## Template background

*(The template's original README follows.)*

First, read the template's `src/main.rs` (now replaced by the collector; the
original demo is in git history). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.
