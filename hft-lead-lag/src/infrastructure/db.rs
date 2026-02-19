//! SQLite persistence for fleet trades and configs.
//!
//! WAL mode for concurrent reads. Async batch writer via mpsc channel
//! flushes every 5s — zero impact on trading hot path.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{Connection, params};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Cumulative count of dropped trade batches (for monitoring/alerting).
static DROPPED_BATCHES: AtomicU64 = AtomicU64::new(0);

use crate::domain::screener::shadow_fleet::FleetTrade;
use crate::domain::screener::trader_config::TraderConfig;

const FLUSH_INTERVAL_SECS: u64 = 5;
const CHANNEL_CAPACITY: usize = 100_000;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS configs (
    id                    INTEGER PRIMARY KEY,
    spike_threshold_bps   REAL NOT NULL,
    spike_window_ms       INTEGER NOT NULL,
    target_ratio          REAL NOT NULL,
    stop_loss_bps         REAL NOT NULL,
    max_hold_ms           INTEGER NOT NULL,
    max_spread_bps        REAL NOT NULL,
    trailing_stop_bps     REAL NOT NULL,
    trailing_decay_ratio  REAL NOT NULL DEFAULT 0.5,
    fill_delay_ms         INTEGER NOT NULL,
    cooldown_ms           INTEGER NOT NULL,
    warmup_ms             INTEGER NOT NULL DEFAULT 30000,
    quote_freshness_ms    INTEGER NOT NULL DEFAULT 1000,
    taker_fee             REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS trades (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    config_id   INTEGER NOT NULL REFERENCES configs(id),
    symbol      TEXT NOT NULL,
    direction   TEXT NOT NULL,
    entry_ts_ms INTEGER NOT NULL,
    exit_ts_ms  INTEGER NOT NULL,
    entry_price REAL NOT NULL,
    exit_price  REAL NOT NULL,
    spike_bps   REAL NOT NULL,
    pnl_pct     REAL NOT NULL,
    exit_reason TEXT NOT NULL,
    gate_spread_at_entry_bps REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_config ON trades(config_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_exit_ts ON trades(exit_ts_ms);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_natural_key ON trades(config_id, symbol, entry_ts_ms, exit_ts_ms);
";

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

/// Open (or create) the optimizer database with WAL mode.
pub fn open_db(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    conn.execute_batch(SCHEMA)?;
    // Migration: add trailing_decay_ratio if missing (existing DBs).
    let _ = conn.execute_batch(
        "ALTER TABLE configs ADD COLUMN trailing_decay_ratio REAL NOT NULL DEFAULT 0.5;"
    );
    let _ = conn.execute_batch(
        "ALTER TABLE configs ADD COLUMN warmup_ms INTEGER NOT NULL DEFAULT 30000;"
    );
    let _ = conn.execute_batch(
        "ALTER TABLE configs ADD COLUMN quote_freshness_ms INTEGER NOT NULL DEFAULT 1000;"
    );
    Ok(conn)
}

/// Insert configs into the database (idempotent — uses OR IGNORE).
pub fn upsert_configs(conn: &Connection, configs: &[TraderConfig]) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO configs (id, spike_threshold_bps, spike_window_ms, target_ratio,
         stop_loss_bps, max_hold_ms, max_spread_bps, trailing_stop_bps, trailing_decay_ratio,
         fill_delay_ms, cooldown_ms, warmup_ms, quote_freshness_ms, taker_fee)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)"
    )?;
    for c in configs {
        stmt.execute(params![
            c.config_id() as i64,
            c.spike_threshold_bps,
            c.spike_window_ms,
            c.target_ratio,
            c.stop_loss_bps,
            c.max_hold_ms,
            c.max_spread_bps,
            c.trailing_stop_bps,
            c.trailing_decay_ratio,
            c.fill_delay_ms,
            c.cooldown_ms,
            c.warmup_ms,
            c.quote_freshness_ms,
            c.taker_fee,
        ])?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch writer
// ---------------------------------------------------------------------------

/// Handle to send trades to the background writer.
#[derive(Clone, Debug)]
pub struct DbWriter {
    tx: mpsc::Sender<Vec<FleetTrade>>,
}

impl DbWriter {
    /// Enqueue a batch of trades for async persistence.
    pub fn send(&self, trades: Vec<FleetTrade>) {
        if trades.is_empty() { return; }
        if let Err(e) = self.tx.try_send(trades) {
            let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
            warn!("db writer channel full, dropping batch (total dropped: {n}): {e}");
        }
    }

    /// Number of trade batches lost to channel overflow since process start.
    pub fn dropped_batches() -> u64 {
        DROPPED_BATCHES.load(Ordering::Relaxed)
    }
}

/// Spawn the background writer task. Returns a handle for sending trades.
pub fn spawn_writer(db_path: &Path) -> DbWriter {
    let (tx, mut rx) = mpsc::channel::<Vec<FleetTrade>>(CHANNEL_CAPACITY);
    let path = db_path.to_path_buf();

    tokio::spawn(async move {
        let conn = match open_db(&path) {
            Ok(c) => c,
            Err(e) => {
                warn!("failed to open optimizer db: {e}");
                return;
            }
        };
        info!("db writer started: {}", path.display());

        let mut buf: Vec<FleetTrade> = Vec::with_capacity(1024);
        let mut interval = tokio::time::interval(
            tokio::time::Duration::from_secs(FLUSH_INTERVAL_SECS),
        );

        loop {
            tokio::select! {
                batch = rx.recv() => {
                    match batch {
                        Some(trades) => buf.extend(trades),
                        None => break, // channel closed
                    }
                }
                _ = interval.tick() => {
                    if !buf.is_empty() {
                        match flush_trades(&conn, &buf) {
                            Ok(_) => buf.clear(),
                            Err(e) => warn!("db flush error (retaining {} trades): {e}", buf.len()),
                        }
                    }
                }
            }
        }
        // Flush remaining on shutdown.
        if !buf.is_empty() {
            let _ = flush_trades(&conn, &buf);
        }
        info!("db writer stopped");
    });

    DbWriter { tx }
}

fn flush_trades(conn: &Connection, trades: &[FleetTrade]) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO trades (config_id, symbol, direction, entry_ts_ms, exit_ts_ms,
             entry_price, exit_price, spike_bps, pnl_pct, exit_reason,
             gate_spread_at_entry_bps)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)"
        )?;
        for ft in trades {
            let t = &ft.trade;
            stmt.execute(params![
                ft.config_id as i64,
                ft.symbol,
                t.direction_str(),
                t.entry_ts_ms,
                t.ts_ms,
                t.entry_price,
                t.exit_price,
                t.spike_bps,
                t.pnl_pct,
                t.exit_reason,
                t.gate_spread_at_entry_bps,
            ])?;
        }
    }
    tx.commit()?;
    info!("flushed {} trades to db", trades.len());
    Ok(())
}
