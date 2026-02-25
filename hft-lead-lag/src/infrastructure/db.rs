//! SQLite persistence for fleet trades and configs.
//!
//! WAL mode for concurrent reads. Async batch writer via mpsc channel
//! flushes every 5s — zero impact on trading hot path.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::{Connection, OpenFlags, params};
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Cumulative count of dropped trade batches (for monitoring/alerting).
static DROPPED_BATCHES: AtomicU64 = AtomicU64::new(0);
/// Cumulative count of batches deferred to overflow queue under backpressure.
static OVERFLOWED_BATCHES: AtomicU64 = AtomicU64::new(0);

use crate::domain::screener::shadow_fleet::FleetTrade;
use crate::domain::screener::trader_config::TraderConfig;

const FLUSH_INTERVAL_SECS: u64 = 5;
const CHANNEL_CAPACITY: usize = 100_000;
/// Secondary bounded queue used only when primary queue is temporarily full.
const OVERFLOW_CHANNEL_CAPACITY: usize = 8_192;
/// Tertiary bounded queue used only when both primary and overflow queues are full.
const RETRY_CHANNEL_CAPACITY: usize = 2_048;
/// Maximum allowed dropped batches under saturation before health degrades.
const DROPPED_BATCH_BUDGET: u64 = 0;
/// Overflowed batch count above this threshold triggers a health warning.
const OVERFLOW_WARN_THRESHOLD: u64 = 1_000;
const DEFAULT_STRATEGY_KIND: &str = "baseline_gap";

fn table_has_column(conn: &Connection, table: &str, column: &str) -> rusqlite::Result<bool> {
    let sql = format!("SELECT 1 FROM pragma_table_info('{table}') WHERE name=?1");
    conn.prepare(&sql)
        .and_then(|mut stmt| stmt.exists([column]))
}

fn add_column_if_missing(
    conn: &Connection,
    table: &str,
    column: &str,
    alter_sql: &str,
) -> rusqlite::Result<()> {
    if !table_has_column(conn, table, column)? {
        conn.execute_batch(alter_sql)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS configs (
    id                    INTEGER PRIMARY KEY,
    strategy_kind         TEXT NOT NULL DEFAULT 'baseline_gap',
    spike_threshold_bps   REAL NOT NULL,
    target_ratio          REAL NOT NULL,
    stop_loss_bps         REAL NOT NULL,
    max_hold_ms           INTEGER NOT NULL,
    max_spread_bps        REAL NOT NULL,
    trailing_decay_ratio  REAL NOT NULL DEFAULT 0.5,
    baseline_window_ms    INTEGER NOT NULL DEFAULT 2000,
    fill_delay_ms         INTEGER NOT NULL,
    cooldown_ms           INTEGER NOT NULL,
    warmup_ms             INTEGER NOT NULL DEFAULT 30000,
    quote_freshness_ms    INTEGER NOT NULL DEFAULT 1000,
    taker_fee             REAL NOT NULL,
    min_baseline_samples  INTEGER NOT NULL DEFAULT 20
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
    gate_spread_at_entry_bps REAL NOT NULL,
    gate_natr_30m_pct_at_entry REAL NOT NULL DEFAULT 0.0,
    hold_ms INTEGER NOT NULL DEFAULT 0,
    early_stop_churn INTEGER NOT NULL DEFAULT 0,
    run_id TEXT
);

CREATE TABLE IF NOT EXISTS trial_runs_meta (
    run_id TEXT PRIMARY KEY,
    submitted_config_count INTEGER NOT NULL,
    applied_at_ms INTEGER NOT NULL,
    drained_trades INTEGER NOT NULL DEFAULT 0,
    apply_mode TEXT NOT NULL DEFAULT 'full_replace',
    symbols_reset INTEGER NOT NULL DEFAULT 0,
    changed_ids_requested INTEGER NOT NULL DEFAULT 0,
    matched_changed_ids_old INTEGER NOT NULL DEFAULT 0,
    matched_changed_ids_new INTEGER NOT NULL DEFAULT 0,
    unmatched_changed_ids INTEGER NOT NULL DEFAULT 0,
    scope_symbols_requested INTEGER NOT NULL DEFAULT 0,
    scope_symbols_matched INTEGER NOT NULL DEFAULT 0,
    closed_at_ms INTEGER
);

CREATE TABLE IF NOT EXISTS config_families (
    family_id TEXT NOT NULL,
    config_id INTEGER NOT NULL REFERENCES configs(id),
    weight REAL NOT NULL DEFAULT 1.0,
    generated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (family_id, config_id)
);

CREATE TABLE IF NOT EXISTS family_symbol_clusters (
    family_id TEXT NOT NULL,
    cluster_id TEXT NOT NULL,
    symbol TEXT NOT NULL,
    useful_winrate REAL NOT NULL DEFAULT 0.0,
    avg_pnl_pct REAL NOT NULL DEFAULT 0.0,
    stop_loss_share_pct REAL NOT NULL DEFAULT 0.0,
    trades INTEGER NOT NULL DEFAULT 0,
    generated_at_ms INTEGER NOT NULL,
    PRIMARY KEY (family_id, cluster_id, symbol)
);

CREATE TABLE IF NOT EXISTS portfolio_state (
    config_id INTEGER PRIMARY KEY REFERENCES configs(id),
    family_id TEXT NOT NULL,
    cluster_id TEXT,
    symbols_json TEXT NOT NULL DEFAULT '[]',
    cooldown_until_ms INTEGER,
    quarantined INTEGER NOT NULL DEFAULT 0,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_config ON trades(config_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_exit_ts ON trades(exit_ts_ms);
CREATE INDEX IF NOT EXISTS idx_trial_runs_meta_applied_at ON trial_runs_meta(applied_at_ms);
CREATE INDEX IF NOT EXISTS idx_config_families_config_id ON config_families(config_id);
CREATE INDEX IF NOT EXISTS idx_family_symbol_clusters_family_id ON family_symbol_clusters(family_id);
CREATE INDEX IF NOT EXISTS idx_family_symbol_clusters_symbol ON family_symbol_clusters(symbol);
CREATE INDEX IF NOT EXISTS idx_portfolio_state_family_id ON portfolio_state(family_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_natural_key ON trades(config_id, symbol, entry_ts_ms, exit_ts_ms);
";

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

/// Open (or create) the optimizer database with WAL mode.
pub fn open_db(path: &Path) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;

    // Migration: rename legacy tables to timestamped backups instead of dropping.
    let has_legacy_col: bool = conn
        .prepare("SELECT 1 FROM pragma_table_info('configs') WHERE name='trailing_stop_bps'")
        .and_then(|mut s| s.exists([]))
        .unwrap_or(false);
    if has_legacy_col {
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        info!("Legacy DB schema detected — renaming tables to *_backup_{ts}");
        conn.execute_batch(&format!(
            "PRAGMA foreign_keys=OFF;
             ALTER TABLE trades RENAME TO trades_backup_{ts};
             ALTER TABLE configs RENAME TO configs_backup_{ts};
             PRAGMA foreign_keys=ON;"
        ))?;
    }

    conn.execute_batch(SCHEMA)?;
    // Compatibility migration: only run ALTER statements when columns are absent.
    add_column_if_missing(
        &conn,
        "configs",
        "strategy_kind",
        "ALTER TABLE configs ADD COLUMN strategy_kind TEXT NOT NULL DEFAULT 'baseline_gap';",
    )?;
    add_column_if_missing(
        &conn,
        "configs",
        "trailing_decay_ratio",
        "ALTER TABLE configs ADD COLUMN trailing_decay_ratio REAL NOT NULL DEFAULT 0.5;",
    )?;
    add_column_if_missing(
        &conn,
        "configs",
        "warmup_ms",
        "ALTER TABLE configs ADD COLUMN warmup_ms INTEGER NOT NULL DEFAULT 30000;",
    )?;
    add_column_if_missing(
        &conn,
        "configs",
        "quote_freshness_ms",
        "ALTER TABLE configs ADD COLUMN quote_freshness_ms INTEGER NOT NULL DEFAULT 1000;",
    )?;
    add_column_if_missing(
        &conn,
        "configs",
        "baseline_window_ms",
        "ALTER TABLE configs ADD COLUMN baseline_window_ms INTEGER NOT NULL DEFAULT 2000;",
    )?;
    add_column_if_missing(
        &conn,
        "configs",
        "min_baseline_samples",
        "ALTER TABLE configs ADD COLUMN min_baseline_samples INTEGER NOT NULL DEFAULT 20;",
    )?;
    add_column_if_missing(
        &conn,
        "trades",
        "gate_natr_30m_pct_at_entry",
        "ALTER TABLE trades ADD COLUMN gate_natr_30m_pct_at_entry REAL NOT NULL DEFAULT 0.0;",
    )?;
    add_column_if_missing(
        &conn,
        "trades",
        "hold_ms",
        "ALTER TABLE trades ADD COLUMN hold_ms INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trades",
        "early_stop_churn",
        "ALTER TABLE trades ADD COLUMN early_stop_churn INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trades",
        "run_id",
        "ALTER TABLE trades ADD COLUMN run_id TEXT;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "closed_at_ms",
        "ALTER TABLE trial_runs_meta ADD COLUMN closed_at_ms INTEGER;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "apply_mode",
        "ALTER TABLE trial_runs_meta ADD COLUMN apply_mode TEXT NOT NULL DEFAULT 'full_replace';",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "symbols_reset",
        "ALTER TABLE trial_runs_meta ADD COLUMN symbols_reset INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "changed_ids_requested",
        "ALTER TABLE trial_runs_meta ADD COLUMN changed_ids_requested INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "matched_changed_ids_old",
        "ALTER TABLE trial_runs_meta ADD COLUMN matched_changed_ids_old INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "matched_changed_ids_new",
        "ALTER TABLE trial_runs_meta ADD COLUMN matched_changed_ids_new INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "unmatched_changed_ids",
        "ALTER TABLE trial_runs_meta ADD COLUMN unmatched_changed_ids INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "scope_symbols_requested",
        "ALTER TABLE trial_runs_meta ADD COLUMN scope_symbols_requested INTEGER NOT NULL DEFAULT 0;",
    )?;
    add_column_if_missing(
        &conn,
        "trial_runs_meta",
        "scope_symbols_matched",
        "ALTER TABLE trial_runs_meta ADD COLUMN scope_symbols_matched INTEGER NOT NULL DEFAULT 0;",
    )?;
    conn.execute_batch("CREATE INDEX IF NOT EXISTS idx_trades_run_id ON trades(run_id);")?;
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_trial_runs_meta_closed_at ON trial_runs_meta(closed_at_ms);",
    )?;
    Ok(conn)
}

/// Open optimizer database in read-only mode for API queries.
pub fn open_db_readonly(path: &Path) -> rusqlite::Result<Connection> {
    let flags = OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI;
    let conn = Connection::open_with_flags(path, flags)?;
    conn.execute_batch("PRAGMA query_only=ON; PRAGMA busy_timeout=5000;")?;
    Ok(conn)
}

/// Insert configs into the database (idempotent — uses OR IGNORE).
pub fn upsert_configs(conn: &Connection, configs: &[TraderConfig]) -> rusqlite::Result<()> {
    let mut stmt = conn.prepare(
        "INSERT OR IGNORE INTO configs (id, strategy_kind, spike_threshold_bps, target_ratio,
         stop_loss_bps, max_hold_ms, max_spread_bps, trailing_decay_ratio,
         baseline_window_ms, fill_delay_ms, cooldown_ms, warmup_ms,
         quote_freshness_ms, taker_fee, min_baseline_samples)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
    )?;
    for c in configs {
        stmt.execute(params![
            c.config_id() as i64,
            DEFAULT_STRATEGY_KIND,
            c.spike_threshold_bps,
            c.target_ratio,
            c.stop_loss_bps,
            c.max_hold_ms,
            c.max_spread_bps,
            c.trailing_decay_ratio,
            c.baseline_window_ms,
            c.fill_delay_ms,
            c.cooldown_ms,
            c.warmup_ms,
            c.quote_freshness_ms,
            c.taker_fee,
            c.min_baseline_samples as i64,
        ])?;
    }
    Ok(())
}

/// Persist submitted config metadata for a trial run.
#[derive(Debug, Clone, Copy)]
pub struct TrialPatchMeta<'a> {
    pub apply_mode: &'a str,
    pub symbols_reset: usize,
    pub changed_ids_requested: usize,
    pub matched_changed_ids_old: usize,
    pub matched_changed_ids_new: usize,
    pub unmatched_changed_ids: usize,
    pub scope_symbols_requested: usize,
    pub scope_symbols_matched: usize,
}

impl Default for TrialPatchMeta<'_> {
    fn default() -> Self {
        Self {
            apply_mode: "full_replace",
            symbols_reset: 0,
            changed_ids_requested: 0,
            matched_changed_ids_old: 0,
            matched_changed_ids_new: 0,
            unmatched_changed_ids: 0,
            scope_symbols_requested: 0,
            scope_symbols_matched: 0,
        }
    }
}

/// Persist submitted config metadata for a trial run.
pub fn upsert_trial_run_meta(
    conn: &Connection,
    run_id: &str,
    submitted_config_count: usize,
    applied_at_ms: i64,
    drained_trades: usize,
    patch: TrialPatchMeta<'_>,
) -> rusqlite::Result<()> {
    conn.execute(
        "INSERT INTO trial_runs_meta (
            run_id, submitted_config_count, applied_at_ms, drained_trades,
            apply_mode, symbols_reset,
            changed_ids_requested, matched_changed_ids_old, matched_changed_ids_new,
            unmatched_changed_ids, scope_symbols_requested, scope_symbols_matched,
            closed_at_ms
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, NULL)
         ON CONFLICT(run_id) DO UPDATE SET
            submitted_config_count = excluded.submitted_config_count,
            applied_at_ms = excluded.applied_at_ms,
            drained_trades = excluded.drained_trades,
            apply_mode = excluded.apply_mode,
            symbols_reset = excluded.symbols_reset,
            changed_ids_requested = excluded.changed_ids_requested,
            matched_changed_ids_old = excluded.matched_changed_ids_old,
            matched_changed_ids_new = excluded.matched_changed_ids_new,
            unmatched_changed_ids = excluded.unmatched_changed_ids,
            scope_symbols_requested = excluded.scope_symbols_requested,
            scope_symbols_matched = excluded.scope_symbols_matched,
            closed_at_ms = NULL",
        params![
            run_id,
            submitted_config_count as i64,
            applied_at_ms,
            drained_trades as i64,
            patch.apply_mode,
            patch.symbols_reset as i64,
            patch.changed_ids_requested as i64,
            patch.matched_changed_ids_old as i64,
            patch.matched_changed_ids_new as i64,
            patch.unmatched_changed_ids as i64,
            patch.scope_symbols_requested as i64,
            patch.scope_symbols_matched as i64,
        ],
    )?;
    Ok(())
}

/// Mark a trial run as closed (idempotent, preserves existing close timestamp).
pub fn close_trial_run_meta(
    conn: &Connection,
    run_id: &str,
    closed_at_ms: i64,
) -> rusqlite::Result<()> {
    conn.execute(
        "UPDATE trial_runs_meta
         SET closed_at_ms = CASE
             WHEN closed_at_ms IS NULL THEN ?2
             ELSE closed_at_ms
         END
         WHERE run_id = ?1",
        params![run_id, closed_at_ms],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch writer
// ---------------------------------------------------------------------------

/// Handle to send trades to the background writer.
#[derive(Clone, Debug)]
pub struct DbWriter {
    tx: mpsc::Sender<DbCommand>,
    overflow_tx: mpsc::Sender<DbCommand>,
    retry_tx: mpsc::Sender<DbCommand>,
}

#[derive(Debug)]
enum DbCommand {
    Trades(Vec<FleetTrade>),
    Flush(tokio::sync::oneshot::Sender<()>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EnqueueOutcome {
    QueuedPrimary,
    QueuedOverflow,
    QueuedRetry,
    DroppedRetryFull,
    DroppedClosed,
}

fn try_enqueue_command(
    tx: &mpsc::Sender<DbCommand>,
    overflow_tx: &mpsc::Sender<DbCommand>,
    retry_tx: &mpsc::Sender<DbCommand>,
    command: DbCommand,
) -> EnqueueOutcome {
    match tx.try_send(command) {
        Ok(()) => EnqueueOutcome::QueuedPrimary,
        Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
            match overflow_tx.try_send(command) {
                Ok(()) => EnqueueOutcome::QueuedOverflow,
                Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                    match retry_tx.try_send(command) {
                        Ok(()) => EnqueueOutcome::QueuedRetry,
                        Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                            let _ = command;
                            EnqueueOutcome::DroppedRetryFull
                        }
                        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                            EnqueueOutcome::DroppedClosed
                        }
                    }
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    EnqueueOutcome::DroppedClosed
                }
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => EnqueueOutcome::DroppedClosed,
    }
}

impl DbWriter {
    /// Enqueue a batch of trades for async persistence.
    pub fn send(&self, trades: Vec<FleetTrade>) {
        if trades.is_empty() {
            return;
        }
        let command = DbCommand::Trades(trades);
        let outcome = try_enqueue_command(&self.tx, &self.overflow_tx, &self.retry_tx, command);
        match outcome {
            EnqueueOutcome::QueuedPrimary => {}
            EnqueueOutcome::QueuedOverflow => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_power_of_two() || n.is_multiple_of(1000) {
                    warn!("db writer primary queue full, deferred batches total: {n}");
                }
            }
            EnqueueOutcome::QueuedRetry => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                if n.is_power_of_two() || n.is_multiple_of(1000) {
                    warn!(
                        "db writer primary+overflow full, queued in retry buffer (total deferred: {n})"
                    );
                }
            }
            EnqueueOutcome::DroppedRetryFull => {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!(
                    "db writer queues saturated (including retry), dropping batch (total dropped: {n})"
                );
            }
            EnqueueOutcome::DroppedClosed => {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer channel closed, dropping batch (total dropped: {n})");
            }
        }
    }

    /// Flush all buffered DB writer data to disk (best effort).
    pub async fn flush_all(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.tx.send(DbCommand::Flush(tx)).await.is_err() {
            warn!("db writer flush requested but channel is closed");
            return;
        }
        let _ = rx.await;
    }

    /// Number of trade batches lost because writer queues are saturated or closed.
    pub fn dropped_batches() -> u64 {
        DROPPED_BATCHES.load(Ordering::Relaxed)
    }

    /// Number of batches that entered overflow/retry buffers due to primary saturation.
    pub fn overflowed_batches() -> u64 {
        OVERFLOWED_BATCHES.load(Ordering::Relaxed)
    }

    /// Allowed dropped-batch budget before health degrades.
    pub const fn dropped_batch_budget() -> u64 {
        DROPPED_BATCH_BUDGET
    }

    /// Overflow counter threshold after which health emits warnings.
    pub const fn overflow_warn_threshold() -> u64 {
        OVERFLOW_WARN_THRESHOLD
    }
}

/// Spawn the background writer task. Returns a handle for sending trades.
pub fn spawn_writer(db_path: &Path) -> DbWriter {
    let (tx, mut rx) = mpsc::channel::<DbCommand>(CHANNEL_CAPACITY);
    let (overflow_tx, mut overflow_rx) = mpsc::channel::<DbCommand>(OVERFLOW_CHANNEL_CAPACITY);
    let (retry_tx, mut retry_rx) = mpsc::channel::<DbCommand>(RETRY_CHANNEL_CAPACITY);
    let path = db_path.to_path_buf();
    let primary_tx = tx.clone();
    let overflow_retry_tx = overflow_tx.clone();

    tokio::spawn(async move {
        while let Some(command) = overflow_rx.recv().await {
            if primary_tx.send(command).await.is_err() {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer closed while draining overflow queue (total dropped: {n})");
                break;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(command) = retry_rx.recv().await {
            if overflow_retry_tx.send(command).await.is_err() {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer closed while draining retry queue (total dropped: {n})");
                break;
            }
        }
    });

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
        let mut interval =
            tokio::time::interval(tokio::time::Duration::from_secs(FLUSH_INTERVAL_SECS));

        loop {
            tokio::select! {
                command = rx.recv() => {
                    match command {
                        Some(DbCommand::Trades(trades)) => buf.extend(trades),
                        Some(DbCommand::Flush(done)) => {
                            if !buf.is_empty() {
                                match flush_trades(&conn, &buf) {
                                    Ok(_) => buf.clear(),
                                    Err(e) => warn!("db flush error on explicit flush (retaining {} trades): {e}", buf.len()),
                                }
                            }
                            let _ = done.send(());
                        }
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

    DbWriter {
        tx,
        overflow_tx,
        retry_tx,
    }
}

fn flush_trades(conn: &Connection, trades: &[FleetTrade]) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT OR IGNORE INTO trades (config_id, symbol, direction, entry_ts_ms, exit_ts_ms,
             entry_price, exit_price, spike_bps, pnl_pct, exit_reason,
             gate_spread_at_entry_bps, gate_natr_30m_pct_at_entry, hold_ms, early_stop_churn, run_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
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
                t.gate_natr_30m_pct_at_entry,
                t.hold_ms,
                if t.early_stop_churn { 1_i64 } else { 0_i64 },
                ft.run_id.as_deref(),
            ])?;
        }
    }
    tx.commit()?;
    info!("flushed {} trades to db", trades.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::screener::shadow_trader::{ClosedTrade, Direction};

    fn temp_db_path(name: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "hft-lead-lag-db-{name}-{}-{}.sqlite",
            std::process::id(),
            crate::domain::screener::utils::now_ms()
        ))
    }

    fn cleanup_temp_db(path: &std::path::Path) {
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }

    fn sample_trade(symbol: &str) -> FleetTrade {
        FleetTrade {
            config_id: 1,
            symbol: symbol.to_string(),
            run_id: None,
            trade: ClosedTrade {
                pnl_pct: 0.1,
                ts_ms: 2,
                direction: Direction::Long,
                entry_ts_ms: 1,
                entry_price: 100.0,
                exit_price: 101.0,
                exit_reason: "target",
                spike_bps: 35.0,
                catchup_pct: 0.5,
                catchup_ms: 5,
                gate_spread_at_entry_bps: 1.0,
                gate_natr_30m_pct_at_entry: 0.2,
                hold_ms: 4,
                early_stop_churn: false,
            },
        }
    }

    #[test]
    fn open_db_adds_strategy_kind_column() {
        let path = temp_db_path("strategy-kind-column");
        let conn = open_db(&path).expect("open db");

        let has_strategy_kind: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('configs') WHERE name='strategy_kind'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("pragma query");
        assert!(has_strategy_kind, "configs.strategy_kind column must exist");

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn open_db_adds_trade_context_columns() {
        let path = temp_db_path("trade-context-columns");
        let conn = open_db(&path).expect("open db");

        let has_natr_col: bool = conn
            .prepare(
                "SELECT 1 FROM pragma_table_info('trades') WHERE name='gate_natr_30m_pct_at_entry'",
            )
            .and_then(|mut stmt| stmt.exists([]))
            .expect("natr column pragma query");
        let has_hold_ms_col: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('trades') WHERE name='hold_ms'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("hold_ms column pragma query");
        let has_early_stop_col: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('trades') WHERE name='early_stop_churn'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("early_stop column pragma query");

        assert!(has_natr_col, "trades.gate_natr_30m_pct_at_entry must exist");
        assert!(has_hold_ms_col, "trades.hold_ms must exist");
        assert!(has_early_stop_col, "trades.early_stop_churn must exist");

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn upsert_configs_persists_default_strategy_kind() {
        let path = temp_db_path("strategy-kind-upsert");
        let conn = open_db(&path).expect("open db");
        let cfg = TraderConfig::default();

        upsert_configs(&conn, &[cfg]).expect("upsert config");

        let strategy_kind: String = conn
            .query_row(
                "SELECT strategy_kind FROM configs WHERE id=?1",
                [cfg.config_id() as i64],
                |row| row.get(0),
            )
            .expect("fetch strategy kind");
        assert_eq!(strategy_kind, "baseline_gap");

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn open_db_creates_trial_runs_meta_table() {
        let path = temp_db_path("trial-runs-meta");
        let conn = open_db(&path).expect("open db");

        let has_table: bool = conn
            .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='trial_runs_meta'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("trial_runs_meta exists query");
        assert!(has_table, "trial_runs_meta table must exist");
        let has_closed_at_col: bool = conn
            .prepare("SELECT 1 FROM pragma_table_info('trial_runs_meta') WHERE name='closed_at_ms'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("trial_runs_meta.closed_at_ms exists query");
        assert!(
            has_closed_at_col,
            "trial_runs_meta.closed_at_ms column must exist"
        );
        for column in [
            "apply_mode",
            "symbols_reset",
            "changed_ids_requested",
            "matched_changed_ids_old",
            "matched_changed_ids_new",
            "unmatched_changed_ids",
            "scope_symbols_requested",
            "scope_symbols_matched",
        ] {
            let has_col: bool = conn
                .prepare("SELECT 1 FROM pragma_table_info('trial_runs_meta') WHERE name=?1")
                .and_then(|mut stmt| stmt.exists([column]))
                .expect("trial_runs_meta metadata column exists query");
            assert!(has_col, "trial_runs_meta.{column} column must exist");
        }

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn upsert_trial_run_meta_updates_existing_row() {
        let path = temp_db_path("trial-runs-meta-upsert");
        let conn = open_db(&path).expect("open db");

        upsert_trial_run_meta(
            &conn,
            "scout-1",
            100,
            1_000,
            5,
            TrialPatchMeta {
                apply_mode: "full_replace",
                symbols_reset: 4,
                ..TrialPatchMeta::default()
            },
        )
        .expect("first upsert");
        upsert_trial_run_meta(
            &conn,
            "scout-1",
            250,
            2_000,
            12,
            TrialPatchMeta {
                apply_mode: "incremental",
                symbols_reset: 2,
                changed_ids_requested: 3,
                matched_changed_ids_old: 2,
                matched_changed_ids_new: 2,
                unmatched_changed_ids: 1,
                scope_symbols_requested: 2,
                scope_symbols_matched: 1,
            },
        )
        .expect("second upsert");

        type TrialRunMetaRow = (
            i64,
            i64,
            i64,
            String,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            i64,
            Option<i64>,
        );

        let (
            submitted,
            applied_at,
            drained,
            apply_mode,
            symbols_reset,
            changed_ids_requested,
            matched_changed_ids_old,
            matched_changed_ids_new,
            unmatched_changed_ids,
            scope_symbols_requested,
            scope_symbols_matched,
            closed_at,
        ): TrialRunMetaRow = conn
            .query_row(
                "SELECT submitted_config_count, applied_at_ms, drained_trades,
                        apply_mode, symbols_reset, changed_ids_requested,
                        matched_changed_ids_old, matched_changed_ids_new, unmatched_changed_ids,
                        scope_symbols_requested, scope_symbols_matched, closed_at_ms
                 FROM trial_runs_meta
                 WHERE run_id = ?1",
                ["scout-1"],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                        row.get(8)?,
                        row.get(9)?,
                        row.get(10)?,
                        row.get(11)?,
                    ))
                },
            )
            .expect("fetch trial_runs_meta");
        assert_eq!(submitted, 250);
        assert_eq!(applied_at, 2_000);
        assert_eq!(drained, 12);
        assert_eq!(apply_mode, "incremental");
        assert_eq!(symbols_reset, 2);
        assert_eq!(changed_ids_requested, 3);
        assert_eq!(matched_changed_ids_old, 2);
        assert_eq!(matched_changed_ids_new, 2);
        assert_eq!(unmatched_changed_ids, 1);
        assert_eq!(scope_symbols_requested, 2);
        assert_eq!(scope_symbols_matched, 1);
        assert_eq!(closed_at, None);

        close_trial_run_meta(&conn, "scout-1", 3_000).expect("close run");
        close_trial_run_meta(&conn, "scout-1", 4_000).expect("close run idempotent");
        let closed_at_after: Option<i64> = conn
            .query_row(
                "SELECT closed_at_ms FROM trial_runs_meta WHERE run_id = ?1",
                ["scout-1"],
                |row| row.get(0),
            )
            .expect("fetch closed_at_ms");
        assert_eq!(closed_at_after, Some(3_000));

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn open_db_creates_family_cluster_tables() {
        let path = temp_db_path("family-cluster-tables");
        let conn = open_db(&path).expect("open db");

        for table_name in [
            "config_families",
            "family_symbol_clusters",
            "portfolio_state",
        ] {
            let has_table: bool = conn
                .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1")
                .and_then(|mut stmt| stmt.exists([table_name]))
                .expect("table exists query");
            assert!(has_table, "{table_name} table must exist");
        }

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn open_db_readonly_rejects_writes() {
        let path = temp_db_path("readonly-open");
        let conn = open_db(&path).expect("open db");
        drop(conn);

        let ro = open_db_readonly(&path).expect("open readonly db");
        let write_res = ro.execute("PRAGMA user_version = 1", []);
        assert!(write_res.is_err(), "readonly connection must reject writes");

        drop(ro);
        cleanup_temp_db(&path);
    }

    #[test]
    fn try_enqueue_command_uses_overflow_when_primary_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, mut rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, mut overflow_rx) = mpsc::channel::<DbCommand>(2);
        let (retry_tx, _retry_rx) = mpsc::channel::<DbCommand>(2);
        tx.try_send(DbCommand::Trades(vec![sample_trade("BTCUSDT")]))
            .expect("pre-fill primary channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            DbCommand::Trades(vec![sample_trade("ETHUSDT")]),
        );

        assert_eq!(outcome, EnqueueOutcome::QueuedOverflow);
        assert!(
            matches!(overflow_rx.try_recv(), Ok(DbCommand::Trades(trades)) if trades.len() == 1 && trades[0].symbol == "ETHUSDT")
        );
        assert!(
            matches!(rx.try_recv(), Ok(DbCommand::Trades(trades)) if trades.len() == 1 && trades[0].symbol == "BTCUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn try_enqueue_command_uses_retry_when_primary_and_overflow_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, mut retry_rx) = mpsc::channel::<DbCommand>(2);
        tx.try_send(DbCommand::Trades(vec![sample_trade("BTCUSDT")]))
            .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades(vec![sample_trade("ETHUSDT")]))
            .expect("pre-fill overflow channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            DbCommand::Trades(vec![sample_trade("SOLUSDT")]),
        );

        assert_eq!(outcome, EnqueueOutcome::QueuedRetry);
        assert!(
            matches!(retry_rx.try_recv(), Ok(DbCommand::Trades(trades)) if trades.len() == 1 && trades[0].symbol == "SOLUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn try_enqueue_command_drops_when_all_queues_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, _retry_rx) = mpsc::channel::<DbCommand>(1);
        tx.try_send(DbCommand::Trades(vec![sample_trade("BTCUSDT")]))
            .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades(vec![sample_trade("ETHUSDT")]))
            .expect("pre-fill overflow channel");
        retry_tx
            .try_send(DbCommand::Trades(vec![sample_trade("SOLUSDT")]))
            .expect("pre-fill retry channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            DbCommand::Trades(vec![sample_trade("ADAUSDT")]),
        );

        assert_eq!(outcome, EnqueueOutcome::DroppedRetryFull);
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }
}
