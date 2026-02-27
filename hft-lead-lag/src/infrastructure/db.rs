//! SQLite persistence for fleet trades and configs.
//!
//! WAL mode for concurrent reads. Async batch writer via mpsc channel
//! flushes every 5s — zero impact on trading hot path.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rusqlite::{params, Connection, OpenFlags};
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
/// Final bounded spillover queue used when primary/overflow/retry are all saturated.
const SPILLOVER_CHANNEL_CAPACITY: usize = 8_192;
/// Last-resort bounded queue: producers block here instead of dropping batches.
const BACKPRESSURE_CHANNEL_CAPACITY: usize = 512;
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

fn ensure_table_columns(conn: &Connection, table: &str, columns: &[&str]) -> rusqlite::Result<()> {
    for column in columns {
        if !table_has_column(conn, table, column)? {
            return Err(rusqlite::Error::InvalidColumnName(format!(
                "{table}.{column}"
            )));
        }
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

CREATE TABLE IF NOT EXISTS portfolio_state_v1 (
    portfolio_id TEXT PRIMARY KEY,
    shortlist_json TEXT NOT NULL,
    active_symbols_json TEXT NOT NULL,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_symbol_guard_v1 (
    symbol TEXT PRIMARY KEY,
    streak_count INTEGER NOT NULL,
    first_streak_ts_ms INTEGER,
    cooldown_until_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS portfolio_paper_state_v1 (
    portfolio_id TEXT PRIMARY KEY,
    equity_usd REAL NOT NULL,
    realized_pnl_usd REAL NOT NULL,
    closed_trades INTEGER NOT NULL,
    profitable_trades INTEGER NOT NULL,
    losing_trades INTEGER NOT NULL,
    last_trade_ts_ms INTEGER,
    updated_at_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_config ON trades(config_id);
CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol);
CREATE INDEX IF NOT EXISTS idx_trades_exit_ts ON trades(exit_ts_ms);
CREATE INDEX IF NOT EXISTS idx_trial_runs_meta_applied_at ON trial_runs_meta(applied_at_ms);
CREATE INDEX IF NOT EXISTS idx_portfolio_symbol_guard_v1_cooldown
    ON portfolio_symbol_guard_v1(cooldown_until_ms);
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
    ensure_table_columns(
        &conn,
        "trial_runs_meta",
        &[
            "run_id",
            "submitted_config_count",
            "applied_at_ms",
            "drained_trades",
            "apply_mode",
            "symbols_reset",
            "changed_ids_requested",
            "matched_changed_ids_old",
            "matched_changed_ids_new",
            "unmatched_changed_ids",
            "scope_symbols_requested",
            "scope_symbols_matched",
            "closed_at_ms",
        ],
    )?;
    ensure_table_columns(
        &conn,
        "portfolio_state_v1",
        &[
            "portfolio_id",
            "shortlist_json",
            "active_symbols_json",
            "updated_at_ms",
        ],
    )?;
    ensure_table_columns(
        &conn,
        "portfolio_symbol_guard_v1",
        &[
            "symbol",
            "streak_count",
            "first_streak_ts_ms",
            "cooldown_until_ms",
            "updated_at_ms",
        ],
    )?;
    ensure_table_columns(
        &conn,
        "portfolio_paper_state_v1",
        &[
            "portfolio_id",
            "equity_usd",
            "realized_pnl_usd",
            "closed_trades",
            "profitable_trades",
            "losing_trades",
            "last_trade_ts_ms",
            "updated_at_ms",
        ],
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

pub use crate::domain::screener::{
    PortfolioCandidateHistoryRecordV1, PortfolioGuardRecordV1, PortfolioPaperStateRecordV1,
    PortfolioStateRecordV1,
};

fn encode_symbols_json(symbols: &[String]) -> rusqlite::Result<String> {
    serde_json::to_string(symbols).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
}

fn decode_symbols_json(value: String) -> rusqlite::Result<Vec<String>> {
    serde_json::from_str(&value).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}

pub fn replace_portfolio_state_v1(
    conn: &Connection,
    rows: &[PortfolioStateRecordV1],
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM portfolio_state_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_state_v1 (
                portfolio_id, shortlist_json, active_symbols_json, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for row in rows {
            let shortlist_json = encode_symbols_json(&row.shortlist)?;
            let active_json = encode_symbols_json(&row.active_symbols)?;
            stmt.execute(params![
                row.portfolio_id,
                shortlist_json,
                active_json,
                row.updated_at_ms
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn replace_portfolio_snapshot_v1(
    conn: &Connection,
    states: &[PortfolioStateRecordV1],
    guards: &[PortfolioGuardRecordV1],
    paper_states: &[PortfolioPaperStateRecordV1],
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM portfolio_state_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_state_v1 (
                portfolio_id, shortlist_json, active_symbols_json, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4)",
        )?;
        for row in states {
            let shortlist_json = encode_symbols_json(&row.shortlist)?;
            let active_json = encode_symbols_json(&row.active_symbols)?;
            stmt.execute(params![
                row.portfolio_id,
                shortlist_json,
                active_json,
                row.updated_at_ms
            ])?;
        }
    }
    tx.execute("DELETE FROM portfolio_symbol_guard_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_symbol_guard_v1 (
                symbol, streak_count, first_streak_ts_ms, cooldown_until_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for row in guards {
            stmt.execute(params![
                row.symbol,
                row.streak_count as i64,
                row.first_streak_ts_ms,
                row.cooldown_until_ms,
                row.updated_at_ms
            ])?;
        }
    }
    tx.execute("DELETE FROM portfolio_paper_state_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_paper_state_v1 (
                portfolio_id, equity_usd, realized_pnl_usd, closed_trades,
                profitable_trades, losing_trades, last_trade_ts_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for row in paper_states {
            stmt.execute(params![
                row.portfolio_id,
                row.equity_usd,
                row.realized_pnl_usd,
                row.closed_trades.min(i64::MAX as u64) as i64,
                row.profitable_trades.min(i64::MAX as u64) as i64,
                row.losing_trades.min(i64::MAX as u64) as i64,
                row.last_trade_ts_ms,
                row.updated_at_ms
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn replace_portfolio_guards_v1(
    conn: &Connection,
    rows: &[PortfolioGuardRecordV1],
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM portfolio_symbol_guard_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_symbol_guard_v1 (
                symbol, streak_count, first_streak_ts_ms, cooldown_until_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;
        for row in rows {
            stmt.execute(params![
                row.symbol,
                row.streak_count as i64,
                row.first_streak_ts_ms,
                row.cooldown_until_ms,
                row.updated_at_ms
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn replace_portfolio_paper_state_v1(
    conn: &Connection,
    rows: &[PortfolioPaperStateRecordV1],
) -> rusqlite::Result<()> {
    let tx = conn.unchecked_transaction()?;
    tx.execute("DELETE FROM portfolio_paper_state_v1", [])?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO portfolio_paper_state_v1 (
                portfolio_id, equity_usd, realized_pnl_usd, closed_trades,
                profitable_trades, losing_trades, last_trade_ts_ms, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )?;
        for row in rows {
            stmt.execute(params![
                row.portfolio_id,
                row.equity_usd,
                row.realized_pnl_usd,
                row.closed_trades.min(i64::MAX as u64) as i64,
                row.profitable_trades.min(i64::MAX as u64) as i64,
                row.losing_trades.min(i64::MAX as u64) as i64,
                row.last_trade_ts_ms,
                row.updated_at_ms
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

pub fn load_portfolio_state_v1(conn: &Connection) -> rusqlite::Result<Vec<PortfolioStateRecordV1>> {
    let mut stmt = conn.prepare(
        "SELECT portfolio_id, shortlist_json, active_symbols_json, updated_at_ms
         FROM portfolio_state_v1
         ORDER BY portfolio_id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let shortlist_json: String = row.get(1)?;
        let active_json: String = row.get(2)?;
        Ok(PortfolioStateRecordV1 {
            portfolio_id: row.get(0)?,
            shortlist: decode_symbols_json(shortlist_json)?,
            active_symbols: decode_symbols_json(active_json)?,
            updated_at_ms: row.get(3)?,
        })
    })?;
    rows.collect()
}

pub fn load_portfolio_guards_v1(
    conn: &Connection,
) -> rusqlite::Result<Vec<PortfolioGuardRecordV1>> {
    let mut stmt = conn.prepare(
        "SELECT symbol, streak_count, first_streak_ts_ms, cooldown_until_ms, updated_at_ms
         FROM portfolio_symbol_guard_v1
         ORDER BY symbol ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let streak_count_i64: i64 = row.get(1)?;
        Ok(PortfolioGuardRecordV1 {
            symbol: row.get(0)?,
            streak_count: streak_count_i64.max(0) as u32,
            first_streak_ts_ms: row.get(2)?,
            cooldown_until_ms: row.get(3)?,
            updated_at_ms: row.get(4)?,
        })
    })?;
    rows.collect()
}

pub fn load_portfolio_paper_state_v1(
    conn: &Connection,
) -> rusqlite::Result<Vec<PortfolioPaperStateRecordV1>> {
    let mut stmt = conn.prepare(
        "SELECT
            portfolio_id, equity_usd, realized_pnl_usd, closed_trades,
            profitable_trades, losing_trades, last_trade_ts_ms, updated_at_ms
         FROM portfolio_paper_state_v1
         ORDER BY portfolio_id ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let closed_trades_i64: i64 = row.get(3)?;
        let profitable_trades_i64: i64 = row.get(4)?;
        let losing_trades_i64: i64 = row.get(5)?;
        Ok(PortfolioPaperStateRecordV1 {
            portfolio_id: row.get(0)?,
            equity_usd: row.get(1)?,
            realized_pnl_usd: row.get(2)?,
            closed_trades: closed_trades_i64.max(0) as u64,
            profitable_trades: profitable_trades_i64.max(0) as u64,
            losing_trades: losing_trades_i64.max(0) as u64,
            last_trade_ts_ms: row.get(6)?,
            updated_at_ms: row.get(7)?,
        })
    })?;
    rows.collect()
}

pub fn load_portfolio_candidate_history_v1(
    conn: &Connection,
) -> rusqlite::Result<Vec<PortfolioCandidateHistoryRecordV1>> {
    let mut stmt = conn.prepare(
        "SELECT
            symbol,
            COUNT(*) AS closed_trades,
            SUM(CASE WHEN pnl_pct > 0.0 THEN 1 ELSE 0 END) AS profitable_trades,
            SUM(CASE WHEN pnl_pct < 0.0 THEN 1 ELSE 0 END) AS losing_trades,
            COALESCE(SUM(pnl_pct), 0.0) AS pnl_sum_pct,
            MIN(entry_ts_ms) AS first_trade_ts_ms
         FROM trades
         GROUP BY symbol
         ORDER BY symbol ASC",
    )?;
    let rows = stmt.query_map([], |row| {
        let closed_trades_i64: i64 = row.get(1)?;
        let profitable_trades_i64: i64 = row.get(2)?;
        let losing_trades_i64: i64 = row.get(3)?;
        Ok(PortfolioCandidateHistoryRecordV1 {
            symbol: row.get(0)?,
            closed_trades: closed_trades_i64.max(0) as u32,
            profitable_trades: profitable_trades_i64.max(0) as u32,
            losing_trades: losing_trades_i64.max(0) as u32,
            pnl_sum_pct: row.get(4)?,
            first_trade_ts_ms: row.get(5)?,
        })
    })?;
    rows.collect()
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
    spillover_tx: mpsc::Sender<DbCommand>,
    backpressure_tx: mpsc::Sender<DbCommand>,
    next_seq: Arc<AtomicU64>,
}

#[derive(Debug)]
enum DbCommand {
    Trades {
        seq: u64,
        trades: Vec<FleetTrade>,
    },
    PortfolioSnapshotV1 {
        seq: u64,
        states: Vec<PortfolioStateRecordV1>,
        guards: Vec<PortfolioGuardRecordV1>,
        paper_states: Vec<PortfolioPaperStateRecordV1>,
    },
    Flush {
        target_seq: u64,
        done: tokio::sync::oneshot::Sender<()>,
    },
}

#[derive(Debug)]
enum EnqueueOutcome {
    QueuedPrimary,
    QueuedOverflow,
    QueuedRetry,
    QueuedSpillover,
    NeedsBackpressure(DbCommand),
    DroppedClosed,
}

fn try_enqueue_command(
    tx: &mpsc::Sender<DbCommand>,
    overflow_tx: &mpsc::Sender<DbCommand>,
    retry_tx: &mpsc::Sender<DbCommand>,
    spillover_tx: &mpsc::Sender<DbCommand>,
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
                            match spillover_tx.try_send(command) {
                                Ok(()) => EnqueueOutcome::QueuedSpillover,
                                Err(tokio::sync::mpsc::error::TrySendError::Full(command)) => {
                                    EnqueueOutcome::NeedsBackpressure(command)
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
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    EnqueueOutcome::DroppedClosed
                }
            }
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => EnqueueOutcome::DroppedClosed,
    }
}

impl DbWriter {
    fn enqueue_command(&self, command: DbCommand, command_label: &'static str) {
        let outcome = try_enqueue_command(
            &self.tx,
            &self.overflow_tx,
            &self.retry_tx,
            &self.spillover_tx,
            command,
        );

        let log_deferred = |stage: &str, total_deferred: u64| {
            if total_deferred.is_power_of_two() || total_deferred.is_multiple_of(1000) {
                warn!(
                    "db writer {stage} while enqueueing {command_label} (total deferred: {total_deferred})"
                );
            }
        };

        match outcome {
            EnqueueOutcome::QueuedPrimary => {}
            EnqueueOutcome::QueuedOverflow => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                log_deferred("primary queue full", n);
            }
            EnqueueOutcome::QueuedRetry => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                log_deferred("primary+overflow queues full, queued in retry buffer", n);
            }
            EnqueueOutcome::QueuedSpillover => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                log_deferred(
                    "primary+overflow+retry queues full, queued in spillover buffer",
                    n,
                );
            }
            EnqueueOutcome::NeedsBackpressure(command) => {
                let n = OVERFLOWED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                log_deferred(
                    "all async queues saturated; applying producer backpressure",
                    n,
                );
                match self.backpressure_tx.try_send(command) {
                    Ok(()) => {}
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        let dropped = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                        warn!(
                            "db writer backpressure queue full, dropping {command_label} (total dropped: {dropped})"
                        );
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        let dropped = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                        warn!(
                            "db writer backpressure queue closed, dropping {command_label} (total dropped: {dropped})"
                        );
                    }
                }
            }
            EnqueueOutcome::DroppedClosed => {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer channel closed, dropping {command_label} (total dropped: {n})");
            }
        }
    }

    /// Enqueue a batch of trades for async persistence.
    pub fn send(&self, trades: Vec<FleetTrade>) {
        if trades.is_empty() {
            return;
        }
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let command = DbCommand::Trades { seq, trades };
        self.enqueue_command(command, "trade batch");
    }

    /// Enqueue a portfolio snapshot (active/shortlist + guard + paper state).
    pub fn send_portfolio_snapshot_v1(
        &self,
        states: Vec<PortfolioStateRecordV1>,
        guards: Vec<PortfolioGuardRecordV1>,
        paper_states: Vec<PortfolioPaperStateRecordV1>,
    ) {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed) + 1;
        let command = DbCommand::PortfolioSnapshotV1 {
            seq,
            states,
            guards,
            paper_states,
        };
        self.enqueue_command(command, "portfolio snapshot");
    }

    /// Flush all currently enqueued DB writer data to disk (best effort).
    ///
    /// The flush waits until the writer has observed all trade batches that
    /// were enqueued before this call, including those temporarily staged in
    /// overflow/retry/spillover/backpressure channels.
    pub async fn flush_all(&self) {
        let target_seq = self.next_seq.load(Ordering::Acquire);
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(DbCommand::Flush {
                target_seq,
                done: tx,
            })
            .await
            .is_err()
        {
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

fn flush_trade_buffer(conn: &Connection, buf: &mut Vec<FleetTrade>) {
    if !buf.is_empty() {
        match flush_trades(conn, buf) {
            Ok(_) => buf.clear(),
            Err(e) => warn!("db flush error (retaining {} trades): {e}", buf.len()),
        }
    }
}

fn complete_ready_flushes(
    observed_max_seq: u64,
    pending_flushes: &mut Vec<(u64, tokio::sync::oneshot::Sender<()>)>,
) {
    let mut idx = 0usize;
    while idx < pending_flushes.len() {
        if pending_flushes[idx].0 <= observed_max_seq {
            let (_, done) = pending_flushes.swap_remove(idx);
            let _ = done.send(());
        } else {
            idx += 1;
        }
    }
}

/// Spawn the background writer task. Returns a handle for sending trades.
pub fn spawn_writer(db_path: &Path) -> DbWriter {
    let (tx, mut rx) = mpsc::channel::<DbCommand>(CHANNEL_CAPACITY);
    let (overflow_tx, mut overflow_rx) = mpsc::channel::<DbCommand>(OVERFLOW_CHANNEL_CAPACITY);
    let (retry_tx, mut retry_rx) = mpsc::channel::<DbCommand>(RETRY_CHANNEL_CAPACITY);
    let (spillover_tx, mut spillover_rx) = mpsc::channel::<DbCommand>(SPILLOVER_CHANNEL_CAPACITY);
    let (backpressure_tx, mut backpressure_rx) =
        mpsc::channel::<DbCommand>(BACKPRESSURE_CHANNEL_CAPACITY);
    let path = db_path.to_path_buf();
    let primary_tx = tx.clone();
    let overflow_retry_tx = overflow_tx.clone();
    let retry_spillover_tx = retry_tx.clone();
    let spillover_backpressure_tx = spillover_tx.clone();

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
        while let Some(command) = spillover_rx.recv().await {
            if retry_spillover_tx.send(command).await.is_err() {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer closed while draining spillover queue (total dropped: {n})");
                break;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(command) = backpressure_rx.recv().await {
            if spillover_backpressure_tx.send(command).await.is_err() {
                let n = DROPPED_BATCHES.fetch_add(1, Ordering::Relaxed) + 1;
                warn!("db writer closed while draining backpressure queue (total dropped: {n})");
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
        let mut observed_max_seq: u64 = 0;
        let mut pending_flushes: Vec<(u64, tokio::sync::oneshot::Sender<()>)> = Vec::new();
        let mut latest_portfolio_snapshot_seq: u64 = 0;

        loop {
            tokio::select! {
                command = rx.recv() => {
                    match command {
                        Some(DbCommand::Trades { seq, trades }) => {
                            observed_max_seq = observed_max_seq.max(seq);
                            buf.extend(trades);
                            if !pending_flushes.is_empty() {
                                flush_trade_buffer(&conn, &mut buf);
                                complete_ready_flushes(observed_max_seq, &mut pending_flushes);
                            }
                        }
                        Some(DbCommand::PortfolioSnapshotV1 {
                            seq,
                            states,
                            guards,
                            paper_states,
                        }) => {
                            observed_max_seq = observed_max_seq.max(seq);
                            flush_trade_buffer(&conn, &mut buf);
                            if seq <= latest_portfolio_snapshot_seq {
                                warn!(
                                    "db writer ignoring stale portfolio snapshot seq={} latest={}",
                                    seq,
                                    latest_portfolio_snapshot_seq
                                );
                            } else {
                                match replace_portfolio_snapshot_v1(
                                    &conn,
                                    &states,
                                    &guards,
                                    &paper_states,
                                ) {
                                    Ok(_) => latest_portfolio_snapshot_seq = seq,
                                    Err(e) => warn!("db portfolio snapshot flush error: {e}"),
                                }
                            }
                            complete_ready_flushes(observed_max_seq, &mut pending_flushes);
                        }
                        Some(DbCommand::Flush { target_seq, done }) => {
                            flush_trade_buffer(&conn, &mut buf);
                            if observed_max_seq >= target_seq {
                                let _ = done.send(());
                            } else {
                                pending_flushes.push((target_seq, done));
                            }
                        }
                        None => break, // channel closed
                    }
                }
                _ = interval.tick() => {
                    flush_trade_buffer(&conn, &mut buf);
                    complete_ready_flushes(observed_max_seq, &mut pending_flushes);
                }
            }
        }
        // Flush remaining on shutdown.
        if !buf.is_empty() {
            if let Err(e) = flush_trades(&conn, &buf) {
                warn!(
                    "db flush error during shutdown (dropping {} trades): {e}",
                    buf.len()
                );
            }
        }
        for (_, done) in pending_flushes.drain(..) {
            let _ = done.send(());
        }
        info!("db writer stopped");
    });

    DbWriter {
        tx,
        overflow_tx,
        retry_tx,
        spillover_tx,
        backpressure_tx,
        next_seq: Arc::new(AtomicU64::new(0)),
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
    use crate::domain::screener::{
        shadow_trader::{ClosedTrade, Direction},
        TraderConfig,
    };
    use tokio::time::{timeout, Duration};

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
            config_id: TraderConfig::default().config_id(),
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

    fn sample_trade_with(
        symbol: &str,
        pnl_pct: f64,
        entry_ts_ms: i64,
        exit_ts_ms: i64,
    ) -> FleetTrade {
        let mut trade = sample_trade(symbol);
        trade.trade.pnl_pct = pnl_pct;
        trade.trade.entry_ts_ms = entry_ts_ms;
        trade.trade.ts_ms = exit_ts_ms;
        trade
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
    fn open_db_renames_legacy_tables_to_timestamped_backups() {
        let path = temp_db_path("legacy-backup-migration");
        let conn = rusqlite::Connection::open(&path).expect("open raw db");
        conn.execute_batch(
            "CREATE TABLE configs (
                id INTEGER PRIMARY KEY,
                trailing_stop_bps REAL NOT NULL
            );
            CREATE TABLE trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                config_id INTEGER NOT NULL,
                symbol TEXT NOT NULL
            );
            INSERT INTO configs (id, trailing_stop_bps) VALUES (1, 12.5);
            INSERT INTO trades (config_id, symbol) VALUES (1, 'BTCUSDT');",
        )
        .expect("seed legacy schema");
        drop(conn);

        let migrated = open_db(&path).expect("open and migrate db");

        let backup_configs_table: String = migrated
            .query_row(
                "SELECT name
                 FROM sqlite_master
                 WHERE type='table' AND name LIKE 'configs_backup_%'
                 ORDER BY name DESC
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("backup configs table exists");
        let backup_trades_table: String = migrated
            .query_row(
                "SELECT name
                 FROM sqlite_master
                 WHERE type='table' AND name LIKE 'trades_backup_%'
                 ORDER BY name DESC
                 LIMIT 1",
                [],
                |row| row.get(0),
            )
            .expect("backup trades table exists");

        let backup_configs_rows: i64 = migrated
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{backup_configs_table}\""),
                [],
                |row| row.get(0),
            )
            .expect("count backup configs");
        let backup_trades_rows: i64 = migrated
            .query_row(
                &format!("SELECT COUNT(*) FROM \"{backup_trades_table}\""),
                [],
                |row| row.get(0),
            )
            .expect("count backup trades");

        assert_eq!(backup_configs_rows, 1);
        assert_eq!(backup_trades_rows, 1);

        let has_strategy_kind: bool = migrated
            .prepare("SELECT 1 FROM pragma_table_info('configs') WHERE name='strategy_kind'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("pragma query");
        assert!(has_strategy_kind, "new configs table must be recreated");

        drop(migrated);
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
    fn open_db_creates_portfolio_runtime_tables_v1() {
        let path = temp_db_path("portfolio-runtime-tables-v1");
        let conn = open_db(&path).expect("open db");

        let has_state_table: bool = conn
            .prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='portfolio_state_v1'")
            .and_then(|mut stmt| stmt.exists([]))
            .expect("portfolio_state_v1 exists query");
        assert!(has_state_table, "portfolio_state_v1 table must exist");

        let has_guard_table: bool = conn
            .prepare(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='portfolio_symbol_guard_v1'",
            )
            .and_then(|mut stmt| stmt.exists([]))
            .expect("portfolio_symbol_guard_v1 exists query");
        assert!(
            has_guard_table,
            "portfolio_symbol_guard_v1 table must exist"
        );

        let has_paper_table: bool = conn
            .prepare(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='portfolio_paper_state_v1'",
            )
            .and_then(|mut stmt| stmt.exists([]))
            .expect("portfolio_paper_state_v1 exists query");
        assert!(has_paper_table, "portfolio_paper_state_v1 table must exist");

        for column in [
            "portfolio_id",
            "shortlist_json",
            "active_symbols_json",
            "updated_at_ms",
        ] {
            let has_col: bool = conn
                .prepare("SELECT 1 FROM pragma_table_info('portfolio_state_v1') WHERE name=?1")
                .and_then(|mut stmt| stmt.exists([column]))
                .expect("portfolio_state_v1 column exists query");
            assert!(has_col, "portfolio_state_v1.{column} column must exist");
        }

        for column in [
            "symbol",
            "streak_count",
            "first_streak_ts_ms",
            "cooldown_until_ms",
            "updated_at_ms",
        ] {
            let has_col: bool = conn
                .prepare(
                    "SELECT 1 FROM pragma_table_info('portfolio_symbol_guard_v1') WHERE name=?1",
                )
                .and_then(|mut stmt| stmt.exists([column]))
                .expect("portfolio_symbol_guard_v1 column exists query");
            assert!(
                has_col,
                "portfolio_symbol_guard_v1.{column} column must exist"
            );
        }

        for column in [
            "portfolio_id",
            "equity_usd",
            "realized_pnl_usd",
            "closed_trades",
            "profitable_trades",
            "losing_trades",
            "last_trade_ts_ms",
            "updated_at_ms",
        ] {
            let has_col: bool = conn
                .prepare(
                    "SELECT 1 FROM pragma_table_info('portfolio_paper_state_v1') WHERE name=?1",
                )
                .and_then(|mut stmt| stmt.exists([column]))
                .expect("portfolio_paper_state_v1 column exists query");
            assert!(
                has_col,
                "portfolio_paper_state_v1.{column} column must exist"
            );
        }

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn replace_and_load_portfolio_runtime_state_v1_roundtrip() {
        let path = temp_db_path("portfolio-runtime-roundtrip-v1");
        let conn = open_db(&path).expect("open db");

        let state_rows = vec![
            PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec![
                    "BTCUSDT".to_string(),
                    "ETHUSDT".to_string(),
                    "SOLUSDT".to_string(),
                ],
                active_symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
                updated_at_ms: 600_000,
            },
            PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: vec!["XRPUSDT".to_string()],
                active_symbols: vec!["XRPUSDT".to_string()],
                updated_at_ms: 600_000,
            },
        ];
        let guard_rows = vec![
            PortfolioGuardRecordV1 {
                symbol: "BTCUSDT".to_string(),
                streak_count: 2,
                first_streak_ts_ms: Some(100_000),
                cooldown_until_ms: None,
                updated_at_ms: 600_000,
            },
            PortfolioGuardRecordV1 {
                symbol: "ETHUSDT".to_string(),
                streak_count: 0,
                first_streak_ts_ms: None,
                cooldown_until_ms: Some(900_000),
                updated_at_ms: 600_000,
            },
        ];
        let paper_rows = vec![
            PortfolioPaperStateRecordV1 {
                portfolio_id: "A".to_string(),
                equity_usd: 10_125.5,
                realized_pnl_usd: 125.5,
                closed_trades: 12,
                profitable_trades: 8,
                losing_trades: 4,
                last_trade_ts_ms: Some(700_000),
                updated_at_ms: 700_000,
            },
            PortfolioPaperStateRecordV1 {
                portfolio_id: "B".to_string(),
                equity_usd: 9_975.0,
                realized_pnl_usd: -25.0,
                closed_trades: 5,
                profitable_trades: 2,
                losing_trades: 3,
                last_trade_ts_ms: Some(650_000),
                updated_at_ms: 700_000,
            },
        ];

        replace_portfolio_state_v1(&conn, &state_rows).expect("replace portfolio_state_v1");
        replace_portfolio_guards_v1(&conn, &guard_rows).expect("replace portfolio_symbol_guard_v1");
        replace_portfolio_paper_state_v1(&conn, &paper_rows)
            .expect("replace portfolio_paper_state_v1");

        let loaded_state = load_portfolio_state_v1(&conn).expect("load portfolio_state_v1");
        let loaded_guards =
            load_portfolio_guards_v1(&conn).expect("load portfolio_symbol_guard_v1");
        let loaded_paper =
            load_portfolio_paper_state_v1(&conn).expect("load portfolio_paper_state_v1");
        assert_eq!(loaded_state, state_rows);
        assert_eq!(loaded_guards, guard_rows);
        assert_eq!(loaded_paper, paper_rows);

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn load_portfolio_candidate_history_v1_aggregates_trade_history() {
        let path = temp_db_path("portfolio-candidate-history-v1");
        let conn = open_db(&path).expect("open db");
        upsert_configs(&conn, &[TraderConfig::default()]).expect("seed config row");
        flush_trades(
            &conn,
            &[
                sample_trade_with("BTCUSDT", 0.5, 100, 200),
                sample_trade_with("BTCUSDT", -0.2, 300, 400),
                sample_trade_with("ETHUSDT", 0.0, 500, 600),
            ],
        )
        .expect("flush sample trades");

        let rows = load_portfolio_candidate_history_v1(&conn).expect("load candidate history");
        assert_eq!(rows.len(), 2);
        let btc = rows
            .iter()
            .find(|row| row.symbol == "BTCUSDT")
            .expect("BTCUSDT aggregate");
        assert_eq!(btc.closed_trades, 2);
        assert_eq!(btc.profitable_trades, 1);
        assert_eq!(btc.losing_trades, 1);
        assert!((btc.pnl_sum_pct - 0.3).abs() < 1e-12);
        assert_eq!(btc.first_trade_ts_ms, Some(100));

        let eth = rows
            .iter()
            .find(|row| row.symbol == "ETHUSDT")
            .expect("ETHUSDT aggregate");
        assert_eq!(eth.closed_trades, 1);
        assert_eq!(eth.profitable_trades, 0);
        assert_eq!(eth.losing_trades, 0);
        assert!((eth.pnl_sum_pct - 0.0).abs() < 1e-12);
        assert_eq!(eth.first_trade_ts_ms, Some(500));

        drop(conn);
        cleanup_temp_db(&path);
    }

    #[test]
    fn open_db_omits_unused_family_cluster_tables() {
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
            assert!(!has_table, "{table_name} table must be omitted");
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
        let (spillover_tx, _spillover_rx) = mpsc::channel::<DbCommand>(2);
        tx.try_send(DbCommand::Trades {
            seq: 1,
            trades: vec![sample_trade("BTCUSDT")],
        })
        .expect("pre-fill primary channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            &spillover_tx,
            DbCommand::Trades {
                seq: 2,
                trades: vec![sample_trade("ETHUSDT")],
            },
        );

        assert!(matches!(outcome, EnqueueOutcome::QueuedOverflow));
        assert!(
            matches!(overflow_rx.try_recv(), Ok(DbCommand::Trades { trades, .. }) if trades.len() == 1 && trades[0].symbol == "ETHUSDT")
        );
        assert!(
            matches!(rx.try_recv(), Ok(DbCommand::Trades { trades, .. }) if trades.len() == 1 && trades[0].symbol == "BTCUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn try_enqueue_command_uses_retry_when_primary_and_overflow_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, mut retry_rx) = mpsc::channel::<DbCommand>(2);
        let (spillover_tx, _spillover_rx) = mpsc::channel::<DbCommand>(2);
        tx.try_send(DbCommand::Trades {
            seq: 1,
            trades: vec![sample_trade("BTCUSDT")],
        })
        .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades {
                seq: 2,
                trades: vec![sample_trade("ETHUSDT")],
            })
            .expect("pre-fill overflow channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            &spillover_tx,
            DbCommand::Trades {
                seq: 3,
                trades: vec![sample_trade("SOLUSDT")],
            },
        );

        assert!(matches!(outcome, EnqueueOutcome::QueuedRetry));
        assert!(
            matches!(retry_rx.try_recv(), Ok(DbCommand::Trades { trades, .. }) if trades.len() == 1 && trades[0].symbol == "SOLUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn try_enqueue_command_uses_spillover_when_primary_overflow_retry_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, _retry_rx) = mpsc::channel::<DbCommand>(1);
        let (spillover_tx, mut spillover_rx) = mpsc::channel::<DbCommand>(2);
        tx.try_send(DbCommand::Trades {
            seq: 1,
            trades: vec![sample_trade("BTCUSDT")],
        })
        .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades {
                seq: 2,
                trades: vec![sample_trade("ETHUSDT")],
            })
            .expect("pre-fill overflow channel");
        retry_tx
            .try_send(DbCommand::Trades {
                seq: 3,
                trades: vec![sample_trade("SOLUSDT")],
            })
            .expect("pre-fill retry channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            &spillover_tx,
            DbCommand::Trades {
                seq: 4,
                trades: vec![sample_trade("ADAUSDT")],
            },
        );

        assert!(matches!(outcome, EnqueueOutcome::QueuedSpillover));
        assert!(
            matches!(spillover_rx.try_recv(), Ok(DbCommand::Trades { trades, .. }) if trades.len() == 1 && trades[0].symbol == "ADAUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn try_enqueue_command_requires_backpressure_when_all_queues_including_spillover_full() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, _retry_rx) = mpsc::channel::<DbCommand>(1);
        let (spillover_tx, _spillover_rx) = mpsc::channel::<DbCommand>(1);
        tx.try_send(DbCommand::Trades {
            seq: 1,
            trades: vec![sample_trade("BTCUSDT")],
        })
        .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades {
                seq: 2,
                trades: vec![sample_trade("ETHUSDT")],
            })
            .expect("pre-fill overflow channel");
        retry_tx
            .try_send(DbCommand::Trades {
                seq: 3,
                trades: vec![sample_trade("SOLUSDT")],
            })
            .expect("pre-fill retry channel");
        spillover_tx
            .try_send(DbCommand::Trades {
                seq: 4,
                trades: vec![sample_trade("XRPUSDT")],
            })
            .expect("pre-fill spillover channel");

        let outcome = try_enqueue_command(
            &tx,
            &overflow_tx,
            &retry_tx,
            &spillover_tx,
            DbCommand::Trades {
                seq: 5,
                trades: vec![sample_trade("ADAUSDT")],
            },
        );

        assert!(
            matches!(outcome, EnqueueOutcome::NeedsBackpressure(DbCommand::Trades { trades, .. }) if trades.len() == 1 && trades[0].symbol == "ADAUSDT")
        );
        assert_eq!(DROPPED_BATCHES.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn enqueue_command_backpressure_path_is_runtime_safe() {
        DROPPED_BATCHES.store(0, Ordering::Relaxed);
        OVERFLOWED_BATCHES.store(0, Ordering::Relaxed);

        let (tx, _rx) = mpsc::channel::<DbCommand>(1);
        let (overflow_tx, _overflow_rx) = mpsc::channel::<DbCommand>(1);
        let (retry_tx, _retry_rx) = mpsc::channel::<DbCommand>(1);
        let (spillover_tx, _spillover_rx) = mpsc::channel::<DbCommand>(1);
        let (backpressure_tx, mut backpressure_rx) = mpsc::channel::<DbCommand>(1);

        tx.try_send(DbCommand::Trades {
            seq: 1,
            trades: vec![sample_trade("BTCUSDT")],
        })
        .expect("pre-fill primary channel");
        overflow_tx
            .try_send(DbCommand::Trades {
                seq: 2,
                trades: vec![sample_trade("ETHUSDT")],
            })
            .expect("pre-fill overflow channel");
        retry_tx
            .try_send(DbCommand::Trades {
                seq: 3,
                trades: vec![sample_trade("SOLUSDT")],
            })
            .expect("pre-fill retry channel");
        spillover_tx
            .try_send(DbCommand::Trades {
                seq: 4,
                trades: vec![sample_trade("XRPUSDT")],
            })
            .expect("pre-fill spillover channel");

        let writer = DbWriter {
            tx,
            overflow_tx,
            retry_tx,
            spillover_tx,
            backpressure_tx,
            next_seq: Arc::new(AtomicU64::new(0)),
        };

        // Must not panic when called from async runtime.
        writer.send(vec![sample_trade("ADAUSDT")]);

        let command = backpressure_rx
            .try_recv()
            .expect("backpressure queue should receive saturated command");
        assert!(
            matches!(command, DbCommand::Trades { trades, .. } if trades.len() == 1 && trades[0].symbol == "ADAUSDT")
        );
    }

    #[tokio::test]
    async fn flush_all_waits_for_backpressure_pipeline_batches() {
        let path = temp_db_path("flush-all-backpressure");
        let conn = open_db(&path).expect("open db");
        upsert_configs(&conn, &[TraderConfig::default()]).expect("seed config row");
        drop(conn);

        let writer = spawn_writer(&path);
        let seq = 1_u64;
        writer.next_seq.store(seq, Ordering::Release);
        writer
            .backpressure_tx
            .send(DbCommand::Trades {
                seq,
                trades: vec![sample_trade("BTCUSDT")],
            })
            .await
            .expect("enqueue into backpressure queue");
        assert_eq!(writer.next_seq.load(Ordering::Acquire), seq);

        timeout(Duration::from_secs(2), writer.flush_all())
            .await
            .expect("flush_all timed out");

        let conn = open_db(&path).expect("open db");
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM trades", [], |row| row.get(0))
            .expect("count trades");
        assert_eq!(
            rows, 1,
            "flush_all must wait for trades staged in backpressure pipeline"
        );

        drop(conn);
        drop(writer);
        cleanup_temp_db(&path);
    }

    #[tokio::test]
    async fn writer_ignores_stale_portfolio_snapshot_sequence() {
        let path = temp_db_path("portfolio-snapshot-seq-guard");
        let conn = open_db(&path).expect("open db");
        drop(conn);

        let writer = spawn_writer(&path);
        writer.next_seq.store(3, Ordering::Release);

        let newer_states = vec![
            PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["NEW".to_string()],
                active_symbols: vec!["NEW".to_string()],
                updated_at_ms: 2,
            },
            PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: Vec::new(),
                active_symbols: Vec::new(),
                updated_at_ms: 2,
            },
        ];
        let stale_states = vec![
            PortfolioStateRecordV1 {
                portfolio_id: "A".to_string(),
                shortlist: vec!["OLD".to_string()],
                active_symbols: vec!["OLD".to_string()],
                updated_at_ms: 1,
            },
            PortfolioStateRecordV1 {
                portfolio_id: "B".to_string(),
                shortlist: Vec::new(),
                active_symbols: Vec::new(),
                updated_at_ms: 1,
            },
        ];

        writer
            .tx
            .send(DbCommand::PortfolioSnapshotV1 {
                seq: 2,
                states: newer_states,
                guards: Vec::new(),
                paper_states: Vec::new(),
            })
            .await
            .expect("enqueue newer snapshot");
        writer
            .backpressure_tx
            .send(DbCommand::PortfolioSnapshotV1 {
                seq: 1,
                states: stale_states,
                guards: Vec::new(),
                paper_states: Vec::new(),
            })
            .await
            .expect("enqueue stale snapshot");
        writer
            .backpressure_tx
            .send(DbCommand::Trades {
                seq: 3,
                trades: Vec::new(),
            })
            .await
            .expect("enqueue completion marker");

        timeout(Duration::from_secs(2), writer.flush_all())
            .await
            .expect("flush_all timed out");

        let conn = open_db(&path).expect("open db");
        let rows = load_portfolio_state_v1(&conn).expect("load portfolio_state_v1");
        let a = rows
            .iter()
            .find(|row| row.portfolio_id == "A")
            .expect("portfolio A row");
        assert_eq!(
            a.active_symbols,
            vec!["NEW".to_string()],
            "older snapshot must not override newer persisted state"
        );

        drop(conn);
        drop(writer);
        cleanup_temp_db(&path);
    }
}
