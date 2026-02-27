use super::{HttpState, SymbolBestConfig, SymbolSnapshot, TrialRunSummary};
use crate::infrastructure::rest::Ticker24h;
use std::sync::Arc;

#[derive(Debug, Clone, Copy)]
pub(super) struct FleetStats {
    pub(super) win_rate_pct: f64,
    pub(super) avg_pnl_pct: f64,
}

pub(super) fn compute_fleet_stats(total: i64, wins: i64, total_pnl: f64) -> FleetStats {
    if total > 0 {
        FleetStats {
            win_rate_pct: (wins as f64 / total as f64) * 100.0,
            avg_pnl_pct: total_pnl / total as f64,
        }
    } else {
        FleetStats {
            win_rate_pct: 0.0,
            avg_pnl_pct: 0.0,
        }
    }
}

pub(super) fn resolve_forward_run_id(
    conn: &rusqlite::Connection,
    requested: Option<&str>,
) -> Result<String, (axum::http::StatusCode, String)> {
    let requested = requested.map(str::trim).filter(|v| !v.is_empty());
    if let Some(rid) = requested {
        if !rid.starts_with("forward-") {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "run_id must start with forward-".to_string(),
            ));
        }
        let exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM trades WHERE run_id = ?1",
                rusqlite::params![rid],
                |row| row.get(0),
            )
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {e}"),
                )
            })?;
        if exists == 0 {
            return Err((
                axum::http::StatusCode::NOT_FOUND,
                format!("forward run not found: {rid}"),
            ));
        }
        return Ok(rid.to_string());
    }

    let latest = conn
        .query_row(
            "SELECT run_id
             FROM trades
             WHERE run_id LIKE 'forward-%'
             GROUP BY run_id
             ORDER BY MAX(exit_ts_ms) DESC
             LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .map_err(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => (
                axum::http::StatusCode::NOT_FOUND,
                "no forward runs found".to_string(),
            ),
            _ => (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            ),
        })?;

    Ok(latest)
}

pub(super) fn to_snapshots(exchange: &'static str, tickers: Vec<Ticker24h>) -> Vec<SymbolSnapshot> {
    tickers
        .into_iter()
        .map(|ticker| SymbolSnapshot {
            exchange,
            symbol: ticker.symbol,
            quote_volume: ticker.quote_volume,
            last_price: ticker.last_price,
            price_change_24h_pct: ticker.price_change_24h_pct,
        })
        .collect()
}

pub(super) fn internal_error(
    error: crate::domain::ExchangeError,
) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::BAD_GATEWAY,
        format!("exchange error: {}", error),
    )
}

pub(super) fn open_readonly_conn(
    state: &HttpState,
) -> Result<rusqlite::Connection, (axum::http::StatusCode, String)> {
    crate::infrastructure::db::open_db_readonly(&state.db_path).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("db: {e}"),
        )
    })
}

pub(super) async fn with_readonly_conn<T, F>(
    state: Arc<HttpState>,
    operation_name: &'static str,
    work: F,
) -> Result<T, (axum::http::StatusCode, String)>
where
    T: Send + 'static,
    F: FnOnce(rusqlite::Connection) -> Result<T, (axum::http::StatusCode, String)> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let conn = open_readonly_conn(state.as_ref())?;
        work(conn)
    })
    .await
    .map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("{operation_name} join error: {e}"),
        )
    })?
}

fn decode_symbol_best_config_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SymbolBestConfig> {
    let total: i64 = row.get(9)?;
    let wins: i64 = row.get(10)?;
    let total_pnl: f64 = row.get(11)?;
    let stats = compute_fleet_stats(total, wins, total_pnl);
    Ok(SymbolBestConfig {
        symbol: row.get(0)?,
        config_id: row.get(1)?,
        spike_threshold_bps: row.get(2)?,
        target_ratio: row.get(3)?,
        stop_loss_bps: row.get(4)?,
        max_hold_ms: row.get(5)?,
        max_spread_bps: row.get(6)?,
        trailing_decay_ratio: row.get(7)?,
        baseline_window_ms: row.get(8)?,
        total_trades: total,
        wins,
        win_rate_pct: stats.win_rate_pct,
        total_pnl_pct: total_pnl,
        avg_pnl_pct: stats.avg_pnl_pct,
    })
}

pub(super) fn load_symbol_best_configs<P: rusqlite::Params>(
    conn: &rusqlite::Connection,
    sql: &str,
    params: P,
) -> Result<Vec<SymbolBestConfig>, (axum::http::StatusCode, String)> {
    let mut stmt = conn.prepare(sql).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("sql: {e}"),
        )
    })?;

    let rows = stmt
        .query_map(params, decode_symbol_best_config_row)
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("row decode: {e}"),
        )
    })
}

fn decode_trial_run_summary_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<TrialRunSummary> {
    let total: i64 = row.get(3)?;
    let wins: i64 = row.get(4)?;
    let total_pnl: f64 = row.get(5)?;
    Ok(TrialRunSummary {
        run_id: row.get(0)?,
        submitted_config_count: row.get(1)?,
        config_count: row.get(2)?,
        total_trades: total,
        wins,
        win_rate_pct: if total > 0 {
            (wins as f64 / total as f64) * 100.0
        } else {
            0.0
        },
        avg_pnl_pct: if total > 0 {
            total_pnl / total as f64
        } else {
            0.0
        },
        total_pnl_pct: total_pnl,
        first_trade_ms: row.get(6)?,
        last_trade_ms: row.get(7)?,
        apply_mode: row.get(8)?,
        symbols_reset: row.get(9)?,
        changed_ids_requested: row.get(10)?,
        matched_changed_ids_old: row.get(11)?,
        matched_changed_ids_new: row.get(12)?,
        unmatched_changed_ids: row.get(13)?,
        scope_symbols_requested: row.get(14)?,
        scope_symbols_matched: row.get(15)?,
    })
}

pub(super) fn load_trial_run_summaries(
    conn: &rusqlite::Connection,
    run_filter_sql: &str,
) -> Result<Vec<TrialRunSummary>, (axum::http::StatusCode, String)> {
    let sql = format!(
        "WITH runs AS (
            SELECT run_id
            FROM trial_runs_meta
            WHERE {run_filter_sql}
            UNION
            SELECT DISTINCT run_id
            FROM trades
            WHERE {run_filter_sql}
        ),
        trade_stats AS (
            SELECT t.run_id,
                   COUNT(DISTINCT t.config_id) as config_count,
                   COUNT(*) as total_trades,
                   SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(t.pnl_pct) as total_pnl,
                   MIN(t.entry_ts_ms) as first_trade,
                   MAX(t.exit_ts_ms) as last_trade
            FROM trades t
            WHERE {run_filter_sql}
            GROUP BY t.run_id
        )
        SELECT r.run_id,
               m.submitted_config_count as submitted_config_count,
               COALESCE(s.config_count, 0) as config_count,
               COALESCE(s.total_trades, 0) as total_trades,
               COALESCE(s.wins, 0) as wins,
               COALESCE(s.total_pnl, 0.0) as total_pnl,
               COALESCE(s.first_trade, m.applied_at_ms, 0) as first_trade,
               COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) as last_trade,
               COALESCE(m.apply_mode, 'full_replace') as apply_mode,
               COALESCE(m.symbols_reset, 0) as symbols_reset,
               COALESCE(m.changed_ids_requested, 0) as changed_ids_requested,
               COALESCE(m.matched_changed_ids_old, 0) as matched_changed_ids_old,
               COALESCE(m.matched_changed_ids_new, 0) as matched_changed_ids_new,
               COALESCE(m.unmatched_changed_ids, 0) as unmatched_changed_ids,
               COALESCE(m.scope_symbols_requested, 0) as scope_symbols_requested,
               COALESCE(m.scope_symbols_matched, 0) as scope_symbols_matched
        FROM runs r
        LEFT JOIN trade_stats s ON s.run_id = r.run_id
        LEFT JOIN trial_runs_meta m ON m.run_id = r.run_id
        ORDER BY COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) DESC"
    );

    let mut stmt = conn.prepare(&sql).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("sql: {e}"),
        )
    })?;

    let rows = stmt
        .query_map([], decode_trial_run_summary_row)
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("row decode: {e}"),
        )
    })
}
