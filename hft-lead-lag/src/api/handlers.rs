//! HTTP handler functions and response types.

use axum::{extract::State, Json};
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug};
use crate::domain::screener::{ScreenerRow, ScreenerStore};
use crate::infrastructure::db::DbWriter;
use crate::infrastructure::enrichment::{self, CachedNatr};
use crate::infrastructure::exchanges::{BinanceMarketData, GateMarketData};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};

use super::http_server::HealthState;
use super::runner::{
    RunnerErrorKind, RunnerStartRequest, RunnerStartResponse, RunnerStatusResponse,
    RunnerStopResponse, RunnerUiConfig, TrialRunnerManager,
};

// ── Shared state ────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct HttpState {
    pub min_volume_usd: f64,
    pub screener: ScreenerStore,
    pub natr_cache: Arc<DashMap<String, CachedNatr>>,
    pub health: Arc<HealthState>,
    pub trial_runner: TrialRunnerManager,
    pub db_path: PathBuf,
}

// ── Response DTOs ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct HealthResponse {
    status: &'static str,
    binance: bool,
    gate: bool,
    binance_last_tick_age_ms: i64,
    gate_last_tick_age_ms: i64,
    binance_dropped_messages: u64,
    gate_dropped_messages: u64,
    db_dropped_batches: u64,
    issues: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SymbolSnapshot {
    exchange: &'static str,
    symbol: String,
    quote_volume: f64,
    last_price: Option<f64>,
    price_change_24h_pct: Option<f64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct SymbolsResponse {
    min_volume_usd: f64,
    total_symbols: usize,
    common_symbols: Vec<String>,
    symbols: Vec<SymbolSnapshot>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ScreenerResponse {
    generated_at_ms: i64,
    period_minutes: u64,
    total_symbols: usize,
    rows: Vec<ScreenerRow>,
}

// ── Handlers ────────────────────────────────────────────────────────

pub(crate) async fn health(
    State(state): State<Arc<HttpState>>,
) -> (axum::http::StatusCode, Json<HealthResponse>) {
    const STALE_TICK_THRESHOLD_MS: i64 = 5_000;

    let now_ms = crate::domain::screener::utils::now_ms();
    let binance_last_tick_ms = state.health.binance_last_tick_ms.load(Ordering::Relaxed);
    let gate_last_tick_ms = state.health.gate_last_tick_ms.load(Ordering::Relaxed);
    let binance_last_tick_age_ms = if binance_last_tick_ms > 0 {
        now_ms.saturating_sub(binance_last_tick_ms)
    } else {
        i64::MAX
    };
    let gate_last_tick_age_ms = if gate_last_tick_ms > 0 {
        now_ms.saturating_sub(gate_last_tick_ms)
    } else {
        i64::MAX
    };

    let binance_connected = state.health.binance_connected.load(Ordering::Relaxed);
    let gate_connected = state.health.gate_connected.load(Ordering::Relaxed);
    let binance = binance_connected && binance_last_tick_age_ms <= STALE_TICK_THRESHOLD_MS;
    let gate = gate_connected && gate_last_tick_age_ms <= STALE_TICK_THRESHOLD_MS;

    let binance_dropped_messages = BinanceMarketData::dropped_messages();
    let gate_dropped_messages = GateMarketData::dropped_messages();
    let db_dropped_batches = DbWriter::dropped_batches();

    let mut issues = Vec::new();
    if !binance_connected {
        issues.push("binance_disconnected");
    } else if binance_last_tick_age_ms > STALE_TICK_THRESHOLD_MS {
        issues.push("binance_stale");
    }
    if !gate_connected {
        issues.push("gate_disconnected");
    } else if gate_last_tick_age_ms > STALE_TICK_THRESHOLD_MS {
        issues.push("gate_stale");
    }
    if binance_dropped_messages > 0 {
        issues.push("binance_dropped_messages");
    }
    if gate_dropped_messages > 0 {
        issues.push("gate_dropped_messages");
    }
    if db_dropped_batches > 0 {
        issues.push("db_dropped_batches");
    }

    let healthy = issues.is_empty();
    let status = if healthy { "ok" } else { "degraded" };
    let code = if healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (
        code,
        Json(HealthResponse {
            status,
            binance,
            gate,
            binance_last_tick_age_ms,
            gate_last_tick_age_ms,
            binance_dropped_messages,
            gate_dropped_messages,
            db_dropped_batches,
            issues,
        }),
    )
}

pub(crate) async fn get_symbols(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<SymbolsResponse>, (axum::http::StatusCode, String)> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();

    let (binance_tickers, gate_tickers) = tokio::join!(
        binance.get_tickers_with_volume(state.min_volume_usd),
        gate.get_tickers_with_volume(state.min_volume_usd)
    );

    let binance_tickers = binance_tickers.map_err(internal_error)?;
    let gate_tickers = gate_tickers.map_err(internal_error)?;

    let binance_symbols: HashSet<String> =
        binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: HashSet<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    let mut common_symbols: Vec<String> = binance_symbols
        .intersection(&gate_symbols)
        .cloned()
        .collect();
    common_symbols.sort_unstable();

    let mut symbols = Vec::with_capacity(binance_tickers.len() + gate_tickers.len());
    symbols.extend(to_snapshots("binance", binance_tickers));
    symbols.extend(to_snapshots("gate", gate_tickers));

    Ok(Json(SymbolsResponse {
        min_volume_usd: state.min_volume_usd,
        total_symbols: symbols.len(),
        common_symbols,
        symbols,
    }))
}

pub(crate) async fn get_screener(State(state): State<Arc<HttpState>>) -> Json<ScreenerResponse> {
    let live_rows = state.screener.rows_sorted();
    let mut rows: Vec<ScreenerRow> = if live_rows.is_empty() {
        enrichment::fallback_screener_rows(state.min_volume_usd).await
    } else {
        live_rows
    };
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    enrichment::enrich_gate_natr_30m(&mut rows, &state.natr_cache).await;

    Json(ScreenerResponse {
        generated_at_ms: crate::domain::screener::utils::now_ms(),
        period_minutes: (state.screener.window_ms() / 60_000) as u64,
        total_symbols: rows.len(),
        rows,
    })
}

pub(crate) async fn get_shadow_debug(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Json<Option<ShadowDebug>> {
    Json(state.screener.shadow_debug(&symbol))
}

pub(crate) async fn get_chart_data(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
) -> Json<Option<ChartData>> {
    Json(state.screener.chart_data(&symbol))
}

// ── Fleet ranking ───────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct FleetConfigRank {
    config_id: i64,
    spike_threshold_bps: f64,
    target_ratio: f64,
    stop_loss_bps: f64,
    max_hold_ms: i64,
    max_spread_bps: f64,
    trailing_decay_ratio: f64,
    baseline_window_ms: i64,
    total_trades: i64,
    wins: i64,
    win_rate_pct: f64,
    total_pnl_pct: f64,
    avg_pnl_pct: f64,
    symbols_traded: i64,
}

pub(crate) async fn get_fleet_ranking(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<FleetConfigRank>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn
        .prepare(
            "SELECT c.id, c.spike_threshold_bps, c.target_ratio,
                c.stop_loss_bps, c.max_hold_ms, c.max_spread_bps,
                c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(*) as total,
                SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.pnl_pct) as total_pnl,
                COUNT(DISTINCT t.symbol) as symbols
         FROM trades t
         JOIN configs c ON t.config_id = c.id
         GROUP BY c.id
         HAVING total >= 10
         ORDER BY total_pnl / total DESC
         LIMIT 50",
        )
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("sql: {e}"),
            )
        })?;

    let rows = stmt
        .query_map([], |row| {
            let total: i64 = row.get(8)?;
            let wins: i64 = row.get(9)?;
            let total_pnl: f64 = row.get(10)?;
            let stats = compute_fleet_stats(total, wins, total_pnl);
            Ok(FleetConfigRank {
                config_id: row.get(0)?,
                spike_threshold_bps: row.get(1)?,
                target_ratio: row.get(2)?,
                stop_loss_bps: row.get(3)?,
                max_hold_ms: row.get(4)?,
                max_spread_bps: row.get(5)?,
                trailing_decay_ratio: row.get(6)?,
                baseline_window_ms: row.get(7)?,
                total_trades: total,
                wins,
                win_rate_pct: stats.win_rate_pct,
                total_pnl_pct: total_pnl,
                avg_pnl_pct: stats.avg_pnl_pct,
                symbols_traded: row.get(11)?,
            })
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<FleetConfigRank> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

// ── Fleet per-symbol best config ────────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct SymbolBestConfig {
    symbol: String,
    config_id: i64,
    spike_threshold_bps: f64,
    target_ratio: f64,
    stop_loss_bps: f64,
    max_hold_ms: i64,
    max_spread_bps: f64,
    trailing_decay_ratio: f64,
    baseline_window_ms: i64,
    total_trades: i64,
    wins: i64,
    win_rate_pct: f64,
    total_pnl_pct: f64,
    avg_pnl_pct: f64,
}

pub(crate) async fn get_fleet_by_symbol(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<SymbolBestConfig>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn.prepare(
        "WITH ranked AS (
            SELECT t.symbol, c.id as config_id,
                   c.spike_threshold_bps, c.target_ratio,
                   c.stop_loss_bps, c.max_hold_ms, c.max_spread_bps,
                   c.trailing_decay_ratio, c.baseline_window_ms,
                   COUNT(*) as total,
                   SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(t.pnl_pct) as total_pnl,
                   ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY SUM(t.pnl_pct)/COUNT(*) DESC) as rn
            FROM trades t
            JOIN configs c ON t.config_id = c.id
            GROUP BY t.symbol, c.id
            HAVING total >= 5
        )
        SELECT symbol, config_id, spike_threshold_bps, target_ratio,
               stop_loss_bps, max_hold_ms, max_spread_bps, trailing_decay_ratio,
               baseline_window_ms, total, wins, total_pnl
        FROM ranked WHERE rn = 1
        ORDER BY total_pnl / total DESC"
    ).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("sql: {e}")))?;

    let rows = stmt
        .query_map([], |row| {
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
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<SymbolBestConfig> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

// ── Fleet ranked (composite scoring) ─────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct FleetRankedConfig {
    config_id: i64,
    spike_threshold_bps: f64,
    target_ratio: f64,
    stop_loss_bps: f64,
    max_hold_ms: i64,
    max_spread_bps: f64,
    trailing_decay_ratio: f64,
    baseline_window_ms: i64,
    total_trades: i64,
    wins: i64,
    win_rate_pct: f64,
    total_pnl_pct: f64,
    avg_pnl_pct: f64,
    stddev_pnl_pct: f64,
    sharpe: f64,
    profit_factor: f64,
    composite: f64,
    symbols_traded: i64,
}

pub(crate) async fn get_fleet_ranked(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<FleetRankedConfig>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn
        .prepare(
            "SELECT c.id, c.spike_threshold_bps, c.target_ratio,
                c.stop_loss_bps, c.max_hold_ms, c.max_spread_bps,
                c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(*) as total,
                SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.pnl_pct) as total_pnl,
                AVG(t.pnl_pct) as avg_pnl,
                AVG(t.pnl_pct * t.pnl_pct) as avg_pnl_sq,
                SUM(CASE WHEN t.pnl_pct > 0 THEN t.pnl_pct ELSE 0 END) as gross_win,
                SUM(CASE WHEN t.pnl_pct < 0 THEN ABS(t.pnl_pct) ELSE 0 END) as gross_loss,
                COUNT(DISTINCT t.symbol) as symbols
         FROM trades t
         JOIN configs c ON t.config_id = c.id
         GROUP BY c.id
         HAVING total >= 10
         ORDER BY total DESC
         LIMIT 100",
        )
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("sql: {e}"),
            )
        })?;

    let rows = stmt
        .query_map([], |row| {
            let total: i64 = row.get(8)?;
            let wins: i64 = row.get(9)?;
            let total_pnl: f64 = row.get(10)?;
            let avg_pnl: f64 = row.get(11)?;
            let avg_pnl_sq: f64 = row.get(12)?;
            let gross_win: f64 = row.get(13)?;
            let gross_loss: f64 = row.get(14)?;

            let variance = (avg_pnl_sq - avg_pnl * avg_pnl).max(0.0);
            let stddev_pnl = variance.sqrt();
            let sharpe = if stddev_pnl > 1e-9 {
                avg_pnl / stddev_pnl
            } else {
                0.0
            };
            let profit_factor = if gross_loss > 1e-9 {
                gross_win / gross_loss
            } else {
                if gross_win > 0.0 {
                    99.0
                } else {
                    0.0
                }
            };
            let pf_capped = profit_factor.min(3.0);
            let composite = sharpe * (total as f64).sqrt() * pf_capped;

            Ok(FleetRankedConfig {
                config_id: row.get(0)?,
                spike_threshold_bps: row.get(1)?,
                target_ratio: row.get(2)?,
                stop_loss_bps: row.get(3)?,
                max_hold_ms: row.get(4)?,
                max_spread_bps: row.get(5)?,
                trailing_decay_ratio: row.get(6)?,
                baseline_window_ms: row.get(7)?,
                total_trades: total,
                wins,
                win_rate_pct: if total > 0 {
                    (wins as f64 / total as f64) * 100.0
                } else {
                    0.0
                },
                total_pnl_pct: total_pnl,
                avg_pnl_pct: avg_pnl,
                stddev_pnl_pct: stddev_pnl,
                sharpe,
                profit_factor,
                composite,
                symbols_traded: row.get(15)?,
            })
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let mut result: Vec<FleetRankedConfig> = rows.filter_map(|r| r.ok()).collect();
    result.sort_by(|a, b| {
        b.composite
            .partial_cmp(&a.composite)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(Json(result))
}

// ── Trial runs ──────────────────────────────────────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct TrialRunSummary {
    run_id: String,
    submitted_config_count: Option<i64>,
    config_count: i64,
    total_trades: i64,
    wins: i64,
    win_rate_pct: f64,
    avg_pnl_pct: f64,
    total_pnl_pct: f64,
    first_trade_ms: i64,
    last_trade_ms: i64,
}

pub(crate) async fn get_trial_runs(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<TrialRunSummary>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn
        .prepare(
            "WITH runs AS (
                SELECT run_id
                FROM trial_runs_meta
                WHERE run_id LIKE 'scout-%' OR run_id LIKE 'expand-%'
                UNION
                SELECT DISTINCT run_id
                FROM trades
                WHERE run_id LIKE 'scout-%' OR run_id LIKE 'expand-%'
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
                WHERE t.run_id LIKE 'scout-%' OR t.run_id LIKE 'expand-%'
                GROUP BY t.run_id
            )
            SELECT r.run_id,
                   m.submitted_config_count as submitted_config_count,
                   COALESCE(s.config_count, 0) as config_count,
                   COALESCE(s.total_trades, 0) as total_trades,
                   COALESCE(s.wins, 0) as wins,
                   COALESCE(s.total_pnl, 0.0) as total_pnl,
                   COALESCE(s.first_trade, m.applied_at_ms, 0) as first_trade,
                   COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) as last_trade
            FROM runs r
            LEFT JOIN trade_stats s ON s.run_id = r.run_id
            LEFT JOIN trial_runs_meta m ON m.run_id = r.run_id
            ORDER BY COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) DESC",
        )
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("sql: {e}"),
            )
        })?;

    let rows = stmt
        .query_map([], |row| {
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
            })
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<TrialRunSummary> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

pub(crate) async fn get_forward_runs(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<TrialRunSummary>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn
        .prepare(
            "WITH runs AS (
                SELECT run_id FROM trial_runs_meta WHERE run_id LIKE 'forward-%'
                UNION
                SELECT DISTINCT run_id FROM trades WHERE run_id LIKE 'forward-%'
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
                WHERE t.run_id LIKE 'forward-%'
                GROUP BY t.run_id
            )
            SELECT r.run_id,
                   m.submitted_config_count as submitted_config_count,
                   COALESCE(s.config_count, 0) as config_count,
                   COALESCE(s.total_trades, 0) as total_trades,
                   COALESCE(s.wins, 0) as wins,
                   COALESCE(s.total_pnl, 0.0) as total_pnl,
                   COALESCE(s.first_trade, m.applied_at_ms, 0) as first_trade,
                   COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) as last_trade
            FROM runs r
            LEFT JOIN trade_stats s ON s.run_id = r.run_id
            LEFT JOIN trial_runs_meta m ON m.run_id = r.run_id
            ORDER BY COALESCE(s.last_trade, m.closed_at_ms, m.applied_at_ms, 0) DESC",
        )
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("sql: {e}"),
            )
        })?;

    let rows = stmt
        .query_map([], |row| {
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
            })
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<TrialRunSummary> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ForwardSymbolsQuery {
    run_id: Option<String>,
}

pub(crate) async fn get_forward_by_symbol(
    State(state): State<Arc<HttpState>>,
    axum::extract::Query(query): axum::extract::Query<ForwardSymbolsQuery>,
) -> Result<Json<Vec<SymbolBestConfig>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let run_id = resolve_forward_run_id(&conn, query.run_id.as_deref())?;

    let mut stmt = conn.prepare(
        "WITH ranked AS (
            SELECT t.symbol, c.id as config_id,
                   c.spike_threshold_bps, c.target_ratio,
                   c.stop_loss_bps, c.max_hold_ms, c.max_spread_bps,
                   c.trailing_decay_ratio, c.baseline_window_ms,
                   COUNT(*) as total,
                   SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(t.pnl_pct) as total_pnl,
                   ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY SUM(t.pnl_pct)/COUNT(*) DESC) as rn
            FROM trades t
            JOIN configs c ON t.config_id = c.id
            WHERE t.run_id = ?1
            GROUP BY t.symbol, c.id
            HAVING total >= 1
        )
        SELECT symbol, config_id, spike_threshold_bps, target_ratio,
               stop_loss_bps, max_hold_ms, max_spread_bps, trailing_decay_ratio,
               baseline_window_ms, total, wins, total_pnl
        FROM ranked WHERE rn = 1
        ORDER BY total_pnl / total DESC"
    ).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("sql: {e}")))?;

    let rows = stmt
        .query_map(rusqlite::params![run_id], |row| {
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
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<SymbolBestConfig> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

#[derive(Debug, Serialize)]
pub(crate) struct TrialConfigDetail {
    config_id: i64,
    spike_threshold_bps: f64,
    target_ratio: f64,
    stop_loss_bps: f64,
    max_hold_ms: i64,
    max_spread_bps: f64,
    trailing_decay_ratio: f64,
    baseline_window_ms: i64,
    total_trades: i64,
    wins: i64,
    win_rate_pct: f64,
    avg_pnl_pct: f64,
    total_pnl_pct: f64,
    stop_loss_share_pct: f64,
    avg_hold_ms: f64,
}

pub(crate) async fn get_trial_configs(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(run_id): axum::extract::Path<String>,
) -> Result<Json<Vec<TrialConfigDetail>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let mut stmt = conn
        .prepare(
            "SELECT c.id, c.spike_threshold_bps, c.target_ratio,
                c.stop_loss_bps, c.max_hold_ms, c.max_spread_bps,
                c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(*) as total,
                SUM(CASE WHEN t.pnl_pct > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.pnl_pct) as total_pnl,
                SUM(CASE WHEN t.exit_reason = 'stop_loss' THEN 1 ELSE 0 END) as sl_count,
                AVG(t.hold_ms) as avg_hold
         FROM trades t
         JOIN configs c ON t.config_id = c.id
         WHERE t.run_id = ?1
         GROUP BY c.id
         ORDER BY SUM(t.pnl_pct) / COUNT(*) DESC",
        )
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("sql: {e}"),
            )
        })?;

    let rows = stmt
        .query_map(rusqlite::params![run_id], |row| {
            let total: i64 = row.get(8)?;
            let wins: i64 = row.get(9)?;
            let total_pnl: f64 = row.get(10)?;
            let sl_count: i64 = row.get(11)?;
            Ok(TrialConfigDetail {
                config_id: row.get(0)?,
                spike_threshold_bps: row.get(1)?,
                target_ratio: row.get(2)?,
                stop_loss_bps: row.get(3)?,
                max_hold_ms: row.get(4)?,
                max_spread_bps: row.get(5)?,
                trailing_decay_ratio: row.get(6)?,
                baseline_window_ms: row.get(7)?,
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
                stop_loss_share_pct: if total > 0 {
                    (sl_count as f64 / total as f64) * 100.0
                } else {
                    0.0
                },
                avg_hold_ms: row.get(12)?,
            })
        })
        .map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("query: {e}"),
            )
        })?;

    let result: Vec<TrialConfigDetail> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

// ── Trial axes breakdown (7D reference matrix) ──────────────────────

#[derive(Debug, Serialize)]
pub(crate) struct AxisValueStats {
    value: f64,
    configs_total: i64,
    configs_with_trades: i64,
    total_trades: i64,
    avg_pnl_pct: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct TrialAxesBreakdown {
    run_id: Option<String>,
    spike_threshold_bps: Vec<AxisValueStats>,
    target_ratio: Vec<AxisValueStats>,
    stop_loss_bps: Vec<AxisValueStats>,
    max_hold_ms: Vec<AxisValueStats>,
    max_spread_bps: Vec<AxisValueStats>,
    trailing_decay_ratio: Vec<AxisValueStats>,
    baseline_window_ms: Vec<AxisValueStats>,
}

pub(crate) async fn get_trial_axes(
    State(state): State<Arc<HttpState>>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> Result<Json<TrialAxesBreakdown>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;

    let run_id = params.get("run_id").cloned();

    // Single query: per-config row with pre-aggregated trade stats.
    let base_sql = if run_id.is_some() {
        "SELECT c.spike_threshold_bps, c.target_ratio, c.stop_loss_bps,
                c.max_hold_ms, c.max_spread_bps, c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(t.id) AS trades, COALESCE(AVG(t.pnl_pct),0) AS avg_pnl
         FROM configs c LEFT JOIN trades t ON t.config_id = c.id AND t.run_id = ?1
         GROUP BY c.id"
    } else {
        "SELECT c.spike_threshold_bps, c.target_ratio, c.stop_loss_bps,
                c.max_hold_ms, c.max_spread_bps, c.trailing_decay_ratio, c.baseline_window_ms,
                COUNT(t.id) AS trades, COALESCE(AVG(t.pnl_pct),0) AS avg_pnl
         FROM configs c LEFT JOIN trades t ON t.config_id = c.id
         GROUP BY c.id"
    };

    struct ConfigRow {
        vals: [f64; 7],
        trades: i64,
        avg_pnl: f64,
    }

    let mut stmt = conn.prepare(base_sql).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("sql: {e}"),
        )
    })?;

    let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<ConfigRow> {
        Ok(ConfigRow {
            vals: [
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
            ],
            trades: row.get(7)?,
            avg_pnl: row.get(8)?,
        })
    };

    let rows: Vec<ConfigRow> = if let Some(ref rid) = run_id {
        stmt.query_map(rusqlite::params![rid], map_row)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {e}"),
                )
            })?
            .filter_map(|r| r.ok())
            .collect()
    } else {
        stmt.query_map([], map_row)
            .map_err(|e| {
                (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    format!("query: {e}"),
                )
            })?
            .filter_map(|r| r.ok())
            .collect()
    };

    // Bucket step for each axis (0 = no bucketing).
    const BUCKET: [f64; 7] = [
        0.0,     // spike_threshold_bps — low cardinality
        0.0,     // target_ratio
        2.0,     // stop_loss_bps — bucket by 2
        5000.0,  // max_hold_ms — bucket by 5s
        0.0,     // max_spread_bps
        0.0,     // trailing_decay_ratio
        10000.0, // baseline_window_ms — bucket by 10s
    ];

    fn bucket_val(v: f64, step: f64) -> f64 {
        if step <= 0.0 {
            v
        } else {
            (v / step).round() * step
        }
    }

    fn aggregate_axis(rows: &[ConfigRow], idx: usize, step: f64) -> Vec<AxisValueStats> {
        let mut map: std::collections::BTreeMap<i64, (i64, i64, i64, f64, f64)> =
            std::collections::BTreeMap::new();
        for r in rows {
            let bv = bucket_val(r.vals[idx], step);
            let key = (bv * 1_000_000.0) as i64; // fixed-point key for BTreeMap ordering
            let e = map.entry(key).or_insert((0, 0, 0, 0.0, bv));
            e.0 += 1; // configs_total
            if r.trades > 0 {
                e.1 += 1;
            } // configs_with_trades
            e.2 += r.trades; // total_trades
            e.3 += r.avg_pnl * r.trades as f64; // weighted pnl sum
        }
        map.values()
            .map(|&(ct, cwt, tt, pnl_sum, bv)| AxisValueStats {
                value: bv,
                configs_total: ct,
                configs_with_trades: cwt,
                total_trades: tt,
                avg_pnl_pct: if tt > 0 { pnl_sum / tt as f64 } else { 0.0 },
            })
            .collect()
    }

    let breakdown = TrialAxesBreakdown {
        run_id,
        spike_threshold_bps: aggregate_axis(&rows, 0, BUCKET[0]),
        target_ratio: aggregate_axis(&rows, 1, BUCKET[1]),
        stop_loss_bps: aggregate_axis(&rows, 2, BUCKET[2]),
        max_hold_ms: aggregate_axis(&rows, 3, BUCKET[3]),
        max_spread_bps: aggregate_axis(&rows, 4, BUCKET[4]),
        trailing_decay_ratio: aggregate_axis(&rows, 5, BUCKET[5]),
        baseline_window_ms: aggregate_axis(&rows, 6, BUCKET[6]),
    };

    Ok(Json(breakdown))
}

#[derive(Debug, Deserialize)]
pub(crate) struct RunnerStatusQuery {
    tail: Option<usize>,
}

pub(crate) async fn get_trial_runner_status(
    State(state): State<Arc<HttpState>>,
    axum::extract::Query(query): axum::extract::Query<RunnerStatusQuery>,
) -> Json<RunnerStatusResponse> {
    let tail = query.tail.unwrap_or(200).max(1).min(500);
    Json(state.trial_runner.status(tail).await)
}

pub(crate) async fn get_trial_runner_config(
    State(_state): State<Arc<HttpState>>,
) -> Json<RunnerUiConfig> {
    Json(TrialRunnerManager::ui_config())
}

pub(crate) async fn start_trial_runner(
    State(state): State<Arc<HttpState>>,
    Json(req): Json<RunnerStartRequest>,
) -> Result<Json<RunnerStartResponse>, (axum::http::StatusCode, String)> {
    state.trial_runner.start(req).await.map(Json).map_err(|e| {
        let code = match e.kind {
            RunnerErrorKind::BadRequest => axum::http::StatusCode::BAD_REQUEST,
            RunnerErrorKind::Conflict => axum::http::StatusCode::CONFLICT,
            RunnerErrorKind::Internal => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        };
        (code, e.message)
    })
}

pub(crate) async fn stop_trial_runner(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<RunnerStopResponse>, (axum::http::StatusCode, String)> {
    state
        .trial_runner
        .stop()
        .await
        .map(Json)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, e.message))
}

// ── Internal helpers ────────────────────────────────────────────────

#[derive(Debug, Clone, Copy)]
struct FleetStats {
    win_rate_pct: f64,
    avg_pnl_pct: f64,
}

fn compute_fleet_stats(total: i64, wins: i64, total_pnl: f64) -> FleetStats {
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

fn resolve_forward_run_id(
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

fn to_snapshots(exchange: &'static str, tickers: Vec<Ticker24h>) -> Vec<SymbolSnapshot> {
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

fn internal_error(error: crate::domain::ExchangeError) -> (axum::http::StatusCode, String) {
    (
        axum::http::StatusCode::BAD_GATEWAY,
        format!("exchange error: {}", error),
    )
}

fn open_readonly_conn(
    state: &HttpState,
) -> Result<rusqlite::Connection, (axum::http::StatusCode, String)> {
    crate::infrastructure::db::open_db_readonly(&state.db_path).map_err(|e| {
        (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("db: {e}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use dashmap::DashMap;
    use std::path::PathBuf;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    #[test]
    fn compute_fleet_stats_handles_zero_trades() {
        let stats = compute_fleet_stats(0, 0, 42.0);
        assert_eq!(stats.win_rate_pct, 0.0);
        assert_eq!(stats.avg_pnl_pct, 0.0);
    }

    #[test]
    fn compute_fleet_stats_calculates_win_rate_and_avg() {
        let stats = compute_fleet_stats(20, 5, 10.0);
        assert_eq!(stats.win_rate_pct, 25.0);
        assert_eq!(stats.avg_pnl_pct, 0.5);
    }

    #[tokio::test]
    async fn health_returns_degraded_when_feed_is_stale() {
        let health_state = Arc::new(HealthState::new());
        health_state
            .binance_connected
            .store(true, Ordering::Relaxed);
        health_state.gate_connected.store(true, Ordering::Relaxed);
        health_state
            .binance_last_tick_ms
            .store(1, Ordering::Relaxed);
        health_state.gate_last_tick_ms.store(1, Ordering::Relaxed);

        let state = Arc::new(HttpState {
            min_volume_usd: 1_000_000.0,
            screener: ScreenerStore::default(),
            natr_cache: Arc::new(DashMap::new()),
            health: health_state,
            trial_runner: TrialRunnerManager::new(
                std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            ),
            db_path: PathBuf::from("data/optimizer.db"),
        });

        let (code, Json(resp)) = health(State(state)).await;
        assert_eq!(code, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.status, "degraded");
        assert!(resp.issues.contains(&"binance_stale"));
        assert!(resp.issues.contains(&"gate_stale"));
    }

    #[tokio::test]
    async fn health_reports_drop_counters() {
        let health_state = Arc::new(HealthState::new());
        let state = Arc::new(HttpState {
            min_volume_usd: 1_000_000.0,
            screener: ScreenerStore::default(),
            natr_cache: Arc::new(DashMap::new()),
            health: health_state,
            trial_runner: TrialRunnerManager::new(
                std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            ),
            db_path: PathBuf::from("data/optimizer.db"),
        });

        let (_code, Json(resp)) = health(State(state)).await;
        assert_eq!(
            resp.binance_dropped_messages,
            crate::infrastructure::exchanges::BinanceMarketData::dropped_messages()
        );
        assert_eq!(
            resp.gate_dropped_messages,
            crate::infrastructure::exchanges::GateMarketData::dropped_messages()
        );
        assert_eq!(
            resp.db_dropped_batches,
            crate::infrastructure::db::DbWriter::dropped_batches()
        );
    }
}
