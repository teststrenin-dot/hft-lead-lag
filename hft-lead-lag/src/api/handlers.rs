//! HTTP handler functions and response types.

mod health_support;
mod helpers;
mod trial_axes_support;

use arc_swap::ArcSwap;
use axum::{extract::Query, extract::State, Json};
use dashmap::DashMap;
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;

use crate::domain::screener::portfolio_runtime::{
    compute_pm_raw, compute_useful_winrate, rank_candidates,
};
use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug};
use crate::domain::screener::PolicyConfigSnapshot;
use crate::domain::screener::{ScreenerRow, ScreenerStore};
#[cfg(test)]
use crate::infrastructure::db::DbWriter;
use crate::infrastructure::db::{load_portfolio_guards_v1, load_portfolio_state_v1};
use crate::infrastructure::enrichment::{self, CachedNatr};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient};

#[cfg(test)]
use self::health_support::{evaluate_db_saturation_health, FALLBACK_ROWS_TTL_MS};
use self::health_support::{
    health_response, maybe_spawn_fallback_rows_refresh, should_refresh_fallback_rows_cache,
};
use self::helpers::{
    compute_fleet_stats, internal_error, load_symbol_best_configs, load_trial_run_summaries,
    open_readonly_conn, resolve_forward_run_id, to_snapshots, with_readonly_conn,
};
use self::trial_axes_support::build_trial_axes_breakdown;
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
    pub fallback_rows_cache: Arc<ArcSwap<Vec<ScreenerRow>>>,
    pub fallback_rows_last_refresh_ms: Arc<AtomicI64>,
    pub fallback_rows_refresh_in_flight: Arc<AtomicBool>,
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
    trial_queue_depth: u64,
    trial_queue_quarantined: u64,
    trial_last_ack_age_ms: Option<i64>,
    trial_last_ack_status: &'static str,
    trial_active_run_id: Option<String>,
    binance_dropped_messages: u64,
    gate_dropped_messages: u64,
    db_dropped_batches: u64,
    db_overflowed_batches: u64,
    db_dropped_batch_budget: u64,
    db_overflow_warn_threshold: u64,
    runtime_stage_timestamps: RuntimeStageTimestamps,
    runtime_latency_us: RuntimeLatencySnapshot,
    runtime_backlog_depth: RuntimeBacklogDepth,
    issues: Vec<&'static str>,
    warnings: Vec<&'static str>,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeStageTimestamps {
    recv_ws_frame_ts_ns: i64,
    parsed_ts_ns: i64,
    state_updated_ts_ns: i64,
    signal_decided_ts_ns: i64,
    order_intent_enqueued_ts_ns: i64,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeLatencyStats {
    samples: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeLatencySnapshot {
    ingest: RuntimeLatencyStats,
    decision: RuntimeLatencyStats,
    end_to_end: RuntimeLatencyStats,
}

#[derive(Debug, Serialize)]
pub(crate) struct RuntimeBacklogDepth {
    binance_msg_queue_depth: u64,
    gate_msg_queue_depth: u64,
    signal_backlog_depth: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DbSaturationHealth {
    pub(super) drop_budget_exhausted: bool,
    pub(super) overflow_warn: bool,
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

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioActiveRow {
    portfolio_id: String,
    shortlist: Vec<String>,
    active_symbols: Vec<String>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioActiveResponse {
    generated_at_ms: i64,
    portfolios: Vec<PortfolioActiveRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioPerformanceRow {
    portfolio_id: String,
    equity_usd: f64,
    realized_pnl_usd: f64,
    closed_trades: u64,
    profitable_trades: u64,
    losing_trades: u64,
    useful_winrate_pct: f64,
    last_trade_ts_ms: Option<i64>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioPerformanceResponse {
    generated_at_ms: i64,
    total_equity_usd: f64,
    total_realized_pnl_usd: f64,
    portfolios: Vec<PortfolioPerformanceRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioCandidateRow {
    symbol: String,
    age_minutes_from_first_tick: u64,
    closed_trades: u32,
    profitable_trades: u32,
    losing_trades: u32,
    useful_winrate: f64,
    useful_winrate_pct: f64,
    pm_raw: i64,
    avg_pnl_pct: f64,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioCandidatesResponse {
    generated_at_ms: i64,
    total_candidates: usize,
    rows: Vec<PortfolioCandidateRow>,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioGuardRow {
    symbol: String,
    streak_count: u32,
    first_streak_ts_ms: Option<i64>,
    cooldown_until_ms: Option<i64>,
    in_cooldown: bool,
}

#[derive(Debug, Serialize)]
pub(crate) struct PortfolioGuardsResponse {
    generated_at_ms: i64,
    total_symbols: usize,
    rows: Vec<PortfolioGuardRow>,
}

// ── Handlers ────────────────────────────────────────────────────────

pub(crate) async fn health(
    State(state): State<Arc<HttpState>>,
) -> (axum::http::StatusCode, Json<HealthResponse>) {
    let (code, response) = health_response(&state);
    (code, Json(response))
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
        let now_ms = crate::domain::screener::utils::now_ms();
        let cached_rows = state.fallback_rows_cache.load_full();
        let last_refresh_ms = state.fallback_rows_last_refresh_ms.load(Ordering::Relaxed);
        if should_refresh_fallback_rows_cache(now_ms, last_refresh_ms, cached_rows.is_empty()) {
            maybe_spawn_fallback_rows_refresh(&state);
        }
        cached_rows.as_ref().clone()
    } else {
        live_rows
    };
    let to_fetch = enrichment::enrich_gate_natr_30m_cached_only(&mut rows, &state.natr_cache);
    if !to_fetch.is_empty() {
        let cache = state.natr_cache.clone();
        tokio::spawn(async move {
            enrichment::warm_gate_natr_30m_cache(to_fetch, cache).await;
        });
    }

    Json(ScreenerResponse {
        generated_at_ms: crate::domain::screener::utils::now_ms(),
        period_minutes: (state.screener.window_ms() / 60_000) as u64,
        total_symbols: rows.len(),
        rows,
    })
}

pub(crate) async fn get_portfolio_active(
    State(state): State<Arc<HttpState>>,
) -> Json<PortfolioActiveResponse> {
    let assignment = state.screener.portfolio_assignment_v1();
    let mut portfolios: Vec<PortfolioActiveRow> = state
        .screener
        .portfolio_ids_v1()
        .into_iter()
        .map(|portfolio_id| {
            let entry = assignment.get(&portfolio_id).cloned().unwrap_or_default();
            PortfolioActiveRow {
                portfolio_id,
                shortlist: entry.shortlist,
                active_symbols: entry.active_symbols,
            }
        })
        .collect();

    let all_empty = portfolios
        .iter()
        .all(|row| row.shortlist.is_empty() && row.active_symbols.is_empty());
    if all_empty {
        if let Ok(conn) = open_readonly_conn(&state) {
            if let Ok(rows) = load_portfolio_state_v1(&conn) {
                for row in rows {
                    if let Some(slot) = portfolios
                        .iter_mut()
                        .find(|slot| slot.portfolio_id == row.portfolio_id)
                    {
                        slot.shortlist = row.shortlist;
                        slot.active_symbols = row.active_symbols;
                    }
                }
                portfolios.sort_by(|a, b| a.portfolio_id.cmp(&b.portfolio_id));
            }
        }
    }

    Json(PortfolioActiveResponse {
        generated_at_ms: crate::domain::screener::utils::now_ms(),
        portfolios,
    })
}

pub(crate) async fn get_portfolio_candidates(
    State(state): State<Arc<HttpState>>,
) -> Json<PortfolioCandidatesResponse> {
    let generated_at_ms = crate::domain::screener::utils::now_ms();
    let ranked = rank_candidates(&state.screener.portfolio_candidate_stats_v1(generated_at_ms));
    let rows: Vec<PortfolioCandidateRow> = ranked
        .iter()
        .map(|stats| {
            let useful_winrate = compute_useful_winrate(stats);
            PortfolioCandidateRow {
                symbol: stats.symbol.clone(),
                age_minutes_from_first_tick: stats.age_minutes_from_first_tick,
                closed_trades: stats.closed_trades,
                profitable_trades: stats.profitable_trades,
                losing_trades: stats.losing_trades,
                useful_winrate,
                useful_winrate_pct: useful_winrate * 100.0,
                pm_raw: compute_pm_raw(stats),
                avg_pnl_pct: stats.avg_pnl_pct,
            }
        })
        .collect();

    Json(PortfolioCandidatesResponse {
        generated_at_ms,
        total_candidates: rows.len(),
        rows,
    })
}

pub(crate) async fn get_portfolio_performance(
    State(state): State<Arc<HttpState>>,
) -> Json<PortfolioPerformanceResponse> {
    let generated_at_ms = crate::domain::screener::utils::now_ms();
    let paper = state.screener.portfolio_paper_states_v1();
    let mut portfolios: Vec<PortfolioPerformanceRow> = state
        .screener
        .portfolio_ids_v1()
        .into_iter()
        .map(|portfolio_id| {
            let stats = paper.get(&portfolio_id).copied().unwrap_or_default();
            let useful_winrate_pct = if stats.closed_trades == 0 {
                0.0
            } else {
                (stats.profitable_trades as f64 / stats.closed_trades as f64) * 100.0
            };
            PortfolioPerformanceRow {
                portfolio_id,
                equity_usd: stats.equity_usd,
                realized_pnl_usd: stats.realized_pnl_usd,
                closed_trades: stats.closed_trades,
                profitable_trades: stats.profitable_trades,
                losing_trades: stats.losing_trades,
                useful_winrate_pct,
                last_trade_ts_ms: stats.last_trade_ts_ms,
            }
        })
        .collect();
    portfolios.sort_by(|a, b| a.portfolio_id.cmp(&b.portfolio_id));

    let total_equity_usd: f64 = portfolios.iter().map(|row| row.equity_usd).sum();
    let total_realized_pnl_usd: f64 = portfolios.iter().map(|row| row.realized_pnl_usd).sum();

    Json(PortfolioPerformanceResponse {
        generated_at_ms,
        total_equity_usd,
        total_realized_pnl_usd,
        portfolios,
    })
}

pub(crate) async fn get_portfolio_guards(
    State(state): State<Arc<HttpState>>,
) -> Json<PortfolioGuardsResponse> {
    let generated_at_ms = crate::domain::screener::utils::now_ms();
    let mut rows: Vec<PortfolioGuardRow> = state
        .screener
        .portfolio_guard_states_v1()
        .into_iter()
        .map(|(symbol, guard)| {
            let in_cooldown = guard
                .cooldown_until_ms
                .map(|until| generated_at_ms < until)
                .unwrap_or(false);
            PortfolioGuardRow {
                symbol,
                streak_count: guard.streak_count,
                first_streak_ts_ms: guard.first_streak_ts_ms,
                cooldown_until_ms: guard.cooldown_until_ms,
                in_cooldown,
            }
        })
        .collect();

    if rows.is_empty() {
        if let Ok(conn) = open_readonly_conn(&state) {
            if let Ok(db_rows) = load_portfolio_guards_v1(&conn) {
                rows = db_rows
                    .into_iter()
                    .map(|row| PortfolioGuardRow {
                        symbol: row.symbol,
                        streak_count: row.streak_count,
                        first_streak_ts_ms: row.first_streak_ts_ms,
                        cooldown_until_ms: row.cooldown_until_ms,
                        in_cooldown: row
                            .cooldown_until_ms
                            .map(|until| generated_at_ms < until)
                            .unwrap_or(false),
                    })
                    .collect();
            }
        }
    }

    Json(PortfolioGuardsResponse {
        generated_at_ms,
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

#[derive(Debug, Deserialize)]
pub(crate) struct FleetPolicyQuery {
    top_k: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct FleetPolicyOverviewQuery {
    top_k: Option<usize>,
    max_symbols: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct FleetPolicyOverviewRow {
    symbol: String,
    policies: Vec<PolicyConfigSnapshot>,
}

pub(crate) async fn get_fleet_policy_overview(
    State(state): State<Arc<HttpState>>,
    Query(query): Query<FleetPolicyOverviewQuery>,
) -> Json<Vec<FleetPolicyOverviewRow>> {
    let top_k = query.top_k.unwrap_or(20).clamp(1, 200);
    let max_symbols = query.max_symbols.unwrap_or(100).clamp(1, 2000);
    Json(
        state
            .screener
            .fleet_policy_overview(top_k, max_symbols)
            .into_iter()
            .map(|(symbol, policies)| FleetPolicyOverviewRow { symbol, policies })
            .collect(),
    )
}

pub(crate) async fn get_fleet_policy_for_symbol(
    State(state): State<Arc<HttpState>>,
    axum::extract::Path(symbol): axum::extract::Path<String>,
    Query(query): Query<FleetPolicyQuery>,
) -> Json<Vec<PolicyConfigSnapshot>> {
    let top_k = query.top_k.unwrap_or(20).clamp(1, 200);
    Json(
        state
            .screener
            .top_policy_configs(&symbol, top_k)
            .unwrap_or_default(),
    )
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
    let result = with_readonly_conn(state.clone(), "fleet ranking", move |conn| {
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

        rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("row decode: {e}"),
            )
        })
    })
    .await?;
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
    let result = load_symbol_best_configs(
        &conn,
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
        ORDER BY total_pnl / total DESC",
        [],
    )?;
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
    let mut result = with_readonly_conn(state.clone(), "fleet ranked", move |conn| {
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
                } else if gross_win > 0.0 {
                    99.0
                } else {
                    0.0
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

        rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("row decode: {e}"),
            )
        })
    })
    .await?;

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
    apply_mode: String,
    symbols_reset: i64,
    changed_ids_requested: i64,
    matched_changed_ids_old: i64,
    matched_changed_ids_new: i64,
    unmatched_changed_ids: i64,
    scope_symbols_requested: i64,
    scope_symbols_matched: i64,
}

pub(crate) async fn get_trial_runs(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<TrialRunSummary>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;
    let result =
        load_trial_run_summaries(&conn, "run_id LIKE 'scout-%' OR run_id LIKE 'expand-%'")?;
    Ok(Json(result))
}

pub(crate) async fn get_forward_runs(
    State(state): State<Arc<HttpState>>,
) -> Result<Json<Vec<TrialRunSummary>>, (axum::http::StatusCode, String)> {
    let conn = open_readonly_conn(&state)?;
    let result = load_trial_run_summaries(&conn, "run_id LIKE 'forward-%'")?;
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
    let result = load_symbol_best_configs(
        &conn,
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
        ORDER BY total_pnl / total DESC",
        rusqlite::params![run_id],
    )?;
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
    let result = with_readonly_conn(state.clone(), "trial configs", move |conn| {
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

        rows.collect::<Result<Vec<_>, _>>().map_err(|e| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("row decode: {e}"),
            )
        })
    })
    .await?;
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
    let run_id = params.get("run_id").cloned();
    let breakdown = with_readonly_conn(state.clone(), "trial axes", move |conn| {
        build_trial_axes_breakdown(&conn, run_id)
    })
    .await?;
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
    let tail = query.tail.unwrap_or(200).clamp(1, 500);
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

#[cfg(test)]
mod tests;
