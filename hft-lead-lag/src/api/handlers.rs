//! HTTP handler functions and response types.

use axum::{Json, extract::State};
use dashmap::DashMap;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug};
use crate::domain::screener::{ScreenerRow, ScreenerStore};
use crate::infrastructure::db::DbWriter;
use crate::infrastructure::enrichment::{self, CachedNatr};
use crate::infrastructure::exchanges::{BinanceMarketData, GateMarketData};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};

use super::http_server::HealthState;

// ── Shared state ────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct HttpState {
    pub min_volume_usd: f64,
    pub screener: ScreenerStore,
    pub natr_cache: Arc<DashMap<String, CachedNatr>>,
    pub health: Arc<HealthState>,
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

    let binance_symbols: HashSet<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: HashSet<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    let mut common_symbols: Vec<String> = binance_symbols.intersection(&gate_symbols).cloned().collect();
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
    State(_state): State<Arc<HttpState>>,
) -> Result<Json<Vec<FleetConfigRank>>, (axum::http::StatusCode, String)> {
    let db_path = std::path::Path::new("data/optimizer.db");
    let conn = crate::infrastructure::db::open_db(db_path)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db: {e}")))?;

    let mut stmt = conn.prepare(
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
         LIMIT 50"
    ).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("sql: {e}")))?;

    let rows = stmt.query_map([], |row| {
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
    }).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

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
    State(_state): State<Arc<HttpState>>,
) -> Result<Json<Vec<SymbolBestConfig>>, (axum::http::StatusCode, String)> {
    let db_path = std::path::Path::new("data/optimizer.db");
    let conn = crate::infrastructure::db::open_db(db_path)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db: {e}")))?;

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

    let rows = stmt.query_map([], |row| {
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
    }).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

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
    State(_state): State<Arc<HttpState>>,
) -> Result<Json<Vec<FleetRankedConfig>>, (axum::http::StatusCode, String)> {
    let db_path = std::path::Path::new("data/optimizer.db");
    let conn = crate::infrastructure::db::open_db(db_path)
        .map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("db: {e}")))?;

    let mut stmt = conn.prepare(
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
         LIMIT 100"
    ).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("sql: {e}")))?;

    let rows = stmt.query_map([], |row| {
        let total: i64 = row.get(8)?;
        let wins: i64 = row.get(9)?;
        let total_pnl: f64 = row.get(10)?;
        let avg_pnl: f64 = row.get(11)?;
        let avg_pnl_sq: f64 = row.get(12)?;
        let gross_win: f64 = row.get(13)?;
        let gross_loss: f64 = row.get(14)?;

        let variance = (avg_pnl_sq - avg_pnl * avg_pnl).max(0.0);
        let stddev_pnl = variance.sqrt();
        let sharpe = if stddev_pnl > 1e-9 { avg_pnl / stddev_pnl } else { 0.0 };
        let profit_factor = if gross_loss > 1e-9 { gross_win / gross_loss } else { if gross_win > 0.0 { 99.0 } else { 0.0 } };
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
            win_rate_pct: if total > 0 { (wins as f64 / total as f64) * 100.0 } else { 0.0 },
            total_pnl_pct: total_pnl,
            avg_pnl_pct: avg_pnl,
            stddev_pnl_pct: stddev_pnl,
            sharpe,
            profit_factor,
            composite,
            symbols_traded: row.get(15)?,
        })
    }).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

    let mut result: Vec<FleetRankedConfig> = rows.filter_map(|r| r.ok()).collect();
    result.sort_by(|a, b| b.composite.partial_cmp(&a.composite).unwrap_or(std::cmp::Ordering::Equal));
    Ok(Json(result))
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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use dashmap::DashMap;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

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
        health_state.binance_connected.store(true, Ordering::Relaxed);
        health_state.gate_connected.store(true, Ordering::Relaxed);
        health_state.binance_last_tick_ms.store(1, Ordering::Relaxed);
        health_state.gate_last_tick_ms.store(1, Ordering::Relaxed);

        let state = Arc::new(HttpState {
            min_volume_usd: 1_000_000.0,
            screener: ScreenerStore::default(),
            natr_cache: Arc::new(DashMap::new()),
            health: health_state,
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
