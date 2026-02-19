//! HTTP handler functions and response types.

use axum::{Json, extract::State};
use dashmap::DashMap;
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::domain::screener::{ScreenerStore, ScreenerRow};
use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug};
use crate::infrastructure::enrichment::{self, CachedNatr};
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

pub(crate) async fn health(State(state): State<Arc<HttpState>>) -> (axum::http::StatusCode, Json<HealthResponse>) {
    let binance = state.health.binance_connected.load(Ordering::Relaxed);
    let gate = state.health.gate_connected.load(Ordering::Relaxed);
    let healthy = binance && gate;
    let status = if healthy { "ok" } else { "degraded" };
    let code = if healthy {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    };
    (code, Json(HealthResponse { status, binance, gate }))
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
                c.trailing_decay_ratio,
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
        let total: i64 = row.get(7)?;
        let wins: i64 = row.get(8)?;
        let total_pnl: f64 = row.get(9)?;
        Ok(FleetConfigRank {
            config_id: row.get(0)?,
            spike_threshold_bps: row.get(1)?,
            target_ratio: row.get(2)?,
            stop_loss_bps: row.get(3)?,
            max_hold_ms: row.get(4)?,
            max_spread_bps: row.get(5)?,
            trailing_decay_ratio: row.get(6)?,
            total_trades: total,
            wins,
            win_rate_pct: if total > 0 { (wins as f64 / total as f64) * 100.0 } else { 0.0 },
            total_pnl_pct: total_pnl,
            avg_pnl_pct: if total > 0 { total_pnl / total as f64 } else { 0.0 },
            symbols_traded: row.get(10)?,
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
                   c.trailing_decay_ratio,
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
               total, wins, total_pnl
        FROM ranked WHERE rn = 1
        ORDER BY total_pnl / total DESC"
    ).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("sql: {e}")))?;

    let rows = stmt.query_map([], |row| {
        let total: i64 = row.get(8)?;
        let wins: i64 = row.get(9)?;
        let total_pnl: f64 = row.get(10)?;
        Ok(SymbolBestConfig {
            symbol: row.get(0)?,
            config_id: row.get(1)?,
            spike_threshold_bps: row.get(2)?,
            target_ratio: row.get(3)?,
            stop_loss_bps: row.get(4)?,
            max_hold_ms: row.get(5)?,
            max_spread_bps: row.get(6)?,
            trailing_decay_ratio: row.get(7)?,
            total_trades: total,
            wins,
            win_rate_pct: if total > 0 { (wins as f64 / total as f64) * 100.0 } else { 0.0 },
            total_pnl_pct: total_pnl,
            avg_pnl_pct: if total > 0 { total_pnl / total as f64 } else { 0.0 },
        })
    }).map_err(|e| (axum::http::StatusCode::INTERNAL_SERVER_ERROR, format!("query: {e}")))?;

    let result: Vec<SymbolBestConfig> = rows.filter_map(|r| r.ok()).collect();
    Ok(Json(result))
}

// ── Internal helpers ────────────────────────────────────────────────

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
