//! HTTP handler functions and response types.

use axum::{Json, extract::State};
use dashmap::DashMap;
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use crate::domain::screener::{ScreenerStore, ScreenerRow};
use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug};
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};

use super::http_server::HealthState;

const NATR_PERIOD_30M: usize = 30;
const NATR_CACHE_TTL_MS: i64 = 15 * 60 * 1000;
const NATR_FETCH_LIMIT_PER_REQUEST: usize = 6;
const NATR_FETCH_TIMEOUT_MS: u64 = 500;

// ── Shared state ────────────────────────────────────────────────────

#[derive(Debug)]
pub(crate) struct HttpState {
    pub min_volume_usd: f64,
    pub screener: ScreenerStore,
    pub natr_cache: Arc<DashMap<String, CachedNatr>>,
    pub health: Arc<HealthState>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CachedNatr {
    value_pct: Option<f64>,
    updated_at_ms: i64,
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
        fallback_screener_rows(state.min_volume_usd).await
    } else {
        live_rows
    };
    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    enrich_gate_natr_30m(&mut rows, &state.natr_cache).await;

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

// ── Internal helpers ────────────────────────────────────────────────

async fn fallback_screener_rows(min_volume_usd: f64) -> Vec<ScreenerRow> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();
    let (binance_tickers, gate_tickers) = tokio::join!(
        binance.get_tickers_with_volume(min_volume_usd),
        gate.get_tickers_with_volume(min_volume_usd)
    );

    let mut binance_volumes: HashMap<String, f64> = HashMap::new();
    let mut gate_volumes: HashMap<String, f64> = HashMap::new();

    if let Ok(tickers) = binance_tickers {
        for t in tickers {
            binance_volumes.insert(t.symbol, t.quote_volume);
        }
    }
    if let Ok(tickers) = gate_tickers {
        for t in tickers {
            gate_volumes.insert(t.symbol, t.quote_volume);
        }
    }

    let binance_symbols: HashSet<String> = binance_volumes.keys().cloned().collect();
    let gate_symbols: HashSet<String> = gate_volumes.keys().cloned().collect();

    binance_symbols
        .intersection(&gate_symbols)
        .cloned()
        .map(|symbol| {
            let binance_volume = binance_volumes.get(&symbol).copied().unwrap_or(0.0);
            let gate_volume = gate_volumes.get(&symbol).copied().unwrap_or(0.0);
            ScreenerRow {
                symbol,
                leader_exchange: if binance_volume >= gate_volume { "binance" } else { "gate" },
                lag_ms: 0.0,
                ws_drift_ms: 0.0,
                ws_drift_binance_ms: 0.0,
                ws_drift_gate_ms: 0.0,
                ws_drift_ingress_binance_ms: 0.0,
                ws_drift_ingress_gate_ms: 0.0,
                entry_half_life_ms: 0.0,
                avg_gt_p90_ms: 0.0,
                gate_natr_30m_pct: 0.0,
                volume_24h_usd: 0.0,
                shadow_pnl_per_hour_pct: 0.0,
                shadow_trades: 0,
                shadow_avg_trade_pct: 0.0,
                shadow_win_rate_pct: 0.0,
                shadow_position: "FLAT",
                shadow_spikes_detected: 0,
                shadow_avg_catchup_pct: 0.0,
                shadow_avg_lag_ms: 0.0,
            }
        })
        .collect()
}

async fn enrich_gate_natr_30m(
    rows: &mut [ScreenerRow],
    cache: &Arc<DashMap<String, CachedNatr>>,
) {
    let now = crate::domain::screener::utils::now_ms();
    let mut to_fetch: Vec<(usize, String)> = Vec::new();

    for (idx, row) in rows.iter_mut().enumerate() {
        if let Some(cached) = cache.get(&row.symbol) {
            if now.saturating_sub(cached.updated_at_ms) <= NATR_CACHE_TTL_MS {
                row.gate_natr_30m_pct = cached.value_pct.unwrap_or(0.0);
                continue;
            }
        }

        if to_fetch.len() < NATR_FETCH_LIMIT_PER_REQUEST {
            to_fetch.push((idx, row.symbol.clone()));
        }
    }

    let futs: Vec<_> = to_fetch
        .iter()
        .map(|(_, symbol)| {
            let sym = symbol.clone();
            let c = GateRestClient::new();
            async move {
                match tokio::time::timeout(
                    Duration::from_millis(NATR_FETCH_TIMEOUT_MS),
                    c.get_natr_30m(&sym, NATR_PERIOD_30M),
                )
                .await
                {
                    Ok(Ok(Some(v))) if v.is_finite() && v >= 0.0 => Some(v),
                    Ok(Ok(Some(_))) => Some(0.0),
                    Ok(Ok(None)) => None,
                    Ok(Err(_)) => None,
                    Err(_) => None,
                }
            }
        })
        .collect();

    let results = futures_util::future::join_all(futs).await;

    for ((idx, symbol), value) in to_fetch.into_iter().zip(results) {
        cache.insert(
            symbol,
            CachedNatr {
                value_pct: value,
                updated_at_ms: now,
            },
        );
        rows[idx].gate_natr_30m_pct = value.unwrap_or(0.0);
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
