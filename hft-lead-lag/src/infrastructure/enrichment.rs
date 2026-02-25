//! Data enrichment services — fetching supplementary data from exchanges.
//!
//! Provides NATR enrichment (with TTL cache) and fallback screener row
//! generation from REST snapshots when live WS data is unavailable.

use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::domain::screener::ScreenerRow;
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient};

const NATR_PERIOD_30M: usize = 30;
const NATR_CACHE_TTL_MS: i64 = 15 * 60 * 1000;
const NATR_FETCH_LIMIT_PER_REQUEST: usize = 6;
const NATR_FETCH_TIMEOUT_MS: u64 = 500;

/// Cached NATR value with TTL
#[derive(Debug, Clone, Copy)]
pub struct CachedNatr {
    value_pct: Option<f64>,
    updated_at_ms: i64,
}

/// Enrich screener rows with Gate 30m NATR (cached, rate-limited).
pub async fn enrich_gate_natr_30m(
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

/// Generate fallback screener rows from REST snapshots when WS data is unavailable.
pub async fn fallback_screener_rows(min_volume_usd: f64) -> Vec<ScreenerRow> {
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
                data_source: "rest_fallback",
                is_fallback: true,
                last_update_ms: crate::domain::screener::utils::now_ms(),
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
                shadow_session_pnl_pct: 0.0,
                shadow_session_trades: 0,
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
