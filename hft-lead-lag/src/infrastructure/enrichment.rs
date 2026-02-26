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

/// Fill screener rows with cached Gate 30m NATR values and return symbols to warm in background.
pub fn enrich_gate_natr_30m_cached_only(
    rows: &mut [ScreenerRow],
    cache: &Arc<DashMap<String, CachedNatr>>,
) -> Vec<String> {
    let now = crate::domain::screener::utils::now_ms();
    let mut to_fetch: Vec<String> = Vec::new();
    let mut seen_for_fetch: HashSet<String> = HashSet::new();

    for row in rows.iter_mut() {
        if let Some(cached) = cache.get(&row.symbol) {
            if now.saturating_sub(cached.updated_at_ms) <= NATR_CACHE_TTL_MS {
                row.gate_natr_30m_pct = cached.value_pct.unwrap_or(0.0);
                continue;
            }
        }

        if to_fetch.len() < NATR_FETCH_LIMIT_PER_REQUEST
            && seen_for_fetch.insert(row.symbol.clone())
        {
            to_fetch.push(row.symbol.clone());
        }
        row.gate_natr_30m_pct = 0.0;
    }
    to_fetch
}

/// Warm Gate 30m NATR cache for requested symbols.
pub async fn warm_gate_natr_30m_cache(
    symbols: Vec<String>,
    cache: Arc<DashMap<String, CachedNatr>>,
) {
    if symbols.is_empty() {
        return;
    }
    let now = crate::domain::screener::utils::now_ms();
    let client = GateRestClient::new();
    let futs: Vec<_> = symbols
        .iter()
        .map(|symbol| {
            let sym = symbol.clone();
            let c = client.clone();
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

    for (symbol, value) in symbols.into_iter().zip(results) {
        cache.insert(
            symbol,
            CachedNatr {
                value_pct: value,
                updated_at_ms: now,
            },
        );
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

    let mut common_symbols: Vec<String> = binance_symbols
        .intersection(&gate_symbols)
        .cloned()
        .collect();
    common_symbols.sort_unstable();

    common_symbols
        .into_iter()
        .map(|symbol| {
            let binance_volume = binance_volumes.get(&symbol).copied().unwrap_or(0.0);
            let gate_volume = gate_volumes.get(&symbol).copied().unwrap_or(0.0);
            ScreenerRow {
                symbol,
                leader_exchange: if binance_volume >= gate_volume {
                    "binance"
                } else {
                    "gate"
                },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str) -> ScreenerRow {
        ScreenerRow {
            symbol: symbol.to_string(),
            leader_exchange: "binance",
            data_source: "ws_live",
            is_fallback: false,
            last_update_ms: 1,
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
    }

    #[test]
    fn cached_only_enrichment_uses_fresh_cache_and_marks_misses() {
        let cache = Arc::new(DashMap::new());
        let now = crate::domain::screener::utils::now_ms();
        cache.insert(
            "BTCUSDT".to_string(),
            CachedNatr {
                value_pct: Some(1.23),
                updated_at_ms: now,
            },
        );
        let mut rows = vec![row("BTCUSDT"), row("ETHUSDT")];

        let to_fetch = enrich_gate_natr_30m_cached_only(&mut rows, &cache);

        assert_eq!(rows[0].gate_natr_30m_pct, 1.23);
        assert_eq!(rows[1].gate_natr_30m_pct, 0.0);
        assert_eq!(to_fetch, vec!["ETHUSDT".to_string()]);
    }

    #[test]
    fn cached_only_enrichment_refreshes_stale_cache_entries() {
        let cache = Arc::new(DashMap::new());
        let now = crate::domain::screener::utils::now_ms();
        cache.insert(
            "BTCUSDT".to_string(),
            CachedNatr {
                value_pct: Some(9.99),
                updated_at_ms: now - NATR_CACHE_TTL_MS - 1,
            },
        );
        let mut rows = vec![row("BTCUSDT")];

        let to_fetch = enrich_gate_natr_30m_cached_only(&mut rows, &cache);

        assert_eq!(rows[0].gate_natr_30m_pct, 0.0);
        assert_eq!(to_fetch, vec!["BTCUSDT".to_string()]);
    }

    #[test]
    fn cached_only_enrichment_deduplicates_fetch_symbols() {
        let cache = Arc::new(DashMap::new());
        let mut rows = vec![
            row("BTCUSDT"),
            row("BTCUSDT"),
            row("ETHUSDT"),
            row("ETHUSDT"),
        ];

        let to_fetch = enrich_gate_natr_30m_cached_only(&mut rows, &cache);

        assert_eq!(to_fetch, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }
}
