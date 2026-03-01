//! Data enrichment services — fetching supplementary data from exchanges.
//!
//! Provides fallback screener row generation from REST snapshots when
//! live WS data is unavailable.

use std::collections::{HashMap, HashSet};

use crate::domain::screener::ScreenerRow;
use crate::infrastructure::rest::{BinanceRestClient, GateRestClient};

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
        .map(|symbol| ScreenerRow {
            symbol,
            data_source: "rest_fallback",
            last_update_ms: crate::domain::screener::utils::now_ms(),
            lag_ms: 0.0,
            shadow_position: "FLAT",
            shadow_spikes_detected: 0,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(symbol: &str) -> ScreenerRow {
        ScreenerRow {
            symbol: symbol.to_string(),
            data_source: "ws_live",
            last_update_ms: 1,
            lag_ms: 0.0,
            shadow_position: "FLAT",
            shadow_spikes_detected: 0,
        }
    }

    #[test]
    fn row_constructor_keeps_compact_projection() {
        let row = row("BTCUSDT");
        assert_eq!(row.symbol, "BTCUSDT");
        assert_eq!(row.shadow_position, "FLAT");
    }
}
