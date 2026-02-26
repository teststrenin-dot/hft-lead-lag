use super::{ConfigManager, STRATEGY_BLACKLIST};
use hft_lead_lag::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};
use std::collections::{HashMap, HashSet};
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SymbolReconcileOutcome {
    Ok,
    BinanceMissing,
    GateMissing,
    BothMissing,
}

pub(super) struct RuntimeUniverse {
    pub(super) common_symbols: Vec<String>,
    pub(super) strategy_symbols: Vec<String>,
    pub(super) screener_symbols: Vec<String>,
    pub(super) gate_vol_map: HashMap<String, f64>,
}

pub(super) async fn fetch_volume_tickers(min_volume_usd: f64) -> (Vec<Ticker24h>, Vec<Ticker24h>) {
    info!("Fetching 24h volume data for symbol filtering");
    let binance_rest = BinanceRestClient::new();
    let gate_rest = GateRestClient::new();
    let (binance_tickers_result, gate_tickers_result) = tokio::join!(
        binance_rest.get_tickers_with_volume(min_volume_usd),
        gate_rest.get_tickers_with_volume(min_volume_usd)
    );

    let binance_tickers = match binance_tickers_result {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get Binance tickers: {}", e);
            Vec::new()
        }
    };
    let gate_tickers = match gate_tickers_result {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get Gate tickers: {}", e);
            Vec::new()
        }
    };
    (binance_tickers, gate_tickers)
}

fn fallback_symbols() -> Vec<String> {
    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
}

pub(super) fn reconcile_volume_symbols(
    mut binance_symbols: Vec<String>,
    mut gate_symbols: Vec<String>,
) -> (Vec<String>, Vec<String>, SymbolReconcileOutcome) {
    let outcome = if binance_symbols.is_empty() && !gate_symbols.is_empty() {
        let fallback = fallback_symbols();
        binance_symbols = fallback.clone();
        gate_symbols = fallback;
        SymbolReconcileOutcome::BinanceMissing
    } else if gate_symbols.is_empty() && !binance_symbols.is_empty() {
        let fallback = fallback_symbols();
        binance_symbols = fallback.clone();
        gate_symbols = fallback;
        SymbolReconcileOutcome::GateMissing
    } else if binance_symbols.is_empty() && gate_symbols.is_empty() {
        let fallback = fallback_symbols();
        binance_symbols = fallback.clone();
        gate_symbols = fallback;
        SymbolReconcileOutcome::BothMissing
    } else {
        SymbolReconcileOutcome::Ok
    };
    (binance_symbols, gate_symbols, outcome)
}

pub(super) fn select_runtime_symbols(
    common_symbols: &[String],
) -> (Vec<String>, Vec<String>, bool) {
    if common_symbols.is_empty() {
        let fallback = fallback_symbols();
        (fallback.clone(), fallback, true)
    } else {
        let symbols = common_symbols.to_vec();
        (symbols.clone(), symbols, false)
    }
}

pub(super) fn compute_common_symbols(
    binance_symbols: &[String],
    gate_symbols: &[String],
    blacklist: &std::collections::HashSet<&str>,
) -> Vec<String> {
    let binance_set: std::collections::HashSet<String> = binance_symbols.iter().cloned().collect();
    let gate_set: std::collections::HashSet<String> = gate_symbols.iter().cloned().collect();
    let mut common_symbols: Vec<String> = binance_set
        .intersection(&gate_set)
        .filter(|s| !blacklist.contains(s.as_str()))
        .cloned()
        .collect();
    common_symbols.sort_unstable();
    common_symbols
}

pub(super) fn build_runtime_universe(
    config_manager: &ConfigManager,
    min_volume_usd: f64,
    binance_tickers: Vec<Ticker24h>,
    gate_tickers: Vec<Ticker24h>,
) -> RuntimeUniverse {
    let binance_symbols: Vec<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: Vec<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_vol_map: HashMap<String, f64> = gate_tickers
        .iter()
        .map(|t| (t.symbol.clone(), t.quote_volume))
        .collect();
    let (binance_symbols, gate_symbols, reconcile_outcome) =
        reconcile_volume_symbols(binance_symbols, gate_symbols);

    match reconcile_outcome {
        SymbolReconcileOutcome::BinanceMissing => {
            warn!(
                "Binance volume fetch failed — cannot safely copy Gate symbols (different listing). Using BTC/ETH fallback for both."
            );
        }
        SymbolReconcileOutcome::GateMissing => {
            warn!(
                "Gate volume fetch failed — cannot safely copy Binance symbols (different listing). Using BTC/ETH fallback for both."
            );
        }
        SymbolReconcileOutcome::BothMissing => {
            warn!("No symbols from REST; using BTC/ETH fallback");
        }
        SymbolReconcileOutcome::Ok => {}
    }

    info!(
        "Binance symbols with 24h vol >= ${:.0}M: {}",
        min_volume_usd / 1_000_000.0,
        binance_symbols.len()
    );
    info!(
        "Gate symbols with 24h vol >= ${:.0}M: {}",
        min_volume_usd / 1_000_000.0,
        gate_symbols.len()
    );

    let blacklist: HashSet<&str> = config_manager
        .binance_blacklist()
        .iter()
        .chain(config_manager.gate_blacklist().iter())
        .map(|s| s.as_str())
        .chain(STRATEGY_BLACKLIST.iter().copied())
        .collect();
    let common_symbols = compute_common_symbols(&binance_symbols, &gate_symbols, &blacklist);

    if !blacklist.is_empty() {
        info!("Blacklisted symbols: {:?}", blacklist);
    }
    info!("Common symbols: {}", common_symbols.len());

    let (mut strategy_symbols, mut screener_symbols, used_fallback) =
        select_runtime_symbols(&common_symbols);
    strategy_symbols.retain(|symbol| !blacklist.contains(symbol.as_str()));
    screener_symbols.retain(|symbol| !blacklist.contains(symbol.as_str()));
    if used_fallback {
        if strategy_symbols.is_empty() {
            warn!("No common symbols found and fallback symbols are blacklisted; runtime universe is empty");
        } else {
            warn!("No common symbols found! Using fallback...");
        }
    }

    info!(
        "Strategy symbols: {} | Screener symbols: {} | WS coverage Binance={} Gate={}",
        strategy_symbols.len(),
        screener_symbols.len(),
        binance_symbols.len(),
        gate_symbols.len()
    );

    RuntimeUniverse {
        common_symbols,
        strategy_symbols,
        screener_symbols,
        gate_vol_map,
    }
}
