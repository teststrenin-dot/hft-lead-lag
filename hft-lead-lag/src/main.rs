//! HFT Lead-Lag Trading System - Main Entry Point
//!
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::api::{
    HealthState, MarketDataEvent, ScreenerStore,
};
use hft_lead_lag::domain::screener::fleet_patch::{FleetPatchMode, FleetPatchPlan};
use hft_lead_lag::domain::screener::{TraderConfig, CONFIG_ID_CONTRACT_VERSION};
use hft_lead_lag::infrastructure::logging::init_centralized_logging;
use hft_lead_lag::{
    build_runtime_strategy, BinanceMarketData, ConfigManager, GateMarketData, RuntimeStrategy,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::SystemTime;
use tracing::{error, info, warn};

mod runtime_hot_reload;
mod runtime_grid;
mod runtime_setup;
mod trial_batch_protocol;
mod trial_batch_apply;
mod trial_queue_io;
mod event_loop_ingest;
mod event_loop_core;
mod event_loop_runtime;
use event_loop_ingest::{
    BatchIngestContext, process_exchange_batch, strategy_ticks_in_order,
    updated_symbols_from_batch,
};
use event_loop_core::{
    EventLoopMetrics, EventLoopState, ExchangeSide, StrategyExchangeRouting,
    resolve_strategy_exchange_routing,
};
use event_loop_runtime::{EventLoopRuntimeContext, run_event_loop};
#[cfg(test)]
use event_loop_ingest::ingest_latest_batch;
use runtime_hot_reload::{spawn_runtime_grid_hot_reload, RuntimeGridHotReloadSpec};
use runtime_grid::{
    RuntimeGridGeneration, ensure_runtime_grid_config_file, load_runtime_grid_generation_async,
};
#[cfg(test)]
use runtime_grid::RuntimeGridConfig;
use runtime_setup::{
    build_runtime_universe, configure_and_connect_exchanges, drain_stale_ticks,
    fetch_volume_tickers, init_screener_persistence, spawn_gate_natr_refresher,
    start_api_servers, subscribe_gate_symbols,
};
use trial_batch_protocol::{
    TrialAck, TrialBatch, build_trial_batch_patch_plan, load_trial_batch, load_trial_control,
};
#[cfg(test)]
use trial_batch_protocol::TrialBatchMode;
use trial_batch_apply::{
    apply_trial_batch, close_trial_run_meta_async, upsert_runtime_configs_async,
};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 2_500_000.0; // 2.5 million USD
const SUBSCRIBE_DELAY_MS: u64 = 15;
const RUNTIME_GRID_CONFIG_PATH: &str = "config/runtime-grid.toml";
/// Symbols excluded from strategy — consistently unprofitable or structurally unsuitable.
const STRATEGY_BLACKLIST: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "DYDXUSDT"];
const SIGNAL_CHECK_BUDGET_PER_TICK: usize = 256;
#[cfg(test)]
const TRIAL_BATCH_ARCHIVE_MAX_FILES: usize = trial_queue_io::TRIAL_BATCH_ARCHIVE_MAX_FILES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolReconcileOutcome {
    Ok,
    BinanceMissing,
    GateMissing,
    BothMissing,
}

struct RuntimeUniverse {
    common_symbols: Vec<String>,
    strategy_symbols: Vec<String>,
    screener_symbols: Vec<String>,
    gate_vol_map: std::collections::HashMap<String, f64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileFingerprint {
    modified: SystemTime,
    len: u64,
    content_hash: u64,
}

fn hash_content_deterministic(bytes: &[u8]) -> u64 {
    // FNV-1a 64-bit hash keeps fingerprinting deterministic and dependency-free.
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn build_trial_batch_error_ack(path: &Path, is_queue_mode: bool, error: String) -> TrialAck {
    trial_queue_io::build_trial_batch_error_ack(path, is_queue_mode, error)
}

fn read_file_fingerprint(path: &Path) -> Option<FileFingerprint> {
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata.modified().ok()?;
    let content = std::fs::read(path).ok()?;
    Some(FileFingerprint {
        modified,
        len: metadata.len(),
        content_hash: hash_content_deterministic(&content),
    })
}

fn file_fingerprint_changed(
    previous: Option<FileFingerprint>,
    current: Option<FileFingerprint>,
) -> bool {
    match current {
        Some(current) => previous != Some(current),
        None => false,
    }
}

#[cfg(test)]
fn trial_batch_queue_dir(config_dir: &Path) -> PathBuf {
    trial_queue_io::trial_batch_queue_dir(config_dir)
}

#[cfg(test)]
fn trial_batch_archive_dir(config_dir: &Path, success: bool) -> PathBuf {
    trial_queue_io::trial_batch_archive_dir(config_dir, success)
}

fn list_trial_batch_queue_files(config_dir: &Path) -> Vec<PathBuf> {
    trial_queue_io::list_trial_batch_queue_files(config_dir)
}

fn archive_trial_batch_queue_file(config_dir: &Path, queued_batch_path: &Path, success: bool) {
    trial_queue_io::archive_trial_batch_queue_file(config_dir, queued_batch_path, success);
}

fn write_trial_ack(dir: &Path, ack: &TrialAck) {
    trial_queue_io::write_trial_ack(dir, ack);
}

#[cfg(test)]
fn validate_trial_batch_run_lease(
    active_run_id: Option<&str>,
    incoming_run_id: &str,
    allow_run_id_takeover: bool,
) -> Result<(), String> {
    trial_batch_apply::validate_trial_batch_run_lease(
        active_run_id,
        incoming_run_id,
        allow_run_id_takeover,
    )
}

fn fallback_symbols() -> Vec<String> {
    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
}

fn reconcile_volume_symbols(
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

fn rebuild_latest_map(
    latest: &mut std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
) -> std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> {
    let mut batch_latest: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> =
        std::collections::HashMap::new();
    let first_symbol = String::from_utf8_lossy(&first.symbol).to_string();
    batch_latest.insert(first_symbol, first);
    for ticker in drained {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        batch_latest.insert(symbol, ticker);
    }
    for (symbol, ticker) in &batch_latest {
        latest.insert(symbol.clone(), ticker.clone());
    }
    batch_latest
}

fn select_runtime_symbols(common_symbols: &[String]) -> (Vec<String>, Vec<String>, bool) {
    if common_symbols.is_empty() {
        let fallback = fallback_symbols();
        (fallback.clone(), fallback, true)
    } else {
        let symbols = common_symbols.to_vec();
        (symbols.clone(), symbols, false)
    }
}

fn compute_common_symbols(
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

#[cfg(test)]
fn drain_runtime_grid_reset_signals(grid_reset_rx: &mut tokio::sync::mpsc::Receiver<()>) -> bool {
    runtime_hot_reload::drain_runtime_grid_reset_signals(grid_reset_rx)
}

#[cfg(test)]
fn runtime_grid_sleep_ms(pending: Option<&RuntimeGridGeneration>) -> u64 {
    runtime_hot_reload::runtime_grid_sleep_ms(pending)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_centralized_logging("logs", "runtime.log")?;

    // Load .env file if present (before reading env vars)
    dotenvy::dotenv().ok();

    info!("HFT Lead-Lag system starting");

    // Load configuration from environment
    let config_manager = ConfigManager::from_env();

    let (binance_tickers, gate_tickers) = fetch_volume_tickers(MIN_VOLUME_USD).await;
    let universe = build_runtime_universe(
        &config_manager,
        MIN_VOLUME_USD,
        binance_tickers,
        gate_tickers,
    );
    let RuntimeUniverse {
        common_symbols,
        strategy_symbols,
        screener_symbols,
        gate_vol_map,
    } = universe;

    // Initialize exchange connectors
    let mut binance = BinanceMarketData::new();
    let mut gate = GateMarketData::new();
    let health_state = Arc::new(HealthState::new());
    configure_and_connect_exchanges(
        &config_manager,
        &mut binance,
        &mut gate,
        health_state.as_ref(),
    )
    .await?;

    // Start external APIs early so checkpoint endpoints are always available.
    let mut screener = ScreenerStore::default();
    let runtime_grid_path = Path::new(RUNTIME_GRID_CONFIG_PATH);
    ensure_runtime_grid_config_file(runtime_grid_path)?;
    let mut runtime_grid_last_modified: Option<FileFingerprint> = None;
    let mut runtime_grid_last_signature: Option<u64> = None;
    match load_runtime_grid_generation_async(runtime_grid_path.to_path_buf()).await {
        Ok(generation) => {
            runtime_grid_last_modified = Some(generation.modified);
            if generation.config.enabled {
                let report = screener.replace_fleet_configs(generation.configs);
                runtime_grid_last_signature = Some(generation.signature);
                info!(
                    "runtime-grid: startup apply old={} new={} symbols_reset={} drained_trades={}",
                    report.old_config_count,
                    report.new_config_count,
                    report.symbols_reset,
                    report.drained_trades
                );
            } else {
                info!("runtime-grid: startup disabled");
            }
        }
        Err(e) => warn!("runtime-grid: startup config ignored: {e}"),
    }

    // Initialize fleet persistence (SQLite WAL mode, async batch writes).
    let db_path = std::path::Path::new("data/optimizer.db");
    init_screener_persistence(&mut screener, db_path)?;

    // Seed 24h volume from Gate REST data
    let vol_pairs: Vec<(String, f64)> = common_symbols
        .iter()
        .map(|s| (s.clone(), gate_vol_map.get(s).copied().unwrap_or(0.0)))
        .collect();
    screener.set_volumes(&vol_pairs);
    spawn_runtime_grid_hot_reload(
        screener.clone(),
        db_path.to_path_buf(),
        health_state.clone(),
        RuntimeGridHotReloadSpec {
            config_path: runtime_grid_path.to_path_buf(),
            trial_batch_path: PathBuf::from("config/trial-batch.json"),
            trial_control_path: PathBuf::from("config/trial-control.json"),
            initial_modified: runtime_grid_last_modified,
            initial_signature: runtime_grid_last_signature,
        },
    );
    spawn_gate_natr_refresher(screener.clone(), common_symbols.clone());
    let ws_tx = start_api_servers(MIN_VOLUME_USD, screener.clone(), health_state.clone()).await?;

    // Subscribe to screener symbols for live WS ticks.
    let (binance_subscribed, binance_subscribe_errors) = match binance
        .subscribe_book_tickers_batch(&screener_symbols)
        .await
    {
        Ok(count) => (count, 0usize),
        Err(e) => {
            error!("Binance batch subscribe error: {}", e);
            (0usize, screener_symbols.len())
        }
    };
    let binance_ws_sockets = screener_symbols.len().div_ceil(2);
    info!(
        "Binance subscription summary: ok={} err={} sockets={} symbols_per_ws=2",
        binance_subscribed, binance_subscribe_errors, binance_ws_sockets
    );

    // Subscribe Gate to screener symbols as well.
    subscribe_gate_symbols(&mut gate, &screener_symbols).await;

    // Build runtime strategy selected via config.
    let strategy = match build_runtime_strategy(&config_manager, strategy_symbols.clone()) {
        Ok(strategy) => strategy,
        Err(e) => {
            error!("Failed to build runtime strategy: {}", e);
            return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }
    };
    let strategy_exchange_routing = resolve_strategy_exchange_routing(&config_manager);

    info!(
        "System initialized; strategy={} symbols={}",
        strategy.strategy_name(),
        strategy_symbols.len()
    );

    // Drain messages that accumulated during subscription phase to avoid
    // stale ticks with misleading local_ts_ns at the start of the main loop.
    drain_stale_ticks(&mut binance, &mut gate).await;

    run_event_loop(
        &mut binance,
        &mut gate,
        strategy.as_ref(),
        &strategy_symbols,
        EventLoopRuntimeContext {
            strategy_exchange_routing,
            screener: &screener,
            health_state: health_state.as_ref(),
            ws_tx: &ws_tx,
        },
    )
    .await
}

#[cfg(test)]
mod main_tests;
