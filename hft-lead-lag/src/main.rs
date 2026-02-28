//! HFT Lead-Lag Trading System - Main Entry Point
//!
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::api::{HealthState, MarketDataEvent, ScreenerStore};
use hft_lead_lag::domain::screener::fleet_patch::{FleetPatchMode, FleetPatchPlan};
use hft_lead_lag::domain::screener::{TraderConfig, CONFIG_ID_CONTRACT_VERSION};
use hft_lead_lag::infrastructure::logging::init_centralized_logging;
use hft_lead_lag::infrastructure::replay::raw_feed::{
    raw_feed_recorder_from_env, verify_signal_replay_determinism_from_file, RawFeedExchange,
    RawFeedReplayRouting, RAW_FEED_RECORD_PATH_ENV,
};
use hft_lead_lag::{
    build_runtime_strategy, BinanceMarketData, ConfigManager, GateMarketData, RuntimeStrategy,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};

mod event_loop_core;
mod event_loop_ingest;
mod event_loop_runtime;
mod file_fingerprint;
mod runtime_grid;
mod runtime_hot_reload;
mod runtime_setup;
mod runtime_symbols;
mod trial_batch_apply;
mod trial_batch_protocol;
mod trial_queue_io;
use event_loop_core::{
    resolve_strategy_exchange_routing, EventLoopMetrics, EventLoopState, ExchangeSide,
    StrategyExchangeRouting, StrategySymbolIndex, SymbolId,
};
use event_loop_ingest::ingest_exchange_batch;
#[cfg(test)]
use event_loop_ingest::ingest_latest_batch;
#[cfg(test)]
use event_loop_ingest::process_exchange_batch;
#[cfg(test)]
use event_loop_ingest::strategy_ticks_in_order;
#[cfg(test)]
use event_loop_ingest::updated_strategy_symbol_ids_from_batch;
#[cfg(test)]
use event_loop_ingest::updated_symbols_from_batch;
use event_loop_ingest::{strategy_symbol_updates_from_batch, BatchIngestContext};
use event_loop_runtime::{run_event_loop, EventLoopRuntimeContext};
use file_fingerprint::{
    file_fingerprint_changed, hash_content_deterministic, read_file_fingerprint, FileFingerprint,
};
#[cfg(test)]
use runtime_grid::RuntimeGridConfig;
use runtime_grid::{
    ensure_runtime_grid_config_file, load_runtime_grid_generation_async, RuntimeGridGeneration,
};
#[cfg(test)]
use runtime_hot_reload::{drain_runtime_grid_reset_signals, runtime_grid_sleep_ms};
use runtime_hot_reload::{spawn_runtime_grid_hot_reload, RuntimeGridHotReloadSpec};
use runtime_setup::{
    configure_and_connect_exchanges, drain_stale_ticks, init_screener_persistence,
    spawn_gate_natr_refresher, start_api_servers, subscribe_gate_symbols,
};
use runtime_symbols::{build_runtime_universe, fetch_volume_tickers, RuntimeUniverse};
#[cfg(test)]
use runtime_symbols::{
    compute_common_symbols, reconcile_volume_symbols, select_runtime_symbols,
    SymbolReconcileOutcome,
};
#[cfg(test)]
use trial_batch_apply::validate_trial_batch_run_lease;
use trial_batch_apply::{
    apply_trial_batch, close_trial_run_meta_async, upsert_runtime_configs_async,
};
#[cfg(test)]
use trial_batch_protocol::TrialBatchMode;
use trial_batch_protocol::{
    build_trial_batch_patch_plan, load_trial_batch, load_trial_control, TrialAck, TrialBatch,
};
use trial_queue_io::{
    archive_trial_batch_queue_file, build_trial_batch_error_ack,
    count_trial_batch_quarantine_markers, list_trial_batch_queue_files, write_trial_ack,
};
#[cfg(test)]
use trial_queue_io::{trial_batch_archive_dir, trial_batch_queue_dir};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 2_500_000.0; // 2.5 million USD
const SUBSCRIBE_DELAY_MS: u64 = 15;
const RUNTIME_GRID_CONFIG_PATH: &str = "config/runtime-grid.toml";
/// Symbols excluded from strategy — consistently unprofitable or structurally unsuitable.
const STRATEGY_BLACKLIST: &[&str] = &[
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "DYDXUSDT", "XRPUSDT", "XAGUSDT", "DOGEUSDT", "ADAUSDT",
    "AVAXUSDT", "DOTUSDT", "XAUUSDT", "SUIUSDT", "LINKUSDT", "LTCUSDT", "BNBUSDT", "ATOMUSDT",
    "AAVEUSDT",
];
const SIGNAL_CHECK_BUDGET_PER_TICK: usize = 256;
const PORTFOLIO_IDS_ENV: &str = "PORTFOLIO_IDS";
const ENABLE_SCREENER_CHART_PIPELINE: bool = false;
const REPLAY_RAW_FEED_PATH_ENV: &str = "REPLAY_RAW_FEED_PATH";
const REPLAY_STRATEGY_SYMBOLS_ENV: &str = "REPLAY_STRATEGY_SYMBOLS";
const REPLAY_PRIMARY_EXCHANGE_ENV: &str = "REPLAY_PRIMARY_EXCHANGE";
#[cfg(test)]
const TRIAL_BATCH_ARCHIVE_MAX_FILES: usize = trial_queue_io::TRIAL_BATCH_ARCHIVE_MAX_FILES;

#[cfg(test)]
fn rebuild_latest_map(
    latest: &mut std::collections::HashMap<bytes::Bytes, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
) -> std::collections::HashMap<bytes::Bytes, hft_lead_lag::domain::BookTicker> {
    let mut batch_latest: std::collections::HashMap<
        bytes::Bytes,
        hft_lead_lag::domain::BookTicker,
    > = std::collections::HashMap::new();
    let first_symbol = first.symbol.clone();
    batch_latest.insert(first_symbol, first);
    for ticker in drained {
        let symbol = ticker.symbol.clone();
        batch_latest.insert(symbol, ticker);
    }
    for (symbol, ticker) in &batch_latest {
        latest.insert(symbol.clone(), ticker.clone());
    }
    batch_latest
}

fn parse_portfolio_ids(raw: &str) -> Vec<String> {
    let mut ids: Vec<String> = Vec::new();
    for token in raw.split(',') {
        let id = token.trim();
        if id.is_empty() {
            continue;
        }
        if ids.iter().any(|existing| existing == id) {
            continue;
        }
        ids.push(id.to_string());
    }
    ids
}

fn portfolio_ids_from_env() -> Option<Vec<String>> {
    let raw = std::env::var(PORTFOLIO_IDS_ENV).ok()?;
    let ids = parse_portfolio_ids(&raw);
    if ids.is_empty() {
        None
    } else {
        Some(ids)
    }
}

fn replay_raw_feed_path_from_env() -> Option<String> {
    std::env::var(REPLAY_RAW_FEED_PATH_ENV)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn parse_csv_symbols(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    for token in raw.split(',') {
        let symbol = token.trim();
        if symbol.is_empty() {
            continue;
        }
        if out.iter().any(|s| s == symbol) {
            continue;
        }
        out.push(symbol.to_string());
    }
    out
}

fn replay_symbols_from_env_or_config(config_manager: &ConfigManager) -> Vec<String> {
    if let Some(env_symbols) = std::env::var(REPLAY_STRATEGY_SYMBOLS_ENV)
        .ok()
        .map(|raw| parse_csv_symbols(&raw))
        .filter(|symbols| !symbols.is_empty())
    {
        return env_symbols;
    }
    if let Some(cfg) = config_manager.lead_lag_config() {
        if !cfg.symbols.is_empty() {
            return cfg.symbols.clone();
        }
    }
    vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
}

fn parse_replay_exchange(raw: &str) -> Option<RawFeedExchange> {
    if raw.eq_ignore_ascii_case("binance") {
        Some(RawFeedExchange::Binance)
    } else if raw.eq_ignore_ascii_case("gate") || raw.eq_ignore_ascii_case("gateio") {
        Some(RawFeedExchange::Gate)
    } else {
        None
    }
}

fn replay_primary_exchange_from_env_or_config(config_manager: &ConfigManager) -> RawFeedExchange {
    if let Some(exchange) = std::env::var(REPLAY_PRIMARY_EXCHANGE_ENV)
        .ok()
        .and_then(|raw| parse_replay_exchange(raw.trim()))
    {
        return exchange;
    }
    if let Some(cfg) = config_manager.lead_lag_config() {
        return match cfg.primary_exchange {
            hft_lead_lag::config::ExchangeId::Binance => RawFeedExchange::Binance,
            hft_lead_lag::config::ExchangeId::Gate => RawFeedExchange::Gate,
        };
    }
    RawFeedExchange::Binance
}

fn run_replay_mode(path: &str, config_manager: &ConfigManager) -> Result<(), std::io::Error> {
    let symbols = replay_symbols_from_env_or_config(config_manager);
    let primary = replay_primary_exchange_from_env_or_config(config_manager);
    let hedge = match primary {
        RawFeedExchange::Binance => RawFeedExchange::Gate,
        RawFeedExchange::Gate => RawFeedExchange::Binance,
    };
    let routing = RawFeedReplayRouting { primary, hedge };
    info!(
        "Replay mode: path={} symbols={} primary={:?}",
        path,
        symbols.len(),
        primary
    );

    let report = verify_signal_replay_determinism_from_file(path, &symbols, routing)?;
    info!(
        "Replay determinism: deterministic={} parsed_tickers={} signals={} mismatch_index={:?}",
        report.deterministic,
        report.parsed_ticker_count,
        report.signal_count,
        report.mismatch_index
    );
    if !report.deterministic {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "replay determinism check failed (mismatch_index={:?})",
                report.mismatch_index
            ),
        ));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_centralized_logging("logs", "runtime.log")?;

    // Load .env file if present (before reading env vars)
    dotenvy::dotenv().ok();

    info!("HFT Lead-Lag system starting");

    // Load configuration from environment
    let config_manager = ConfigManager::from_env()?;
    info!(
        "Runtime execution mode: {}",
        config_manager.trading_mode().as_str()
    );
    if let Some(replay_path) = replay_raw_feed_path_from_env() {
        run_replay_mode(&replay_path, &config_manager)?;
        return Ok(());
    }

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
    match raw_feed_recorder_from_env() {
        Ok(Some(recorder)) => {
            info!(
                "Raw feed recorder enabled (env: {}, file path resolved)",
                RAW_FEED_RECORD_PATH_ENV
            );
            binance.set_raw_feed_recorder(Some(recorder.clone()));
            gate.set_raw_feed_recorder(Some(recorder));
        }
        Ok(None) => {}
        Err(e) => {
            warn!("Raw feed recorder init failed; continuing without recording: {e}");
        }
    }
    let health_state = Arc::new(HealthState::new());
    configure_and_connect_exchanges(
        &config_manager,
        &mut binance,
        &mut gate,
        health_state.as_ref(),
    )
    .await?;
    binance.set_strategy_symbol_ids(&strategy_symbols)?;
    gate.set_strategy_symbol_ids(&strategy_symbols)?;

    // Start external APIs early so checkpoint endpoints are always available.
    let mut screener = ScreenerStore::default();
    if let Some(portfolio_ids) = portfolio_ids_from_env() {
        screener.set_portfolio_ids_v1(portfolio_ids.clone());
        info!(
            "Configured portfolio ids from {PORTFOLIO_IDS_ENV}: {:?}",
            portfolio_ids
        );
    }
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
    let ws_tx = start_api_servers(
        MIN_VOLUME_USD,
        screener.clone(),
        health_state.clone(),
        ENABLE_SCREENER_CHART_PIPELINE,
    )
    .await?;

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
    let mut strategy = match build_runtime_strategy(&config_manager, strategy_symbols.clone()) {
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
        strategy.as_mut(),
        &strategy_symbols,
        EventLoopRuntimeContext {
            strategy_exchange_routing,
            screener: &screener,
            health_state: health_state.as_ref(),
            ws_tx: ws_tx.as_ref(),
        },
    )
    .await
}

#[cfg(test)]
mod main_tests;
