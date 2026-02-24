//! HFT Lead-Lag Trading System - Main Entry Point
//!
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::api::{
    HealthState, HttpServer, HttpServerConfig, MarketDataEvent, MarketDataServer, ScreenerStore,
    WsServerConfig,
};
use hft_lead_lag::infrastructure::logging::init_centralized_logging;
use hft_lead_lag::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};
use hft_lead_lag::{
    build_runtime_strategy, BinanceMarketData, ConfigManager, GateMarketData, MarketDataStream,
    RuntimeStrategy,
};
use hft_lead_lag::domain::screener::TraderConfig;
use serde::Deserialize;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 2_500_000.0; // 2.5 million USD
const SUBSCRIBE_DELAY_MS: u64 = 15;
const GATE_NATR_PERIOD_30M: usize = 30;
const GATE_NATR_REFRESH_INTERVAL_SECS: u64 = 60;
const GATE_NATR_BATCH_SIZE: usize = 12;
const GATE_NATR_REQUEST_TIMEOUT_MS: u64 = 500;
const RUNTIME_GRID_CONFIG_PATH: &str = "config/runtime-grid.toml";
/// Symbols excluded from strategy — consistently unprofitable or structurally unsuitable.
const STRATEGY_BLACKLIST: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "DYDXUSDT"];

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

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct GridAxisF64 {
    min: f64,
    max: f64,
    step: f64,
}

impl Default for GridAxisF64 {
    fn default() -> Self {
        Self {
            min: 0.0,
            max: 0.0,
            step: 1.0,
        }
    }
}

impl GridAxisF64 {
    fn values(&self, name: &str) -> Result<Vec<f64>, String> {
        if !self.min.is_finite() || !self.max.is_finite() || !self.step.is_finite() {
            return Err(format!("{name}: min/max/step must be finite"));
        }
        if self.step <= 0.0 {
            return Err(format!("{name}: step must be > 0"));
        }
        if self.max < self.min {
            return Err(format!("{name}: max must be >= min"));
        }

        let mut values = Vec::new();
        let mut current = self.min;
        let mut guard = 0usize;
        while current <= self.max + self.step * 1e-9 {
            values.push((current * 1_000_000.0).round() / 1_000_000.0);
            current += self.step;
            guard += 1;
            if guard > 10_000 {
                return Err(format!("{name}: generated too many points (>10000)"));
            }
        }
        if values.is_empty() {
            values.push(self.min);
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct GridAxisI64 {
    min: i64,
    max: i64,
    step: i64,
}

impl Default for GridAxisI64 {
    fn default() -> Self {
        Self {
            min: 0,
            max: 0,
            step: 1,
        }
    }
}

impl GridAxisI64 {
    fn values(&self, name: &str) -> Result<Vec<i64>, String> {
        if self.step <= 0 {
            return Err(format!("{name}: step must be > 0"));
        }
        if self.max < self.min {
            return Err(format!("{name}: max must be >= min"));
        }

        let mut values = Vec::new();
        let mut current = self.min;
        let mut guard = 0usize;
        while current <= self.max {
            values.push(current);
            current = current.saturating_add(self.step);
            guard += 1;
            if guard > 10_000 {
                return Err(format!("{name}: generated too many points (>10000)"));
            }
        }
        if values.is_empty() {
            values.push(self.min);
        }
        Ok(values)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
struct RuntimeGridConfig {
    enabled: bool,
    watch_interval_ms: u64,
    apply_interval_ms: u64,
    max_configs: usize,
    gap_threshold_bps: GridAxisF64,
    target_ratio: GridAxisF64,
    stop_loss_bps: GridAxisF64,
    max_hold_ms: GridAxisI64,
    max_spread_bps: GridAxisF64,
    trailing_decay_ratio: GridAxisF64,
    baseline_window_ms: GridAxisI64,
}

impl Default for RuntimeGridConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            watch_interval_ms: 5_000,
            apply_interval_ms: 5 * 60 * 1_000,
            max_configs: 1_500,
            gap_threshold_bps: GridAxisF64 {
                min: 30.0,
                max: 80.0,
                step: 10.0,
            },
            target_ratio: GridAxisF64 {
                min: 0.3,
                max: 0.7,
                step: 0.1,
            },
            stop_loss_bps: GridAxisF64 {
                min: 8.0,
                max: 40.0,
                step: 4.0,
            },
            max_hold_ms: GridAxisI64 {
                min: 5_000,
                max: 30_000,
                step: 5_000,
            },
            max_spread_bps: GridAxisF64 {
                min: 3.0,
                max: 5.0,
                step: 1.0,
            },
            trailing_decay_ratio: GridAxisF64 {
                min: 0.3,
                max: 0.7,
                step: 0.1,
            },
            baseline_window_ms: GridAxisI64 {
                min: 10_000,
                max: 60_000,
                step: 10_000,
            },
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimeGridGeneration {
    config: RuntimeGridConfig,
    configs: Vec<TraderConfig>,
    signature: u64,
    modified: SystemTime,
}

// ---------------------------------------------------------------------------
// Trial batch — programmatic config injection (Ray driver)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
struct TrialBatch {
    run_id: String,
    configs: Vec<TraderConfig>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
struct TrialControl {
    clear_run_id: bool,
    run_id: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
struct TrialAck {
    run_id: String,
    applied_at_ms: i64,
    config_count: usize,
    drained_trades: usize,
}

fn load_trial_batch(path: &Path) -> Result<TrialBatch, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read trial batch {}: {e}", path.display()))?;
    let batch: TrialBatch = serde_json::from_str(&content)
        .map_err(|e| format!("parse trial batch {}: {e}", path.display()))?;
    if batch.configs.is_empty() {
        return Err("trial batch has no configs".to_string());
    }
    if batch.run_id.is_empty() {
        return Err("trial batch run_id is empty".to_string());
    }
    Ok(batch)
}

fn load_trial_control(path: &Path) -> Result<TrialControl, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read trial control {}: {e}", path.display()))?;
    serde_json::from_str::<TrialControl>(&content)
        .map_err(|e| format!("parse trial control {}: {e}", path.display()))
}

fn write_trial_ack(dir: &Path, ack: &TrialAck) {
    let path = dir.join(".trial-ack");
    match serde_json::to_string_pretty(ack) {
        Ok(json) => {
            if let Err(e) = std::fs::write(&path, json) {
                warn!("trial-ack: failed to write {}: {e}", path.display());
            }
        }
        Err(e) => warn!("trial-ack: serialize error: {e}"),
    }
}

const DEFAULT_RUNTIME_GRID_CONFIG_TOML: &str = r#"# Runtime grid hot-reload (deal-hunt phase A)
enabled = true
watch_interval_ms = 5000
apply_interval_ms = 300000
max_configs = 1500

[gap_threshold_bps]
min = 30.0
max = 80.0
step = 10.0

[target_ratio]
min = 0.3
max = 0.7
step = 0.1

[stop_loss_bps]
min = 8.0
max = 40.0
step = 4.0

[max_hold_ms]
min = 5000
max = 30000
step = 5000

[max_spread_bps]
min = 3.0
max = 5.0
step = 1.0

[trailing_decay_ratio]
min = 0.3
max = 0.7
step = 0.1

[baseline_window_ms]
min = 10000
max = 60000
step = 10000
"#;

fn ensure_runtime_grid_config_file(
    path: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if path.exists() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, DEFAULT_RUNTIME_GRID_CONFIG_TOML)?;
    info!("Created default runtime grid config: {}", path.display());
    Ok(())
}

fn runtime_grid_signature(configs: &[TraderConfig]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    configs.len().hash(&mut hasher);
    for cfg in configs {
        cfg.config_id().hash(&mut hasher);
    }
    hasher.finish()
}

fn downsample_configs(configs: Vec<TraderConfig>, limit: usize) -> Vec<TraderConfig> {
    if limit == 0 || configs.is_empty() || configs.len() <= limit {
        return configs;
    }
    let stride = configs.len() as f64 / limit as f64;
    let mut selected = Vec::with_capacity(limit);
    let mut seen = HashSet::with_capacity(limit * 2);
    for i in 0..limit {
        let idx = ((i as f64) * stride).floor() as usize;
        let cfg = configs[idx.min(configs.len() - 1)];
        if seen.insert(cfg.config_id()) {
            selected.push(cfg);
        }
    }
    if selected.len() < limit {
        for cfg in configs {
            if seen.insert(cfg.config_id()) {
                selected.push(cfg);
                if selected.len() == limit {
                    break;
                }
            }
        }
    }
    selected
}

fn build_runtime_grid(cfg: &RuntimeGridConfig) -> Result<Vec<TraderConfig>, String> {
    if cfg.max_configs == 0 {
        return Err("max_configs must be > 0".to_string());
    }
    let gaps = cfg.gap_threshold_bps.values("gap_threshold_bps")?;
    let targets = cfg.target_ratio.values("target_ratio")?;
    let stops = cfg.stop_loss_bps.values("stop_loss_bps")?;
    let holds = cfg.max_hold_ms.values("max_hold_ms")?;
    let spreads = cfg.max_spread_bps.values("max_spread_bps")?;
    let trails = cfg.trailing_decay_ratio.values("trailing_decay_ratio")?;
    let baselines = cfg.baseline_window_ms.values("baseline_window_ms")?;

    let total = gaps.len()
        * targets.len()
        * stops.len()
        * holds.len()
        * spreads.len()
        * trails.len()
        * baselines.len();
    if total == 0 {
        return Err("runtime grid produced zero combinations".to_string());
    }

    let mut configs = Vec::with_capacity(total.min(cfg.max_configs));
    let base = TraderConfig::default();
    for &gap in &gaps {
        for &target in &targets {
            for &stop in &stops {
                for &hold in &holds {
                    for &spread in &spreads {
                        for &trail in &trails {
                            for &baseline in &baselines {
                                configs.push(TraderConfig {
                                    spike_threshold_bps: gap,
                                    target_ratio: target,
                                    stop_loss_bps: stop,
                                    max_hold_ms: hold,
                                    max_spread_bps: spread,
                                    trailing_decay_ratio: trail,
                                    baseline_window_ms: baseline,
                                    ..base
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    let mut deduped = Vec::with_capacity(configs.len());
    let mut ids = HashSet::with_capacity(configs.len());
    for cfg in configs {
        if ids.insert(cfg.config_id()) {
            deduped.push(cfg);
        }
    }
    Ok(downsample_configs(deduped, cfg.max_configs))
}

fn load_runtime_grid_generation(path: &Path) -> Result<RuntimeGridGeneration, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("read runtime grid {}: {e}", path.display()))?;
    let config: RuntimeGridConfig = toml::from_str(&content)
        .map_err(|e| format!("parse runtime grid {}: {e}", path.display()))?;
    let modified = std::fs::metadata(path)
        .and_then(|m| m.modified())
        .unwrap_or(SystemTime::UNIX_EPOCH);
    let configs = if config.enabled {
        build_runtime_grid(&config)?
    } else {
        Vec::new()
    };
    let signature = runtime_grid_signature(&configs);
    Ok(RuntimeGridGeneration {
        config,
        configs,
        signature,
        modified,
    })
}

fn upsert_runtime_configs(db_path: &Path, configs: &[TraderConfig]) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::upsert_configs(&conn, configs)
        .map_err(|e| format!("upsert runtime configs: {e}"))?;
    Ok(())
}

fn upsert_trial_run_meta(
    db_path: &Path,
    run_id: &str,
    submitted_config_count: usize,
    applied_at_ms: i64,
    drained_trades: usize,
) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::upsert_trial_run_meta(
        &conn,
        run_id,
        submitted_config_count,
        applied_at_ms,
        drained_trades,
    )
    .map_err(|e| format!("upsert trial run meta: {e}"))?;
    Ok(())
}

fn close_trial_run_meta(db_path: &Path, run_id: &str, closed_at_ms: i64) -> Result<(), String> {
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
        .map_err(|e| format!("open db {}: {e}", db_path.display()))?;
    hft_lead_lag::infrastructure::db::close_trial_run_meta(&conn, run_id, closed_at_ms)
        .map_err(|e| format!("close trial run meta: {e}"))?;
    Ok(())
}

fn spawn_runtime_grid_hot_reload(
    screener: ScreenerStore,
    db_path: PathBuf,
    config_path: PathBuf,
    trial_batch_path: PathBuf,
    trial_control_path: PathBuf,
    initial_modified: Option<SystemTime>,
    initial_signature: Option<u64>,
) {
    tokio::spawn(async move {
        let mut last_modified = initial_modified;
        let mut last_applied_signature = initial_signature;
        let mut pending: Option<RuntimeGridGeneration> = None;
        let mut last_apply_ms = EventLoopState::now_ms();
        let mut last_trial_modified: Option<SystemTime> = None;
        let mut last_trial_control_modified: Option<SystemTime> = None;

        loop {
            // --- Trial control: clear current run_id ---
            let trial_control_modified = std::fs::metadata(&trial_control_path)
                .and_then(|m| m.modified())
                .ok();
            if let Some(mod_time) = trial_control_modified {
                let control_changed =
                    last_trial_control_modified.map_or(true, |prev| mod_time > prev);
                if control_changed {
                    last_trial_control_modified = Some(mod_time);
                    match load_trial_control(&trial_control_path) {
                        Ok(control) => {
                            if control.clear_run_id {
                                let active_run_id = screener.current_run_id();
                                match active_run_id {
                                    Some(active) => {
                                        let request_matches = control
                                            .run_id
                                            .as_ref()
                                            .map_or(true, |requested| requested == &active);
                                        if request_matches {
                                            let closed_at_ms = EventLoopState::now_ms();
                                            if let Err(e) =
                                                close_trial_run_meta(&db_path, &active, closed_at_ms)
                                            {
                                                warn!("trial-control: failed to close run {active}: {e}");
                                            }
                                            screener.set_run_id(None);
                                            info!(
                                                "trial-control: cleared run_id={active} closed_at_ms={closed_at_ms}"
                                            );
                                        } else if let Some(requested) = control.run_id {
                                            warn!(
                                                "trial-control: requested run_id={} does not match active run_id={}",
                                                requested, active
                                            );
                                        }
                                    }
                                    None => info!("trial-control: no active run_id to clear"),
                                }
                            }
                        }
                        Err(e) => warn!("trial-control: {e}"),
                    }
                }
            }
            // --- End trial control ---

            // --- Trial batch: immediate apply, no debounce ---
            let trial_modified = std::fs::metadata(&trial_batch_path)
                .and_then(|m| m.modified())
                .ok();
            if let Some(mod_time) = trial_modified {
                let trial_changed = last_trial_modified.map_or(true, |prev| mod_time > prev);
                if trial_changed {
                    last_trial_modified = Some(mod_time);
                    match load_trial_batch(&trial_batch_path) {
                        Ok(batch) => {
                            let run_id = batch.run_id.clone();
                            let config_count = batch.configs.len();
                            if let Err(e) = upsert_runtime_configs(&db_path, &batch.configs) {
                                warn!("trial-batch: db upsert failed: {e}");
                            } else {
                                let applied_at_ms = EventLoopState::now_ms();
                                if let Some(previous_run_id) = screener.current_run_id() {
                                    if previous_run_id != run_id {
                                        if let Err(e) = close_trial_run_meta(
                                            &db_path,
                                            &previous_run_id,
                                            applied_at_ms,
                                        ) {
                                            warn!(
                                                "trial-batch: failed to close previous run_id={previous_run_id}: {e}"
                                            );
                                        }
                                    }
                                }
                                screener.set_run_id(Some(run_id.clone()));
                                let report = screener.replace_fleet_configs(batch.configs);
                                screener.flush_db_writer().await;
                                if let Err(e) = upsert_trial_run_meta(
                                    &db_path,
                                    &run_id,
                                    config_count,
                                    applied_at_ms,
                                    report.drained_trades,
                                ) {
                                    warn!("trial-batch: meta upsert failed: {e}");
                                }
                                info!(
                                    "trial-batch: applied run_id={run_id} configs={config_count} \
                                     drained_trades={}",
                                    report.drained_trades
                                );
                                write_trial_ack(
                                    trial_batch_path.parent().unwrap_or(Path::new(".")),
                                    &TrialAck {
                                        run_id,
                                        applied_at_ms,
                                        config_count,
                                        drained_trades: report.drained_trades,
                                    },
                                );
                                pending = None;
                            }
                        }
                        Err(e) => warn!("trial-batch: {e}"),
                    }
                }
            }
            // --- End trial batch ---

            let modified = std::fs::metadata(&config_path)
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH);
            let changed = match last_modified {
                Some(prev) => modified > prev,
                None => true,
            };
            if changed {
                last_modified = Some(modified);
                match load_runtime_grid_generation(&config_path) {
                    Ok(generation) => {
                        if generation.config.enabled {
                            info!(
                                "runtime-grid: detected update, pending apply configs={} max_configs={} apply_interval_ms={}",
                                generation.configs.len(),
                                generation.config.max_configs,
                                generation.config.apply_interval_ms
                            );
                            pending = Some(generation);
                        } else {
                            info!("runtime-grid: disabled in {}", config_path.display());
                            pending = None;
                        }
                    }
                    Err(e) => {
                        warn!("runtime-grid: invalid update ignored: {e}");
                    }
                }
            }

            if let Some(generation) = pending.as_ref() {
                let now_ms = EventLoopState::now_ms();
                let apply_interval_ms = generation.config.apply_interval_ms.max(1_000) as i64;
                if now_ms.saturating_sub(last_apply_ms) >= apply_interval_ms {
                    if Some(generation.signature) == last_applied_signature {
                        pending = None;
                    } else if let Err(e) = upsert_runtime_configs(&db_path, &generation.configs) {
                        warn!("runtime-grid: apply postponed, db upsert failed: {e}");
                    } else {
                        let report = screener.replace_fleet_configs(generation.configs.clone());
                        screener.flush_db_writer().await;
                        last_apply_ms = now_ms;
                        last_applied_signature = Some(generation.signature);
                        info!(
                            "runtime-grid: applied configs old={} new={} symbols_reset={} drained_trades={} (flushed)",
                            report.old_config_count,
                            report.new_config_count,
                            report.symbols_reset,
                            report.drained_trades
                        );
                        pending = None;
                    }
                }
            }

            let sleep_ms = pending
                .as_ref()
                .map(|g| g.config.watch_interval_ms)
                .unwrap_or(5_000)
                .max(500);
            tokio::time::sleep(tokio::time::Duration::from_millis(sleep_ms)).await;
        }
    });
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

fn strategy_ticks_in_order<'a>(
    strategy_symbols: &'a [&'a str],
    latest: &'a std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
) -> impl Iterator<Item = &'a hft_lead_lag::domain::BookTicker> + 'a {
    strategy_symbols
        .iter()
        .filter_map(|symbol| latest.get(*symbol))
}

fn updated_symbols_from_batch(
    first: &hft_lead_lag::domain::BookTicker,
    drained: &[hft_lead_lag::domain::BookTicker],
) -> Vec<String> {
    let mut symbols = Vec::with_capacity(drained.len() + 1);
    symbols.push(String::from_utf8_lossy(&first.symbol).to_string());
    for ticker in drained {
        symbols.push(String::from_utf8_lossy(&ticker.symbol).to_string());
    }
    symbols.sort_unstable();
    symbols.dedup();
    symbols
}

fn ingest_latest_batch<F: Fn() -> i64>(
    latest: &std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    for (symbol, ticker) in latest {
        *ctx.ticker_count += 1;
        ctx.metrics
            .record_tick_drift((ctx.now_ms)(), ticker.exchange_ts_ns);
        let bid = ticker.bid_price();
        let ask = ticker.ask_price();
        ctx.screener.update(
            symbol,
            ctx.exchange,
            bid,
            ask,
            ticker.exchange_ts_ns,
            ticker.local_ts_ns,
        );
        let _ = ctx.ws_tx.send(MarketDataEvent {
            symbol: symbol.clone(),
            exchange: ctx.exchange,
            bid,
            ask,
            timestamp_ns: ticker.exchange_ts_ns,
        });
    }
}

struct BatchIngestContext<'a, F: Fn() -> i64> {
    exchange: &'static str,
    ticker_count: &'a mut usize,
    metrics: &'a mut EventLoopMetrics,
    now_ms: &'a F,
    screener: &'a ScreenerStore,
    ws_tx: &'a tokio::sync::broadcast::Sender<MarketDataEvent>,
}

fn process_exchange_batch<F: Fn() -> i64>(
    latest: &mut std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    ctx: &mut BatchIngestContext<'_, F>,
) {
    let updated_batch = rebuild_latest_map(latest, first, drained);
    ingest_latest_batch(&updated_batch, ctx);
}

#[derive(Debug)]
struct EventLoopMetrics {
    drift_samples: Vec<i64>,
    last_status_ticker_count: usize,
}

impl EventLoopMetrics {
    fn new() -> Self {
        Self {
            drift_samples: Vec::with_capacity(8192),
            last_status_ticker_count: 0,
        }
    }

    fn record_tick_drift(&mut self, local_ms: i64, exchange_ts_ns: i64) {
        let exch_ms = exchange_ts_ns / 1_000_000;
        if exch_ms > 0 {
            self.drift_samples.push(local_ms - exch_ms);
        }
    }

    fn drift_stats_string_and_reset(&mut self) -> String {
        if self.drift_samples.is_empty() {
            return "no_data".to_string();
        }

        self.drift_samples.sort_unstable();
        let n = self.drift_samples.len();
        let p50 = self.drift_samples[n / 2];
        let p95 = self.drift_samples[n * 95 / 100];
        let p99 = self.drift_samples[n * 99 / 100];
        let max = self.drift_samples[n - 1];
        let avg = self.drift_samples.iter().sum::<i64>() / n as i64;
        self.drift_samples.clear();
        format!(
            "n={} avg={}ms p50={}ms p95={}ms p99={}ms max={}ms",
            n, avg, p50, p95, p99, max
        )
    }

    fn snapshot_and_roll_status(&mut self, ticker_count: usize) -> usize {
        let interval_tickers = ticker_count.saturating_sub(self.last_status_ticker_count);
        self.last_status_ticker_count = ticker_count;
        interval_tickers
    }
}

struct EventLoopState {
    ticker_count: usize,
    signal_count: usize,
    last_status_at: Instant,
    signal_interval: tokio::time::Interval,
    latest_bn: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    latest_gt: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    metrics: EventLoopMetrics,
}

#[derive(Clone, Copy)]
enum ExchangeSide {
    Binance,
    Gate,
}

impl ExchangeSide {
    fn exchange_name(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Gate => "gate",
        }
    }

    fn log_data_error(self, error: &hft_lead_lag::domain::ExchangeError) {
        match self {
            Self::Binance => error!("Binance data error: {}", error),
            Self::Gate => warn!("Gate data error: {}", error),
        }
    }

    fn mark_alive(self, health: &HealthState, now_ms: i64) {
        match self {
            Self::Binance => {
                health.binance_connected.store(true, Ordering::Relaxed);
                health.binance_last_tick_ms.store(now_ms, Ordering::Relaxed);
            }
            Self::Gate => {
                health.gate_connected.store(true, Ordering::Relaxed);
                health.gate_last_tick_ms.store(now_ms, Ordering::Relaxed);
            }
        }
    }

    fn maybe_mark_disconnected(
        self,
        health: &HealthState,
        error: &hft_lead_lag::domain::ExchangeError,
    ) {
        let is_connectivity_error = matches!(
            error,
            hft_lead_lag::domain::ExchangeError::WebSocketError(_)
                | hft_lead_lag::domain::ExchangeError::ConnectionClosed(_)
                | hft_lead_lag::domain::ExchangeError::Timeout(_)
        );
        if !is_connectivity_error {
            return;
        }
        match self {
            Self::Binance => {
                health.binance_connected.store(false, Ordering::Relaxed);
            }
            Self::Gate => {
                health.gate_connected.store(false, Ordering::Relaxed);
            }
        }
    }
}

impl EventLoopState {
    fn new() -> Self {
        let mut signal_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
        signal_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        Self {
            ticker_count: 0,
            signal_count: 0,
            last_status_at: Instant::now(),
            signal_interval,
            latest_bn: std::collections::HashMap::new(),
            latest_gt: std::collections::HashMap::new(),
            metrics: EventLoopMetrics::new(),
        }
    }

    fn now_ms() -> i64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    }

    fn process_exchange_result(
        &mut self,
        side: ExchangeSide,
        result: Result<hft_lead_lag::domain::BookTicker, hft_lead_lag::domain::ExchangeError>,
        drained: Vec<hft_lead_lag::domain::BookTicker>,
        screener: &ScreenerStore,
        ws_tx: &tokio::sync::broadcast::Sender<MarketDataEvent>,
    ) -> Result<Vec<String>, hft_lead_lag::domain::ExchangeError> {
        let ticker = result?;
        let updated_symbols = updated_symbols_from_batch(&ticker, &drained);
        let mut ctx = BatchIngestContext {
            exchange: side.exchange_name(),
            ticker_count: &mut self.ticker_count,
            metrics: &mut self.metrics,
            now_ms: &Self::now_ms,
            screener,
            ws_tx,
        };
        match side {
            ExchangeSide::Binance => {
                process_exchange_batch(&mut self.latest_bn, ticker, drained, &mut ctx)
            }
            ExchangeSide::Gate => {
                process_exchange_batch(&mut self.latest_gt, ticker, drained, &mut ctx)
            }
        }
        Ok(updated_symbols)
    }

    async fn update_strategy_books(
        &self,
        side: ExchangeSide,
        strategy: &dyn RuntimeStrategy,
        updated_symbols: &[String],
        strategy_symbol_set: &std::collections::HashSet<&str>,
    ) {
        let symbols_for_side: Vec<&str> = updated_symbols
            .iter()
            .map(String::as_str)
            .filter(|symbol| strategy_symbol_set.contains(*symbol))
            .collect();

        match side {
            ExchangeSide::Binance => {
                for ticker in strategy_ticks_in_order(&symbols_for_side, &self.latest_bn) {
                    strategy.on_primary_book(ticker.clone()).await;
                }
            }
            ExchangeSide::Gate => {
                for ticker in strategy_ticks_in_order(&symbols_for_side, &self.latest_gt) {
                    strategy.on_hedge_book(ticker.clone()).await;
                }
            }
        }
    }

    async fn handle_signal_tick(
        &mut self,
        strategy: &dyn RuntimeStrategy,
        strategy_symbols: &[String],
    ) {
        for symbol in strategy_symbols {
            if let Some(signal) = strategy.check_signal(symbol).await {
                self.signal_count += 1;
                info!(
                    "{} signal #{}: {} | spread={:.2}bps | {}",
                    signal.strategy,
                    self.signal_count,
                    signal.symbol,
                    signal.spread_bps,
                    signal.context
                );
            }
        }
        self.maybe_log_status();
    }

    fn maybe_log_status(&mut self) {
        if self.last_status_at.elapsed() >= Duration::from_secs(5) {
            let interval_tickers = self.metrics.snapshot_and_roll_status(self.ticker_count);
            let drift_stats = self.metrics.drift_stats_string_and_reset();
            info!(
                "Status: tickers={} (+{}/5s) signals={} drift=[{}]",
                self.ticker_count, interval_tickers, self.signal_count, drift_stats
            );
            self.last_status_at = Instant::now();
        }
    }
}

#[derive(Clone, Copy)]
enum GateSubscribeAttempt {
    Success,
    Error,
    Timeout,
}

fn should_delay_after_gate_subscribe_attempt(attempt: GateSubscribeAttempt) -> bool {
    match attempt {
        GateSubscribeAttempt::Success
        | GateSubscribeAttempt::Error
        | GateSubscribeAttempt::Timeout => true,
    }
}

async fn subscribe_gate_symbols(gate: &mut GateMarketData, symbols: &[String]) {
    let mut ok = 0usize;
    let mut errs = 0usize;
    let mut timeouts = 0usize;
    for symbol in symbols {
        let attempt = match tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            gate.subscribe_book_ticker(symbol),
        )
        .await
        {
            Ok(Ok(_)) => {
                ok += 1;
                GateSubscribeAttempt::Success
            }
            Ok(Err(e)) => {
                errs += 1;
                error!("Gate subscribe error {}: {}", symbol, e);
                GateSubscribeAttempt::Error
            }
            Err(_) => {
                timeouts += 1;
                warn!(
                    "Gate subscription timeout on {}; proceeding with available streams",
                    symbol
                );
                GateSubscribeAttempt::Timeout
            }
        };
        if should_delay_after_gate_subscribe_attempt(attempt) {
            tokio::time::sleep(tokio::time::Duration::from_millis(SUBSCRIBE_DELAY_MS)).await;
        }
    }
    info!(
        "Gate subscription summary: ok={} err={} timeout={}",
        ok, errs, timeouts
    );
}

async fn drain_stale_ticks(binance: &mut BinanceMarketData, gate: &mut GateMarketData) {
    let stale_binance = binance.drain_book_tickers().len();
    let stale_gate = gate.drain_book_tickers().len();
    if stale_binance + stale_gate > 0 {
        info!(
            "Drained {} stale startup ticks (binance={} gate={})",
            stale_binance + stale_gate,
            stale_binance,
            stale_gate
        );
    }
}

async fn fetch_volume_tickers(min_volume_usd: f64) -> (Vec<Ticker24h>, Vec<Ticker24h>) {
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

async fn refresh_gate_natr_batch(
    screener: &ScreenerStore,
    symbols: &[String],
    start_idx: usize,
) -> usize {
    if symbols.is_empty() {
        return 0;
    }

    let batch_size = GATE_NATR_BATCH_SIZE.min(symbols.len());
    let rest = GateRestClient::new();
    let mut updates: Vec<(String, f64)> = Vec::with_capacity(batch_size);
    let mut fetched = 0usize;
    let mut missing = 0usize;

    for offset in 0..batch_size {
        let idx = (start_idx + offset) % symbols.len();
        let symbol = &symbols[idx];
        let natr = match tokio::time::timeout(
            tokio::time::Duration::from_millis(GATE_NATR_REQUEST_TIMEOUT_MS),
            rest.get_natr_30m(symbol, GATE_NATR_PERIOD_30M),
        )
        .await
        {
            Ok(Ok(Some(v))) if v.is_finite() && v >= 0.0 => Some(v),
            _ => None,
        };
        if let Some(v) = natr {
            updates.push((symbol.clone(), v));
            fetched += 1;
        } else {
            updates.push((symbol.clone(), 0.0));
            missing += 1;
        }
    }

    screener.set_gate_natr_30m(&updates);
    info!(
        "Gate NATR refresh: fetched={} missing={} batch={} symbols={}",
        fetched,
        missing,
        batch_size,
        symbols.len()
    );

    (start_idx + batch_size) % symbols.len()
}

fn spawn_gate_natr_refresher(screener: ScreenerStore, symbols: Vec<String>) {
    if symbols.is_empty() {
        warn!("Gate NATR refresher skipped: no symbols");
        return;
    }

    tokio::spawn(async move {
        let mut idx = 0usize;
        loop {
            idx = refresh_gate_natr_batch(&screener, &symbols, idx).await;
            tokio::time::sleep(tokio::time::Duration::from_secs(
                GATE_NATR_REFRESH_INTERVAL_SECS,
            ))
            .await;
        }
    });
}

fn build_runtime_universe(
    config_manager: &ConfigManager,
    min_volume_usd: f64,
    binance_tickers: Vec<Ticker24h>,
    gate_tickers: Vec<Ticker24h>,
) -> RuntimeUniverse {
    let binance_symbols: Vec<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: Vec<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_vol_map: std::collections::HashMap<String, f64> = gate_tickers
        .iter()
        .map(|t| (t.symbol.clone(), t.quote_volume))
        .collect();
    let (binance_symbols, gate_symbols, reconcile_outcome) =
        reconcile_volume_symbols(binance_symbols, gate_symbols);

    match reconcile_outcome {
        SymbolReconcileOutcome::BinanceMissing => {
            warn!("Binance volume fetch failed — cannot safely copy Gate symbols (different listing). Using BTC/ETH fallback for both.");
        }
        SymbolReconcileOutcome::GateMissing => {
            warn!("Gate volume fetch failed — cannot safely copy Binance symbols (different listing). Using BTC/ETH fallback for both.");
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

    let blacklist: std::collections::HashSet<&str> = config_manager
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

    let (strategy_symbols, screener_symbols, used_fallback) =
        select_runtime_symbols(&common_symbols);
    if used_fallback {
        warn!("No common symbols found! Using fallback...");
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

async fn configure_and_connect_exchanges(
    config_manager: &ConfigManager,
    binance: &mut BinanceMarketData,
    gate: &mut GateMarketData,
    health_state: &HealthState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(creds) = config_manager.binance_credentials() {
        binance.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Binance credentials configured");
    }
    if let Some(creds) = config_manager.gate_credentials() {
        gate.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Gate credentials configured");
    }

    info!("Connecting to Binance Futures...");
    if let Err(e) = binance.connect().await {
        error!("Failed to connect to Binance: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state
        .binance_connected
        .store(true, Ordering::Relaxed);
    health_state
        .binance_last_tick_ms
        .store(EventLoopState::now_ms(), Ordering::Relaxed);

    info!("Connecting to Gate.io Futures...");
    if let Err(e) = gate.connect().await {
        error!("Failed to connect to Gate: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state.gate_connected.store(true, Ordering::Relaxed);
    health_state
        .gate_last_tick_ms
        .store(EventLoopState::now_ms(), Ordering::Relaxed);
    Ok(())
}

fn init_screener_persistence(
    screener: &mut ScreenerStore,
    db_path: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(parent) = db_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let conn = hft_lead_lag::infrastructure::db::open_db(db_path)?;
    let fleet_configs = screener.fleet_configs();
    hft_lead_lag::infrastructure::db::upsert_configs(&conn, fleet_configs.as_ref())?;
    info!(
        "Seeded {} fleet configs into {}",
        fleet_configs.len(),
        db_path.display()
    );
    let db_writer = hft_lead_lag::infrastructure::db::spawn_writer(db_path);
    screener.set_db_writer(db_writer);
    Ok(())
}

async fn start_api_servers(
    min_volume_usd: f64,
    screener: ScreenerStore,
    health_state: Arc<HealthState>,
) -> Result<tokio::sync::broadcast::Sender<MarketDataEvent>, Box<dyn std::error::Error + Send + Sync>>
{
    let http_server = HttpServer::with_runtime(
        HttpServerConfig::default(),
        min_volume_usd,
        screener,
        health_state,
    );
    let http_listener = tokio::net::TcpListener::bind(http_server.bind_address()).await?;
    info!("HTTP server bound on {}", http_server.bind_address());

    let ws_server = MarketDataServer::new(WsServerConfig::default());
    let ws_tx = ws_server.transmitter();
    let ws_listener = tokio::net::TcpListener::bind(ws_server.bind_address()).await?;
    info!("WS server bound on {}", ws_server.bind_address());

    tokio::spawn(async move {
        if let Err(e) = http_server.serve(http_listener).await {
            error!("HTTP server failed: {}", e);
        }
    });
    tokio::spawn(async move {
        if let Err(e) = ws_server.serve(ws_listener).await {
            error!("WS server failed: {}", e);
        }
    });

    Ok(ws_tx)
}

async fn run_event_loop(
    binance: &mut BinanceMarketData,
    gate: &mut GateMarketData,
    strategy: &dyn RuntimeStrategy,
    strategy_symbols: &[String],
    screener: &ScreenerStore,
    health_state: &HealthState,
    ws_tx: &tokio::sync::broadcast::Sender<MarketDataEvent>,
) -> ! {
    let mut state = EventLoopState::new();
    let strategy_symbol_set: std::collections::HashSet<&str> =
        strategy_symbols.iter().map(String::as_str).collect();

    loop {
        tokio::select! {
            result = binance.recv_book_ticker() => {
                match state.process_exchange_result(
                    ExchangeSide::Binance,
                    result,
                    binance.drain_book_tickers(),
                    screener,
                    ws_tx,
                ) {
                    Ok(updated_symbols) => {
                        ExchangeSide::Binance.mark_alive(health_state, EventLoopState::now_ms());
                        state
                            .update_strategy_books(
                                ExchangeSide::Binance,
                                strategy,
                                &updated_symbols,
                                &strategy_symbol_set,
                            )
                            .await;
                    }
                    Err(e) => {
                        ExchangeSide::Binance.maybe_mark_disconnected(health_state, &e);
                        ExchangeSide::Binance.log_data_error(&e);
                    }
                }
            }

            result = gate.recv_book_ticker() => {
                match state.process_exchange_result(
                    ExchangeSide::Gate,
                    result,
                    gate.drain_book_tickers(),
                    screener,
                    ws_tx,
                ) {
                    Ok(updated_symbols) => {
                        ExchangeSide::Gate.mark_alive(health_state, EventLoopState::now_ms());
                        state
                            .update_strategy_books(
                                ExchangeSide::Gate,
                                strategy,
                                &updated_symbols,
                                &strategy_symbol_set,
                            )
                        .await;
                    }
                    Err(e) => {
                        ExchangeSide::Gate.maybe_mark_disconnected(health_state, &e);
                        ExchangeSide::Gate.log_data_error(&e);
                    }
                }
            }

            _ = state.signal_interval.tick() => {
                state.handle_signal_tick(strategy, strategy_symbols).await;
            }
        }
    }
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
    let mut runtime_grid_last_modified: Option<SystemTime> = None;
    let mut runtime_grid_last_signature: Option<u64> = None;
    match load_runtime_grid_generation(runtime_grid_path) {
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
        runtime_grid_path.to_path_buf(),
        PathBuf::from("config/trial-batch.json"),
        PathBuf::from("config/trial-control.json"),
        runtime_grid_last_modified,
        runtime_grid_last_signature,
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
        &screener,
        health_state.as_ref(),
        &ws_tx,
    )
    .await
}

#[cfg(test)]
mod main_tests;
