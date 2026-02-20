//! HFT Lead-Lag Trading System - Main Entry Point
//! 
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::{
    BinanceMarketData, GateMarketData,
    LeadLagStrategy, LeadLagStrategyConfig,
    ConfigManager, MarketDataStream,
};
use hft_lead_lag::api::{HealthState, HttpServer, HttpServerConfig, MarketDataEvent, MarketDataServer, ScreenerStore, WsServerConfig};
use hft_lead_lag::infrastructure::logging::init_centralized_logging;
use hft_lead_lag::infrastructure::rest::{BinanceRestClient, GateRestClient, Ticker24h};
use tracing::{error, info, warn};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 2_500_000.0;  // 2.5 million USD
const SUBSCRIBE_DELAY_MS: u64 = 15;
/// Symbols excluded from strategy — consistently unprofitable or structurally unsuitable.
const STRATEGY_BLACKLIST: &[&str] = &["BTCUSDT", "ETHUSDT", "SOLUSDT", "DYDXUSDT"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymbolReconcileOutcome {
    Ok,
    BinanceMissing,
    GateMissing,
    BothMissing,
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
) {
    latest.clear();
    let first_symbol = String::from_utf8_lossy(&first.symbol).to_string();
    latest.insert(first_symbol, first);
    for ticker in drained {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        latest.insert(symbol, ticker);
    }
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

fn strategy_ticks_in_order<'a>(
    strategy_symbols: &'a [String],
    latest: &'a std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
) -> impl Iterator<Item = &'a hft_lead_lag::domain::BookTicker> + 'a {
    strategy_symbols.iter().filter_map(|symbol| latest.get(symbol))
}

fn ingest_latest_batch<F: Fn() -> i64>(
    latest: &std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    exchange: &'static str,
    ticker_count: &mut usize,
    metrics: &mut EventLoopMetrics,
    now_ms: &F,
    screener: &ScreenerStore,
    ws_tx: &tokio::sync::broadcast::Sender<MarketDataEvent>,
) {
    for (symbol, ticker) in latest {
        *ticker_count += 1;
        metrics.record_tick_drift(now_ms(), ticker.exchange_ts_ns);
        let bid = ticker.bid_price();
        let ask = ticker.ask_price();
        screener.update(
            symbol,
            exchange,
            bid,
            ask,
            ticker.exchange_ts_ns,
            ticker.local_ts_ns,
        );
        let _ = ws_tx.send(MarketDataEvent {
            symbol: symbol.clone(),
            exchange,
            bid,
            ask,
            timestamp_ns: ticker.exchange_ts_ns,
        });
    }
}

fn process_exchange_batch<F: Fn() -> i64>(
    latest: &mut std::collections::HashMap<String, hft_lead_lag::domain::BookTicker>,
    first: hft_lead_lag::domain::BookTicker,
    drained: Vec<hft_lead_lag::domain::BookTicker>,
    exchange: &'static str,
    ticker_count: &mut usize,
    metrics: &mut EventLoopMetrics,
    now_ms: &F,
    screener: &ScreenerStore,
    ws_tx: &tokio::sync::broadcast::Sender<MarketDataEvent>,
) {
    rebuild_latest_map(latest, first, drained);
    ingest_latest_batch(latest, exchange, ticker_count, metrics, now_ms, screener, ws_tx);
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
}

async fn run_event_loop(
    binance: &mut BinanceMarketData,
    gate: &mut GateMarketData,
    strategy: &LeadLagStrategy,
    strategy_symbols: &[String],
    screener: &ScreenerStore,
    ws_tx: &tokio::sync::broadcast::Sender<MarketDataEvent>,
) -> ! {
    let mut state = EventLoopState::new();

    loop {
        tokio::select! {
            result = binance.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        process_exchange_batch(
                            &mut state.latest_bn,
                            ticker,
                            binance.drain_book_tickers(),
                            "binance",
                            &mut state.ticker_count,
                            &mut state.metrics,
                            &EventLoopState::now_ms,
                            screener,
                            ws_tx,
                        );
                        for ticker in strategy_ticks_in_order(strategy_symbols, &state.latest_bn) {
                            strategy.update_primary_book(ticker.clone()).await;
                        }
                    }
                    Err(e) => {
                        error!("Binance data error: {}", e);
                    }
                }
            }

            result = gate.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        process_exchange_batch(
                            &mut state.latest_gt,
                            ticker,
                            gate.drain_book_tickers(),
                            "gate",
                            &mut state.ticker_count,
                            &mut state.metrics,
                            &EventLoopState::now_ms,
                            screener,
                            ws_tx,
                        );
                        for ticker in strategy_ticks_in_order(strategy_symbols, &state.latest_gt) {
                            strategy.update_hedge_book(ticker.clone()).await;
                        }
                    }
                    Err(e) => {
                        warn!("Gate data error: {}", e);
                    }
                }
            }

            _ = state.signal_interval.tick() => {
                for symbol in strategy_symbols {
                    if let Some(signal) = strategy.check_signal(symbol).await {
                        state.signal_count += 1;
                        info!(
                            "Lead-lag signal #{}: {} | spread={:.2}bps | leader={:?} | lagger={:?}",
                            state.signal_count,
                            signal.symbol,
                            signal.spread_bps,
                            signal.leader,
                            signal.lagger
                        );
                    }
                }

                if state.last_status_at.elapsed() >= Duration::from_secs(5) {
                    let interval_tickers = state.metrics.snapshot_and_roll_status(state.ticker_count);
                    let drift_stats = state.metrics.drift_stats_string_and_reset();
                    info!(
                        "Status: tickers={} (+{}/5s) signals={} drift=[{}]",
                        state.ticker_count, interval_tickers, state.signal_count, drift_stats
                    );
                    state.last_status_at = Instant::now();
                }
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

    // Initialize REST clients for volume filtering
    info!("Fetching 24h volume data for symbol filtering");
    
    let binance_rest = BinanceRestClient::new();
    let gate_rest = GateRestClient::new();

    // Get symbols with sufficient volume from both exchanges
    let (binance_tickers_result, gate_tickers_result) = tokio::join!(
        binance_rest.get_tickers_with_volume(MIN_VOLUME_USD),
        gate_rest.get_tickers_with_volume(MIN_VOLUME_USD)
    );

    let binance_tickers: Vec<Ticker24h> = match binance_tickers_result {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get Binance tickers: {}", e);
            Vec::new()
        }
    };
    let gate_tickers: Vec<Ticker24h> = match gate_tickers_result {
        Ok(t) => t,
        Err(e) => {
            warn!("Failed to get Gate tickers: {}", e);
            Vec::new()
        }
    };
    let binance_symbols: Vec<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let gate_symbols: Vec<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    // Build volume lookup (Gate volume for execution venue)
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

    info!("Binance symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, binance_symbols.len());
    info!("Gate symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, gate_symbols.len());

    // Find common symbols (available on both exchanges with sufficient volume)
    let binance_set: std::collections::HashSet<String> = binance_symbols.iter().cloned().collect();
    let gate_set: std::collections::HashSet<String> = gate_symbols.iter().cloned().collect();
    let blacklist: std::collections::HashSet<&str> = config_manager
        .binance_blacklist()
        .iter()
        .chain(config_manager.gate_blacklist().iter())
        .map(|s| s.as_str())
        .chain(STRATEGY_BLACKLIST.iter().copied())
        .collect();
    let mut common_symbols: Vec<String> = binance_set
        .intersection(&gate_set)
        .filter(|s| !blacklist.contains(s.as_str()))
        .cloned()
        .collect();
    common_symbols.sort_unstable();
    
    if !blacklist.is_empty() {
        info!("Blacklisted symbols: {:?}", blacklist);
    }
    info!("Common symbols: {}", common_symbols.len());

    // Strategy fleet runs on all common symbols.
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

    // Initialize exchange connectors
    let mut binance = BinanceMarketData::new();
    let mut gate = GateMarketData::new();
    let health_state = Arc::new(HealthState::new());

    // Set credentials if available
    if let Some(creds) = config_manager.binance_credentials() {
        binance.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Binance credentials configured");
    }

    if let Some(creds) = config_manager.gate_credentials() {
        gate.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("Gate credentials configured");
    }

    // Connect to exchanges
    info!("Connecting to Binance Futures...");
    if let Err(e) = binance.connect().await {
        error!("Failed to connect to Binance: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state.binance_connected.store(true, Ordering::Relaxed);

    info!("Connecting to Gate.io Futures...");
    if let Err(e) = gate.connect().await {
        error!("Failed to connect to Gate: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }
    health_state.gate_connected.store(true, Ordering::Relaxed);

    // Start external APIs early so checkpoint endpoints are always available.
    let mut screener = ScreenerStore::default();

    // Initialize fleet persistence (SQLite WAL mode, async batch writes).
    let db_path = std::path::Path::new("data/optimizer.db");
    if let Some(parent) = db_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    {
        let conn = hft_lead_lag::infrastructure::db::open_db(db_path)
            .expect("failed to open optimizer db");
        hft_lead_lag::infrastructure::db::upsert_configs(&conn, screener.fleet_configs())
            .expect("failed to seed configs");
        info!("Seeded {} fleet configs into {}", screener.fleet_configs().len(), db_path.display());
    }
    let db_writer = hft_lead_lag::infrastructure::db::spawn_writer(db_path);
    screener.set_db_writer(db_writer);

    // Seed 24h volume from Gate REST data
    let vol_pairs: Vec<(String, f64)> = common_symbols
        .iter()
        .map(|s| (s.clone(), gate_vol_map.get(s).copied().unwrap_or(0.0)))
        .collect();
    screener.set_volumes(&vol_pairs);
    let http_server = HttpServer::with_runtime(
        HttpServerConfig::default(),
        MIN_VOLUME_USD,
        screener.clone(),
        health_state.clone(),
    );

    // Bind listeners before spawning to fail fast on "Address already in use"
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

    // Subscribe to screener symbols for live WS ticks.
    let (binance_subscribed, binance_subscribe_errors) =
        match binance.subscribe_book_tickers_batch(&screener_symbols).await {
            Ok(count) => (count, 0usize),
            Err(e) => {
                error!("Binance batch subscribe error: {}", e);
                (0usize, screener_symbols.len())
            }
        };
    let binance_ws_sockets = (screener_symbols.len() + 1) / 2;
    info!(
        "Binance subscription summary: ok={} err={} sockets={} symbols_per_ws=2",
        binance_subscribed, binance_subscribe_errors, binance_ws_sockets
    );

    // Subscribe Gate to screener symbols as well.
    let mut gate_subscribed = 0usize;
    let mut gate_subscribe_errors = 0usize;
    let mut gate_subscribe_timeouts = 0usize;
    for symbol in &screener_symbols {
        match tokio::time::timeout(
            tokio::time::Duration::from_millis(500),
            gate.subscribe_book_ticker(symbol),
        )
        .await
        {
            Ok(Ok(_)) => gate_subscribed += 1,
            Ok(Err(e)) => {
                gate_subscribe_errors += 1;
                error!("Gate subscribe error {}: {}", symbol, e);
            }
            Err(_) => {
                gate_subscribe_timeouts += 1;
                warn!("Gate subscription timeout on {}; proceeding with available streams", symbol);
                continue;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(SUBSCRIBE_DELAY_MS)).await;
    }
    info!(
        "Gate subscription summary: ok={} err={} timeout={}",
        gate_subscribed, gate_subscribe_errors, gate_subscribe_timeouts
    );

    // Initialize lead-lag strategy
    let config = LeadLagStrategyConfig {
        min_entry_spread_bps: 5.0,
        target_exit_spread_bps: 1.0,
        symbols: strategy_symbols.clone(),
        ..Default::default()
    };

    let strategy = LeadLagStrategy::new(config);

    info!("System initialized; monitoring {} strategy symbols", strategy_symbols.len());

    // Drain messages that accumulated during subscription phase to avoid
    // stale ticks with misleading local_ts_ns at the start of the main loop.
    let stale_binance = binance.drain_book_tickers().len();
    let stale_gate = gate.drain_book_tickers().len();
    if stale_binance + stale_gate > 0 {
        info!(
            "Drained {} stale startup ticks (binance={} gate={})",
            stale_binance + stale_gate, stale_binance, stale_gate
        );
    }

    run_event_loop(
        &mut binance,
        &mut gate,
        &strategy,
        &strategy_symbols,
        &screener,
        &ws_tx,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_binance_missing() {
        let (binance, gate, outcome) = reconcile_volume_symbols(
            Vec::new(),
            vec!["XRPUSDT".to_string()],
        );
        assert_eq!(outcome, SymbolReconcileOutcome::BinanceMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_gate_missing() {
        let (binance, gate, outcome) = reconcile_volume_symbols(
            vec!["XRPUSDT".to_string()],
            Vec::new(),
        );
        assert_eq!(outcome, SymbolReconcileOutcome::GateMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_keeps_lists_when_both_present() {
        let (binance, gate, outcome) = reconcile_volume_symbols(
            vec!["XRPUSDT".to_string()],
            vec!["XRPUSDT".to_string()],
        );
        assert_eq!(outcome, SymbolReconcileOutcome::Ok);
        assert_eq!(binance, vec!["XRPUSDT".to_string()]);
        assert_eq!(gate, vec!["XRPUSDT".to_string()]);
    }

    #[test]
    fn reconcile_volume_symbols_uses_fallback_when_both_missing() {
        let (binance, gate, outcome) = reconcile_volume_symbols(Vec::new(), Vec::new());
        assert_eq!(outcome, SymbolReconcileOutcome::BothMissing);
        assert_eq!(binance, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(gate, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn event_loop_metrics_returns_no_data_when_empty() {
        let mut metrics = EventLoopMetrics::new();
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_metrics_formats_stats_and_clears_samples() {
        let mut metrics = EventLoopMetrics::new();
        metrics.record_tick_drift(130, 100_000_000);
        metrics.record_tick_drift(120, 110_000_000);
        metrics.record_tick_drift(130, 110_000_000);

        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=3 avg=20ms p50=20ms p95=30ms p99=30ms max=30ms"
        );
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_metrics_snapshot_rolls_interval_count() {
        let mut metrics = EventLoopMetrics::new();
        assert_eq!(metrics.snapshot_and_roll_status(10), 10);
        assert_eq!(metrics.snapshot_and_roll_status(16), 6);
        assert_eq!(metrics.snapshot_and_roll_status(8), 0);
    }

    #[tokio::test]
    async fn event_loop_state_starts_clean() {
        let mut state = EventLoopState::new();
        assert_eq!(state.ticker_count, 0);
        assert_eq!(state.signal_count, 0);
        assert!(state.latest_bn.is_empty());
        assert!(state.latest_gt.is_empty());
        assert_eq!(state.metrics.drift_stats_string_and_reset(), "no_data");
    }

    #[test]
    fn event_loop_state_now_ms_is_positive() {
        assert!(EventLoopState::now_ms() > 0);
    }

    fn test_ticker(symbol: &str, exchange_ts_ns: i64) -> hft_lead_lag::domain::BookTicker {
        hft_lead_lag::domain::BookTicker::new(
            bytes::Bytes::copy_from_slice(symbol.as_bytes()),
            100,
            101,
            1,
            1,
            exchange_ts_ns,
            exchange_ts_ns + 1,
        )
    }

    #[test]
    fn rebuild_latest_map_clears_old_entries() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("OLD".to_string(), test_ticker("OLD", 1));

        rebuild_latest_map(&mut latest, test_ticker("BTCUSDT", 10), Vec::new());

        assert!(!latest.contains_key("OLD"));
        assert!(latest.contains_key("BTCUSDT"));
    }

    #[test]
    fn rebuild_latest_map_keeps_latest_ticker_per_symbol() {
        let mut latest = std::collections::HashMap::new();
        rebuild_latest_map(
            &mut latest,
            test_ticker("BTCUSDT", 10),
            vec![test_ticker("BTCUSDT", 20), test_ticker("ETHUSDT", 30)],
        );

        assert_eq!(latest.len(), 2);
        assert_eq!(latest["BTCUSDT"].exchange_ts_ns, 20);
        assert_eq!(latest["ETHUSDT"].exchange_ts_ns, 30);
    }

    #[test]
    fn select_runtime_symbols_uses_common_when_present() {
        let common = vec!["XRPUSDT".to_string(), "ADAUSDT".to_string()];
        let (strategy, screener, used_fallback) = select_runtime_symbols(&common);

        assert!(!used_fallback);
        assert_eq!(strategy, common);
        assert_eq!(screener, common);
    }

    #[test]
    fn select_runtime_symbols_uses_fallback_when_common_empty() {
        let common: Vec<String> = Vec::new();
        let (strategy, screener, used_fallback) = select_runtime_symbols(&common);

        assert!(used_fallback);
        assert_eq!(strategy, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
        assert_eq!(screener, vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    #[test]
    fn strategy_ticks_in_order_skips_missing_symbols() {
        let strategy_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 10));

        let ticks: Vec<i64> = strategy_ticks_in_order(&strategy_symbols, &latest)
            .map(|t| t.exchange_ts_ns)
            .collect();
        assert_eq!(ticks, vec![10]);
    }

    #[test]
    fn strategy_ticks_in_order_preserves_strategy_order() {
        let strategy_symbols = vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()];
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 10));
        latest.insert("ETHUSDT".to_string(), test_ticker("ETHUSDT", 20));

        let symbols: Vec<String> = strategy_ticks_in_order(&strategy_symbols, &latest)
            .map(|t| String::from_utf8_lossy(&t.symbol).to_string())
            .collect();
        assert_eq!(symbols, vec!["ETHUSDT".to_string(), "BTCUSDT".to_string()]);
    }

    #[test]
    fn ingest_latest_batch_is_noop_for_empty_map() {
        let latest = std::collections::HashMap::new();
        let mut ticker_count = 3usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;

        ingest_latest_batch(
            &latest,
            "binance",
            &mut ticker_count,
            &mut metrics,
            &now_ms,
            &screener,
            &ws_tx,
        );

        assert_eq!(ticker_count, 3);
        assert_eq!(metrics.drift_stats_string_and_reset(), "no_data");
        assert!(screener.rows_sorted().is_empty());
        assert!(matches!(
            ws_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));
    }

    #[test]
    fn ingest_latest_batch_updates_counter_metrics_screener_and_ws() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("BTCUSDT".to_string(), test_ticker("BTCUSDT", 100_000_000));
        let mut ticker_count = 0usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;

        ingest_latest_batch(
            &latest,
            "gate",
            &mut ticker_count,
            &mut metrics,
            &now_ms,
            &screener,
            &ws_tx,
        );

        assert_eq!(ticker_count, 1);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=1 avg=30ms p50=30ms p95=30ms p99=30ms max=30ms"
        );

        let event = ws_rx.try_recv().expect("market data event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "gate");
        assert_eq!(event.timestamp_ns, 100_000_000);

        let rows = screener.rows_sorted();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].symbol, "BTCUSDT");
        assert_eq!(rows[0].leader_exchange, "gate");
    }

    #[test]
    fn process_exchange_batch_rebuilds_and_ingests_latest_state() {
        let mut latest = std::collections::HashMap::new();
        latest.insert("OLD".to_string(), test_ticker("OLD", 1));
        let mut ticker_count = 5usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 150i64;

        process_exchange_batch(
            &mut latest,
            test_ticker("BTCUSDT", 100_000_000),
            vec![test_ticker("ETHUSDT", 110_000_000), test_ticker("BTCUSDT", 120_000_000)],
            "binance",
            &mut ticker_count,
            &mut metrics,
            &now_ms,
            &screener,
            &ws_tx,
        );

        assert!(!latest.contains_key("OLD"));
        assert_eq!(latest.len(), 2);
        assert_eq!(latest["BTCUSDT"].exchange_ts_ns, 120_000_000);
        assert_eq!(ticker_count, 7);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=2 avg=35ms p50=40ms p95=40ms p99=40ms max=40ms"
        );

        let mut events = vec![
            ws_rx.try_recv().expect("first ws event"),
            ws_rx.try_recv().expect("second ws event"),
        ];
        events.sort_by(|a, b| a.symbol.cmp(&b.symbol));
        assert_eq!(events[0].symbol, "BTCUSDT");
        assert_eq!(events[0].exchange, "binance");
        assert_eq!(events[0].timestamp_ns, 120_000_000);
        assert_eq!(events[1].symbol, "ETHUSDT");
        assert_eq!(events[1].exchange, "binance");
        assert_eq!(events[1].timestamp_ns, 110_000_000);
        assert!(matches!(
            ws_rx.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ));

        let rows = screener.rows_sorted();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].leader_exchange, "binance");
        assert_eq!(rows[1].leader_exchange, "binance");
    }

    #[test]
    fn process_exchange_batch_with_single_tick_updates_once() {
        let mut latest = std::collections::HashMap::new();
        let mut ticker_count = 0usize;
        let mut metrics = EventLoopMetrics::new();
        let screener = ScreenerStore::default();
        let (ws_tx, mut ws_rx) = tokio::sync::broadcast::channel(8);
        let now_ms = || 130i64;

        process_exchange_batch(
            &mut latest,
            test_ticker("BTCUSDT", 100_000_000),
            Vec::new(),
            "gate",
            &mut ticker_count,
            &mut metrics,
            &now_ms,
            &screener,
            &ws_tx,
        );

        assert_eq!(latest.len(), 1);
        assert_eq!(ticker_count, 1);
        assert_eq!(
            metrics.drift_stats_string_and_reset(),
            "n=1 avg=30ms p50=30ms p95=30ms p99=30ms max=30ms"
        );
        let event = ws_rx.try_recv().expect("ws event");
        assert_eq!(event.symbol, "BTCUSDT");
        assert_eq!(event.exchange, "gate");
    }
}
