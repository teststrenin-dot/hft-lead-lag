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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tracing::{error, info, warn};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 2_500_000.0; // 2.5 million USD
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

struct RuntimeUniverse {
    common_symbols: Vec<String>,
    strategy_symbols: Vec<String>,
    screener_symbols: Vec<String>,
    gate_vol_map: std::collections::HashMap<String, f64>,
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

    fn maybe_mark_disconnected(self, health: &HealthState, error: &hft_lead_lag::domain::ExchangeError) {
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
    hft_lead_lag::infrastructure::db::upsert_configs(&conn, screener.fleet_configs())?;
    info!(
        "Seeded {} fleet configs into {}",
        screener.fleet_configs().len(),
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
    configure_and_connect_exchanges(&config_manager, &mut binance, &mut gate, health_state.as_ref())
        .await?;

    // Start external APIs early so checkpoint endpoints are always available.
    let mut screener = ScreenerStore::default();

    // Initialize fleet persistence (SQLite WAL mode, async batch writes).
    let db_path = std::path::Path::new("data/optimizer.db");
    init_screener_persistence(&mut screener, db_path)?;

    // Seed 24h volume from Gate REST data
    let vol_pairs: Vec<(String, f64)> = common_symbols
        .iter()
        .map(|s| (s.clone(), gate_vol_map.get(s).copied().unwrap_or(0.0)))
        .collect();
    screener.set_volumes(&vol_pairs);
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
