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
    let mut binance_symbols: Vec<String> = binance_tickers.iter().map(|t| t.symbol.clone()).collect();
    let mut gate_symbols: Vec<String> = gate_tickers.iter().map(|t| t.symbol.clone()).collect();
    // Build volume lookup (Gate volume for execution venue)
    let gate_vol_map: std::collections::HashMap<String, f64> = gate_tickers
        .iter()
        .map(|t| (t.symbol.clone(), t.quote_volume))
        .collect();
    if binance_symbols.is_empty() && !gate_symbols.is_empty() {
        warn!("Binance volume fetch failed — cannot safely copy Gate symbols (different listing). Using BTC/ETH fallback for both.");
        binance_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        gate_symbols = binance_symbols.clone();
    } else if gate_symbols.is_empty() && !binance_symbols.is_empty() {
        warn!("Gate volume fetch failed — cannot safely copy Binance symbols (different listing). Using BTC/ETH fallback for both.");
        binance_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        gate_symbols = binance_symbols.clone();
    } else if binance_symbols.is_empty() && gate_symbols.is_empty() {
        warn!("No symbols from REST; using BTC/ETH fallback");
        binance_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        gate_symbols = binance_symbols.clone();
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
    let mut strategy_symbols: Vec<String> = common_symbols
        .iter()
        .cloned()
        .collect();
    if strategy_symbols.is_empty() {
        warn!("No common symbols found! Using fallback...");
        strategy_symbols.extend_from_slice(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }
    let screener_symbols: Vec<String> = if common_symbols.is_empty() {
        strategy_symbols.clone()
    } else {
        common_symbols.clone()
    };

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

    // Main event loop
    let mut ticker_count = 0usize;
    let mut signal_count = 0usize;
    let mut last_status_at = Instant::now();
    let mut last_status_ticker_count = 0usize;
    let mut signal_interval = tokio::time::interval(tokio::time::Duration::from_millis(100));
    signal_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut latest_bn: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> = std::collections::HashMap::new();
    let mut latest_gt: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> = std::collections::HashMap::new();

    // Drift tracking for benchmarking
    let mut drift_samples: Vec<i64> = Vec::with_capacity(8192);
    let now_ms = || -> i64 {
        SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64
    };

    loop {
        tokio::select! {
            // Receive from Binance
            result = binance.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        // Process this tick + drain all buffered ticks, keep latest per symbol
                        latest_bn.clear();
                        let sym = String::from_utf8_lossy(&ticker.symbol).to_string();
                        latest_bn.insert(sym, ticker);
                        for t in binance.drain_book_tickers() {
                            let s = String::from_utf8_lossy(&t.symbol).to_string();
                            latest_bn.insert(s, t);
                        }
                        for (symbol, ticker) in &latest_bn {
                            ticker_count += 1;
                            let local_ms = now_ms();
                            let exch_ms = ticker.exchange_ts_ns / 1_000_000;
                            if exch_ms > 0 { drift_samples.push(local_ms - exch_ms); }
                            screener.update(
                                symbol,
                                "binance",
                                ticker.bid_price(),
                                ticker.ask_price(),
                                ticker.exchange_ts_ns,
                                ticker.local_ts_ns,
                            );
                            let _ = ws_tx.send(MarketDataEvent {
                                symbol: symbol.clone(),
                                exchange: "binance",
                                bid: ticker.bid_price(),
                                ask: ticker.ask_price(),
                                timestamp_ns: ticker.exchange_ts_ns,
                            });
                        }
                        // Forward strategy ticks for bounded symbol set
                        for sym in &strategy_symbols {
                            if let Some(t) = latest_bn.get(sym) {
                                strategy.update_primary_book(t.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        error!("Binance data error: {}", e);
                    }
                }
            }
            
            // Receive from Gate
            result = gate.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        latest_gt.clear();
                        let sym = String::from_utf8_lossy(&ticker.symbol).to_string();
                        latest_gt.insert(sym, ticker);
                        for t in gate.drain_book_tickers() {
                            let s = String::from_utf8_lossy(&t.symbol).to_string();
                            latest_gt.insert(s, t);
                        }
                        for (symbol, ticker) in &latest_gt {
                            ticker_count += 1;
                            let local_ms = now_ms();
                            let exch_ms = ticker.exchange_ts_ns / 1_000_000;
                            if exch_ms > 0 { drift_samples.push(local_ms - exch_ms); }
                            screener.update(
                                symbol,
                                "gate",
                                ticker.bid_price(),
                                ticker.ask_price(),
                                ticker.exchange_ts_ns,
                                ticker.local_ts_ns,
                            );
                            let _ = ws_tx.send(MarketDataEvent {
                                symbol: symbol.clone(),
                                exchange: "gate",
                                bid: ticker.bid_price(),
                                ask: ticker.ask_price(),
                                timestamp_ns: ticker.exchange_ts_ns,
                            });
                        }
                        for sym in &strategy_symbols {
                            if let Some(t) = latest_gt.get(sym) {
                                strategy.update_hedge_book(t.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Gate data error: {}", e);
                    }
                }
            }

            // Check for lead-lag signals periodically
            _ = signal_interval.tick() => {
                for symbol in &strategy_symbols {
                    if let Some(signal) = strategy.check_signal(symbol).await {
                        signal_count += 1;
                        info!("Lead-lag signal #{}: {} | spread={:.2}bps | leader={:?} | lagger={:?}", 
                            signal_count,
                            signal.symbol, 
                            signal.spread_bps, 
                            signal.leader, 
                            signal.lagger
                        );
                    }
                }
                
                // Status report every 5 seconds
                if last_status_at.elapsed() >= Duration::from_secs(5) {
                    let interval_tickers = ticker_count.saturating_sub(last_status_ticker_count);
                    // Compute drift percentiles
                    let drift_stats = if drift_samples.is_empty() {
                        "no_data".to_string()
                    } else {
                        drift_samples.sort_unstable();
                        let n = drift_samples.len();
                        let p50 = drift_samples[n / 2];
                        let p95 = drift_samples[n * 95 / 100];
                        let p99 = drift_samples[n * 99 / 100];
                        let max = drift_samples[n - 1];
                        let avg = drift_samples.iter().sum::<i64>() / n as i64;
                        format!("n={} avg={}ms p50={}ms p95={}ms p99={}ms max={}ms", n, avg, p50, p95, p99, max)
                    };
                    drift_samples.clear();
                    info!(
                        "Status: tickers={} (+{}/5s) signals={} drift=[{}]",
                        ticker_count, interval_tickers, signal_count, drift_stats
                    );
                    last_status_ticker_count = ticker_count;
                    last_status_at = Instant::now();
                }
            }
        }
    }
}
