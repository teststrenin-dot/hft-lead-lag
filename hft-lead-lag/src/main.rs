//! HFT Lead-Lag Trading System - Main Entry Point
//! 
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::{
    BinanceMarketData, GateMarketData,
    LeadLagStrategy, LeadLagStrategyConfig,
    ConfigManager, MarketDataStream,
};
use hft_lead_lag::api::{HttpServer, HttpServerConfig, MarketDataEvent, MarketDataServer, ScreenerStore, WsServerConfig};
use hft_lead_lag::infrastructure::logging::init_centralized_logging;
use hft_lead_lag::infrastructure::rest::{BinanceRestClient, GateRestClient};
use tracing::{error, info, warn};
use std::time::{Duration, Instant};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 1_000_000.0;  // 1 million USD
const MAX_STRATEGY_SYMBOLS: usize = 8;
const SUBSCRIBE_DELAY_MS: u64 = 250;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_centralized_logging("logs", "runtime.log")?;

    info!("HFT Lead-Lag system starting");

    // Load configuration from environment
    let config_manager = ConfigManager::from_env();

    // Initialize REST clients for volume filtering
    info!("Fetching 24h volume data for symbol filtering");
    
    let binance_rest = BinanceRestClient::new();
    let gate_rest = GateRestClient::new();

    // Get symbols with sufficient volume from both exchanges
    let (binance_symbols, gate_symbols) = tokio::join!(
        binance_rest.get_symbols_with_volume(MIN_VOLUME_USD),
        gate_rest.get_symbols_with_volume(MIN_VOLUME_USD)
    );

    let binance_symbols = binance_symbols.unwrap_or_else(|e| {
        warn!("Failed to get Binance symbols: {}", e);
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    });

    let gate_symbols = gate_symbols.unwrap_or_else(|e| {
        warn!("Failed to get Gate symbols: {}", e);
        vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
    });

    info!("Binance symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, binance_symbols.len());
    info!("Gate symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, gate_symbols.len());

    // Find common symbols (available on both exchanges with sufficient volume)
    let binance_set: std::collections::HashSet<String> = binance_symbols.iter().cloned().collect();
    let gate_set: std::collections::HashSet<String> = gate_symbols.iter().cloned().collect();
    let mut common_symbols: Vec<String> = binance_set.intersection(&gate_set).cloned().collect();
    common_symbols.sort_unstable();
    
    info!("Common symbols: {}", common_symbols.len());

    // Strategy runs on a bounded subset; WS snapshot covers full symbol universe.
    let mut strategy_symbols: Vec<String> = common_symbols
        .iter()
        .take(MAX_STRATEGY_SYMBOLS)
        .cloned()
        .collect();
    if strategy_symbols.is_empty() {
        warn!("No common symbols found! Using fallback...");
        strategy_symbols.extend_from_slice(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    info!(
        "Strategy symbols: {} | WS coverage Binance={} Gate={}",
        strategy_symbols.len(),
        binance_symbols.len(),
        gate_symbols.len()
    );

    // Initialize exchange connectors
    let mut binance = BinanceMarketData::new();
    let mut gate = GateMarketData::new();

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

    info!("Connecting to Gate.io Futures...");
    if let Err(e) = gate.connect().await {
        error!("Failed to connect to Gate: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }

    // Start external APIs early so checkpoint endpoints are always available.
    let screener = ScreenerStore::default();
    let http_server = HttpServer::with_runtime(
        HttpServerConfig::default(),
        MIN_VOLUME_USD,
        screener.clone(),
    );
    tokio::spawn(async move {
        if let Err(e) = http_server.start().await {
            error!("HTTP server failed: {}", e);
        }
    });

    let ws_server = MarketDataServer::new(WsServerConfig::default());
    let ws_tx = ws_server.transmitter();
    tokio::spawn(async move {
        if let Err(e) = ws_server.start().await {
            error!("WS server failed: {}", e);
        }
    });

    // Subscribe to strategy symbols for live WS ticks.
    let mut binance_subscribed = 0usize;
    let mut binance_subscribe_errors = 0usize;
    for symbol in &strategy_symbols {
        match binance.subscribe_book_ticker(symbol).await {
            Ok(_) => binance_subscribed += 1,
            Err(e) => {
                binance_subscribe_errors += 1;
                error!("Binance subscribe error {}: {}", symbol, e);
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(SUBSCRIBE_DELAY_MS)).await;
    }
    info!(
        "Binance subscription summary: ok={} err={}",
        binance_subscribed, binance_subscribe_errors
    );

    // Subscribe Gate only to strategy symbols; full symbol universe is served via WS snapshot.
    let mut gate_subscribed = 0usize;
    let mut gate_subscribe_errors = 0usize;
    for symbol in &strategy_symbols {
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
                warn!("Gate subscription timeout on {}; proceeding with available streams", symbol);
                break;
            }
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(SUBSCRIBE_DELAY_MS)).await;
    }
    info!(
        "Gate subscription summary: ok={} err={}",
        gate_subscribed, gate_subscribe_errors
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

    // Main event loop
    let mut ticker_count = 0usize;
    let mut signal_count = 0usize;
    let mut last_status_at = Instant::now();
    let mut last_status_ticker_count = 0usize;

    loop {
        tokio::select! {
            // Receive from Binance
            result = binance.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        ticker_count += 1;
                        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
                        screener.update(
                            &symbol,
                            "binance",
                            ticker.bid_price(),
                            ticker.ask_price(),
                            ticker.exchange_ts_ns,
                        );
                        let _ = ws_tx.send(MarketDataEvent {
                            symbol: symbol.clone(),
                            exchange: "binance".to_string(),
                            bid: ticker.bid_price(),
                            ask: ticker.ask_price(),
                            timestamp_ns: ticker.exchange_ts_ns,
                        });
                        
                        strategy.update_primary_book(ticker).await;
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
                        ticker_count += 1;
                        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
                        screener.update(
                            &symbol,
                            "gate",
                            ticker.bid_price(),
                            ticker.ask_price(),
                            ticker.exchange_ts_ns,
                        );
                        let _ = ws_tx.send(MarketDataEvent {
                            symbol: symbol.clone(),
                            exchange: "gate".to_string(),
                            bid: ticker.bid_price(),
                            ask: ticker.ask_price(),
                            timestamp_ns: ticker.exchange_ts_ns,
                        });
                        
                        strategy.update_hedge_book(ticker).await;
                    }
                    Err(e) => {
                        warn!("Gate data error: {}", e);
                    }
                }
            }

            // Check for lead-lag signals periodically
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
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
                    info!(
                        "Status: total_tickers={} (+{} / 5s) signals={} strategy_symbols={}",
                        ticker_count, interval_tickers, signal_count, strategy_symbols.len()
                    );
                    last_status_ticker_count = ticker_count;
                    last_status_at = Instant::now();
                }
            }
        }
    }
}
