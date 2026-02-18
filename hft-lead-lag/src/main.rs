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
const MIN_VOLUME_USD: f64 = 10_000_000.0;  // 10 million USD
const MAX_STRATEGY_SYMBOLS: usize = 8;
const SUBSCRIBE_DELAY_MS: u64 = 15;

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
    let (binance_symbols_result, gate_symbols_result) = tokio::join!(
        binance_rest.get_symbols_with_volume(MIN_VOLUME_USD),
        gate_rest.get_symbols_with_volume(MIN_VOLUME_USD)
    );

    let mut binance_symbols = match binance_symbols_result {
        Ok(symbols) => symbols,
        Err(e) => {
            warn!("Failed to get Binance symbols: {}", e);
            Vec::new()
        }
    };
    let mut gate_symbols = match gate_symbols_result {
        Ok(symbols) => symbols,
        Err(e) => {
            warn!("Failed to get Gate symbols: {}", e);
            Vec::new()
        }
    };
    if binance_symbols.is_empty() && !gate_symbols.is_empty() {
        warn!("Using Gate symbol universe as temporary Binance fallback");
        binance_symbols = gate_symbols.clone();
    }
    if gate_symbols.is_empty() && !binance_symbols.is_empty() {
        warn!("Using Binance symbol universe as temporary Gate fallback");
        gate_symbols = binance_symbols.clone();
    }
    if binance_symbols.is_empty() && gate_symbols.is_empty() {
        warn!("No symbols from REST; using BTC/ETH fallback");
        binance_symbols = vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()];
        gate_symbols = binance_symbols.clone();
    }

    info!("Binance symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, binance_symbols.len());
    info!("Gate symbols with 24h vol >= ${:.0}M: {}", MIN_VOLUME_USD / 1_000_000.0, gate_symbols.len());

    // Find common symbols (available on both exchanges with sufficient volume)
    let binance_set: std::collections::HashSet<String> = binance_symbols.iter().cloned().collect();
    let gate_set: std::collections::HashSet<String> = gate_symbols.iter().cloned().collect();
    let mut common_symbols: Vec<String> = binance_set.intersection(&gate_set).cloned().collect();
    common_symbols.sort_unstable();
    
    info!("Common symbols: {}", common_symbols.len());

    // Strategy checks run on a bounded subset.
    let mut strategy_symbols: Vec<String> = common_symbols
        .iter()
        .take(MAX_STRATEGY_SYMBOLS)
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

    loop {
        tokio::select! {
            // Receive from Binance
            result = binance.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        // Process this tick + drain all buffered ticks, keep latest per symbol
                        let mut latest: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> = std::collections::HashMap::new();
                        let sym = String::from_utf8_lossy(&ticker.symbol).to_string();
                        latest.insert(sym, ticker);
                        for t in binance.drain_book_tickers() {
                            let s = String::from_utf8_lossy(&t.symbol).to_string();
                            latest.insert(s, t);
                        }
                        for (symbol, ticker) in &latest {
                            ticker_count += 1;
                            screener.update(
                                symbol,
                                "binance",
                                ticker.bid_price(),
                                ticker.ask_price(),
                                ticker.bid_qty(),
                                ticker.ask_qty(),
                                ticker.exchange_ts_ns,
                                ticker.local_ts_ns,
                            );
                            let _ = ws_tx.send(MarketDataEvent {
                                symbol: symbol.clone(),
                                exchange: "binance".to_string(),
                                bid: ticker.bid_price(),
                                ask: ticker.ask_price(),
                                timestamp_ns: ticker.exchange_ts_ns,
                            });
                        }
                        // Forward strategy ticks for bounded symbol set
                        for sym in &strategy_symbols {
                            if let Some(t) = latest.get(sym) {
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
                        let mut latest: std::collections::HashMap<String, hft_lead_lag::domain::BookTicker> = std::collections::HashMap::new();
                        let sym = String::from_utf8_lossy(&ticker.symbol).to_string();
                        latest.insert(sym, ticker);
                        for t in gate.drain_book_tickers() {
                            let s = String::from_utf8_lossy(&t.symbol).to_string();
                            latest.insert(s, t);
                        }
                        for (symbol, ticker) in &latest {
                            ticker_count += 1;
                            screener.update(
                                symbol,
                                "gate",
                                ticker.bid_price(),
                                ticker.ask_price(),
                                ticker.bid_qty(),
                                ticker.ask_qty(),
                                ticker.exchange_ts_ns,
                                ticker.local_ts_ns,
                            );
                            let _ = ws_tx.send(MarketDataEvent {
                                symbol: symbol.clone(),
                                exchange: "gate".to_string(),
                                bid: ticker.bid_price(),
                                ask: ticker.ask_price(),
                                timestamp_ns: ticker.exchange_ts_ns,
                            });
                        }
                        for sym in &strategy_symbols {
                            if let Some(t) = latest.get(sym) {
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
