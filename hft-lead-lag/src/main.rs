//! HFT Lead-Lag Trading System - Main Entry Point
//! 
//! This binary demonstrates the usage of the HFT lead-lag system
//! with volume-filtered symbols.

use hft_lead_lag::{
    BinanceMarketData, GateMarketData,
    LeadLagStrategy, LeadLagStrategyConfig,
    ConfigManager, MarketDataStream,
};
use hft_lead_lag::infrastructure::rest::{BinanceRestClient, GateRestClient};
use tracing::{info, error, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

/// Minimum 24h USD volume for symbol filtering
const MIN_VOLUME_USD: f64 = 1_000_000.0;  // 1 million USD

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "hft_lead_lag=info,tokio_tungstenite=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║     HFT Lead-Lag Trading System Starting...               ║");
    info!("╚═══════════════════════════════════════════════════════════╝");

    // Load configuration from environment
    let config_manager = ConfigManager::from_env();

    // Initialize REST clients for volume filtering
    info!("📊 Fetching 24h volume data for symbol filtering...");
    
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

    info!("✅ Binance: {} symbols with volume >= ${:.0}M", binance_symbols.len(), MIN_VOLUME_USD / 1_000_000.0);
    info!("✅ Gate: {} symbols with volume >= ${:.0}M", gate_symbols.len(), MIN_VOLUME_USD / 1_000_000.0);

    // Find common symbols (available on both exchanges with sufficient volume)
    let binance_set: std::collections::HashSet<_> = binance_symbols.iter().collect();
    let gate_set: std::collections::HashSet<_> = gate_symbols.iter().collect();
    let common_symbols: Vec<&String> = binance_set.intersection(&gate_set).copied().collect();
    
    info!("📈 Common symbols: {}", common_symbols.len());
    
    // Take top 10 by volume (simplified: just take first 10 common)
    let mut symbols: Vec<String> = common_symbols
        .iter()
        .take(10)
        .map(|s| (*s).clone())
        .collect();

    if symbols.is_empty() {
        warn!("No common symbols found! Using fallback...");
        symbols.extend_from_slice(&["BTCUSDT".to_string(), "ETHUSDT".to_string()]);
    }

    info!("🎯 Trading {} symbols: {:?}", symbols.len(), symbols);

    // Initialize exchange connectors
    let mut binance = BinanceMarketData::new();
    let mut gate = GateMarketData::new();

    // Set credentials if available
    if let Some(creds) = config_manager.binance_credentials() {
        binance.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("🔑 Binance credentials configured");
    }

    if let Some(creds) = config_manager.gate_credentials() {
        gate.set_credentials(creds.api_key.clone(), creds.api_secret.clone());
        info!("🔑 Gate credentials configured");
    }

    // Connect to exchanges
    info!("🔌 Connecting to Binance Futures...");
    if let Err(e) = binance.connect().await {
        error!("❌ Failed to connect to Binance: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }

    info!("🔌 Connecting to Gate.io Futures...");
    if let Err(e) = gate.connect().await {
        error!("❌ Failed to connect to Gate: {}", e);
        return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
    }

    // Subscribe to market data for volume-filtered symbols
    for symbol in &symbols {
        info!("📡 Subscribing to {}", symbol);
        
        match binance.subscribe_book_ticker(symbol).await {
            Ok(id) => info!("✅ Binance subscribed to {} (id={})", symbol, id),
            Err(e) => error!("❌ Binance subscribe error {}: {}", symbol, e),
        }
        
        match gate.subscribe_book_ticker(symbol).await {
            Ok(id) => info!("✅ Gate subscribed to {} (id={})", symbol, id),
            Err(e) => error!("❌ Gate subscribe error {}: {}", symbol, e),
        }
        
        // Rate limiting - 100ms between subscriptions
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    // Initialize lead-lag strategy
    let config = LeadLagStrategyConfig {
        min_entry_spread_bps: 5.0,
        target_exit_spread_bps: 1.0,
        symbols: symbols.clone(),
        ..Default::default()
    };

    let strategy = LeadLagStrategy::new(config);

    info!("╔═══════════════════════════════════════════════════════════╗");
    info!("║  System Initialized. Monitoring {} symbols...            ║", symbols.len());
    info!("╚═══════════════════════════════════════════════════════════╝");

    // Main event loop
    let mut ticker_count = 0usize;
    let mut signal_count = 0usize;

    loop {
        tokio::select! {
            // Receive from Binance
            result = binance.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        ticker_count += 1;
                        let symbol = String::from_utf8_lossy(&ticker.symbol);
                        
                        // Log EVERY ticker
                        let spread_bps = ticker.spread_pct() * 10000.0;
                        info!("📊 Binance #{} {}: bid=${:.6} ask=${:.6} spread={:.3}bps", 
                            ticker_count, symbol, 
                            ticker.bid_price(), 
                            ticker.ask_price(),
                            spread_bps
                        );
                        
                        strategy.update_primary_book(ticker).await;
                    }
                    Err(e) => {
                        error!("❌ Binance error: {}", e);
                    }
                }
            }
            
            // Receive from Gate
            result = gate.recv_book_ticker() => {
                match result {
                    Ok(ticker) => {
                        ticker_count += 1;
                        let symbol = String::from_utf8_lossy(&ticker.symbol);
                        
                        let spread_bps = ticker.spread_pct() * 10000.0;
                        if ticker_count <= 50 || ticker_count.is_multiple_of(10) || spread_bps > 0.5 {
                            info!("📊 Gate {}: bid=${:.6} ask=${:.6} spread={:.3}bps [Total: {}]", 
                                symbol, 
                                ticker.bid_price(), 
                                ticker.ask_price(),
                                spread_bps,
                                ticker_count
                            );
                        }
                        
                        strategy.update_hedge_book(ticker).await;
                    }
                    Err(e) => {
                        warn!("Gate error: {}", e);
                    }
                }
            }

            // Check for lead-lag signals periodically
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                for symbol in &symbols {
                    if let Some(signal) = strategy.check_signal(symbol).await {
                        signal_count += 1;
                        info!("🚨 LEAD-LAG SIGNAL #{}: {} | spread={:.2}bps | leader={:?} | lagger={:?}", 
                            signal_count,
                            signal.symbol, 
                            signal.spread_bps, 
                            signal.leader, 
                            signal.lagger
                        );
                    }
                }
                
                // Status report every 5 seconds
                if ticker_count > 0 && ticker_count.is_multiple_of(50) {
                    info!("📈 Status: {} tickers | {} signals | {} symbols", 
                        ticker_count, signal_count, symbols.len());
                }
            }
        }
    }
}
