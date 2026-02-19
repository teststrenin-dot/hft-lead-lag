//! HFT Lead-Lag Trading System
//! 
//! A high-frequency trading system implementing lead-lag arbitrage
//! between Binance Futures and Gate.io Futures exchanges.
//! 
//! # Architecture
//! 
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                         API Layer                            │
//! │  (WebSocket server, HTTP endpoints for monitoring/control)   │
//! ├─────────────────────────────────────────────────────────────┤
//! │                     Application Layer                        │
//! │  ┌──────────────────┐  ┌──────────────────┐                 │
//! │  │  Lead-Lag Strat. │  │  Risk Manager    │                 │
//! │  └──────────────────┘  └──────────────────┘                 │
//! ├─────────────────────────────────────────────────────────────┤
//! │                      Domain Layer                            │
//! │  ┌────────────┐  ┌────────────┐  ┌────────────┐             │
//! │  │  Exchange  │  │  Messages  │  │   Models   │             │
//! │  │   Traits   │  │   (Zero)   │  │            │             │
//! │  └────────────┘  └────────────┘  └────────────┘             │
//! ├─────────────────────────────────────────────────────────────┤
//! │                   Infrastructure Layer                       │
//! │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
//! │  │   Binance    │  │    Gate.io   │  │  WebSocket   │       │
//! │  │  Connector   │  │  Connector   │  │    Utils     │       │
//! │  └──────────────┘  └──────────────┘  └──────────────┘       │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//! 
//! # Design Principles
//! 
//! - **SOLID**: Each module has single responsibility
//! - **Zero-copy hot path**: Market data parsing avoids allocations
//! - **WebSocket-first**: All market data via WS (except cold auth)
//! - **Deterministic cognitive load**: Each module < 500 LOC
//! - **No god objects**: Clear separation of concerns
//! 
//! # Example Usage
//! 
//! ```rust,no_run
//! use hft_lead_lag::domain::{MarketDataStream, ExchangeId};
//! use hft_lead_lag::infrastructure::exchanges::{BinanceMarketData, GateMarketData};
//! use hft_lead_lag::application::services::{LeadLagStrategy, LeadLagStrategyConfig};
//! 
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Initialize connectors
//!     let mut binance = BinanceMarketData::new();
//!     let mut gate = GateMarketData::new();
//!     
//!     // Connect to exchanges
//!     binance.connect().await?;
//!     gate.connect().await?;
//!     
//!     // Subscribe to market data
//!     binance.subscribe_book_ticker("BTCUSDT").await?;
//!     gate.subscribe_book_ticker("BTCUSDT").await?;
//!     
//!     // Run lead-lag strategy
//!     let config = LeadLagStrategyConfig::default();
//!     let strategy = LeadLagStrategy::new(config);
//!     
//!     Ok(())
//! }
//! ```

pub mod domain;
pub mod infrastructure;
pub mod application;
pub mod config;

/// API layer for external integrations
pub mod api;

// Re-export commonly used types
pub use domain::{
    ExchangeId, ExchangeError, ExchangeResult,
    MarketDataStream, OrderExecutor, Exchange,
    BookTicker, Trade, SubscriptionId,
    OrderRequest, OrderResponse, Position, Side,
};

pub use infrastructure::exchanges::{
    BinanceMarketData, GateMarketData,
};

pub use application::services::{
    LeadLagStrategy, LeadLagStrategyConfig, LeadLagSignal,
    RiskManager, RiskLimits,
};

pub use config::{ConfigManager, AppConfig, ExchangeCredentials};
