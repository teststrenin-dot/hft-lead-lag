//! Exchange connector traits - the ports for our architecture
//!
//! Defines the interface that all exchange connectors must implement.
//! Follows SOLID: Interface Segregation Principle - small, focused traits.

use crate::domain::messages::{BookTicker, Trade};
use crate::domain::models::{OrderRequest, OrderResponse, Position};

/// Unique identifier for an exchange
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExchangeId {
    BinanceFutures,
    GateFutures,
}

impl std::fmt::Display for ExchangeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BinanceFutures => write!(f, "binance_futures"),
            Self::GateFutures => write!(f, "gate_futures"),
        }
    }
}

/// Market data subscription handle
pub type SubscriptionId = u64;

/// Result type for exchange operations
pub type ExchangeResult<T> = Result<T, ExchangeError>;

/// Exchange error types
#[derive(Debug, thiserror::Error)]
pub enum ExchangeError {
    #[error("WebSocket connection failed: {0}")]
    WebSocketError(String),

    #[error("Authentication failed: {0}")]
    AuthError(String),

    #[error("Order rejected: {0}")]
    OrderRejected(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimited(String),

    #[error("Symbol not found: {0}")]
    SymbolNotFound(String),

    #[error("Invalid message format: {0}")]
    ParseError(String),

    #[error("Connection closed: {0}")]
    ConnectionClosed(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

/// Market data stream trait - focused on receiving market data
/// Single Responsibility: Only market data, no orders
#[async_trait::async_trait]
pub trait MarketDataStream: Send + Sync {
    /// Get exchange identifier
    fn exchange_id(&self) -> ExchangeId;

    /// Connect to the exchange WebSocket
    async fn connect(&mut self) -> ExchangeResult<()>;

    /// Disconnect from the exchange
    async fn disconnect(&mut self) -> ExchangeResult<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;

    /// Subscribe to book ticker for a symbol
    async fn subscribe_book_ticker(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId>;

    /// Subscribe to trades for a symbol
    async fn subscribe_trades(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId>;

    /// Unsubscribe from a stream
    async fn unsubscribe(&mut self, subscription_id: SubscriptionId) -> ExchangeResult<()>;

    /// Receive next book ticker update (zero-copy where possible)
    async fn recv_book_ticker(&mut self) -> ExchangeResult<BookTicker>;

    /// Receive next trade update
    async fn recv_trade(&mut self) -> ExchangeResult<Trade>;
}

/// Order execution trait - focused on order management
/// Single Responsibility: Only orders, no market data
#[async_trait::async_trait]
pub trait OrderExecutor: Send + Sync {
    /// Get exchange identifier
    fn exchange_id(&self) -> ExchangeId;

    /// Place a new order
    async fn place_order(&self, request: OrderRequest) -> ExchangeResult<OrderResponse>;

    /// Cancel an order
    async fn cancel_order(&self, symbol: &str, order_id: &str) -> ExchangeResult<OrderResponse>;

    /// Cancel all orders for a symbol
    async fn cancel_all_orders(&self, symbol: &str) -> ExchangeResult<Vec<OrderResponse>>;

    /// Get current position for a symbol
    async fn get_position(&self, symbol: &str) -> ExchangeResult<Position>;
}

/// Combined exchange trait for convenience
/// Composition of MarketDataStream + OrderExecutor
pub trait Exchange: MarketDataStream + OrderExecutor {}

/// Blanket implementation - any type implementing both traits is an Exchange
impl<T: MarketDataStream + OrderExecutor> Exchange for T {}
