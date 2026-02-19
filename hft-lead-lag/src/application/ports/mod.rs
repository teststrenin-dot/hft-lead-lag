//! Application ports — interfaces for infrastructure.
//!
//! These traits define the hexagonal-architecture boundary between application
//! and infrastructure layers. Not yet wired in runtime; kept for future use.

/// Market data port
#[allow(dead_code)]
pub trait MarketDataPort: Send + Sync {
    fn on_book_ticker(&self, ticker: crate::domain::BookTicker);
    fn on_trade(&self, trade: crate::domain::Trade);
}

/// Order execution port
#[allow(dead_code)]
pub trait OrderExecutionPort: Send + Sync {
    fn execute_order(&self, request: crate::domain::OrderRequest);
    fn cancel_order(&self, symbol: &str, order_id: &str);
}
