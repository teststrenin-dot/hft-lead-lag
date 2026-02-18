//! Application ports - interfaces for infrastructure

/// Market data port
pub trait MarketDataPort: Send + Sync {
    fn on_book_ticker(&self, ticker: crate::domain::BookTicker);
    fn on_trade(&self, trade: crate::domain::Trade);
}

/// Order execution port
pub trait OrderExecutionPort: Send + Sync {
    fn execute_order(&self, request: crate::domain::OrderRequest);
    fn cancel_order(&self, symbol: &str, order_id: &str);
}
