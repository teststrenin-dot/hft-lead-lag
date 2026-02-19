//! Binance Futures order executor (stub — will be wired for real trading).

use crate::domain::{
    ExchangeId, ExchangeError, ExchangeResult,
    OrderExecutor, OrderRequest, OrderResponse, Position,
};

/// Binance order executor
#[allow(dead_code)]
pub struct BinanceOrderExecutor {
    api_key: String,
    api_secret: String,
    client: reqwest::Client,
}

impl BinanceOrderExecutor {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self { api_key, api_secret, client: reqwest::Client::new() }
    }
}

#[async_trait::async_trait]
impl OrderExecutor for BinanceOrderExecutor {
    fn exchange_id(&self) -> ExchangeId { ExchangeId::BinanceFutures }
    async fn place_order(&self, _request: OrderRequest) -> ExchangeResult<OrderResponse> { Err(ExchangeError::Internal("TODO".into())) }
    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> ExchangeResult<OrderResponse> { Err(ExchangeError::Internal("TODO".into())) }
    async fn cancel_all_orders(&self, _symbol: &str) -> ExchangeResult<Vec<OrderResponse>> { Ok(vec![]) }
    async fn get_position(&self, _symbol: &str) -> ExchangeResult<Position> { Ok(Position::default()) }
}
