//! Gate.io Futures order executor (stub — will be wired for real trading).

use crate::domain::{
    ExchangeId, ExchangeError, ExchangeResult,
    OrderExecutor, OrderRequest, OrderResponse, Position,
};
use crate::infrastructure::exchanges::common::HmacSha512;

/// Gate.io Futures order executor
#[allow(dead_code)]
pub struct GateOrderExecutor {
    api_key: String,
    api_secret: String,
    client: reqwest::Client,
}

impl GateOrderExecutor {
    pub fn new(api_key: String, api_secret: String) -> Self {
        Self {
            api_key,
            api_secret,
            client: reqwest::Client::new(),
        }
    }

    /// Generate Gate.io signature
    #[allow(dead_code)]
    fn generate_signature(&self, method: &str, path: &str, body: &str, timestamp: i64) -> String {
        use sha2::Sha512;
        use sha2::Digest;
        let sign_payload = format!("{}\n{}\n{}\n{}\n{}",
            method,
            path,
            body,
            hex::encode(Sha512::digest(body.as_bytes())),
            timestamp
        );
        HmacSha512::sign_static(self.api_secret.as_bytes(), sign_payload.as_bytes())
    }
}

#[async_trait::async_trait]
impl OrderExecutor for GateOrderExecutor {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::GateFutures
    }

    async fn place_order(&self, _request: OrderRequest) -> ExchangeResult<OrderResponse> {
        Err(ExchangeError::Internal("Not implemented".into()))
    }

    async fn cancel_order(&self, _symbol: &str, _order_id: &str) -> ExchangeResult<OrderResponse> {
        Err(ExchangeError::Internal("Not implemented".into()))
    }

    async fn cancel_all_orders(&self, _symbol: &str) -> ExchangeResult<Vec<OrderResponse>> {
        Ok(vec![])
    }

    async fn get_position(&self, _symbol: &str) -> ExchangeResult<Position> {
        Ok(Position::default())
    }
}
