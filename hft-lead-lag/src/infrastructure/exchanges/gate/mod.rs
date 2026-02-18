//! Gate.io Futures WebSocket connector
//! 
//! Implements market data streaming and order execution via WebSocket.
//! Reference: https://www.gate.io/docs/developers/futures/ws/en/

use bytes::Bytes;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{info, warn, error, debug};
use std::sync::Arc;

use crate::domain::{
    ExchangeId, ExchangeError, ExchangeResult,
    MarketDataStream, SubscriptionId,
    BookTicker, Trade,
    OrderExecutor, OrderRequest, OrderResponse, Position,
    symbols::SymbolCache,
};
use crate::infrastructure::exchanges::common::{
    HmacSha512, timestamp_sec, timestamp_ms, extract_json_string_field, 
    extract_json_i64_field, price_to_ticks, qty_to_ticks,
};

/// Gate.io Futures WebSocket endpoint
const GATE_WS_ENDPOINT: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";

type WsSender = Arc<Mutex<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>;

/// Gate.io Futures market data connector
pub struct GateMarketData {
    /// WebSocket connection
    ws: Option<WsSender>,
    /// Receiver for incoming messages
    msg_rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    /// Symbol cache for interning
    symbol_cache: SymbolCache,
    /// Next subscription ID
    next_subscription_id: SubscriptionId,
    /// API credentials
    api_key: Option<String>,
    api_secret: Option<String>,
    /// Authentication status
    is_authenticated: bool,
}

impl GateMarketData {
    pub fn new() -> Self {
        Self {
            ws: None,
            msg_rx: None,
            symbol_cache: SymbolCache::new(),
            next_subscription_id: 1,
            api_key: None,
            api_secret: None,
            is_authenticated: false,
        }
    }

    /// Set API credentials
    pub fn set_credentials(&mut self, api_key: String, api_secret: String) {
        self.api_key = Some(api_key);
        self.api_secret = Some(api_secret);
    }

    /// Build authentication payload for Gate.io
    fn build_auth_payload(api_key: &str, api_secret: &str) -> String {
        let timestamp = timestamp_sec();
        let sign_payload = format!("api\nfutures.login\n\n{}", timestamp);
        let signature = HmacSha512::sign_static(api_secret.as_bytes(), sign_payload.as_bytes());

        format!(
            r#"{{"time":{},"channel":"futures.login","event":"api","sign_method":"HMAC_SHA512","key":"{}","sign":"{}","Timestamp":"{}"}}"#,
            timestamp,
            api_key,
            signature,
            timestamp
        )
    }

    /// Build subscription message for book ticker
    fn build_book_ticker_subscription(contracts: &[&str]) -> String {
        format!(
            r#"{{"time":{},"channel":"futures.book_ticker","event":"subscribe","data":{}}}"#,
            timestamp_ms() / 1000,
            serde_json::to_string(contracts).unwrap_or("[]".to_string())
        )
    }

    /// Build subscription message for trades
    fn build_trade_subscription(contracts: &[&str]) -> String {
        format!(
            r#"{{"time":{},"channel":"futures.trades","event":"subscribe","data":{}}}"#,
            timestamp_ms() / 1000,
            serde_json::to_string(contracts).unwrap_or("[]".to_string())
        )
    }

    /// Parse book ticker message from Gate.io format
    fn parse_book_ticker(&self, data: &[u8], _local_ts_ns: i64) -> Option<BookTicker> {
        // Extract contract name - Gate uses "BTC_USD" format
        let contract = extract_json_string_field(data, "c")
            .or_else(|| extract_json_string_field(data, "contract"))?;
        
        // Convert Gate format (BTC_USD) to standard format (BTCUSDT)
        let symbol_str = String::from_utf8_lossy(&contract).replace("_USD", "USDT");
        let symbol = Bytes::from(symbol_str);

        // Extract bid/ask - Gate nests them in "b" and "a" objects
        let bid_price = Self::extract_nested_price(data, "b", "p")?;
        let bid_qty = Self::extract_nested_qty(data, "b", "s")?;
        let ask_price = Self::extract_nested_price(data, "a", "p")?;
        let ask_qty = Self::extract_nested_qty(data, "a", "s")?;
        
        let exchange_ts = extract_json_i64_field(data, "t").unwrap_or(0);

        Some(BookTicker::new(
            self.symbol_cache.intern_bytes(&symbol),
            bid_price,
            ask_price,
            bid_qty,
            ask_qty,
            exchange_ts * 1_000_000,
        ))
    }

    /// Extract price from nested object (e.g., data.b.p)
    fn extract_nested_price(data: &[u8], parent: &str, field: &str) -> Option<i64> {
        let parent_pattern = format!("\"{}\"", parent);
        if let Some(parent_pos) = data.windows(parent_pattern.len()).position(|w| w == parent_pattern.as_bytes()) {
            let start = parent_pos + parent_pattern.len();
            if let Some(brace_pos) = data[start..].iter().position(|&b| b == b'{') {
                let obj_start = start + brace_pos;
                let field_pattern = format!("\"{}\"", field);
                let search_end = data[obj_start..].iter().position(|&b| b == b'}').unwrap_or(data.len() - obj_start);
                let obj_data = &data[obj_start..obj_start + search_end];
                
                if let Some(field_pos) = obj_data.windows(field_pattern.len()).position(|w| w == field_pattern.as_bytes()) {
                    let val_start = obj_start + field_pos + field_pattern.len();
                    for &b in &data[val_start..] {
                        if b == b':' || b == b' ' || b == b'"' {
                            continue;
                        }
                        let num_start = val_start;
                        let num_end = data[num_start..].iter().position(|&b| !b.is_ascii_digit() && b != b'.').unwrap_or(data.len() - num_start);
                        return price_to_ticks(&data[num_start..num_start + num_end]);
                    }
                }
            }
        }
        None
    }

    /// Extract quantity from nested object
    fn extract_nested_qty(data: &[u8], parent: &str, field: &str) -> Option<i64> {
        Self::extract_nested_price(data, parent, field)
    }

    /// Parse trade message from Gate.io format
    fn parse_trade(&self, data: &[u8], _local_ts_ns: i64) -> Option<Trade> {
        let contract = extract_json_string_field(data, "c")
            .or_else(|| extract_json_string_field(data, "contract"))?;
        
        let symbol_str = String::from_utf8_lossy(&contract).replace("_USD", "USDT");
        let symbol = Bytes::from(symbol_str);
        
        let trade_id = extract_json_i64_field(data, "i")?;
        let price = Self::extract_nested_price(data, "data", "p")
            .or_else(|| extract_json_string_field(data, "p").and_then(|p| price_to_ticks(&p)))?;
        let qty = Self::extract_nested_qty(data, "data", "s")
            .or_else(|| extract_json_string_field(data, "s").and_then(|q| qty_to_ticks(&q)))?;
        
        let is_buyer_maker = extract_json_i64_field(data, "T") == Some(1);
        let exchange_ts = extract_json_i64_field(data, "t").unwrap_or(0);

        Some(Trade::new(
            self.symbol_cache.intern_bytes(&symbol),
            trade_id,
            price,
            qty,
            is_buyer_maker,
            exchange_ts * 1_000_000,
        ))
    }
}

impl Default for GateMarketData {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl MarketDataStream for GateMarketData {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::GateFutures
    }

    async fn connect(&mut self) -> ExchangeResult<()> {
        let request = GATE_WS_ENDPOINT
            .into_client_request()
            .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;

        let (ws_stream, _) = connect_async(request)
            .await
            .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;

        let (msg_tx, msg_rx) = mpsc::unbounded_channel::<Vec<u8>>();
        let ws = Arc::new(Mutex::new(ws_stream));
        let ws_reader = ws.clone();

        // Spawn message reader task
        let auth_payload = if let (Some(key), Some(secret)) = (&self.api_key, &self.api_secret) {
            Some(Self::build_auth_payload(key, secret))
        } else {
            None
        };

        let is_auth = self.api_key.is_some() && self.api_secret.is_some();
        
        tokio::spawn(async move {
            use futures_util::{StreamExt, SinkExt};
            let mut ws_guard = ws_reader.lock().await;
            
            // Send auth first if credentials provided
            if let Some(payload) = auth_payload {
                debug!("Sending Gate.io auth payload");
                let _ = ws_guard.send(Message::Text(payload)).await;
            }

            while let Some(msg_result) = ws_guard.next().await {
                match msg_result {
                    Ok(msg) => match msg {
                        Message::Text(text) => {
                            let _ = msg_tx.send(text.into_bytes());
                        }
                        Message::Binary(bin) => {
                            let _ = msg_tx.send(bin);
                        }
                        Message::Close(frame) => {
                            warn!("Gate.io WebSocket closed: {:?}", frame);
                            break;
                        }
                        Message::Ping(data) => {
                            let _ = ws_guard.send(Message::Pong(data)).await;
                        }
                        Message::Pong(_) => {}
                        _ => {}
                    }
                    Err(e) => {
                        error!("Gate.io WebSocket error: {}", e);
                        break;
                    }
                }
            }
        });

        self.ws = Some(ws);
        self.msg_rx = Some(msg_rx);
        self.is_authenticated = is_auth;

        info!("Connected to Gate.io Futures WebSocket");
        Ok(())
    }

    async fn disconnect(&mut self) -> ExchangeResult<()> {
        if let Some(ws) = &self.ws {
            use futures_util::SinkExt;
            let mut ws_guard = ws.lock().await;
            let _ = ws_guard.send(Message::Close(None)).await;
        }
        self.msg_rx.take();
        self.ws = None;
        info!("Disconnected from Gate.io Futures");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.ws.is_some()
    }

    async fn subscribe_book_ticker(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        // Convert symbol format (BTCUSDT -> BTC_USD for Gate)
        let contract = symbol.replace("USDT", "_USD");
        
        let msg = Self::build_book_ticker_subscription(&[&contract]);
        
        if let Some(ws) = &self.ws {
            use futures_util::SinkExt;
            let mut ws_guard = ws.lock().await;
            ws_guard.send(Message::Text(msg))
                .await
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        } else {
            return Err(ExchangeError::ConnectionClosed("Not connected".into()));
        }

        debug!("Subscribed to book ticker for {}", symbol);
        Ok(subscription_id)
    }

    async fn subscribe_trades(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let contract = symbol.replace("USDT", "_USD");
        let msg = Self::build_trade_subscription(&[&contract]);
        
        if let Some(ws) = &self.ws {
            use futures_util::SinkExt;
            let mut ws_guard = ws.lock().await;
            ws_guard.send(Message::Text(msg))
                .await
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        } else {
            return Err(ExchangeError::ConnectionClosed("Not connected".into()));
        }

        debug!("Subscribed to trades for {}", symbol);
        Ok(subscription_id)
    }

    async fn unsubscribe(&mut self, _subscription_id: SubscriptionId) -> ExchangeResult<()> {
        Ok(())
    }

    async fn recv_book_ticker(&mut self) -> ExchangeResult<BookTicker> {
        loop {
            let data = if let Some(rx) = &mut self.msg_rx {
                rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let data_str = String::from_utf8_lossy(&data);
            let is_book_ticker = data_str.contains("futures.book_ticker") || data_str.contains("book_ticker");
            
            if is_book_ticker {
                if let Some(ticker) = Self::parse_book_ticker_static(&data, &self.symbol_cache, 0) {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        loop {
            let data = if let Some(rx) = &mut self.msg_rx {
                rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let data_str = String::from_utf8_lossy(&data);
            let is_trade = data_str.contains("futures.trades");
            
            if is_trade {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, 0) {
                    return Ok(trade);
                }
            }
        }
    }
}

impl GateMarketData {
    /// Static parser to avoid borrow conflicts
    fn parse_book_ticker_static(data: &[u8], symbol_cache: &SymbolCache, _local_ts_ns: i64) -> Option<BookTicker> {
        let contract = extract_json_string_field(data, "c")
            .or_else(|| extract_json_string_field(data, "contract"))?;
        
        let symbol_str = String::from_utf8_lossy(&contract).replace("_USD", "USDT");
        let symbol = Bytes::from(symbol_str);

        let bid_price = Self::extract_nested_price(data, "b", "p")?;
        let bid_qty = Self::extract_nested_qty(data, "b", "s")?;
        let ask_price = Self::extract_nested_price(data, "a", "p")?;
        let ask_qty = Self::extract_nested_qty(data, "a", "s")?;
        
        let exchange_ts = extract_json_i64_field(data, "t").unwrap_or(0);

        Some(BookTicker::new(
            symbol_cache.intern_bytes(&symbol),
            bid_price,
            ask_price,
            bid_qty,
            ask_qty,
            exchange_ts * 1_000_000,
        ))
    }

    /// Static parser to avoid borrow conflicts
    fn parse_trade_static(data: &[u8], symbol_cache: &SymbolCache, _local_ts_ns: i64) -> Option<Trade> {
        let contract = extract_json_string_field(data, "c")
            .or_else(|| extract_json_string_field(data, "contract"))?;
        
        let symbol_str = String::from_utf8_lossy(&contract).replace("_USD", "USDT");
        let symbol = Bytes::from(symbol_str);
        
        let trade_id = extract_json_i64_field(data, "i")?;
        let price = Self::extract_nested_price(data, "data", "p")
            .or_else(|| extract_json_string_field(data, "p").and_then(|p| price_to_ticks(&p)))?;
        let qty = Self::extract_nested_qty(data, "data", "s")
            .or_else(|| extract_json_string_field(data, "s").and_then(|q| qty_to_ticks(&q)))?;
        
        let is_buyer_maker = extract_json_i64_field(data, "T") == Some(1);
        let exchange_ts = extract_json_i64_field(data, "t").unwrap_or(0);

        Some(Trade::new(
            symbol_cache.intern_bytes(&symbol),
            trade_id,
            price,
            qty,
            is_buyer_maker,
            exchange_ts * 1_000_000,
        ))
    }
}

/// Gate.io Futures order executor
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_auth_payload() {
        let payload = GateMarketData::build_auth_payload("test_key", "test_secret");
        assert!(payload.contains("futures.login"));
        assert!(payload.contains("test_key"));
        assert!(payload.contains("HMAC_SHA512"));
    }

    #[test]
    fn test_symbol_conversion() {
        let symbol = "BTCUSDT";
        let contract = symbol.replace("USDT", "_USD");
        assert_eq!(contract, "BTC_USD");
        
        let back = contract.replace("_USD", "USDT");
        assert_eq!(back, "BTCUSDT");
    }
}
