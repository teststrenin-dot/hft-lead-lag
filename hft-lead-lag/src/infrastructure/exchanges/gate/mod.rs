//! Gate.io Futures WebSocket connector
//! 
//! Implements market data streaming and order execution via WebSocket.
//! Reference: https://www.gate.io/docs/developers/futures/ws/en/

use std::time::Duration;
use std::sync::atomic::{AtomicU64, Ordering};
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tracing::{info, warn, error, debug};

use crate::domain::{
    ExchangeId, ExchangeError, ExchangeResult,
    MarketDataStream, SubscriptionId,
    BookTicker, Trade,
    symbols::SymbolCache,
};
use crate::infrastructure::exchanges::common::{
    HmacSha512, timestamp_sec, timestamp_ms, now_ns, StampedBytes,
    extract_json_string_field, extract_json_i64_field, price_to_ticks, qty_to_ticks,
};

/// Gate.io Futures WebSocket endpoint
const GATE_WS_ENDPOINT: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
/// Bounded fan-in channel capacity (protects against OOM on 3.8 GiB server)
const MSG_CHANNEL_CAPACITY: usize = 10_000;

/// Cumulative count of market-data messages dropped due to channel backpressure.
static DROPPED_MESSAGES: AtomicU64 = AtomicU64::new(0);

/// Gate.io Futures market data connector
pub struct GateMarketData {
    /// WebSocket writer channel
    ws_tx: Option<mpsc::UnboundedSender<Message>>,
    /// Receiver for incoming messages
    msg_rx: Option<mpsc::Receiver<StampedBytes>>,
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
            ws_tx: None,
            msg_rx: None,
            symbol_cache: SymbolCache::new(),
            next_subscription_id: 1,
            api_key: None,
            api_secret: None,
            is_authenticated: false,
        }
    }

    /// Drain all pending book ticker messages, returning only the latest per symbol.
    pub fn drain_book_tickers(&mut self) -> Vec<BookTicker> {
        let rx = match self.msg_rx.as_mut() {
            Some(rx) => rx,
            None => return Vec::new(),
        };
        let mut latest: std::collections::HashMap<Bytes, BookTicker> = std::collections::HashMap::new();
        loop {
            match rx.try_recv() {
                Ok((data, recv_ts_ns)) => {
                    let data_str = String::from_utf8_lossy(&data);
                    if data_str.contains("book_ticker") {
                        if let Some(ticker) = Self::parse_book_ticker_static(&data, &self.symbol_cache, recv_ts_ns) {
                            latest.insert(ticker.symbol.clone(), ticker);
                        }
                    }
                }
                Err(_) => break,
            }
        }
        latest.into_values().collect()
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
            r#"{{"time":{},"channel":"futures.book_ticker","event":"subscribe","payload":{}}}"#,
            timestamp_ms() / 1000,
            serde_json::to_string(contracts).unwrap_or("[]".to_string())
        )
    }

    /// Build subscription message for trades
    fn build_trade_subscription(contracts: &[&str]) -> String {
        format!(
            r#"{{"time":{},"channel":"futures.trades","event":"subscribe","payload":{}}}"#,
            timestamp_ms() / 1000,
            serde_json::to_string(contracts).unwrap_or("[]".to_string())
        )
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

    /// Number of market-data messages dropped due to channel backpressure.
    pub fn dropped_messages() -> u64 {
        DROPPED_MESSAGES.load(Ordering::Relaxed)
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

        let (write_half, read_half) = futures_util::stream::StreamExt::split(ws_stream);
        let (ws_tx, ws_rx): (mpsc::UnboundedSender<Message>, mpsc::UnboundedReceiver<Message>) =
            mpsc::unbounded_channel();
        let (msg_tx, msg_rx) = mpsc::channel::<StampedBytes>(MSG_CHANNEL_CAPACITY);

        let auth_payload = if let (Some(key), Some(secret)) = (&self.api_key, &self.api_secret) {
            Some(Self::build_auth_payload(key, secret))
        } else {
            None
        };

        let is_auth = self.api_key.is_some() && self.api_secret.is_some();

        // Record subscriptions for reconnect replay
        let subs: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let auth_for_task = auth_payload.clone();

        // Single unified task: owns both read and write halves.
        // On reconnect, both halves are replaced atomically.
        tokio::spawn(async move {
            use futures_util::{StreamExt, SinkExt};
            let mut read = read_half;
            let mut write = write_half;
            let mut ws_rx = ws_rx;
            let mut reconnect_delay = Duration::from_secs(1);
            let auth_payload = auth_for_task;

            'outer: loop {
                loop {
                    tokio::select! {
                        biased;
                        msg_result = read.next() => {
                            match msg_result {
                                Some(Ok(msg)) => match msg {
                                    Message::Text(text) => {
                                        reconnect_delay = Duration::from_secs(1);
                                        let recv_ts = now_ns();
                                        if let Err(_) = msg_tx.try_send((text.into_bytes(), recv_ts)) {
                                            let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                            if n.is_power_of_two() || n % 1000 == 0 {
                                                warn!("Gate msg channel full, dropped total: {n}");
                                            }
                                        }
                                    }
                                    Message::Binary(bin) => {
                                        reconnect_delay = Duration::from_secs(1);
                                        let recv_ts = now_ns();
                                        if let Err(_) = msg_tx.try_send((bin, recv_ts)) {
                                            let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                            if n.is_power_of_two() || n % 1000 == 0 {
                                                warn!("Gate msg channel full, dropped total: {n}");
                                            }
                                        }
                                    }
                                    Message::Close(frame) => {
                                        warn!("Gate.io WebSocket closed: {:?}", frame);
                                        break; // -> reconnect
                                    }
                                    Message::Ping(data) => {
                                        let _ = write.send(Message::Pong(data)).await;
                                    }
                                    Message::Pong(_) => {}
                                    _ => {}
                                }
                                Some(Err(e)) => {
                                    error!("Gate.io WebSocket error: {}", e);
                                    break; // -> reconnect
                                }
                                None => break, // stream ended -> reconnect
                            }
                        }
                        msg = ws_rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    if let Message::Text(ref text) = msg {
                                        if text.contains("subscribe") {
                                            subs.lock().unwrap().push(text.clone());
                                        }
                                    }
                                    if write.send(msg).await.is_err() {
                                        error!("Gate.io WebSocket write error");
                                        break; // -> reconnect
                                    }
                                }
                                None => break 'outer, // channel closed -> shutdown
                            }
                        }
                    }
                }

                // Connection lost — reconnect with backoff
                warn!("Gate.io WS disconnected, reconnecting in {:?}...", reconnect_delay);
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));

                let request = match GATE_WS_ENDPOINT.into_client_request() {
                    Ok(r) => r,
                    Err(e) => { error!("Gate bad reconnect request: {}", e); continue; }
                };
                let (new_stream, _) = match connect_async(request).await {
                    Ok(s) => s,
                    Err(e) => { error!("Gate reconnect failed: {}", e); continue; }
                };
                let (new_write, new_read) = futures_util::stream::StreamExt::split(new_stream);
                read = new_read;
                write = new_write;

                // Re-authenticate on new connection
                if let Some(ref auth) = auth_payload {
                    let _ = write.send(Message::Text(auth.clone())).await;
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }

                // Replay subscriptions on the new write half
                let sub_msgs = subs.lock().unwrap().clone();
                info!("Gate.io WS reconnected, replaying {} subscriptions", sub_msgs.len());
                for msg in sub_msgs {
                    let _ = write.send(Message::Text(msg)).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        if let Some(payload) = auth_payload {
            debug!("Sending Gate.io auth payload");
            let _ = ws_tx.send(Message::Text(payload));
        }

        self.ws_tx = Some(ws_tx);
        self.msg_rx = Some(msg_rx);
        self.is_authenticated = is_auth;

        info!("Connected to Gate.io Futures WebSocket");
        Ok(())
    }

    async fn disconnect(&mut self) -> ExchangeResult<()> {
        if let Some(tx) = &self.ws_tx {
            let _ = tx.send(Message::Close(None));
        }
        self.msg_rx.take();
        self.ws_tx = None;
        info!("Disconnected from Gate.io Futures");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.ws_tx.is_some()
    }

    async fn subscribe_book_ticker(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        // Convert symbol format (BTCUSDT -> BTC_USDT for Gate)
        let contract = symbol.replace("USDT", "_USDT");
        
        let msg = Self::build_book_ticker_subscription(&[&contract]);
        
        if let Some(tx) = &self.ws_tx {
            tx.send(Message::Text(msg))
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

        let contract = symbol.replace("USDT", "_USDT");
        let msg = Self::build_trade_subscription(&[&contract]);
        
        if let Some(tx) = &self.ws_tx {
            tx.send(Message::Text(msg))
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
            let (data, recv_ts_ns) = if let Some(rx) = &mut self.msg_rx {
                rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let data_str = String::from_utf8_lossy(&data);
            let is_book_ticker = data_str.contains("futures.book_ticker") || data_str.contains("book_ticker");
            
            if is_book_ticker {
                if let Some(ticker) = Self::parse_book_ticker_static(&data, &self.symbol_cache, recv_ts_ns) {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        loop {
            let (data, recv_ts_ns) = if let Some(rx) = &mut self.msg_rx {
                rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let data_str = String::from_utf8_lossy(&data);
            let is_trade = data_str.contains("futures.trades");
            
            if is_trade {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, recv_ts_ns) {
                    return Ok(trade);
                }
            }
        }
    }
}

impl GateMarketData {
    /// Static parser to avoid borrow conflicts
    fn parse_book_ticker_static(data: &[u8], symbol_cache: &SymbolCache, local_ts_ns: i64) -> Option<BookTicker> {
        let contract = extract_json_string_field(data, "s")
            .or_else(|| extract_json_string_field(data, "contract"))
            .or_else(|| extract_json_string_field(data, "c"))?;
        
        let symbol_str = String::from_utf8_lossy(&contract)
            .replace("_USDT", "USDT")
            .replace("_USD", "USDT");
        let symbol = Bytes::from(symbol_str);

        let bid_price = extract_json_string_field(data, "b")
            .and_then(|p| price_to_ticks(&p))?;
        let ask_price = extract_json_string_field(data, "a")
            .and_then(|p| price_to_ticks(&p))?;
        let bid_qty = extract_json_string_field(data, "B")
            .and_then(|q| qty_to_ticks(&q))
            .or_else(|| extract_json_i64_field(data, "B").map(|v| v.saturating_mul(100_000_000)))
            .unwrap_or(0);
        let ask_qty = extract_json_string_field(data, "A")
            .and_then(|q| qty_to_ticks(&q))
            .or_else(|| extract_json_i64_field(data, "A").map(|v| v.saturating_mul(100_000_000)))
            .unwrap_or(0);
        
        let exchange_ts = extract_json_i64_field(data, "t")
            .or_else(|| extract_json_i64_field(data, "time_ms"))
            .unwrap_or(0);

        Some(BookTicker::new(
            symbol_cache.intern_bytes(&symbol),
            bid_price,
            ask_price,
            bid_qty,
            ask_qty,
            exchange_ts.saturating_mul(1_000_000),
            local_ts_ns,
        ))
    }

    /// Static parser to avoid borrow conflicts
    fn parse_trade_static(data: &[u8], symbol_cache: &SymbolCache, local_ts_ns: i64) -> Option<Trade> {
        let contract = extract_json_string_field(data, "c")
            .or_else(|| extract_json_string_field(data, "contract"))?;
        
        let symbol_str = String::from_utf8_lossy(&contract)
            .replace("_USDT", "USDT")
            .replace("_USD", "USDT");
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
            exchange_ts.saturating_mul(1_000_000),
            local_ts_ns,
        ))
    }
}

pub mod executor;
pub use executor::GateOrderExecutor;

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
        let contract = symbol.replace("USDT", "_USDT");
        assert_eq!(contract, "BTC_USDT");
        
        let back = contract.replace("_USDT", "USDT");
        assert_eq!(back, "BTCUSDT");
    }
}
