//! Gate.io Futures WebSocket connector
//!
//! Implements market data streaming and order execution via WebSocket.
//! Reference: https://www.gate.io/docs/developers/futures/ws/en/

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{client::IntoClientRequest, Message},
};
use tracing::{debug, error, info, warn};

use crate::domain::{
    symbols::SymbolCache, BookTicker, ExchangeError, ExchangeId, ExchangeResult, MarketDataStream,
    SubscriptionId, Trade,
};
use crate::infrastructure::exchanges::common::{
    contains_bytes, extract_json_bool_field_by_pattern, extract_json_i64_field_by_pattern,
    extract_json_string_field_ref, extract_json_string_field_ref_by_pattern, now_ns,
    price_to_ticks, qty_to_ticks, timestamp_ms, timestamp_sec, HmacSha512, StampedBytes,
};

/// Gate.io Futures WebSocket endpoint
const GATE_WS_ENDPOINT: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
/// Bounded fan-in channel capacity (protects against OOM on 3.8 GiB server)
const MSG_CHANNEL_CAPACITY: usize = 25_000;
const MIN_MSG_CHANNEL_CAPACITY: usize = 1_024;
const MSG_CHANNEL_CAPACITY_ENV: &str = "GATE_MSG_CHANNEL_CAPACITY";
const SUBSCRIPTION_REGISTRY_MAX: usize = 4_096;
const FIELD_S: &[u8] = b"\"s\"";
const FIELD_CONTRACT: &[u8] = b"\"contract\"";
const FIELD_C: &[u8] = b"\"c\"";
const FIELD_BID_PRICE: &[u8] = b"\"b\"";
const FIELD_BID_QTY: &[u8] = b"\"B\"";
const FIELD_ASK_PRICE: &[u8] = b"\"a\"";
const FIELD_ASK_QTY: &[u8] = b"\"A\"";
const FIELD_TS_T: &[u8] = b"\"t\"";
const FIELD_TS_TIME_MS: &[u8] = b"\"time_ms\"";
const FIELD_TRADE_ID_I: &[u8] = b"\"i\"";
const FIELD_TRADE_ID_ID: &[u8] = b"\"id\"";
const FIELD_TRADE_SIZE: &[u8] = b"\"size\"";
const FIELD_IS_BUYER_MAKER: &[u8] = b"\"m\"";
const FIELD_CREATE_TIME_MS: &[u8] = b"\"create_time_ms\"";
const EVENT_BOOK_TICKER: &[u8] = b"book_ticker";
const EVENT_BOOK_TICKER_CHANNEL: &[u8] = b"futures.book_ticker";
const EVENT_TRADES_CHANNEL: &[u8] = b"futures.trades";

/// Cumulative count of market-data messages dropped due to channel backpressure.
static DROPPED_MESSAGES: AtomicU64 = AtomicU64::new(0);

fn resolve_msg_channel_capacity(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(MSG_CHANNEL_CAPACITY)
        .max(MIN_MSG_CHANNEL_CAPACITY)
}

fn configured_msg_channel_capacity() -> usize {
    let raw = std::env::var(MSG_CHANNEL_CAPACITY_ENV).ok();
    resolve_msg_channel_capacity(raw.as_deref())
}

fn record_subscription(subs: &Arc<Mutex<Vec<String>>>, text: &str) {
    if !text.contains("subscribe") {
        return;
    }
    let mut guard = match subs.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Gate subscription registry lock poisoned; recovering");
            poisoned.into_inner()
        }
    };
    if guard.iter().any(|msg| msg == text) {
        return;
    }
    guard.push(text.to_string());
    if guard.len() > SUBSCRIPTION_REGISTRY_MAX {
        let overflow = guard.len() - SUBSCRIPTION_REGISTRY_MAX;
        guard.drain(0..overflow);
    }
}

fn snapshot_subscriptions(subs: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    match subs.lock() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => {
            warn!("Gate subscription registry lock poisoned during snapshot; recovering");
            poisoned.into_inner().clone()
        }
    }
}

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
        }
    }

    /// Drain all pending book ticker messages, returning only the latest per symbol.
    pub fn drain_book_tickers(&mut self) -> Vec<BookTicker> {
        let rx = match self.msg_rx.as_mut() {
            Some(rx) => rx,
            None => return Vec::new(),
        };
        let mut latest: std::collections::HashMap<Bytes, BookTicker> =
            std::collections::HashMap::new();
        while let Ok((data, recv_ts_ns)) = rx.try_recv() {
            if contains_bytes(&data, EVENT_BOOK_TICKER) {
                if let Some(ticker) =
                    Self::parse_book_ticker_static(&data, &self.symbol_cache, recv_ts_ns)
                {
                    latest.insert(ticker.symbol.clone(), ticker);
                }
            }
        }
        latest.into_values().collect()
    }

    /// Current bounded WS message backlog depth.
    pub fn msg_queue_depth(&self) -> usize {
        self.msg_rx.as_ref().map(|rx| rx.len()).unwrap_or(0)
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
            timestamp, api_key, signature, timestamp
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
        if let Some(parent_pos) = data
            .windows(parent_pattern.len())
            .position(|w| w == parent_pattern.as_bytes())
        {
            let start = parent_pos + parent_pattern.len();
            if let Some(brace_pos) = data[start..].iter().position(|&b| b == b'{') {
                let obj_start = start + brace_pos;
                let field_pattern = format!("\"{}\"", field);
                let search_end = data[obj_start..]
                    .iter()
                    .position(|&b| b == b'}')
                    .unwrap_or(data.len() - obj_start);
                let obj_data = &data[obj_start..obj_start + search_end];

                if let Some(field_pos) = obj_data
                    .windows(field_pattern.len())
                    .position(|w| w == field_pattern.as_bytes())
                {
                    let mut num_start = obj_start + field_pos + field_pattern.len();
                    for &b in &data[num_start..] {
                        if b == b':' || b == b' ' || b == b'"' {
                            num_start += 1;
                            continue;
                        }
                        let num_end = data[num_start..]
                            .iter()
                            .position(|&b| !b.is_ascii_digit() && b != b'.' && b != b'-')
                            .unwrap_or(data.len() - num_start);
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

    /// Extract signed integer from nested object (e.g., data.s side/size signal).
    fn extract_nested_i64(data: &[u8], parent: &str, field: &str) -> Option<i64> {
        let parent_pattern = format!("\"{}\"", parent);
        if let Some(parent_pos) = data
            .windows(parent_pattern.len())
            .position(|w| w == parent_pattern.as_bytes())
        {
            let start = parent_pos + parent_pattern.len();
            if let Some(brace_pos) = data[start..].iter().position(|&b| b == b'{') {
                let obj_start = start + brace_pos;
                let field_pattern = format!("\"{}\"", field);
                let search_end = data[obj_start..]
                    .iter()
                    .position(|&b| b == b'}')
                    .unwrap_or(data.len() - obj_start);
                let obj_data = &data[obj_start..obj_start + search_end];

                if let Some(field_pos) = obj_data
                    .windows(field_pattern.len())
                    .position(|w| w == field_pattern.as_bytes())
                {
                    let mut num_start = obj_start + field_pos + field_pattern.len();
                    for &b in &data[num_start..] {
                        if b == b':' || b == b' ' || b == b'"' {
                            num_start += 1;
                            continue;
                        }
                        let num_end = data[num_start..]
                            .iter()
                            .position(|&b| !b.is_ascii_digit() && b != b'-')
                            .unwrap_or(data.len() - num_start);
                        let raw =
                            std::str::from_utf8(&data[num_start..num_start + num_end]).ok()?;
                        return raw.parse::<i64>().ok();
                    }
                }
            }
        }
        None
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
        let (ws_tx, ws_rx): (
            mpsc::UnboundedSender<Message>,
            mpsc::UnboundedReceiver<Message>,
        ) = mpsc::unbounded_channel();
        let msg_channel_capacity = configured_msg_channel_capacity();
        let (msg_tx, msg_rx) = mpsc::channel::<StampedBytes>(msg_channel_capacity);

        let auth_payload = if let (Some(key), Some(secret)) = (&self.api_key, &self.api_secret) {
            Some(Self::build_auth_payload(key, secret))
        } else {
            None
        };

        // Record subscriptions for reconnect replay
        let subs: std::sync::Arc<std::sync::Mutex<Vec<String>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

        let auth_for_task = auth_payload.clone();

        // Single unified task: owns both read and write halves.
        // On reconnect, both halves are replaced atomically.
        tokio::spawn(async move {
            use futures_util::{SinkExt, StreamExt};
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
                                        if msg_tx.try_send((text.into_bytes(), recv_ts)).is_err() {
                                            let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                            if n.is_power_of_two() || n.is_multiple_of(1000) {
                                                warn!("Gate msg channel full, dropped total: {n}");
                                            }
                                        }
                                    }
                                    Message::Binary(bin) => {
                                        reconnect_delay = Duration::from_secs(1);
                                        let recv_ts = now_ns();
                                        if msg_tx.try_send((bin, recv_ts)).is_err() {
                                            let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                            if n.is_power_of_two() || n.is_multiple_of(1000) {
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
                                        record_subscription(&subs, text);
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
                warn!(
                    "Gate.io WS disconnected, reconnecting in {:?}...",
                    reconnect_delay
                );
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));

                let request = match GATE_WS_ENDPOINT.into_client_request() {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Gate bad reconnect request: {}", e);
                        continue;
                    }
                };
                let (new_stream, _) = match connect_async(request).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Gate reconnect failed: {}", e);
                        continue;
                    }
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
                let sub_msgs = snapshot_subscriptions(&subs);
                info!(
                    "Gate.io WS reconnected, replaying {} subscriptions",
                    sub_msgs.len()
                );
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

        info!(
            "Connected to Gate.io Futures WebSocket (msg_channel_capacity={})",
            msg_channel_capacity
        );
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
                rx.recv()
                    .await
                    .ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let is_book_ticker = contains_bytes(&data, EVENT_BOOK_TICKER_CHANNEL)
                || contains_bytes(&data, EVENT_BOOK_TICKER);

            if is_book_ticker {
                if let Some(ticker) =
                    Self::parse_book_ticker_static(&data, &self.symbol_cache, recv_ts_ns)
                {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        loop {
            let (data, recv_ts_ns) = if let Some(rx) = &mut self.msg_rx {
                rx.recv()
                    .await
                    .ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?
            } else {
                return Err(ExchangeError::ConnectionClosed("Not connected".into()));
            };

            let is_trade = contains_bytes(&data, EVENT_TRADES_CHANNEL);

            if is_trade {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, recv_ts_ns)
                {
                    return Ok(trade);
                }
            }
        }
    }
}

impl GateMarketData {
    /// Static parser to avoid borrow conflicts
    fn parse_book_ticker_static(
        data: &[u8],
        symbol_cache: &SymbolCache,
        local_ts_ns: i64,
    ) -> Option<BookTicker> {
        let contract = extract_json_string_field_ref_by_pattern(data, FIELD_S)
            .or_else(|| extract_json_string_field_ref_by_pattern(data, FIELD_CONTRACT))
            .or_else(|| extract_json_string_field_ref_by_pattern(data, FIELD_C))?;
        let symbol = symbol_cache.intern_gate_contract(contract);

        let bid_price = extract_json_string_field_ref_by_pattern(data, FIELD_BID_PRICE)
            .and_then(price_to_ticks)?;
        let ask_price = extract_json_string_field_ref_by_pattern(data, FIELD_ASK_PRICE)
            .and_then(price_to_ticks)?;
        let bid_qty = extract_json_string_field_ref_by_pattern(data, FIELD_BID_QTY)
            .and_then(qty_to_ticks)
            .or_else(|| {
                extract_json_i64_field_by_pattern(data, FIELD_BID_QTY)
                    .map(|v| v.saturating_mul(100_000_000))
            })
            .unwrap_or(0);
        let ask_qty = extract_json_string_field_ref_by_pattern(data, FIELD_ASK_QTY)
            .and_then(qty_to_ticks)
            .or_else(|| {
                extract_json_i64_field_by_pattern(data, FIELD_ASK_QTY)
                    .map(|v| v.saturating_mul(100_000_000))
            })
            .unwrap_or(0);

        let exchange_ts = extract_json_i64_field_by_pattern(data, FIELD_TS_T)
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_TS_TIME_MS))
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
    fn parse_trade_static(
        data: &[u8],
        symbol_cache: &SymbolCache,
        local_ts_ns: i64,
    ) -> Option<Trade> {
        let contract = extract_json_string_field_ref_by_pattern(data, FIELD_C)
            .or_else(|| extract_json_string_field_ref_by_pattern(data, FIELD_CONTRACT))
            .or_else(|| {
                let maybe_symbol = extract_json_string_field_ref_by_pattern(data, FIELD_S)?;
                if maybe_symbol.contains(&b'_') {
                    Some(maybe_symbol)
                } else {
                    None
                }
            })?;
        let symbol = symbol_cache.intern_gate_contract(contract);

        let trade_id = extract_json_i64_field_by_pattern(data, FIELD_TRADE_ID_I)
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_TRADE_ID_ID))?;
        let price = Self::extract_nested_price(data, "data", "p")
            .or_else(|| extract_json_string_field_ref(data, "p").and_then(price_to_ticks))?;
        let qty = Self::extract_nested_qty(data, "data", "s")
            .map(i64::saturating_abs)
            .or_else(|| {
                extract_json_i64_field_by_pattern(data, FIELD_TRADE_SIZE)
                    .map(|v| v.saturating_abs().saturating_mul(100_000_000))
            })
            .or_else(|| {
                extract_json_string_field_ref(data, "size")
                    .and_then(qty_to_ticks)
                    .map(i64::saturating_abs)
            })
            .or_else(|| extract_json_string_field_ref(data, "s").and_then(qty_to_ticks))?;

        let is_buyer_maker = extract_json_bool_field_by_pattern(data, FIELD_IS_BUYER_MAKER)
            .or_else(|| {
                extract_json_string_field_ref(data, "side").and_then(|side| {
                    if side.eq_ignore_ascii_case(b"sell") || side.eq_ignore_ascii_case(b"ask") {
                        Some(true)
                    } else if side.eq_ignore_ascii_case(b"buy") || side.eq_ignore_ascii_case(b"bid")
                    {
                        Some(false)
                    } else {
                        None
                    }
                })
            })
            .or_else(|| {
                Self::extract_nested_i64(data, "data", "s")
                    .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_TRADE_SIZE))
                    .map(|v| v < 0)
            })
            .unwrap_or(false);
        let exchange_ts = extract_json_i64_field_by_pattern(data, FIELD_TS_T)
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_CREATE_TIME_MS))
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_TS_TIME_MS))
            .unwrap_or(0);

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

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

    #[test]
    fn test_extract_nested_price_reads_field_value() {
        let payload = br#"{"data":{"p":"123.45","s":"-2"}}"#;
        let price_ticks = GateMarketData::extract_nested_price(payload, "data", "p")
            .expect("nested price parsed");
        assert_eq!(price_ticks, 12_345_000_000);
    }

    #[test]
    fn test_parse_book_ticker_normalizes_contract_symbol() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "channel":"futures.book_ticker",
            "event":"update",
            "contract":"BTC_USDT",
            "b":"50000.1",
            "B":"1.5",
            "a":"50000.2",
            "A":"2.0",
            "t":1700000000000
        }"#;

        let ticker = GateMarketData::parse_book_ticker_static(payload, &cache, 123)
            .expect("book ticker parses");
        assert_eq!(ticker.symbol.as_ref(), b"BTCUSDT");
        assert_eq!(ticker.bid_qty_ticks, 150_000_000);
        assert_eq!(ticker.ask_qty_ticks, 200_000_000);
    }

    #[test]
    fn test_parse_trade_signed_size_sets_direction_and_abs_qty() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "channel":"futures.trades",
            "event":"update",
            "data":{
                "i": 77,
                "c":"BTC_USDT",
                "p":"50000.25",
                "s":"-2",
                "t":1700000000000
            }
        }"#;
        let trade = GateMarketData::parse_trade_static(payload, &cache, 123).expect("trade parses");

        assert_eq!(trade.trade_id, 77);
        assert_eq!(trade.symbol.as_ref(), b"BTCUSDT");
        assert_eq!(trade.qty_ticks, 200_000_000);
        assert!(trade.is_buyer_maker);
    }

    #[test]
    fn test_parse_trade_side_field_sets_direction() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "id": 88,
            "contract":"ETH_USDT",
            "p":"2500.5",
            "size":"3",
            "side":"buy",
            "create_time_ms":1700000000100
        }"#;
        let trade = GateMarketData::parse_trade_static(payload, &cache, 123).expect("trade parses");
        assert!(!trade.is_buyer_maker);
    }

    #[test]
    fn resolve_msg_channel_capacity_applies_default_parse_and_min_bound() {
        assert_eq!(resolve_msg_channel_capacity(None), MSG_CHANNEL_CAPACITY);
        assert_eq!(
            resolve_msg_channel_capacity(Some("not-a-number")),
            MSG_CHANNEL_CAPACITY
        );
        assert_eq!(
            resolve_msg_channel_capacity(Some("64")),
            MIN_MSG_CHANNEL_CAPACITY
        );
        assert_eq!(resolve_msg_channel_capacity(Some("25000")), 25_000);
    }

    #[test]
    fn subscription_registry_deduplicates_subscribe_messages() {
        let subs = Arc::new(Mutex::new(Vec::new()));
        let subscribe =
            r#"{"event":"subscribe","channel":"futures.book_ticker","payload":["BTC_USDT"]}"#;

        record_subscription(&subs, subscribe);
        record_subscription(&subs, subscribe);

        assert_eq!(snapshot_subscriptions(&subs).len(), 1);
    }

    #[test]
    fn subscription_registry_ignores_non_subscribe_messages() {
        let subs = Arc::new(Mutex::new(Vec::new()));
        let non_subscribe = r#"{"event":"pong"}"#;

        record_subscription(&subs, non_subscribe);

        assert!(snapshot_subscriptions(&subs).is_empty());
    }

    #[test]
    fn subscription_registry_trims_old_entries() {
        let subs = Arc::new(Mutex::new(Vec::new()));
        for idx in 0..(SUBSCRIPTION_REGISTRY_MAX + 8) {
            let msg = format!(
                r#"{{"event":"subscribe","channel":"futures.book_ticker","payload":["S{}_USDT"]}}"#,
                idx
            );
            record_subscription(&subs, &msg);
        }

        let snapshot = snapshot_subscriptions(&subs);
        assert_eq!(snapshot.len(), SUBSCRIPTION_REGISTRY_MAX);
        assert!(
            !snapshot.iter().any(|msg| msg.contains(r#"S0_USDT"#)),
            "oldest entries should be trimmed"
        );
    }
}
