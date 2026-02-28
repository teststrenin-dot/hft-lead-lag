//! Binance Futures WebSocket connector
//!
//! Uses split WebSocket for concurrent read/write without mutex contention.

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
    build_strategy_symbol_id_pairs, symbols::SymbolCache, BookTicker, ExchangeError, ExchangeId,
    ExchangeResult, MarketDataStream, StrategySymbolIdCapacityError, SubscriptionId, SymbolId,
    Trade,
};
use crate::infrastructure::exchanges::common::{
    contains_bytes, extract_json_bool_field_by_pattern, extract_json_i64_field_by_pattern,
    extract_json_string_field_ref_by_pattern, now_ns, price_to_ticks, qty_to_ticks, timestamp_ms,
    StampedBytes,
};

const BINANCE_WS_ENDPOINT: &str = "wss://fstream.binance.com/ws";
/// Bounded fan-in channel capacity (protects against OOM on 3.8 GiB server)
const MSG_CHANNEL_CAPACITY: usize = 25_000;
const MIN_MSG_CHANNEL_CAPACITY: usize = 1_024;
const MSG_CHANNEL_CAPACITY_ENV: &str = "BINANCE_MSG_CHANNEL_CAPACITY";
const SUBSCRIPTION_REGISTRY_MAX: usize = 4_096;
const FIELD_S: &[u8] = b"\"s\"";
const FIELD_BID_PRICE: &[u8] = b"\"b\"";
const FIELD_BID_QTY: &[u8] = b"\"B\"";
const FIELD_ASK_PRICE: &[u8] = b"\"a\"";
const FIELD_ASK_QTY: &[u8] = b"\"A\"";
const FIELD_TRADE_PRICE: &[u8] = b"\"p\"";
const FIELD_TRADE_QTY: &[u8] = b"\"q\"";
const FIELD_TRADE_TS: &[u8] = b"\"T\"";
const FIELD_EVENT_TS: &[u8] = b"\"E\"";
const FIELD_TRADE_ID: &[u8] = b"\"t\"";
const FIELD_AGG_TRADE_ID: &[u8] = b"\"a\"";
const FIELD_IS_BUYER_MAKER: &[u8] = b"\"m\"";
const EVENT_BOOK_TICKER: &[u8] = b"bookTicker";
const EVENT_AGG_TRADE: &[u8] = b"\"e\":\"aggTrade\"";

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
    if !text.contains("SUBSCRIBE") {
        return;
    }
    let mut guard = match subs.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            warn!("Binance subscription registry lock poisoned; recovering");
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
            warn!("Binance subscription registry lock poisoned during snapshot; recovering");
            poisoned.into_inner().clone()
        }
    }
}

pub struct BinanceMarketData {
    /// WebSocket sender channels (2 symbols per socket in batch mode)
    ws_txs: Vec<mpsc::UnboundedSender<Message>>,
    /// Shared fan-in channel for all WS reader tasks
    msg_tx: Option<mpsc::Sender<StampedBytes>>,
    /// Receiver for incoming messages
    msg_rx: Option<mpsc::Receiver<StampedBytes>>,
    symbol_cache: SymbolCache,
    strategy_symbol_ids: std::collections::HashMap<Vec<u8>, SymbolId>,
    next_subscription_id: SubscriptionId,
}

impl BinanceMarketData {
    pub fn new() -> Self {
        Self {
            ws_txs: Vec::new(),
            msg_tx: None,
            msg_rx: None,
            symbol_cache: SymbolCache::new(),
            strategy_symbol_ids: std::collections::HashMap::new(),
            next_subscription_id: 1,
        }
    }

    pub fn set_strategy_symbol_ids(
        &mut self,
        strategy_symbols: &[String],
    ) -> Result<(), StrategySymbolIdCapacityError> {
        let pairs = build_strategy_symbol_id_pairs(strategy_symbols)?;
        self.strategy_symbol_ids = pairs
            .into_iter()
            .map(|(symbol, symbol_id)| (symbol.to_vec(), symbol_id))
            .collect();
        Ok(())
    }

    fn upsert_latest_ticker_for_drain(
        ticker: BookTicker,
        latest_by_id: &mut std::collections::HashMap<SymbolId, BookTicker>,
        latest_by_symbol: &mut std::collections::HashMap<bytes::Bytes, BookTicker>,
    ) {
        if let Some(symbol_id) = ticker.strategy_symbol_id {
            latest_by_id.insert(symbol_id, ticker);
            return;
        }
        latest_by_symbol.insert(ticker.symbol.clone(), ticker);
    }

    /// Drain all pending book ticker messages, returning only the latest per symbol.
    pub fn drain_book_tickers(&mut self) -> Vec<BookTicker> {
        let rx = match self.msg_rx.as_mut() {
            Some(rx) => rx,
            None => return Vec::new(),
        };
        let mut latest_by_id: std::collections::HashMap<SymbolId, BookTicker> =
            std::collections::HashMap::new();
        let mut latest_by_symbol: std::collections::HashMap<bytes::Bytes, BookTicker> =
            std::collections::HashMap::new();
        while let Ok((data, recv_ts_ns)) = rx.try_recv() {
            if contains_bytes(&data, EVENT_BOOK_TICKER) {
                if let Some(ticker) = Self::parse_book_ticker_static(
                    &data,
                    &self.symbol_cache,
                    &self.strategy_symbol_ids,
                    recv_ts_ns,
                ) {
                    Self::upsert_latest_ticker_for_drain(
                        ticker,
                        &mut latest_by_id,
                        &mut latest_by_symbol,
                    );
                }
            }
        }
        let mut latest =
            Vec::with_capacity(latest_by_id.len().saturating_add(latest_by_symbol.len()));
        latest.extend(latest_by_id.into_values());
        latest.extend(latest_by_symbol.into_values());
        latest
    }

    /// Current bounded WS message backlog depth.
    pub fn msg_queue_depth(&self) -> usize {
        self.msg_rx.as_ref().map(|rx| rx.len()).unwrap_or(0)
    }

    pub fn set_credentials(&mut self, _api_key: String, _api_secret: String) {}

    fn build_book_ticker_subscription(symbols: &[&str]) -> String {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();

        format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":{}}}"#,
            streams
                .iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(","),
            timestamp_ms()
        )
    }

    fn build_trade_subscription(symbols: &[&str]) -> String {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@aggTrade", s.to_lowercase()))
            .collect();

        format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":{}}}"#,
            streams
                .iter()
                .map(|s| format!("\"{}\"", s))
                .collect::<Vec<_>>()
                .join(","),
            timestamp_ms()
        )
    }

    fn parse_book_ticker_static(
        data: &[u8],
        symbol_cache: &SymbolCache,
        strategy_symbol_ids: &std::collections::HashMap<Vec<u8>, SymbolId>,
        local_ts_ns: i64,
    ) -> Option<BookTicker> {
        let symbol = extract_json_string_field_ref_by_pattern(data, FIELD_S)?;
        let symbol_id = strategy_symbol_ids.get(symbol).copied();
        let bid_price = extract_json_string_field_ref_by_pattern(data, FIELD_BID_PRICE)
            .and_then(price_to_ticks)?;
        let bid_qty =
            extract_json_string_field_ref_by_pattern(data, FIELD_BID_QTY).and_then(qty_to_ticks)?;
        let ask_price = extract_json_string_field_ref_by_pattern(data, FIELD_ASK_PRICE)
            .and_then(price_to_ticks)?;
        let ask_qty =
            extract_json_string_field_ref_by_pattern(data, FIELD_ASK_QTY).and_then(qty_to_ticks)?;
        let exchange_ts_ms = extract_json_i64_field_by_pattern(data, FIELD_TRADE_TS)
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_EVENT_TS))
            .unwrap_or(0);

        Some(
            BookTicker::new(
                symbol_cache.intern_bytes(symbol),
                bid_price,
                ask_price,
                bid_qty,
                ask_qty,
                exchange_ts_ms.saturating_mul(1_000_000),
                local_ts_ns,
            )
            .with_strategy_symbol_id(symbol_id),
        )
    }

    fn parse_trade_static(
        data: &[u8],
        symbol_cache: &SymbolCache,
        local_ts_ns: i64,
    ) -> Option<Trade> {
        let symbol = extract_json_string_field_ref_by_pattern(data, FIELD_S)?;
        let trade_id = extract_json_i64_field_by_pattern(data, FIELD_TRADE_ID)
            .or_else(|| extract_json_i64_field_by_pattern(data, FIELD_AGG_TRADE_ID))?;
        let price = extract_json_string_field_ref_by_pattern(data, FIELD_TRADE_PRICE)
            .and_then(price_to_ticks)?;
        let qty = extract_json_string_field_ref_by_pattern(data, FIELD_TRADE_QTY)
            .and_then(qty_to_ticks)?;
        let is_buyer_maker =
            extract_json_bool_field_by_pattern(data, FIELD_IS_BUYER_MAKER).unwrap_or(false);
        let exchange_ts = extract_json_i64_field_by_pattern(data, FIELD_TRADE_TS).unwrap_or(0);

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

    async fn spawn_ws_worker(
        msg_tx: mpsc::Sender<StampedBytes>,
    ) -> ExchangeResult<mpsc::UnboundedSender<Message>> {
        let request = BINANCE_WS_ENDPOINT
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

        let subs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

        // Single unified task: owns both read and write halves.
        // On reconnect, both halves are replaced atomically.
        tokio::spawn(async move {
            use futures_util::{SinkExt, StreamExt};
            let mut read = read_half;
            let mut write = write_half;
            let mut ws_rx = ws_rx;
            let mut reconnect_delay = Duration::from_secs(1);

            'outer: loop {
                // Inner select loop: forward messages in both directions
                loop {
                    tokio::select! {
                        biased;
                        msg_result = read.next() => {
                            match msg_result {
                                Some(Ok(Message::Text(text))) => {
                                    reconnect_delay = Duration::from_secs(1);
                                    let recv_ts = now_ns();
                                    if msg_tx.try_send((text.into_bytes(), recv_ts)).is_err() {
                                        let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                        if n.is_power_of_two() || n.is_multiple_of(1000) {
                                            warn!("Binance msg channel full, dropped total: {n}");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(bin))) => {
                                    reconnect_delay = Duration::from_secs(1);
                                    let recv_ts = now_ns();
                                    if msg_tx.try_send((bin, recv_ts)).is_err() {
                                        let n = DROPPED_MESSAGES.fetch_add(1, Ordering::Relaxed) + 1;
                                        if n.is_power_of_two() || n.is_multiple_of(1000) {
                                            warn!("Binance msg channel full, dropped total: {n}");
                                        }
                                    }
                                }
                                Some(Ok(Message::Close(frame))) => {
                                    warn!("Binance WS closed: {:?}", frame);
                                    break; // -> reconnect
                                }
                                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                                Some(Err(e)) => {
                                    error!("Binance WS read error: {}", e);
                                    break; // -> reconnect
                                }
                                None => break, // stream ended -> reconnect
                                _ => {}
                            }
                        }
                        msg = ws_rx.recv() => {
                            match msg {
                                Some(msg) => {
                                    if let Message::Text(ref text) = msg {
                                        record_subscription(&subs, text);
                                    }
                                    if write.send(msg).await.is_err() {
                                        error!("Binance WS write error - connection lost");
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
                    "Binance WS disconnected, reconnecting in {:?}...",
                    reconnect_delay
                );
                tokio::time::sleep(reconnect_delay).await;
                reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(30));

                let request = match BINANCE_WS_ENDPOINT.into_client_request() {
                    Ok(r) => r,
                    Err(e) => {
                        error!("Bad reconnect request: {}", e);
                        continue;
                    }
                };
                let (new_stream, _) = match connect_async(request).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Binance reconnect failed: {}", e);
                        continue;
                    }
                };
                let (new_write, new_read) = futures_util::stream::StreamExt::split(new_stream);
                read = new_read;
                write = new_write;

                // Replay subscriptions on the new write half
                let sub_msgs = snapshot_subscriptions(&subs);
                info!(
                    "Binance WS reconnected, replaying {} subscriptions",
                    sub_msgs.len()
                );
                for msg in sub_msgs {
                    let _ = write.send(Message::Text(msg)).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        Ok(ws_tx)
    }

    /// Number of market-data messages dropped due to channel backpressure.
    pub fn dropped_messages() -> u64 {
        DROPPED_MESSAGES.load(Ordering::Relaxed)
    }

    /// Subscribe to many symbols using chunked requests to respect WS rate limits.
    pub async fn subscribe_book_tickers_batch(
        &mut self,
        symbols: &[String],
    ) -> ExchangeResult<usize> {
        if symbols.is_empty() {
            return Ok(0);
        }
        let shared_msg_tx = self
            .msg_tx
            .as_ref()
            .cloned()
            .ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        let symbols_per_ws: usize = std::env::var("SYMBOLS_PER_WS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(20)
            .max(1);
        let required_ws_count = symbols.len().div_ceil(symbols_per_ws);
        while self.ws_txs.len() < required_ws_count {
            let ws = Self::spawn_ws_worker(shared_msg_tx.clone()).await?;
            self.ws_txs.push(ws);
            tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
        }

        let mut subscribed = 0usize;
        for (socket_idx, chunk) in symbols.chunks(symbols_per_ws).enumerate() {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            let msg = Self::build_book_ticker_subscription(&refs);
            if self.ws_txs[socket_idx]
                .send(Message::Text(msg.clone()))
                .is_err()
            {
                let replacement = Self::spawn_ws_worker(shared_msg_tx.clone()).await?;
                self.ws_txs[socket_idx] = replacement;
                self.ws_txs[socket_idx]
                    .send(Message::Text(msg))
                    .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
            }
            subscribed += chunk.len();
            tokio::time::sleep(tokio::time::Duration::from_millis(90)).await;
        }

        info!(
            "Binance socket allocation: symbols={} sockets={} symbols_per_ws={}",
            symbols.len(),
            required_ws_count,
            symbols_per_ws
        );

        Ok(subscribed)
    }
}

impl Default for BinanceMarketData {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl MarketDataStream for BinanceMarketData {
    fn exchange_id(&self) -> ExchangeId {
        ExchangeId::BinanceFutures
    }

    async fn connect(&mut self) -> ExchangeResult<()> {
        let msg_channel_capacity = configured_msg_channel_capacity();
        let (msg_tx, msg_rx) = mpsc::channel::<StampedBytes>(msg_channel_capacity);
        let primary_ws = Self::spawn_ws_worker(msg_tx.clone()).await?;
        self.ws_txs.clear();
        self.ws_txs.push(primary_ws);
        self.msg_tx = Some(msg_tx);
        self.msg_rx = Some(msg_rx);
        info!(
            "Connected to Binance Futures WebSocket (msg_channel_capacity={})",
            msg_channel_capacity
        );
        Ok(())
    }

    async fn disconnect(&mut self) -> ExchangeResult<()> {
        for tx in &self.ws_txs {
            let _ = tx.send(Message::Close(None));
        }
        self.ws_txs.clear();
        self.msg_tx = None;
        self.msg_rx.take();
        info!("Disconnected from Binance Futures");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        !self.ws_txs.is_empty()
    }

    async fn subscribe_book_ticker(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let msg = Self::build_book_ticker_subscription(&[symbol]);

        if let Some(tx) = self.ws_txs.first() {
            // NO MUTEX NEEDED! Just send to channel
            tx.send(Message::Text(msg))
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
            debug!("Subscribed to {}", symbol);
        } else {
            return Err(ExchangeError::ConnectionClosed("Not connected".into()));
        }

        Ok(subscription_id)
    }

    async fn subscribe_trades(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let msg = Self::build_trade_subscription(&[symbol]);

        if let Some(tx) = self.ws_txs.first() {
            tx.send(Message::Text(msg))
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        } else {
            return Err(ExchangeError::ConnectionClosed("Not connected".into()));
        }

        Ok(subscription_id)
    }

    async fn unsubscribe(&mut self, _subscription_id: SubscriptionId) -> ExchangeResult<()> {
        Ok(())
    }

    async fn recv_book_ticker(&mut self) -> ExchangeResult<BookTicker> {
        let rx = self
            .msg_rx
            .as_mut()
            .ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let (data, recv_ts_ns) = rx
                .recv()
                .await
                .ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;

            if contains_bytes(&data, EVENT_BOOK_TICKER) {
                if let Some(ticker) = Self::parse_book_ticker_static(
                    &data,
                    &self.symbol_cache,
                    &self.strategy_symbol_ids,
                    recv_ts_ns,
                ) {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        let rx = self
            .msg_rx
            .as_mut()
            .ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let (data, recv_ts_ns) = rx
                .recv()
                .await
                .ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;

            if contains_bytes(&data, EVENT_AGG_TRADE) {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, recv_ts_ns)
                {
                    return Ok(trade);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn parse_agg_trade_bool_and_id() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "e":"aggTrade",
            "E":123456789,
            "s":"BTCUSDT",
            "a":5933014,
            "p":"50000.12",
            "q":"0.02",
            "T":123456785,
            "m":true
        }"#;
        let trade =
            BinanceMarketData::parse_trade_static(payload, &cache, 42).expect("agg trade parses");

        assert_eq!(trade.symbol.as_ref(), b"BTCUSDT");
        assert_eq!(trade.trade_id, 5_933_014);
        assert!(trade.is_buyer_maker);
        assert!(trade.qty_ticks > 0);
    }

    #[test]
    fn parse_book_ticker_uses_interned_symbol_without_copying_field_buffer() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "e":"bookTicker",
            "s":"BTCUSDT",
            "b":"50000.1",
            "B":"1.5",
            "a":"50000.2",
            "A":"2.0",
            "T":1700000000000
        }"#;

        let strategy_symbol_ids = std::collections::HashMap::new();
        let ticker =
            BinanceMarketData::parse_book_ticker_static(payload, &cache, &strategy_symbol_ids, 99)
                .expect("ticker parses");
        assert_eq!(ticker.symbol.as_ref(), b"BTCUSDT");
        assert_eq!(ticker.bid_qty_ticks, 150_000_000);
        assert_eq!(ticker.ask_qty_ticks, 200_000_000);
    }

    #[test]
    fn parse_book_ticker_sets_preconfigured_strategy_symbol_id() {
        let mut market = BinanceMarketData::new();
        market
            .set_strategy_symbol_ids(&["BTCUSDT".to_string(), "ETHUSDT".to_string()])
            .expect("symbol-id map");
        let payload = br#"{
            "e":"bookTicker",
            "s":"ETHUSDT",
            "b":"2500.1",
            "B":"1.0",
            "a":"2500.2",
            "A":"1.1",
            "T":1700000000000
        }"#;

        let ticker = BinanceMarketData::parse_book_ticker_static(
            payload,
            &market.symbol_cache,
            &market.strategy_symbol_ids,
            7,
        )
        .expect("ticker parses");
        assert_eq!(ticker.strategy_symbol_id, Some(1));
    }

    #[test]
    fn set_strategy_symbol_ids_keeps_first_seen_id_for_duplicates() {
        let mut market = BinanceMarketData::new();
        market
            .set_strategy_symbol_ids(&[
                "BTCUSDT".to_string(),
                "ETHUSDT".to_string(),
                "BTCUSDT".to_string(),
            ])
            .expect("symbol-id map");
        assert_eq!(
            market.strategy_symbol_ids.get(b"BTCUSDT".as_slice()),
            Some(&0)
        );
    }

    #[test]
    fn drain_dedupe_uses_strategy_symbol_id_when_present() {
        let mut latest_by_id: std::collections::HashMap<SymbolId, BookTicker> =
            std::collections::HashMap::new();
        let mut latest_by_symbol: std::collections::HashMap<bytes::Bytes, BookTicker> =
            std::collections::HashMap::new();

        let first = BookTicker::new(bytes::Bytes::from_static(b"BTCUSDT"), 1, 2, 3, 4, 5, 6)
            .with_strategy_symbol_id(Some(0));
        let newer = BookTicker::new(bytes::Bytes::from_static(b"BTCUSDT"), 7, 8, 3, 4, 9, 10)
            .with_strategy_symbol_id(Some(0));

        BinanceMarketData::upsert_latest_ticker_for_drain(
            first,
            &mut latest_by_id,
            &mut latest_by_symbol,
        );
        BinanceMarketData::upsert_latest_ticker_for_drain(
            newer,
            &mut latest_by_id,
            &mut latest_by_symbol,
        );

        assert_eq!(latest_by_id.len(), 1);
        assert!(latest_by_symbol.is_empty());
        assert_eq!(
            latest_by_id.get(&0).map(|ticker| ticker.bid_price_ticks),
            Some(7)
        );
    }

    #[test]
    fn parse_trade_supports_numeric_m_flag() {
        let cache = SymbolCache::new();
        let payload = br#"{
            "s":"ETHUSDT",
            "t":1234,
            "p":"2500.5",
            "q":"1.25",
            "T":1700000000000,
            "m":0
        }"#;
        let trade =
            BinanceMarketData::parse_trade_static(payload, &cache, 7).expect("trade parses");

        assert_eq!(trade.trade_id, 1234);
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
        let subscribe = r#"{"method":"SUBSCRIBE","params":["btcusdt@bookTicker"],"id":1}"#;

        record_subscription(&subs, subscribe);
        record_subscription(&subs, subscribe);

        assert_eq!(snapshot_subscriptions(&subs).len(), 1);
    }

    #[test]
    fn subscription_registry_ignores_non_subscribe_messages() {
        let subs = Arc::new(Mutex::new(Vec::new()));
        let non_subscribe = r#"{"method":"PING"}"#;

        record_subscription(&subs, non_subscribe);

        assert!(snapshot_subscriptions(&subs).is_empty());
    }

    #[test]
    fn subscription_registry_trims_old_entries() {
        let subs = Arc::new(Mutex::new(Vec::new()));
        for idx in 0..(SUBSCRIPTION_REGISTRY_MAX + 8) {
            let msg = format!(
                r#"{{"method":"SUBSCRIBE","params":["s{}@bookTicker"],"id":{}}}"#,
                idx, idx
            );
            record_subscription(&subs, &msg);
        }

        let snapshot = snapshot_subscriptions(&subs);
        assert_eq!(snapshot.len(), SUBSCRIPTION_REGISTRY_MAX);
        assert!(
            !snapshot.iter().any(|msg| msg.contains(r#""id":0"#)),
            "oldest entries should be trimmed"
        );
    }
}
