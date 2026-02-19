//! Binance Futures WebSocket connector
//! 
//! Uses split WebSocket for concurrent read/write without mutex contention.

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
    OrderExecutor, OrderRequest, OrderResponse, Position,
    symbols::SymbolCache,
};
use crate::infrastructure::exchanges::common::{
    timestamp_ms, now_ns, StampedBytes, extract_json_string_field, 
    extract_json_i64_field, price_to_ticks, qty_to_ticks,
};

const BINANCE_WS_ENDPOINT: &str = "wss://fstream.binance.com/ws";
/// Bounded fan-in channel capacity (protects against OOM on 3.8 GiB server)
const MSG_CHANNEL_CAPACITY: usize = 10_000;

pub struct BinanceMarketData {
    /// WebSocket sender channels (2 symbols per socket in batch mode)
    ws_txs: Vec<mpsc::UnboundedSender<Message>>,
    /// Shared fan-in channel for all WS reader tasks
    msg_tx: Option<mpsc::Sender<StampedBytes>>,
    /// Receiver for incoming messages
    msg_rx: Option<mpsc::Receiver<StampedBytes>>,
    symbol_cache: SymbolCache,
    next_subscription_id: SubscriptionId,
    api_key: Option<String>,
}

impl BinanceMarketData {
    pub fn new() -> Self {
        Self {
            ws_txs: Vec::new(),
            msg_tx: None,
            msg_rx: None,
            symbol_cache: SymbolCache::new(),
            next_subscription_id: 1,
            api_key: None,
        }
    }

    /// Drain all pending book ticker messages, returning only the latest per symbol.
    pub fn drain_book_tickers(&mut self) -> Vec<BookTicker> {
        let rx = match self.msg_rx.as_mut() {
            Some(rx) => rx,
            None => return Vec::new(),
        };
        let mut latest: std::collections::HashMap<bytes::Bytes, BookTicker> = std::collections::HashMap::new();
        loop {
            match rx.try_recv() {
                Ok((data, recv_ts_ns)) => {
                    let data_str = String::from_utf8_lossy(&data);
                    if data_str.contains("bookTicker") {
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

    pub fn set_credentials(&mut self, api_key: String, _api_secret: String) {
        self.api_key = Some(api_key);
    }

    fn build_book_ticker_subscription(symbols: &[&str]) -> String {
        let streams: Vec<String> = symbols
            .iter()
            .map(|s| format!("{}@bookTicker", s.to_lowercase()))
            .collect();
        
        format!(
            r#"{{"method":"SUBSCRIBE","params":[{}],"id":{}}}"#,
            streams.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(","),
            timestamp_ms()
        )
    }

    fn parse_book_ticker_static(data: &[u8], symbol_cache: &SymbolCache, local_ts_ns: i64) -> Option<BookTicker> {
        let symbol = extract_json_string_field(data, "s")?;
        let bid_price = extract_json_string_field(data, "b").and_then(|p| price_to_ticks(&p))?;
        let bid_qty = extract_json_string_field(data, "B").and_then(|q| qty_to_ticks(&q))?;
        let ask_price = extract_json_string_field(data, "a").and_then(|p| price_to_ticks(&p))?;
        let ask_qty = extract_json_string_field(data, "A").and_then(|q| qty_to_ticks(&q))?;
        let exchange_ts_ms = extract_json_i64_field(data, "T")
            .or_else(|| extract_json_i64_field(data, "E"))
            .unwrap_or(0);

        Some(BookTicker::new(
            symbol_cache.intern_bytes(&symbol),
            bid_price, ask_price, bid_qty, ask_qty,
            exchange_ts_ms.saturating_mul(1_000_000),
            local_ts_ns,
        ))
    }

    fn parse_trade_static(data: &[u8], symbol_cache: &SymbolCache, local_ts_ns: i64) -> Option<Trade> {
        let symbol = extract_json_string_field(data, "s")?;
        let trade_id = extract_json_i64_field(data, "t")?;
        let price = extract_json_string_field(data, "p").and_then(|p| price_to_ticks(&p))?;
        let qty = extract_json_string_field(data, "q").and_then(|q| qty_to_ticks(&q))?;
        let is_buyer_maker = extract_json_i64_field(data, "m") == Some(1);
        let exchange_ts = extract_json_i64_field(data, "T").unwrap_or(0);

        Some(Trade::new(
            symbol_cache.intern_bytes(&symbol),
            trade_id, price, qty, is_buyer_maker, exchange_ts.saturating_mul(1_000_000),
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
        let (ws_tx, mut ws_rx): (mpsc::UnboundedSender<Message>, mpsc::UnboundedReceiver<Message>) =
            mpsc::unbounded_channel();

        tokio::spawn(async move {
            use futures_util::StreamExt;
            let mut read = read_half;

            while let Some(msg_result) = read.next().await {
                let recv_ts = now_ns();
                match msg_result {
                    Ok(Message::Text(text)) => {
                        let _ = msg_tx.try_send((text.into_bytes(), recv_ts));
                    }
                    Ok(Message::Binary(bin)) => {
                        let _ = msg_tx.try_send((bin, recv_ts));
                    }
                    Ok(Message::Close(frame)) => {
                        warn!("WS closed: {:?}", frame);
                        break;
                    }
                    Ok(Message::Ping(_data)) => {
                        debug!("Ping received");
                    }
                    Ok(Message::Pong(_)) => {
                        debug!("Pong received");
                    }
                    Err(e) => {
                        error!("WS read error: {}", e);
                        break;
                    }
                    _ => {}
                }
            }
        });

        tokio::spawn(async move {
            use futures_util::SinkExt;
            let mut write = write_half;

            while let Some(msg) = ws_rx.recv().await {
                if write.send(msg).await.is_err() {
                    error!("WS write error - connection lost");
                    break;
                }
            }
        });

        Ok(ws_tx)
    }

    /// Subscribe to many symbols using chunked requests to respect WS rate limits.
    pub async fn subscribe_book_tickers_batch(&mut self, symbols: &[String]) -> ExchangeResult<usize> {
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
            .unwrap_or(20);
        let required_ws_count = (symbols.len() + symbols_per_ws - 1) / symbols_per_ws;
        while self.ws_txs.len() < required_ws_count {
            let ws = Self::spawn_ws_worker(shared_msg_tx.clone()).await?;
            self.ws_txs.push(ws);
            tokio::time::sleep(tokio::time::Duration::from_millis(120)).await;
        }

        let mut subscribed = 0usize;
        for (socket_idx, chunk) in symbols.chunks(symbols_per_ws).enumerate() {
            let refs: Vec<&str> = chunk.iter().map(String::as_str).collect();
            let msg = Self::build_book_ticker_subscription(&refs);
            if self.ws_txs[socket_idx].send(Message::Text(msg.clone())).is_err() {
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
        let (msg_tx, msg_rx) = mpsc::channel::<StampedBytes>(MSG_CHANNEL_CAPACITY);
        let primary_ws = Self::spawn_ws_worker(msg_tx.clone()).await?;
        self.ws_txs.clear();
        self.ws_txs.push(primary_ws);
        self.msg_tx = Some(msg_tx);
        self.msg_rx = Some(msg_rx);
        info!("Connected to Binance Futures WebSocket");
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

        let msg = Self::build_book_ticker_subscription(&[symbol]);
        
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
        let rx = self.msg_rx.as_mut().ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let (data, recv_ts_ns) = rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;
            let data_str = String::from_utf8_lossy(&data);
            
            if data_str.contains("bookTicker") {
                if let Some(ticker) = Self::parse_book_ticker_static(&data, &self.symbol_cache, recv_ts_ns) {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        let rx = self.msg_rx.as_mut().ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let (data, recv_ts_ns) = rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;
            let data_str = String::from_utf8_lossy(&data);
            
            if data_str.contains("\"e\":\"trade\"") {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, recv_ts_ns) {
                    return Ok(trade);
                }
            }
        }
    }
}

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
