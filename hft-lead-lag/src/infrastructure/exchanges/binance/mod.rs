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
    timestamp_ms, extract_json_string_field, 
    extract_json_i64_field, price_to_ticks, qty_to_ticks,
};

const BINANCE_WS_ENDPOINT: &str = "wss://fstream.binance.com/ws";

pub struct BinanceMarketData {
    /// WebSocket sender channel - no mutex needed!
    ws_tx: Option<mpsc::UnboundedSender<Message>>,
    /// Receiver for incoming messages
    msg_rx: Option<mpsc::UnboundedReceiver<Vec<u8>>>,
    symbol_cache: SymbolCache,
    next_subscription_id: SubscriptionId,
    api_key: Option<String>,
}

impl BinanceMarketData {
    pub fn new() -> Self {
        Self {
            ws_tx: None,
            msg_rx: None,
            symbol_cache: SymbolCache::new(),
            next_subscription_id: 1,
            api_key: None,
        }
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

    fn parse_book_ticker_static(data: &[u8], symbol_cache: &SymbolCache, _local_ts_ns: i64) -> Option<BookTicker> {
        let symbol = extract_json_string_field(data, "s")?;
        let bid_price = extract_json_string_field(data, "b").and_then(|p| price_to_ticks(&p))?;
        let bid_qty = extract_json_string_field(data, "B").and_then(|q| qty_to_ticks(&q))?;
        let ask_price = extract_json_string_field(data, "a").and_then(|p| price_to_ticks(&p))?;
        let ask_qty = extract_json_string_field(data, "A").and_then(|q| qty_to_ticks(&q))?;
        let update_id = extract_json_i64_field(data, "u").unwrap_or(0);

        Some(BookTicker::new(
            symbol_cache.intern_bytes(&symbol),
            bid_price, ask_price, bid_qty, ask_qty,
            update_id * 1_000_000,
        ))
    }

    fn parse_trade_static(data: &[u8], symbol_cache: &SymbolCache, _local_ts_ns: i64) -> Option<Trade> {
        let symbol = extract_json_string_field(data, "s")?;
        let trade_id = extract_json_i64_field(data, "t")?;
        let price = extract_json_string_field(data, "p").and_then(|p| price_to_ticks(&p))?;
        let qty = extract_json_string_field(data, "q").and_then(|q| qty_to_ticks(&q))?;
        let is_buyer_maker = extract_json_i64_field(data, "m") == Some(1);
        let exchange_ts = extract_json_i64_field(data, "T").unwrap_or(0);

        Some(Trade::new(
            symbol_cache.intern_bytes(&symbol),
            trade_id, price, qty, is_buyer_maker, exchange_ts * 1_000_000,
        ))
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
        let request = BINANCE_WS_ENDPOINT
            .into_client_request()
            .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;

        let (ws_stream, _) = connect_async(request)
            .await
            .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;

        // CRITICAL FIX: Split WebSocket into independent read/write halves
        // Using futures_util::stream::StreamExt::split() for thread-safe splitting
        let (write_half, read_half) = futures_util::stream::StreamExt::split(ws_stream);

        // Channel for sending messages to WebSocket
        let (ws_tx, mut ws_rx): (mpsc::UnboundedSender<Message>, mpsc::UnboundedReceiver<Message>) = mpsc::unbounded_channel();
        // Channel for receiving parsed messages
        let (msg_tx, msg_rx): (mpsc::UnboundedSender<Vec<u8>>, mpsc::UnboundedReceiver<Vec<u8>>) = mpsc::unbounded_channel();

        // Reader task - owns read_half, receives messages from exchange
        tokio::spawn(async move {
            use futures_util::StreamExt;
            let mut read = read_half;
            
            while let Some(msg_result) = read.next().await {
                match msg_result {
                    Ok(Message::Text(text)) => { let _ = msg_tx.send(text.into_bytes()); }
                    Ok(Message::Binary(bin)) => { let _ = msg_tx.send(bin); }
                    Ok(Message::Close(frame)) => { warn!("WS closed: {:?}", frame); break; }
                    Ok(Message::Ping(_data)) => { debug!("Ping received"); }
                    Ok(Message::Pong(_)) => { debug!("Pong received"); }
                    Err(e) => { error!("WS read error: {}", e); break; }
                    _ => {}
                }
            }
        });

        // Writer task - owns write_half, sends messages to exchange
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

        self.ws_tx = Some(ws_tx);
        self.msg_rx = Some(msg_rx);
        info!("Connected to Binance Futures WebSocket");
        Ok(())
    }

    async fn disconnect(&mut self) -> ExchangeResult<()> {
        if let Some(tx) = &self.ws_tx {
            let _ = tx.send(Message::Close(None));
        }
        self.msg_rx.take();
        self.ws_tx = None;
        info!("Disconnected from Binance Futures");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.ws_tx.is_some()
    }

    async fn subscribe_book_ticker(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let msg = Self::build_book_ticker_subscription(&[symbol]);
        
        if let Some(tx) = &self.ws_tx {
            // NO MUTEX NEEDED! Just send to channel
            tx.send(Message::Text(msg))
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
            info!("✅ Subscribed to {}", symbol);
        } else {
            return Err(ExchangeError::ConnectionClosed("Not connected".into()));
        }

        Ok(subscription_id)
    }

    async fn subscribe_trades(&mut self, symbol: &str) -> ExchangeResult<SubscriptionId> {
        let subscription_id = self.next_subscription_id;
        self.next_subscription_id += 1;

        let msg = Self::build_book_ticker_subscription(&[symbol]);
        
        if let Some(tx) = &self.ws_tx {
            tx.send(Message::Text(msg))
                .map_err(|e| ExchangeError::WebSocketError(e.to_string()))?;
        }

        Ok(subscription_id)
    }

    async fn unsubscribe(&mut self, _subscription_id: SubscriptionId) -> ExchangeResult<()> {
        Ok(())
    }

    async fn recv_book_ticker(&mut self) -> ExchangeResult<BookTicker> {
        let rx = self.msg_rx.as_mut().ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let data = rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;
            let data_str = String::from_utf8_lossy(&data);
            
            if data_str.contains("bookTicker") {
                if let Some(ticker) = Self::parse_book_ticker_static(&data, &self.symbol_cache, 0) {
                    return Ok(ticker);
                }
            }
        }
    }

    async fn recv_trade(&mut self) -> ExchangeResult<Trade> {
        let rx = self.msg_rx.as_mut().ok_or_else(|| ExchangeError::ConnectionClosed("Not connected".into()))?;

        loop {
            let data = rx.recv().await.ok_or_else(|| ExchangeError::ConnectionClosed("Channel closed".into()))?;
            let data_str = String::from_utf8_lossy(&data);
            
            if data_str.contains("\"e\":\"trade\"") {
                if let Some(trade) = Self::parse_trade_static(&data, &self.symbol_cache, 0) {
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
