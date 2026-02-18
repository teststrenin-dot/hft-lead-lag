//! WebSocket server for streaming market data to clients

use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::RecvError;
use tracing::info;

use crate::infrastructure::rest::{BinanceRestClient, GateRestClient};

const MIN_VOLUME_USD: f64 = 1_000_000.0;
const SNAPSHOT_TIMEOUT_SECONDS: u64 = 2;

#[derive(Debug, Clone, Serialize)]
struct SnapshotRow {
    exchange: &'static str,
    symbol: String,
    quote_volume: f64,
    last_price: Option<f64>,
    price_change_24h_pct: Option<f64>,
}

/// Market data event for broadcasting
#[derive(Debug, Clone, Serialize)]
pub struct MarketDataEvent {
    pub symbol: String,
    pub exchange: String,
    pub bid: f64,
    pub ask: f64,
    pub timestamp_ns: i64,
}

/// WebSocket server configuration
#[derive(Debug, Clone)]
pub struct WsServerConfig {
    pub bind_address: String,
    pub port: u16,
    pub max_clients: usize,
}

impl Default for WsServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 8181,
            max_clients: 100,
        }
    }
}

/// WebSocket server for broadcasting market data
pub struct MarketDataServer {
    config: WsServerConfig,
    /// Broadcast channel for market data
    tx: broadcast::Sender<MarketDataEvent>,
}

impl MarketDataServer {
    pub fn new(config: WsServerConfig) -> Self {
        let (tx, _) = broadcast::channel(10000);
        Self { config, tx }
    }

    /// Get transmitter for publishing market data
    pub fn transmitter(&self) -> broadcast::Sender<MarketDataEvent> {
        self.tx.clone()
    }

    /// Publish market data event
    pub fn publish(&self, event: MarketDataEvent) {
        let _ = self.tx.send(event);
    }

    /// Get bind address
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.config.bind_address, self.config.port)
    }

    /// Start WebSocket server for market data broadcasting
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let app = Router::new()
            .route("/ws", get(ws_upgrade))
            .with_state(self.tx.clone());

        let listener = tokio::net::TcpListener::bind(self.bind_address()).await?;
        info!("Market data server listening on {}", self.bind_address());
        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(tx): State<broadcast::Sender<MarketDataEvent>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_client(socket, tx.subscribe()))
}

async fn handle_client(mut socket: WebSocket, mut rx: broadcast::Receiver<MarketDataEvent>) {
    if send_snapshot(&mut socket).await.is_err() {
        return;
    }

    loop {
        match rx.recv().await {
            Ok(event) => {
                let Ok(payload) = serde_json::to_string(&event) else {
                    continue;
                };
                if socket.send(Message::Text(payload)).await.is_err() {
                    break;
                }
            }
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => break,
        }
    }
}

async fn send_snapshot(socket: &mut WebSocket) -> Result<(), ()> {
    let binance = BinanceRestClient::new();
    let gate = GateRestClient::new();
    let (binance_tickers, gate_tickers) = tokio::join!(
        tokio::time::timeout(
            std::time::Duration::from_secs(SNAPSHOT_TIMEOUT_SECONDS),
            binance.get_tickers_with_volume(MIN_VOLUME_USD),
        ),
        tokio::time::timeout(
            std::time::Duration::from_secs(SNAPSHOT_TIMEOUT_SECONDS),
            gate.get_tickers_with_volume(MIN_VOLUME_USD),
        ),
    );

    let mut symbols: Vec<SnapshotRow> = Vec::new();

    if let Ok(Ok(tickers)) = binance_tickers {
        symbols.extend(tickers.into_iter().map(|ticker| SnapshotRow {
            exchange: "binance",
            symbol: ticker.symbol,
            quote_volume: ticker.quote_volume,
            last_price: ticker.last_price,
            price_change_24h_pct: ticker.price_change_24h_pct,
        }));
    }

    if let Ok(Ok(tickers)) = gate_tickers {
        symbols.extend(tickers.into_iter().map(|ticker| SnapshotRow {
            exchange: "gate",
            symbol: ticker.symbol,
            quote_volume: ticker.quote_volume,
            last_price: ticker.last_price,
            price_change_24h_pct: ticker.price_change_24h_pct,
        }));
    }

    let payload = serde_json::json!({
        "type": "snapshot",
        "min_volume_usd": MIN_VOLUME_USD,
        "total_symbols": symbols.len(),
        "symbols": symbols,
    })
    .to_string();
    socket.send(Message::Text(payload)).await.map_err(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_config() {
        let config = WsServerConfig::default();
        assert_eq!(config.port, 8181);
    }
}
