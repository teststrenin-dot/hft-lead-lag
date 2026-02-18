//! WebSocket server for streaming market data to clients

use tokio::sync::broadcast;
use tracing::info;

/// Market data event for broadcasting
#[derive(Debug, Clone)]
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

    /// Start the server (stub - full implementation needs hyper/tokio-tungstenite server)
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Market data server would start on {}", self.bind_address());
        // TODO: Implement actual WebSocket server
        Ok(())
    }
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
