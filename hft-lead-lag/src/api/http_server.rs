//! HTTP server for monitoring and control

use tracing::info;

/// HTTP server configuration
#[derive(Debug, Clone)]
pub struct HttpServerConfig {
    pub bind_address: String,
    pub port: u16,
}

impl Default for HttpServerConfig {
    fn default() -> Self {
        Self {
            bind_address: "0.0.0.0".to_string(),
            port: 5000,
        }
    }
}

/// HTTP server for REST API
pub struct HttpServer {
    config: HttpServerConfig,
}

impl HttpServer {
    pub fn new(config: HttpServerConfig) -> Self {
        Self { config }
    }

    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.config.bind_address, self.config.port)
    }

    /// Start the server (stub)
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("HTTP server would start on {}", self.bind_address());
        // TODO: Implement actual HTTP server with axum/warp
        Ok(())
    }
}

/// API endpoints
pub mod endpoints {
    /// Health check endpoint
    pub const HEALTH: &str = "/health";
    
    /// Metrics endpoint
    pub const METRICS: &str = "/metrics";
    
    /// Positions endpoint
    pub const POSITIONS: &str = "/api/v1/positions";
    
    /// Orders endpoint
    pub const ORDERS: &str = "/api/v1/orders";
    
    /// Config endpoint
    pub const CONFIG: &str = "/api/v1/config";
    
    /// Start trading endpoint
    pub const START_TRADING: &str = "/api/v1/trading/start";
    
    /// Stop trading endpoint
    pub const STOP_TRADING: &str = "/api/v1/trading/stop";
}
