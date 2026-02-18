//! Configuration module for API keys and exchange settings
//! 
//! Loads configuration from environment variables or config files.

use serde::Deserialize;
use std::sync::Arc;

/// Exchange credentials
#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeCredentials {
    pub api_key: String,
    pub api_secret: String,
}

/// Binance configuration
#[derive(Debug, Clone, Deserialize)]
pub struct BinanceConfig {
    pub enabled: bool,
    pub credentials: Option<ExchangeCredentials>,
    pub volume_filter: Option<VolumeFilter>,
    pub blacklist: Vec<String>,
}

/// Gate.io configuration
#[derive(Debug, Clone, Deserialize)]
pub struct GateConfig {
    pub enabled: bool,
    pub credentials: Option<ExchangeCredentials>,
    pub volume_filter: Option<VolumeFilter>,
    pub blacklist: Vec<String>,
}

/// Volume filter settings
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeFilter {
    pub min_usd_volume: f64,
    pub max_usd_volume: f64,
}

/// Trading bot configuration
#[derive(Debug, Clone, Deserialize)]
pub struct TradingConfig {
    pub order_qty_usd: f64,
    pub min_min_spread: f64,
    pub min_profit_spread: f64,
    pub max_position_size_usd: f64,
}

/// Lead-lag strategy configuration
#[derive(Debug, Clone, Deserialize)]
pub struct LeadLagConfig {
    /// Primary exchange (leads)
    pub primary_exchange: ExchangeId,
    /// Hedge exchange (lags)
    pub hedge_exchange: ExchangeId,
    /// Minimum spread to trigger (in basis points)
    pub trigger_spread_bps: f64,
    /// Maximum position age in milliseconds
    pub max_position_age_ms: u64,
    /// Symbols to trade
    pub symbols: Vec<String>,
}

/// Exchange identifier for config
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExchangeId {
    Binance,
    Gate,
}

/// Full application configuration
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub binance: BinanceConfig,
    pub gate: GateConfig,
    pub trading: Option<TradingConfig>,
    pub lead_lag: Option<LeadLagConfig>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            binance: BinanceConfig {
                enabled: true,
                credentials: None,
                volume_filter: None,
                blacklist: vec![],
            },
            gate: GateConfig {
                enabled: true,
                credentials: None,
                volume_filter: None,
                blacklist: vec![],
            },
            trading: None,
            lead_lag: None,
        }
    }
}

/// Configuration manager
pub struct ConfigManager {
    config: Arc<AppConfig>,
}

impl ConfigManager {
    /// Create config from environment variables
    pub fn from_env() -> Self {
        let binance_key = std::env::var("BINANCE_API_KEY").ok();
        let binance_secret = std::env::var("BINANCE_API_SECRET").ok();
        let gate_key = std::env::var("GATE_API_KEY").ok();
        let gate_secret = std::env::var("GATE_API_SECRET").ok();

        let mut config = AppConfig::default();

        if let (Some(key), Some(secret)) = (binance_key, binance_secret) {
            config.binance.credentials = Some(ExchangeCredentials {
                api_key: key,
                api_secret: secret,
            });
        }

        if let (Some(key), Some(secret)) = (gate_key, gate_secret) {
            config.gate.credentials = Some(ExchangeCredentials {
                api_key: key,
                api_secret: secret,
            });
        }

        Self {
            config: Arc::new(config),
        }
    }

    /// Load config from TOML file
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(Self {
            config: Arc::new(config),
        })
    }

    /// Get Binance credentials
    pub fn binance_credentials(&self) -> Option<&ExchangeCredentials> {
        self.config.binance.credentials.as_ref()
    }

    /// Get Gate credentials
    pub fn gate_credentials(&self) -> Option<&ExchangeCredentials> {
        self.config.gate.credentials.as_ref()
    }

    /// Check if Binance is enabled
    pub fn is_binance_enabled(&self) -> bool {
        self.config.binance.enabled
    }

    /// Check if Gate is enabled
    pub fn is_gate_enabled(&self) -> bool {
        self.config.gate.enabled
    }

    /// Get blacklist for Binance
    pub fn binance_blacklist(&self) -> &[String] {
        &self.config.binance.blacklist
    }

    /// Get blacklist for Gate
    pub fn gate_blacklist(&self) -> &[String] {
        &self.config.gate.blacklist
    }

    /// Get lead-lag config
    pub fn lead_lag_config(&self) -> Option<&LeadLagConfig> {
        self.config.lead_lag.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        std::env::set_var("BINANCE_API_KEY", "test_key");
        std::env::set_var("BINANCE_API_SECRET", "test_secret");
        
        let config = ConfigManager::from_env();
        assert!(config.binance_credentials().is_some());
    }
}
