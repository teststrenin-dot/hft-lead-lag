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
    pub blacklist: Vec<String>,
}

/// Gate.io configuration
#[derive(Debug, Clone, Deserialize)]
pub struct GateConfig {
    pub enabled: bool,
    pub credentials: Option<ExchangeCredentials>,
    pub blacklist: Vec<String>,
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

/// Runtime strategy selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum StrategyKind {
    #[default]
    LeadLagClassic,
    DislocationReversion,
}

impl StrategyKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LeadLagClassic => "lead_lag_classic",
            Self::DislocationReversion => "dislocation_reversion",
        }
    }
}

impl std::fmt::Display for StrategyKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Strategy selection config.
#[derive(Debug, Clone, Deserialize)]
pub struct StrategyRuntimeConfig {
    #[serde(default)]
    pub active: StrategyKind,
}

impl Default for StrategyRuntimeConfig {
    fn default() -> Self {
        Self {
            active: StrategyKind::LeadLagClassic,
        }
    }
}

/// Full application configuration
#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    pub binance: BinanceConfig,
    pub gate: GateConfig,
    pub lead_lag: Option<LeadLagConfig>,
    #[serde(default)]
    pub strategy: StrategyRuntimeConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            binance: BinanceConfig {
                enabled: true,
                credentials: None,
                blacklist: vec![],
            },
            gate: GateConfig {
                enabled: true,
                credentials: None,
                blacklist: vec![],
            },
            lead_lag: None,
            strategy: StrategyRuntimeConfig::default(),
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
        // Try loading config.toml first, fall back to defaults
        let mut config = std::fs::read_to_string("config/config.toml")
            .ok()
            .and_then(|c| toml::from_str::<AppConfig>(&c).ok())
            .unwrap_or_default();

        // Environment variables override file config for credentials
        let binance_key = std::env::var("BINANCE_API_KEY").ok();
        let binance_secret = std::env::var("BINANCE_API_SECRET").ok();
        let gate_key = std::env::var("GATE_API_KEY").ok();
        let gate_secret = std::env::var("GATE_API_SECRET").ok();

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

    /// Get active runtime strategy kind.
    pub fn strategy_kind(&self) -> StrategyKind {
        self.config.strategy.active
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn test_config_from_env() {
        std::env::set_var("BINANCE_API_KEY", "test_key");
        std::env::set_var("BINANCE_API_SECRET", "test_secret");

        let config = ConfigManager::from_env();
        assert!(config.binance_credentials().is_some());
    }

    fn write_temp_config(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hft-lead-lag-config-{name}-{}.toml",
            std::process::id()
        ));
        fs::write(&path, content).expect("write temp config");
        path
    }

    #[test]
    fn config_defaults_to_lead_lag_strategy_when_field_is_missing() {
        let path = write_temp_config(
            "default-strategy",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []
"#,
        );

        let manager =
            ConfigManager::from_file(path.to_str().expect("utf-8 temp path")).expect("load config");
        assert_eq!(manager.strategy_kind(), StrategyKind::LeadLagClassic);

        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn config_reads_explicit_strategy_selection() {
        let path = write_temp_config(
            "explicit-strategy",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[strategy]
active = "dislocation_reversion"
"#,
        );

        let manager =
            ConfigManager::from_file(path.to_str().expect("utf-8 temp path")).expect("load config");
        assert_eq!(manager.strategy_kind(), StrategyKind::DislocationReversion);

        fs::remove_file(path).expect("cleanup temp config");
    }
}
