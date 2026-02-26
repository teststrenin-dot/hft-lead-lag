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
    /// Optional override for max allowed local quote skew between exchanges.
    pub max_quote_skew_ms: Option<u64>,
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

/// Runtime execution mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradingMode {
    Paper,
}

impl TradingMode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Paper => "paper",
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
    trading_mode: TradingMode,
}

impl ConfigManager {
    fn parse_trading_mode(raw: Option<&str>) -> Result<TradingMode, std::io::Error> {
        let Some(raw) = raw.map(str::trim).filter(|v| !v.is_empty()) else {
            return Ok(TradingMode::Paper);
        };
        if raw.eq_ignore_ascii_case("paper") {
            return Ok(TradingMode::Paper);
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Unsupported TRADING_MODE='{raw}'. Allowed values: paper"),
        ))
    }

    /// Create config from environment variables
    pub fn from_env() -> Result<Self, std::io::Error> {
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

        let trading_mode = Self::parse_trading_mode(std::env::var("TRADING_MODE").ok().as_deref())?;

        Ok(Self {
            config: Arc::new(config),
            trading_mode,
        })
    }

    /// Load config from TOML file.
    ///
    /// This path is deterministic and does not read `TRADING_MODE` from env.
    /// Runtime entrypoints that need env-based mode selection should use
    /// `ConfigManager::from_env()`.
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: AppConfig = toml::from_str(&content)?;
        Ok(Self {
            config: Arc::new(config),
            trading_mode: TradingMode::Paper,
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

    /// Get runtime execution mode.
    pub fn trading_mode(&self) -> TradingMode {
        self.trading_mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    fn env_test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock")
    }

    #[test]
    fn test_config_from_env() {
        let _lock = env_test_lock();
        std::env::remove_var("TRADING_MODE");
        std::env::set_var("BINANCE_API_KEY", "test_key");
        std::env::set_var("BINANCE_API_SECRET", "test_secret");

        let config = ConfigManager::from_env().expect("load config from env");
        assert!(config.binance_credentials().is_some());
        std::env::remove_var("BINANCE_API_KEY");
        std::env::remove_var("BINANCE_API_SECRET");
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
        let _lock = env_test_lock();
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
        let _lock = env_test_lock();
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

    #[test]
    fn from_file_ignores_trading_mode_env() {
        let _lock = env_test_lock();
        std::env::set_var("TRADING_MODE", "shadow_only");
        let path = write_temp_config(
            "from-file-ignores-env-mode",
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
        assert_eq!(manager.trading_mode().as_str(), "paper");

        std::env::remove_var("TRADING_MODE");
        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn config_defaults_to_paper_runtime_mode() {
        let _lock = env_test_lock();
        std::env::remove_var("TRADING_MODE");
        let config = ConfigManager::from_env().expect("load config from env");
        assert_eq!(config.trading_mode().as_str(), "paper");
    }

    #[test]
    fn config_accepts_explicit_paper_mode() {
        let _lock = env_test_lock();
        std::env::set_var("TRADING_MODE", "paper");
        let config = ConfigManager::from_env().expect("load config from env");
        assert_eq!(config.trading_mode().as_str(), "paper");
        std::env::remove_var("TRADING_MODE");
    }

    #[test]
    fn config_rejects_shadow_only_mode() {
        let _lock = env_test_lock();
        std::env::set_var("TRADING_MODE", "shadow_only");
        let err = ConfigManager::from_env()
            .err()
            .expect("shadow_only must be rejected");
        assert!(
            err.to_string().contains("Allowed values: paper"),
            "unexpected error: {err}"
        );
        std::env::remove_var("TRADING_MODE");
    }

    #[test]
    fn config_rejects_unknown_mode() {
        let _lock = env_test_lock();
        std::env::set_var("TRADING_MODE", "papre");
        let err = ConfigManager::from_env()
            .err()
            .expect("unknown mode must be rejected");
        assert!(
            err.to_string().contains("Allowed values: paper"),
            "unexpected error: {err}"
        );
        std::env::remove_var("TRADING_MODE");
    }
}
