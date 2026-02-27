//! Runtime strategy wiring:
//! - strategy selection from config
//! - uniform interface used by main event loop
//! - lead-lag adapter (current production strategy)

use crate::application::services::{LeadLagSignal, LeadLagStrategy, LeadLagStrategyConfig};
use crate::config::{ConfigManager, ExchangeId as ConfigExchangeId, StrategyKind};
use crate::domain::{BookTicker, ExchangeId, SymbolId};

const MIN_TRIGGER_SPREAD_BPS: f64 = 25.0;
const MAX_TRIGGER_SPREAD_BPS: f64 = 100.0;

/// Normalized signal type consumed by the runtime event loop.
#[derive(Debug, Clone)]
pub struct StrategySignal {
    pub strategy: &'static str,
    pub symbol: String,
    pub spread_bps: f64,
    pub direction: &'static str,
    pub bid_ask_bps: f64,
    pub ask_bid_bps: f64,
    pub context: String,
}

pub trait RuntimeStrategy: Send {
    fn strategy_name(&self) -> &'static str;
    fn on_primary_book(&mut self, ticker: BookTicker);
    fn on_hedge_book(&mut self, ticker: BookTicker);
    fn check_signal(&mut self, symbol_id: SymbolId, now_ns: i64) -> Option<StrategySignal>;
}

#[derive(Debug, thiserror::Error)]
pub enum StrategyBuildError {
    #[error("strategy '{0}' is configured but not implemented yet")]
    NotImplemented(StrategyKind),
}

/// Builds the runtime strategy declared in config.
pub fn build_runtime_strategy(
    config_manager: &ConfigManager,
    strategy_symbols: Vec<String>,
) -> Result<Box<dyn RuntimeStrategy>, StrategyBuildError> {
    match config_manager.strategy_kind() {
        StrategyKind::LeadLagClassic => {
            let config = resolve_lead_lag_config(config_manager, strategy_symbols);
            Ok(Box::new(LeadLagRuntimeStrategy::new(config)))
        }
        other => Err(StrategyBuildError::NotImplemented(other)),
    }
}

fn resolve_lead_lag_config(
    config_manager: &ConfigManager,
    strategy_symbols: Vec<String>,
) -> LeadLagStrategyConfig {
    // Keep runtime symbol universe sourced from live cross-exchange reconciliation.
    // File-level lead_lag.symbols is treated as documentation/default intent, not runtime override.
    let mut config = LeadLagStrategyConfig {
        symbols: strategy_symbols,
        ..Default::default()
    };
    if let Some(file_config) = config_manager.lead_lag_config() {
        config.primary_exchange = map_exchange_id(file_config.primary_exchange);
        config.hedge_exchange = map_exchange_id(file_config.hedge_exchange);
        config.min_entry_spread_bps = file_config
            .trigger_spread_bps
            .clamp(MIN_TRIGGER_SPREAD_BPS, MAX_TRIGGER_SPREAD_BPS);
        config.max_position_age_ms = file_config.max_position_age_ms;
        if let Some(max_quote_skew_ms) = file_config.max_quote_skew_ms {
            config.max_quote_skew_ms = max_quote_skew_ms;
        }
        if let Some(max_quote_age_ms) = file_config.max_quote_age_ms {
            config.max_quote_age_ms = max_quote_age_ms;
        }
    }
    config
}

fn map_exchange_id(exchange: ConfigExchangeId) -> ExchangeId {
    match exchange {
        ConfigExchangeId::Binance => ExchangeId::BinanceFutures,
        ConfigExchangeId::Gate => ExchangeId::GateFutures,
    }
}

struct LeadLagRuntimeStrategy {
    inner: LeadLagStrategy,
    symbols: Vec<String>,
}

impl LeadLagRuntimeStrategy {
    fn new(config: LeadLagStrategyConfig) -> Self {
        let symbols = config.symbols.clone();
        Self {
            inner: LeadLagStrategy::new(config),
            symbols,
        }
    }
}

impl RuntimeStrategy for LeadLagRuntimeStrategy {
    fn strategy_name(&self) -> &'static str {
        "lead_lag_classic"
    }

    fn on_primary_book(&mut self, ticker: BookTicker) {
        self.inner.update_primary_book(ticker);
    }

    fn on_hedge_book(&mut self, ticker: BookTicker) {
        self.inner.update_hedge_book(ticker);
    }

    fn check_signal(&mut self, symbol_id: SymbolId, now_ns: i64) -> Option<StrategySignal> {
        let symbol = self.symbols.get(symbol_id as usize)?;
        self.inner.check_signal(symbol, now_ns).map(Into::into)
    }
}

impl From<LeadLagSignal> for StrategySignal {
    fn from(signal: LeadLagSignal) -> Self {
        Self {
            strategy: "lead_lag_classic",
            symbol: signal.symbol,
            spread_bps: signal.spread_bps,
            direction: signal.direction.as_str(),
            bid_ask_bps: signal.bid_ask_bps,
            ask_bid_bps: signal.ask_bid_bps,
            context: format!(
                "leader={} lagger={} direction={} bid_ask={:.2} ask_bid={:.2}",
                signal.leader,
                signal.lagger,
                signal.direction.as_str(),
                signal.bid_ask_bps,
                signal.ask_bid_bps
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    fn write_temp_config(name: &str, content: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "hft-lead-lag-strategy-config-{name}-{}.toml",
            std::process::id()
        ));
        fs::write(&path, content).expect("write temp config");
        path
    }

    fn ticker(symbol: &str, bid: i64, ask: i64) -> BookTicker {
        BookTicker::new(
            bytes::Bytes::copy_from_slice(symbol.as_bytes()),
            bid,
            ask,
            1,
            1,
            123,
            time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
        )
    }

    #[test]
    fn lead_lag_runtime_emits_normalized_signal() {
        let mut runtime = LeadLagRuntimeStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 1.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });
        let now_ns = time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64;
        runtime.on_primary_book(ticker("BTCUSDT", 110, 111));
        runtime.on_hedge_book(ticker("BTCUSDT", 100, 101));

        let signal = runtime.check_signal(0, now_ns).expect("signal expected");
        assert_eq!(signal.strategy, "lead_lag_classic");
        assert_eq!(signal.symbol, "BTCUSDT");
        assert!(signal.spread_bps > 1.0);
        assert_eq!(signal.direction, "LONG_LAGGER");
        assert!(signal.bid_ask_bps >= signal.ask_bid_bps);
        assert!(signal.context.contains("leader="));
        assert!(signal.context.contains("direction="));
    }

    #[test]
    fn resolve_lead_lag_config_clamps_trigger_spread_low_to_25bps() {
        let path = write_temp_config(
            "clamp-low",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[lead_lag]
primary_exchange = "binance"
hedge_exchange = "gate"
trigger_spread_bps = 10.0
max_position_age_ms = 5000
symbols = ["BTCUSDT"]
"#,
        );
        let manager =
            crate::config::ConfigManager::from_file(path.to_str().expect("utf-8 temp path"))
                .expect("load config");
        let runtime = resolve_lead_lag_config(&manager, vec!["BTCUSDT".to_string()]);
        assert_eq!(runtime.min_entry_spread_bps, 25.0);
        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn resolve_lead_lag_config_clamps_trigger_spread_high_to_100bps() {
        let path = write_temp_config(
            "clamp-high",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[lead_lag]
primary_exchange = "binance"
hedge_exchange = "gate"
trigger_spread_bps = 120.0
max_position_age_ms = 5000
symbols = ["BTCUSDT"]
"#,
        );
        let manager =
            crate::config::ConfigManager::from_file(path.to_str().expect("utf-8 temp path"))
                .expect("load config");
        let runtime = resolve_lead_lag_config(&manager, vec!["BTCUSDT".to_string()]);
        assert_eq!(runtime.min_entry_spread_bps, 100.0);
        fs::remove_file(path).expect("cleanup temp config");
    }

    #[test]
    fn resolve_lead_lag_config_applies_quote_freshness_overrides() {
        let path = write_temp_config(
            "freshness-overrides",
            r#"
[binance]
enabled = true
blacklist = []

[gate]
enabled = true
blacklist = []

[lead_lag]
primary_exchange = "binance"
hedge_exchange = "gate"
trigger_spread_bps = 30.0
max_position_age_ms = 5000
max_quote_skew_ms = 750
max_quote_age_ms = 400
symbols = ["BTCUSDT"]
"#,
        );
        let manager =
            crate::config::ConfigManager::from_file(path.to_str().expect("utf-8 temp path"))
                .expect("load config");
        let runtime = resolve_lead_lag_config(&manager, vec!["BTCUSDT".to_string()]);
        assert_eq!(runtime.max_quote_skew_ms, 750);
        assert_eq!(runtime.max_quote_age_ms, 400);
        fs::remove_file(path).expect("cleanup temp config");
    }
}
