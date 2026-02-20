//! Runtime strategy wiring:
//! - strategy selection from config
//! - uniform interface used by main event loop
//! - lead-lag adapter (current production strategy)

use async_trait::async_trait;

use crate::application::services::{LeadLagSignal, LeadLagStrategy, LeadLagStrategyConfig};
use crate::config::{ConfigManager, ExchangeId as ConfigExchangeId, StrategyKind};
use crate::domain::{BookTicker, ExchangeId};

/// Normalized signal type consumed by the runtime event loop.
#[derive(Debug, Clone)]
pub struct StrategySignal {
    pub strategy: &'static str,
    pub symbol: String,
    pub spread_bps: f64,
    pub context: String,
}

#[async_trait]
pub trait RuntimeStrategy: Send + Sync {
    fn strategy_name(&self) -> &'static str;
    async fn on_primary_book(&self, ticker: BookTicker);
    async fn on_hedge_book(&self, ticker: BookTicker);
    async fn check_signal(&self, symbol: &str) -> Option<StrategySignal>;
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
        config.min_entry_spread_bps = file_config.trigger_spread_bps;
        config.max_position_age_ms = file_config.max_position_age_ms;
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
}

impl LeadLagRuntimeStrategy {
    fn new(config: LeadLagStrategyConfig) -> Self {
        Self {
            inner: LeadLagStrategy::new(config),
        }
    }
}

#[async_trait]
impl RuntimeStrategy for LeadLagRuntimeStrategy {
    fn strategy_name(&self) -> &'static str {
        "lead_lag_classic"
    }

    async fn on_primary_book(&self, ticker: BookTicker) {
        self.inner.update_primary_book(ticker).await;
    }

    async fn on_hedge_book(&self, ticker: BookTicker) {
        self.inner.update_hedge_book(ticker).await;
    }

    async fn check_signal(&self, symbol: &str) -> Option<StrategySignal> {
        self.inner.check_signal(symbol).await.map(Into::into)
    }
}

impl From<LeadLagSignal> for StrategySignal {
    fn from(signal: LeadLagSignal) -> Self {
        Self {
            strategy: "lead_lag_classic",
            symbol: signal.symbol,
            spread_bps: signal.spread_bps,
            context: format!("leader={} lagger={}", signal.leader, signal.lagger),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ticker(symbol: &str, bid: i64, ask: i64) -> BookTicker {
        BookTicker::new(
            bytes::Bytes::copy_from_slice(symbol.as_bytes()),
            bid,
            ask,
            1,
            1,
            123,
            124,
        )
    }

    #[tokio::test]
    async fn lead_lag_runtime_emits_normalized_signal() {
        let runtime = LeadLagRuntimeStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 1.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });
        runtime.on_primary_book(ticker("BTCUSDT", 110, 111)).await;
        runtime.on_hedge_book(ticker("BTCUSDT", 100, 101)).await;

        let signal = runtime
            .check_signal("BTCUSDT")
            .await
            .expect("signal expected");
        assert_eq!(signal.strategy, "lead_lag_classic");
        assert_eq!(signal.symbol, "BTCUSDT");
        assert!(signal.spread_bps > 1.0);
        assert!(signal.context.contains("leader="));
    }
}
