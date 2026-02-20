//! Lead-Lag Strategy Service
//! 
//! Implements the core HFT lead-lag trading logic:
//! - Detects price leadership between exchanges
//! - Executes arbitrage when spread exceeds threshold
//! - Manages position lifecycle

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::domain::{
    BookTicker, ExchangeId,
    messages::ticks_to_decimal,
};

/// Lead-lag signal
#[derive(Debug, Clone)]
pub struct LeadLagSignal {
    /// Symbol being traded
    pub symbol: String,
    /// Leading exchange
    pub leader: ExchangeId,
    /// Lagging exchange
    pub lagger: ExchangeId,
    /// Leader bid price (ticks)
    pub leader_bid_ticks: i64,
    /// Leader ask price (ticks)
    pub leader_ask_ticks: i64,
    /// Lagger bid price (ticks)
    pub lagger_bid_ticks: i64,
    /// Lagger ask price (ticks)
    pub lagger_ask_ticks: i64,
    /// Spread in basis points
    pub spread_bps: f64,
    /// Timestamp of signal (ns)
    pub timestamp_ns: i64,
}

impl LeadLagSignal {
    /// Calculate spread in basis points
    pub fn calculate_spread_bps(leader_price: f64, lagger_price: f64) -> f64 {
        if lagger_price == 0.0 {
            return 0.0;
        }
        ((leader_price - lagger_price) / lagger_price) * 10000.0
    }
}

#[inline]
fn directional_spread_bps(top: i64, bottom: i64) -> f64 {
    LeadLagSignal::calculate_spread_bps(ticks_to_decimal(top), ticks_to_decimal(bottom))
}

/// Position state
#[derive(Debug, Clone)]
pub struct PositionState {
    pub symbol: String,
    pub entry_time_ns: i64,
    pub primary_side: crate::domain::Side,
    pub primary_qty: f64,
    pub hedge_qty: f64,
    pub entry_spread_bps: f64,
    pub current_spread_bps: f64,
    pub unrealized_pnl_usd: f64,
}

/// Lead-lag strategy configuration
#[derive(Debug, Clone)]
pub struct LeadLagStrategyConfig {
    /// Primary exchange (the one we consider as leader)
    pub primary_exchange: ExchangeId,
    /// Hedge exchange
    pub hedge_exchange: ExchangeId,
    /// Minimum spread to enter (basis points)
    pub min_entry_spread_bps: f64,
    /// Target spread to exit (basis points)
    pub target_exit_spread_bps: f64,
    /// Maximum position age before forced exit (ms)
    pub max_position_age_ms: u64,
    /// Order quantity in USD
    pub order_qty_usd: f64,
    /// Symbols to trade
    pub symbols: Vec<String>,
}

impl Default for LeadLagStrategyConfig {
    fn default() -> Self {
        Self {
            primary_exchange: ExchangeId::BinanceFutures,
            hedge_exchange: ExchangeId::GateFutures,
            min_entry_spread_bps: 30.0, // 0.30%
            target_exit_spread_bps: 1.0, // 0.01%
            max_position_age_ms: 5000, // 5 seconds
            order_qty_usd: 10.0,
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
        }
    }
}

/// Lead-lag strategy service
pub struct LeadLagStrategy {
    config: LeadLagStrategyConfig,
    /// Current positions
    positions: Arc<RwLock<Vec<PositionState>>>,
    /// Latest book tickers from primary exchange
    primary_books: Arc<RwLock<std::collections::HashMap<String, BookTicker>>>,
    /// Latest book tickers from hedge exchange
    hedge_books: Arc<RwLock<std::collections::HashMap<String, BookTicker>>>,
}

impl LeadLagStrategy {
    pub fn new(config: LeadLagStrategyConfig) -> Self {
        Self {
            config,
            positions: Arc::new(RwLock::new(Vec::new())),
            primary_books: Arc::new(RwLock::new(std::collections::HashMap::new())),
            hedge_books: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Update primary exchange book ticker
    pub async fn update_primary_book(&self, ticker: BookTicker) {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        let mut books = self.primary_books.write().await;
        books.insert(symbol, ticker);
    }

    /// Update hedge exchange book ticker
    pub async fn update_hedge_book(&self, ticker: BookTicker) {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        let mut books = self.hedge_books.write().await;
        books.insert(symbol, ticker);
    }

    /// Check for lead-lag signal
    pub async fn check_signal(&self, symbol: &str) -> Option<LeadLagSignal> {
        let primary_books = self.primary_books.read().await;
        let hedge_books = self.hedge_books.read().await;

        let primary = primary_books.get(symbol)?;
        let hedge = hedge_books.get(symbol)?;

        // Binance(primary) is the oracle. If hedge quote is newer, skip this cycle:
        // we do not trade when lead source is unclear.
        if primary.exchange_ts_ns < hedge.exchange_ts_ns {
            return None;
        }

        // Two directional legs, no mid-price mixing:
        // 1) bid/ask leg:  primary.bid  vs hedge.ask  (up-dislocation)
        // 2) ask/bid leg:  hedge.bid    vs primary.ask (down-dislocation)
        let bid_ask_bps = directional_spread_bps(primary.bid_price_ticks, hedge.ask_price_ticks);
        let ask_bid_bps = directional_spread_bps(hedge.bid_price_ticks, primary.ask_price_ticks);
        let spread_bps = bid_ask_bps.max(ask_bid_bps);

        if spread_bps >= self.config.min_entry_spread_bps {
            Some(LeadLagSignal {
                symbol: symbol.to_string(),
                leader: self.config.primary_exchange,
                lagger: self.config.hedge_exchange,
                leader_bid_ticks: primary.bid_price_ticks,
                leader_ask_ticks: primary.ask_price_ticks,
                lagger_bid_ticks: hedge.bid_price_ticks,
                lagger_ask_ticks: hedge.ask_price_ticks,
                spread_bps,
                timestamp_ns: time::OffsetDateTime::now_utc().unix_timestamp_nanos() as i64,
            })
        } else {
            None
        }
    }

    /// Get current positions
    pub async fn get_positions(&self) -> Vec<PositionState> {
        self.positions.read().await.clone()
    }

    /// Check if we have an open position for symbol
    pub async fn has_position(&self, symbol: &str) -> bool {
        let positions = self.positions.read().await;
        positions.iter().any(|p| p.symbol == symbol)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use crate::domain::messages::decimal_to_ticks;

    #[test]
    fn default_config_uses_30bps_entry_threshold() {
        let cfg = LeadLagStrategyConfig::default();
        assert_eq!(cfg.min_entry_spread_bps, 30.0);
    }

    #[test]
    fn test_spread_calculation() {
        let spread = LeadLagSignal::calculate_spread_bps(100.05, 100.00);
        assert!((spread - 5.0).abs() < 0.01);
    }

    fn ticker(symbol: &str, bid: f64, ask: f64, exchange_ts_ns: i64) -> BookTicker {
        BookTicker::new(
            Bytes::copy_from_slice(symbol.as_bytes()),
            decimal_to_ticks(bid),
            decimal_to_ticks(ask),
            1,
            1,
            exchange_ts_ns,
            exchange_ts_ns,
        )
    }

    #[tokio::test]
    async fn check_signal_ignores_when_primary_not_leading() {
        let strategy = LeadLagStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 1.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });
        strategy
            .update_primary_book(ticker("BTCUSDT", 110.0, 111.0, 100))
            .await;
        strategy
            .update_hedge_book(ticker("BTCUSDT", 100.0, 101.0, 200))
            .await;

        assert!(strategy.check_signal("BTCUSDT").await.is_none());
    }

    #[tokio::test]
    async fn check_signal_triggers_on_bid_ask_leg_when_primary_leads() {
        let strategy = LeadLagStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 50.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });
        strategy
            .update_primary_book(ticker("BTCUSDT", 110.0, 111.0, 200))
            .await;
        strategy
            .update_hedge_book(ticker("BTCUSDT", 100.0, 101.0, 100))
            .await;

        let signal = strategy
            .check_signal("BTCUSDT")
            .await
            .expect("bid/ask leg should trigger");
        assert!(signal.spread_bps > 50.0);
        assert_eq!(signal.leader, ExchangeId::BinanceFutures);
        assert_eq!(signal.lagger, ExchangeId::GateFutures);
    }

    #[tokio::test]
    async fn check_signal_triggers_on_ask_bid_leg_when_primary_leads() {
        let strategy = LeadLagStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 20.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });
        strategy
            .update_primary_book(ticker("BTCUSDT", 100.0, 100.5, 200))
            .await;
        strategy
            .update_hedge_book(ticker("BTCUSDT", 101.0, 101.5, 100))
            .await;

        let signal = strategy
            .check_signal("BTCUSDT")
            .await
            .expect("ask/bid leg should trigger");
        assert!(signal.spread_bps > 20.0);
        assert_eq!(signal.leader, ExchangeId::BinanceFutures);
        assert_eq!(signal.lagger, ExchangeId::GateFutures);
    }
}
