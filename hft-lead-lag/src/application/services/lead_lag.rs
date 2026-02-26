//! Lead-Lag Strategy Service
//!
//! Implements the core HFT lead-lag trading logic:
//! - Detects price leadership between exchanges
//! - Executes arbitrage when spread exceeds threshold
//! - Manages position lifecycle

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

use crate::domain::{messages::ticks_to_decimal, BookTicker, ExchangeId};

const OFFSET_WINDOW_SAMPLES: usize = 256;
const OFFSET_RECOMPUTE_INTERVAL: u32 = 64;
const MAX_OFFSET_SAMPLE_ABS_NS: i64 = 6 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Clone)]
struct ExchangeClockOffset {
    samples: VecDeque<i64>,
    cached_median_ns: i64,
    pending_updates: u32,
}

impl Default for ExchangeClockOffset {
    fn default() -> Self {
        Self {
            samples: VecDeque::with_capacity(OFFSET_WINDOW_SAMPLES),
            cached_median_ns: 0,
            pending_updates: 0,
        }
    }
}

impl ExchangeClockOffset {
    fn observe(&mut self, local_ts_ns: i64, exchange_ts_ns: i64) {
        if local_ts_ns <= 0 || exchange_ts_ns <= 0 {
            return;
        }
        let sample = local_ts_ns.saturating_sub(exchange_ts_ns);
        if sample.abs() > MAX_OFFSET_SAMPLE_ABS_NS {
            return;
        }

        self.samples.push_back(sample);
        while self.samples.len() > OFFSET_WINDOW_SAMPLES {
            self.samples.pop_front();
        }

        self.pending_updates = self.pending_updates.saturating_add(1);
        if self.samples.len() == 1 || self.pending_updates >= OFFSET_RECOMPUTE_INTERVAL {
            self.recompute_median();
            self.pending_updates = 0;
        }
    }

    fn corrected_exchange_ts_ns(&self, exchange_ts_ns: i64) -> i64 {
        exchange_ts_ns.saturating_add(self.cached_median_ns)
    }

    fn recompute_median(&mut self) {
        if self.samples.is_empty() {
            self.cached_median_ns = 0;
            return;
        }
        let mut sorted: Vec<i64> = self.samples.iter().copied().collect();
        sorted.sort_unstable();
        self.cached_median_ns = sorted[sorted.len() / 2];
    }
}

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
    /// Direction selected by winning spread branch.
    pub direction: SignalDirection,
    /// `spread(primary.bid, hedge.ask)` branch (up-dislocation).
    pub bid_ask_bps: f64,
    /// `spread(hedge.bid, primary.ask)` branch (down-dislocation).
    pub ask_bid_bps: f64,
    /// Timestamp of signal (ns)
    pub timestamp_ns: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalDirection {
    LongLagger,
    ShortLagger,
}

impl SignalDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::LongLagger => "LONG_LAGGER",
            Self::ShortLagger => "SHORT_LAGGER",
        }
    }
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
    /// Maximum allowed local receive skew between primary and hedge quotes (ms).
    /// If exceeded, signal is suppressed as stale cross-exchange pair.
    pub max_quote_skew_ms: u64,
}

impl Default for LeadLagStrategyConfig {
    fn default() -> Self {
        Self {
            primary_exchange: ExchangeId::BinanceFutures,
            hedge_exchange: ExchangeId::GateFutures,
            min_entry_spread_bps: 30.0,  // 0.30%
            target_exit_spread_bps: 1.0, // 0.01%
            max_position_age_ms: 5000,   // 5 seconds
            order_qty_usd: 10.0,
            symbols: vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()],
            max_quote_skew_ms: 1_000,
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
    primary_clock_offset: Arc<Mutex<ExchangeClockOffset>>,
    hedge_clock_offset: Arc<Mutex<ExchangeClockOffset>>,
}

impl LeadLagStrategy {
    pub fn new(config: LeadLagStrategyConfig) -> Self {
        Self {
            config,
            positions: Arc::new(RwLock::new(Vec::new())),
            primary_books: Arc::new(RwLock::new(std::collections::HashMap::new())),
            hedge_books: Arc::new(RwLock::new(std::collections::HashMap::new())),
            primary_clock_offset: Arc::new(Mutex::new(ExchangeClockOffset::default())),
            hedge_clock_offset: Arc::new(Mutex::new(ExchangeClockOffset::default())),
        }
    }

    /// Update primary exchange book ticker
    pub async fn update_primary_book(&self, ticker: BookTicker) {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        self.primary_clock_offset
            .lock()
            .expect("primary clock offset mutex poisoned")
            .observe(ticker.local_ts_ns, ticker.exchange_ts_ns);
        let mut books = self.primary_books.write().await;
        books.insert(symbol, ticker);
    }

    /// Update hedge exchange book ticker
    pub async fn update_hedge_book(&self, ticker: BookTicker) {
        let symbol = String::from_utf8_lossy(&ticker.symbol).to_string();
        self.hedge_clock_offset
            .lock()
            .expect("hedge clock offset mutex poisoned")
            .observe(ticker.local_ts_ns, ticker.exchange_ts_ns);
        let mut books = self.hedge_books.write().await;
        books.insert(symbol, ticker);
    }

    /// Check for lead-lag signal
    pub async fn check_signal(&self, symbol: &str) -> Option<LeadLagSignal> {
        let primary_books = self.primary_books.read().await;
        let hedge_books = self.hedge_books.read().await;

        let primary = primary_books.get(symbol)?;
        let hedge = hedge_books.get(symbol)?;

        if primary.local_ts_ns <= 0 || hedge.local_ts_ns <= 0 {
            return None;
        }

        let max_quote_skew_ns = self.config.max_quote_skew_ms.saturating_mul(1_000_000);
        if max_quote_skew_ns > 0
            && primary.local_ts_ns.abs_diff(hedge.local_ts_ns) > max_quote_skew_ns
        {
            return None;
        }

        // Binance(primary) is the oracle. If hedge quote is newer, skip this cycle:
        // we do not trade when lead source is unclear.
        let primary_corrected_ts_ns = self
            .primary_clock_offset
            .lock()
            .expect("primary clock offset mutex poisoned")
            .corrected_exchange_ts_ns(primary.exchange_ts_ns);
        let hedge_corrected_ts_ns = self
            .hedge_clock_offset
            .lock()
            .expect("hedge clock offset mutex poisoned")
            .corrected_exchange_ts_ns(hedge.exchange_ts_ns);

        if primary_corrected_ts_ns < hedge_corrected_ts_ns {
            return None;
        }

        // Two directional legs, no mid-price mixing:
        // 1) bid/ask leg:  primary.bid  vs hedge.ask  (up-dislocation)
        // 2) ask/bid leg:  hedge.bid    vs primary.ask (down-dislocation)
        let bid_ask_bps = directional_spread_bps(primary.bid_price_ticks, hedge.ask_price_ticks);
        let ask_bid_bps = directional_spread_bps(hedge.bid_price_ticks, primary.ask_price_ticks);
        let (spread_bps, direction) = if bid_ask_bps >= ask_bid_bps {
            (bid_ask_bps, SignalDirection::LongLagger)
        } else {
            (ask_bid_bps, SignalDirection::ShortLagger)
        };

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
                direction,
                bid_ask_bps,
                ask_bid_bps,
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
    use crate::domain::messages::decimal_to_ticks;
    use bytes::Bytes;

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
        ticker_with_local(symbol, bid, ask, exchange_ts_ns, exchange_ts_ns)
    }

    fn ticker_with_local(
        symbol: &str,
        bid: f64,
        ask: f64,
        exchange_ts_ns: i64,
        local_ts_ns: i64,
    ) -> BookTicker {
        BookTicker::new(
            Bytes::copy_from_slice(symbol.as_bytes()),
            decimal_to_ticks(bid),
            decimal_to_ticks(ask),
            1,
            1,
            exchange_ts_ns,
            local_ts_ns,
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
        assert_eq!(signal.direction, SignalDirection::LongLagger);
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
        assert_eq!(signal.direction, SignalDirection::ShortLagger);
        assert_eq!(signal.leader, ExchangeId::BinanceFutures);
        assert_eq!(signal.lagger, ExchangeId::GateFutures);
    }

    #[tokio::test]
    async fn check_signal_does_not_drop_primary_when_hedge_clock_is_ahead() {
        let strategy = LeadLagStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 50.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });

        // Hedge exchange clock is +1h ahead, but local receive order still shows
        // primary as fresher for this decision cycle.
        strategy
            .update_hedge_book(ticker_with_local(
                "BTCUSDT",
                100.0,
                101.0,
                3_601_900_000_000,
                1_900_000_000,
            ))
            .await;
        strategy
            .update_primary_book(ticker_with_local(
                "BTCUSDT",
                110.0,
                111.0,
                2_000_000_000,
                2_000_000_000,
            ))
            .await;

        let signal = strategy
            .check_signal("BTCUSDT")
            .await
            .expect("clock offset on hedge must not suppress valid primary-led signal");
        assert!(signal.spread_bps > 50.0);
    }

    #[tokio::test]
    async fn check_signal_ignores_when_quotes_are_too_far_apart_in_local_time() {
        let strategy = LeadLagStrategy::new(LeadLagStrategyConfig {
            min_entry_spread_bps: 50.0,
            symbols: vec!["BTCUSDT".to_string()],
            ..Default::default()
        });

        // Large local receive skew means one side is stale for this decision.
        strategy
            .update_primary_book(ticker_with_local(
                "BTCUSDT",
                110.0,
                111.0,
                2_000_000_000,
                10_000_000_000,
            ))
            .await;
        strategy
            .update_hedge_book(ticker_with_local(
                "BTCUSDT",
                100.0,
                101.0,
                100_000_000,
                100_000_000,
            ))
            .await;

        assert!(strategy.check_signal("BTCUSDT").await.is_none());
    }
}
