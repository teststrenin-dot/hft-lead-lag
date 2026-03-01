//! Per-symbol state: quotes, drift, lag, shadow trader and fleet state.

use super::price_samples::PriceSamples;
use super::shadow_fleet::ShadowFleet;
use super::shadow_trader::ShadowTrader;

/// Snapshot of one side of the order book for a single exchange.
/// Only bid/ask prices are stored — quantities are not used in screener logic.
#[derive(Debug, Clone)]
pub struct Quote {
    pub bid: f64,
    pub ask: f64,
    pub ts_ms: i64,
}

#[derive(Debug, Default)]
pub struct SymbolState {
    pub(crate) first_tick_ms: Option<i64>,
    pub(crate) binance: Option<Quote>,
    pub(crate) gate: Option<Quote>,
    pub(crate) binance_event_ts_ms: Option<i64>,
    pub(crate) gate_event_ts_ms: Option<i64>,
    pub(crate) lag_ms: f64,
    pub(crate) updated_at_ms: i64,
    pub(crate) gate_natr_30m_pct: f64,
    pub(crate) price_samples: PriceSamples,
    pub(crate) shadow: ShadowTrader,
    pub(crate) fleet: Option<ShadowFleet>,
}

impl SymbolState {
    /// Ingest a quote + drift measurement for one exchange.
    /// Returns `false` if the exchange is unknown (caller should skip).
    pub(crate) fn ingest_quote(
        &mut self,
        exchange: &str,
        quote: Quote,
        exchange_event_ts_ms: i64,
    ) -> bool {
        if quote.ask < quote.bid {
            return false;
        }
        let monotonic_ts_ms = if exchange_event_ts_ms > 0 {
            exchange_event_ts_ms
        } else {
            quote.ts_ms
        };

        match exchange {
            "binance" => {
                if self
                    .binance_event_ts_ms
                    .is_some_and(|previous_ts_ms| monotonic_ts_ms < previous_ts_ms)
                {
                    return false;
                }
                self.binance = Some(quote);
                self.binance_event_ts_ms = Some(monotonic_ts_ms);
            }
            "gate" => {
                if self
                    .gate_event_ts_ms
                    .is_some_and(|previous_ts_ms| monotonic_ts_ms < previous_ts_ms)
                {
                    return false;
                }
                self.gate = Some(quote);
                self.gate_event_ts_ms = Some(monotonic_ts_ms);
            }
            _ => return false,
        }
        true
    }

    /// Compute lag metrics from both quotes. Requires both binance + gate present.
    pub(crate) fn update_lag(&mut self, exchange_ts_ms: i64, lag_window_ms: i64) {
        let (Some(binance), Some(gate)) = (self.binance.as_ref(), self.gate.as_ref()) else {
            return;
        };
        let _ = (exchange_ts_ms, lag_window_ms);
        self.lag_ms = (binance.ts_ms - gate.ts_ms).unsigned_abs() as f64;
    }

    /// Push price sample and tick shadow trader.
    pub(crate) fn tick_shadow(&mut self, exchange_ts_ms: i64, window_ms: i64) {
        let (Some(binance), Some(gate)) = (self.binance.as_ref(), self.gate.as_ref()) else {
            return;
        };
        self.price_samples.push(super::price_samples::PriceSample {
            ts_ms: exchange_ts_ms,
            gate_bid: gate.bid,
            gate_ask: gate.ask,
            binance_bid: binance.bid,
            binance_ask: binance.ask,
        });
        self.price_samples.cleanup(exchange_ts_ms);
        self.shadow.tick_with_context(
            exchange_ts_ms,
            binance,
            gate,
            &self.price_samples,
            window_ms,
            self.gate_natr_30m_pct,
            None,
        );
    }
}
