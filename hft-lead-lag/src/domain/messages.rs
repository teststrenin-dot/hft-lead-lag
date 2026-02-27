//! Market data message types optimized for HFT
//!
//! Design principles:
//! - Zero-copy parsing where possible (using bytes::Bytes)
//! - Fixed-point arithmetic for prices (i64 ticks at 1e-8 precision)
//! - Minimal allocations in hot path
//! - Cache-friendly field ordering

use bytes::Bytes;

/// Price represented as fixed-point ticks (1e-8 precision)
/// Using i64 avoids decimal overhead in hot path
pub type PriceTicks = i64;
pub type QuantityTicks = i64;
pub type SymbolId = u16;

/// Convert ticks to decimal for display/calculation
#[inline]
pub fn ticks_to_decimal(ticks: PriceTicks) -> f64 {
    ticks as f64 / 100_000_000.0
}

/// Convert decimal to ticks
#[inline]
pub fn decimal_to_ticks(decimal: f64) -> PriceTicks {
    (decimal * 100_000_000.0) as i64
}

/// Book ticker - best bid/ask update
/// Field order optimized for cache alignment (64 bytes total)
#[derive(Debug, Clone)]
pub struct BookTicker {
    /// Symbol as interned string (Arc<str> equivalent)
    pub symbol: Bytes,
    /// Best bid price in ticks
    pub bid_price_ticks: PriceTicks,
    /// Best ask price in ticks
    pub ask_price_ticks: PriceTicks,
    /// Best bid quantity in ticks
    pub bid_qty_ticks: QuantityTicks,
    /// Best ask quantity in ticks
    pub ask_qty_ticks: QuantityTicks,
    /// Exchange timestamp (nanoseconds since epoch)
    pub exchange_ts_ns: i64,
    /// Local receive timestamp (nanoseconds since epoch)
    pub local_ts_ns: i64,
    /// Runtime strategy symbol id (when symbol belongs to strategy universe)
    pub strategy_symbol_id: Option<SymbolId>,
}

impl BookTicker {
    /// Create new book ticker.
    /// `local_ts_ns` must be captured at WS receive time (see `common::now_ns`).
    pub fn new(
        symbol: Bytes,
        bid_price_ticks: PriceTicks,
        ask_price_ticks: PriceTicks,
        bid_qty_ticks: QuantityTicks,
        ask_qty_ticks: QuantityTicks,
        exchange_ts_ns: i64,
        local_ts_ns: i64,
    ) -> Self {
        Self {
            symbol,
            bid_price_ticks,
            ask_price_ticks,
            bid_qty_ticks,
            ask_qty_ticks,
            exchange_ts_ns,
            local_ts_ns,
            strategy_symbol_id: None,
        }
    }

    #[inline]
    pub fn with_strategy_symbol_id(mut self, symbol_id: Option<SymbolId>) -> Self {
        self.strategy_symbol_id = symbol_id;
        self
    }

    /// Get bid price as f64
    #[inline]
    pub fn bid_price(&self) -> f64 {
        ticks_to_decimal(self.bid_price_ticks)
    }

    /// Get ask price as f64
    #[inline]
    pub fn ask_price(&self) -> f64 {
        ticks_to_decimal(self.ask_price_ticks)
    }

    /// Get mid price
    #[inline]
    pub fn mid_price(&self) -> f64 {
        (self.bid_price() + self.ask_price()) / 2.0
    }

    /// Get spread in ticks
    #[inline]
    pub fn spread_ticks(&self) -> PriceTicks {
        self.ask_price_ticks - self.bid_price_ticks
    }

    /// Get bid quantity as f64
    #[inline]
    pub fn bid_qty(&self) -> f64 {
        ticks_to_decimal(self.bid_qty_ticks)
    }

    /// Get ask quantity as f64
    #[inline]
    pub fn ask_qty(&self) -> f64 {
        ticks_to_decimal(self.ask_qty_ticks)
    }

    /// Get spread as percentage of mid
    #[inline]
    pub fn spread_pct(&self) -> f64 {
        let mid = self.mid_price();
        if mid == 0.0 {
            0.0
        } else {
            ticks_to_decimal(self.spread_ticks()) / mid
        }
    }
}

/// Trade message
#[derive(Debug, Clone)]
pub struct Trade {
    /// Symbol
    pub symbol: Bytes,
    /// Trade ID
    pub trade_id: i64,
    /// Trade price in ticks
    pub price_ticks: PriceTicks,
    /// Trade quantity in ticks
    pub qty_ticks: QuantityTicks,
    /// Is buyer the maker?
    pub is_buyer_maker: bool,
    /// Exchange timestamp
    pub exchange_ts_ns: i64,
    /// Local receive timestamp
    pub local_ts_ns: i64,
}

impl Trade {
    pub fn new(
        symbol: Bytes,
        trade_id: i64,
        price_ticks: PriceTicks,
        qty_ticks: QuantityTicks,
        is_buyer_maker: bool,
        exchange_ts_ns: i64,
        local_ts_ns: i64,
    ) -> Self {
        Self {
            symbol,
            trade_id,
            price_ticks,
            qty_ticks,
            is_buyer_maker,
            exchange_ts_ns,
            local_ts_ns,
        }
    }

    #[inline]
    pub fn price(&self) -> f64 {
        ticks_to_decimal(self.price_ticks)
    }

    #[inline]
    pub fn qty(&self) -> f64 {
        ticks_to_decimal(self.qty_ticks)
    }
}
