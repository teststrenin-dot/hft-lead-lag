//! Domain models for orders and positions

use crate::domain::messages::PriceTicks;

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Side {
    #[default]
    Buy,
    Sell,
}

impl Side {
    pub fn opposite(&self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Order type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit,
    Market,
    StopLimit,
    StopMarket,
}

/// Order status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
}

/// Time in force
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeInForce {
    Gtc, // Good till cancel
    Fok, // Fill or kill
    Ioc, // Immediate or cancel
    Gtx, // Post only (Binance)
}

/// Order request for placing new orders
#[derive(Debug, Clone)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub order_type: OrderType,
    pub price_ticks: Option<PriceTicks>,
    pub quantity: f64,
    pub time_in_force: TimeInForce,
    pub client_order_id: Option<String>,
}

impl OrderRequest {
    pub fn new_limit(
        symbol: String,
        side: Side,
        price_ticks: PriceTicks,
        quantity: f64,
        time_in_force: TimeInForce,
    ) -> Self {
        Self {
            symbol,
            side,
            order_type: OrderType::Limit,
            price_ticks: Some(price_ticks),
            quantity,
            time_in_force,
            client_order_id: None,
        }
    }

    pub fn with_client_order_id(mut self, id: String) -> Self {
        self.client_order_id = Some(id);
        self
    }
}

/// Order response after placement/cancellation
#[derive(Debug, Clone)]
pub struct OrderResponse {
    pub symbol: String,
    pub order_id: String,
    pub client_order_id: Option<String>,
    pub side: Side,
    pub order_type: OrderType,
    pub status: OrderStatus,
    pub price_ticks: Option<PriceTicks>,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub average_fill_price_ticks: Option<PriceTicks>,
    pub exchange_ts_ns: i64,
}

/// Position representation
#[derive(Debug, Clone, Default)]
pub struct Position {
    pub symbol: String,
    pub side: Side,
    pub quantity: f64,
    pub entry_price_ticks: PriceTicks,
    pub unrealized_pnl_ticks: PriceTicks,
    pub leverage: i32,
    pub margin_type: MarginType,
}

/// Margin type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MarginType {
    #[default]
    Cross,
    Isolated,
}

impl Position {
    pub fn is_flat(&self) -> bool {
        self.quantity == 0.0
    }

    pub fn notional_usd(&self, current_price_ticks: PriceTicks) -> f64 {
        use crate::domain::messages::ticks_to_decimal;
        self.quantity * ticks_to_decimal(current_price_ticks)
    }
}
