//! Domain layer - Core trading domain entities and exchange abstractions
//!
//! This module contains:
//! - Exchange trait definitions (ports)
//! - Market data message types (zero-copy optimized)
//! - Order and position domain models
//! - Symbol and price representations
//! - Screener: lead-lag metrics, cycle analysis, shadow trading

pub mod exchange;
pub mod messages;
pub mod models;
pub mod screener;
pub mod strategy_symbol_ids;
pub mod symbols;

pub use exchange::*;
pub use messages::*;
pub use models::*;
pub use strategy_symbol_ids::*;
pub use symbols::*;
