//! API layer - External interfaces
//!
//! Provides:
//! - WebSocket server for streaming market data
//! - HTTP endpoints for monitoring and control
//! - Health check endpoints

pub mod handlers;
pub mod http_server;
pub mod runner;
pub mod templates;
pub mod ws_server;

pub use http_server::*;
pub use ws_server::*;

// Re-export screener types from domain layer for backwards compatibility.
pub use crate::domain::screener::shadow_trader::{ChartData, ChartTrade, ShadowDebug, ShadowStats};
pub use crate::domain::screener::{ScreenerRow, ScreenerStore};
