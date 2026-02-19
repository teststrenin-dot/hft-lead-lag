//! API layer - External interfaces
//! 
//! Provides:
//! - WebSocket server for streaming market data
//! - HTTP endpoints for monitoring and control
//! - Health check endpoints

pub mod ws_server;
pub mod http_server;
pub mod handlers;
pub mod templates;

pub use ws_server::*;
pub use http_server::*;

// Re-export screener types from domain layer for backwards compatibility.
pub use crate::domain::screener::{ScreenerStore, ScreenerRow};
pub use crate::domain::screener::shadow_trader::{ChartData, ShadowDebug, ShadowStats, ChartTrade};
