//! Infrastructure layer - Exchange implementations, WebSocket, REST
//!
//! This module contains:
//! - Exchange-specific WebSocket connectors
//! - REST clients (cold path)
//! - Authentication implementations
//! - Message parsers

pub mod db;
pub mod enrichment;
pub mod exchanges;
pub mod logging;
pub mod replay;
pub mod rest;

pub use exchanges::*;
