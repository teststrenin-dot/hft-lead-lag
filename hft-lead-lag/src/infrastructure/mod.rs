//! Infrastructure layer - Exchange implementations, WebSocket, REST
//! 
//! This module contains:
//! - Exchange-specific WebSocket connectors
//! - REST clients (cold path)
//! - Authentication implementations
//! - Message parsers

pub mod exchanges;
pub mod websocket;
pub mod rest;
pub mod logging;
pub mod enrichment;
pub mod db;

pub use exchanges::*;
