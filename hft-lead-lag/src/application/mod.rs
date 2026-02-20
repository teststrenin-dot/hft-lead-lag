//! Application layer - Business logic and services
//!
//! This module contains:
//! - Lead-lag strategy service
//! - Portfolio management
//! - Risk management

pub mod services;
pub mod strategies;

pub use services::*;
pub use strategies::*;
