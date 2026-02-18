//! Application layer - Business logic and services
//! 
//! This module contains:
//! - Lead-lag strategy service
//! - Portfolio management
//! - Risk management

pub mod services;
pub mod ports;

pub use services::*;
pub use ports::*;
