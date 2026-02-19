//! Health check utilities
//!
//! Legacy module — primary health tracking uses `HealthState` in `http_server.rs`.
//! Kept for potential future use with richer health aggregation.

use std::time::Duration;
use tokio::time::Instant;

/// Health status
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub status: String,
    pub checks: Vec<HealthCheck>,
}

/// Individual health check
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HealthCheck {
    pub name: String,
    pub is_healthy: bool,
    pub message: Option<String>,
    pub latency_ms: Option<f64>,
}

/// Health checker for the application
#[allow(dead_code)]
pub struct HealthChecker {
    start_time: Instant,
    binance_connected: bool,
    gate_connected: bool,
    last_heartbeat: Option<Instant>,
}

impl HealthChecker {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            binance_connected: false,
            gate_connected: false,
            last_heartbeat: None,
        }
    }

    pub fn uptime(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn set_binance_connected(&mut self, connected: bool) {
        self.binance_connected = connected;
    }

    pub fn set_gate_connected(&mut self, connected: bool) {
        self.gate_connected = connected;
    }

    pub fn heartbeat(&mut self) {
        self.last_heartbeat = Some(Instant::now());
    }

    /// Get current health status
    pub fn get_status(&self) -> HealthStatus {
        let mut checks = Vec::new();
        let mut is_healthy = true;

        // Check Binance connection
        checks.push(HealthCheck {
            name: "binance_connection".to_string(),
            is_healthy: self.binance_connected,
            message: if self.binance_connected {
                Some("Connected".to_string())
            } else {
                Some("Disconnected".to_string())
            },
            latency_ms: None,
        });
        if !self.binance_connected {
            is_healthy = false;
        }

        // Check Gate connection
        checks.push(HealthCheck {
            name: "gate_connection".to_string(),
            is_healthy: self.gate_connected,
            message: if self.gate_connected {
                Some("Connected".to_string())
            } else {
                Some("Disconnected".to_string())
            },
            latency_ms: None,
        });
        if !self.gate_connected {
            is_healthy = false;
        }

        // Check heartbeat
        let heartbeat_healthy = self.last_heartbeat
            .map(|h| h.elapsed() < Duration::from_secs(60))
            .unwrap_or(false);
        
        checks.push(HealthCheck {
            name: "heartbeat".to_string(),
            is_healthy: heartbeat_healthy,
            message: if heartbeat_healthy {
                Some("OK".to_string())
            } else {
                Some("No recent heartbeat".to_string())
            },
            latency_ms: None,
        });
        if !heartbeat_healthy {
            is_healthy = false;
        }

        HealthStatus {
            is_healthy,
            status: if is_healthy { "healthy" } else { "unhealthy" }.to_string(),
            checks,
        }
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status() {
        let mut checker = HealthChecker::new();
        checker.set_binance_connected(true);
        checker.set_gate_connected(true);
        checker.heartbeat();

        let status = checker.get_status();
        assert!(status.is_healthy);
        assert_eq!(status.status, "healthy");
    }
}
