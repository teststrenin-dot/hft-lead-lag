//! Risk management service

/// Risk limits configuration
#[derive(Debug, Clone)]
pub struct RiskLimits {
    /// Maximum total position size in USD
    pub max_total_exposure_usd: f64,
    /// Maximum position per symbol in USD
    pub max_symbol_exposure_usd: f64,
    /// Maximum daily loss in USD
    pub max_daily_loss_usd: f64,
    /// Maximum number of open positions
    pub max_open_positions: usize,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            max_total_exposure_usd: 1000.0,
            max_symbol_exposure_usd: 100.0,
            max_daily_loss_usd: 50.0,
            max_open_positions: 10,
        }
    }
}

/// Risk check result
#[derive(Debug)]
pub enum RiskCheckResult {
    Ok,
    ExceedsTotalExposure { current: f64, limit: f64 },
    ExceedsSymbolExposure { symbol: String, current: f64, limit: f64 },
    ExceedsDailyLoss { current: f64, limit: f64 },
    TooManyPositions { current: usize, limit: usize },
}

impl RiskCheckResult {
    pub fn is_ok(&self) -> bool {
        matches!(self, Self::Ok)
    }
}

/// Risk management service
pub struct RiskManager {
    limits: RiskLimits,
    current_exposure_usd: f64,
    daily_pnl_usd: f64,
    open_positions: usize,
}

impl RiskManager {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            current_exposure_usd: 0.0,
            daily_pnl_usd: 0.0,
            open_positions: 0,
        }
    }

    /// Check if new order is within risk limits
    pub fn check_order(&self, _symbol: &str, quantity_usd: f64) -> RiskCheckResult {
        if self.current_exposure_usd + quantity_usd > self.limits.max_total_exposure_usd {
            return RiskCheckResult::ExceedsTotalExposure {
                current: self.current_exposure_usd + quantity_usd,
                limit: self.limits.max_total_exposure_usd,
            };
        }

        if self.open_positions >= self.limits.max_open_positions {
            return RiskCheckResult::TooManyPositions {
                current: self.open_positions,
                limit: self.limits.max_open_positions,
            };
        }

        if self.daily_pnl_usd < -self.limits.max_daily_loss_usd {
            return RiskCheckResult::ExceedsDailyLoss {
                current: self.daily_pnl_usd.abs(),
                limit: self.limits.max_daily_loss_usd,
            };
        }

        RiskCheckResult::Ok
    }

    /// Update exposure after order fill
    pub fn update_exposure(&mut self, delta_usd: f64) {
        self.current_exposure_usd += delta_usd;
    }

    /// Update position count
    pub fn add_position(&mut self) {
        self.open_positions += 1;
    }

    pub fn remove_position(&mut self) {
        if self.open_positions > 0 {
            self.open_positions -= 1;
        }
    }

    /// Update daily PnL
    pub fn update_daily_pnl(&mut self, pnl_usd: f64) {
        self.daily_pnl_usd += pnl_usd;
    }

    /// Reset daily PnL (call at start of trading day)
    pub fn reset_daily_pnl(&mut self) {
        self.daily_pnl_usd = 0.0;
    }

    /// Get current exposure
    pub fn current_exposure(&self) -> f64 {
        self.current_exposure_usd
    }

    /// Get available exposure
    pub fn available_exposure(&self) -> f64 {
        self.limits.max_total_exposure_usd - self.current_exposure_usd
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_check() {
        let limits = RiskLimits::default();
        let manager = RiskManager::new(limits);

        let result = manager.check_order("BTCUSDT", 50.0);
        assert!(result.is_ok());

        let result = manager.check_order("BTCUSDT", 1001.0);
        assert!(!result.is_ok());
    }
}
