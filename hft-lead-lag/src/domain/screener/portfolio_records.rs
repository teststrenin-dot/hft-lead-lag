#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioStateRecordV1 {
    pub portfolio_id: String,
    pub shortlist: Vec<String>,
    pub active_symbols: Vec<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortfolioGuardRecordV1 {
    pub symbol: String,
    pub streak_count: u32,
    pub first_streak_ts_ms: Option<i64>,
    pub cooldown_until_ms: Option<i64>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PortfolioCandidateHistoryRecordV1 {
    pub symbol: String,
    pub closed_trades: u32,
    pub profitable_trades: u32,
    pub losing_trades: u32,
    pub pnl_sum_pct: f64,
    pub first_trade_ts_ms: Option<i64>,
}
