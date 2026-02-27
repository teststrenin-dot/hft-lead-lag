use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

pub const SHORTLIST_SIZE: usize = 5;
pub const MAX_ACTIVE_SYMBOLS: usize = 4;
pub const FAST_STREAK_WINDOW_MS: i64 = 120_000;
pub const COOLDOWN_MS: i64 = 300_000;
pub const DEFAULT_PORTFOLIO_IDS: &[&str] = &["A", "B"];
pub const PORTFOLIO_PAPER_INITIAL_EQUITY_USD: f64 = 10_000.0;
pub const PORTFOLIO_PAPER_TRADE_NOTIONAL_USD: f64 = 100.0;

pub type PortfolioId = String;

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolStatsV1 {
    pub symbol: String,
    pub age_minutes_from_first_tick: u64,
    pub closed_trades: u32,
    pub profitable_trades: u32,
    pub losing_trades: u32,
    pub avg_pnl_pct: f64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PortfolioStateV1 {
    pub shortlist: Vec<String>,
    pub active_symbols: Vec<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SymbolGuardStateV1 {
    pub streak_count: u32,
    pub first_streak_ts_ms: Option<i64>,
    pub cooldown_until_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PortfolioPaperStateV1 {
    pub equity_usd: f64,
    pub realized_pnl_usd: f64,
    pub closed_trades: u64,
    pub profitable_trades: u64,
    pub losing_trades: u64,
    pub last_trade_ts_ms: Option<i64>,
}

impl Default for PortfolioPaperStateV1 {
    fn default() -> Self {
        Self {
            equity_usd: PORTFOLIO_PAPER_INITIAL_EQUITY_USD,
            realized_pnl_usd: 0.0,
            closed_trades: 0,
            profitable_trades: 0,
            losing_trades: 0,
            last_trade_ts_ms: None,
        }
    }
}

impl PortfolioPaperStateV1 {
    pub fn observe_trade(&mut self, pnl_pct: f64, ts_ms: i64) {
        let pnl_usd = PORTFOLIO_PAPER_TRADE_NOTIONAL_USD * (pnl_pct / 100.0);
        self.realized_pnl_usd += pnl_usd;
        self.equity_usd += pnl_usd;
        self.closed_trades = self.closed_trades.saturating_add(1);
        if pnl_pct > 0.0 {
            self.profitable_trades = self.profitable_trades.saturating_add(1);
        } else if pnl_pct < 0.0 {
            self.losing_trades = self.losing_trades.saturating_add(1);
        }
        self.last_trade_ts_ms = Some(
            self.last_trade_ts_ms
                .map(|last| last.max(ts_ms))
                .unwrap_or(ts_ms),
        );
    }
}

#[derive(Debug)]
pub struct PortfolioEngineV1 {
    guards: HashMap<String, SymbolGuardStateV1>,
    portfolio_ids: Vec<PortfolioId>,
}

impl Default for PortfolioEngineV1 {
    fn default() -> Self {
        Self {
            guards: HashMap::new(),
            portfolio_ids: default_portfolio_ids(),
        }
    }
}

pub fn default_portfolio_ids() -> Vec<PortfolioId> {
    DEFAULT_PORTFOLIO_IDS
        .iter()
        .map(|id| (*id).to_string())
        .collect()
}

pub fn default_portfolio_paper_states_v1(
    portfolio_ids: &[PortfolioId],
) -> BTreeMap<PortfolioId, PortfolioPaperStateV1> {
    portfolio_ids
        .iter()
        .map(|id| (id.clone(), PortfolioPaperStateV1::default()))
        .collect()
}

fn normalize_portfolio_ids(portfolio_ids: Vec<String>) -> Vec<PortfolioId> {
    let mut normalized: Vec<PortfolioId> = Vec::new();
    for raw in portfolio_ids {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        if normalized.iter().any(|existing| existing == trimmed) {
            continue;
        }
        normalized.push(trimmed.to_string());
    }
    if normalized.is_empty() {
        default_portfolio_ids()
    } else {
        normalized
    }
}

pub fn compute_useful_winrate(stats: &SymbolStatsV1) -> f64 {
    if stats.closed_trades == 0 {
        return 0.0;
    }
    stats.profitable_trades as f64 / stats.closed_trades as f64
}

pub fn compute_pm_raw(stats: &SymbolStatsV1) -> i64 {
    stats.profitable_trades as i64 - stats.losing_trades as i64
}

pub fn eligible(stats: &SymbolStatsV1) -> bool {
    stats.age_minutes_from_first_tick > 5
        && stats.closed_trades > 5
        && compute_useful_winrate(stats) >= 0.30
        && stats.avg_pnl_pct >= 0.0
}

pub fn rank_candidates(candidates: &[SymbolStatsV1]) -> Vec<SymbolStatsV1> {
    let mut ranked = candidates.to_vec();
    ranked.sort_by(rank_tuple_cmp);
    ranked
}

fn safe_f64(value: f64) -> f64 {
    if value.is_nan() {
        f64::NEG_INFINITY
    } else {
        value
    }
}

fn rank_tuple_cmp(left: &SymbolStatsV1, right: &SymbolStatsV1) -> Ordering {
    safe_f64(compute_useful_winrate(right))
        .partial_cmp(&safe_f64(compute_useful_winrate(left)))
        .unwrap_or(Ordering::Equal)
        .then_with(|| compute_pm_raw(right).cmp(&compute_pm_raw(left)))
        .then_with(|| {
            safe_f64(right.avg_pnl_pct)
                .partial_cmp(&safe_f64(left.avg_pnl_pct))
                .unwrap_or(Ordering::Equal)
        })
        .then_with(|| right.closed_trades.cmp(&left.closed_trades))
        .then_with(|| left.symbol.cmp(&right.symbol))
}

impl PortfolioEngineV1 {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_portfolio_ids(portfolio_ids: Vec<String>) -> Self {
        Self {
            guards: HashMap::new(),
            portfolio_ids: normalize_portfolio_ids(portfolio_ids),
        }
    }

    pub fn portfolio_ids(&self) -> &[PortfolioId] {
        &self.portfolio_ids
    }

    pub fn set_portfolio_ids(&mut self, portfolio_ids: Vec<String>) {
        self.portfolio_ids = normalize_portfolio_ids(portfolio_ids);
    }

    pub fn assign_without_overlap(
        &self,
        candidates: &[SymbolStatsV1],
        now_ms: i64,
    ) -> BTreeMap<PortfolioId, PortfolioStateV1> {
        let mut states = BTreeMap::new();
        let ranked_pool = self.build_shortlist_pool(candidates, now_ms);
        let shortlist_by_id = self.build_shortlists_no_overlap(&ranked_pool);
        let active_by_id = self.assign_active_symbols_no_overlap(&shortlist_by_id);
        for portfolio_id in &self.portfolio_ids {
            let shortlist = shortlist_by_id
                .get(portfolio_id)
                .cloned()
                .unwrap_or_default();
            let active_symbols = active_by_id.get(portfolio_id).cloned().unwrap_or_default();
            states.insert(
                portfolio_id.clone(),
                PortfolioStateV1 {
                    shortlist,
                    active_symbols,
                },
            );
        }
        states
    }

    pub fn record_closed_trade(
        &mut self,
        symbol: &str,
        pnl_pct: f64,
        is_stop_loss: bool,
        ts_ms: i64,
    ) -> bool {
        let guard = self.guards.entry(symbol.to_string()).or_default();

        if pnl_pct > 0.0 {
            guard.streak_count = 0;
            guard.first_streak_ts_ms = None;
            return false;
        }

        if !is_stop_loss {
            return false;
        }

        if guard.streak_count == 0 {
            guard.first_streak_ts_ms = Some(ts_ms);
        }
        guard.streak_count = guard.streak_count.saturating_add(1);

        let first_ts = guard.first_streak_ts_ms.unwrap_or(ts_ms);
        let fast_trigger =
            guard.streak_count >= 5 && ts_ms.saturating_sub(first_ts) <= FAST_STREAK_WINDOW_MS;
        let persistent_trigger = guard.streak_count >= 6;

        if fast_trigger || persistent_trigger {
            guard.cooldown_until_ms = Some(ts_ms.saturating_add(COOLDOWN_MS));
            guard.streak_count = 0;
            guard.first_streak_ts_ms = None;
            return true;
        }

        false
    }

    pub fn can_reenter(&self, symbol: &str, stats: &SymbolStatsV1, now_ms: i64) -> bool {
        if let Some(guard) = self.guards.get(symbol) {
            if let Some(until) = guard.cooldown_until_ms {
                if now_ms < until {
                    return false;
                }
            }
        }
        eligible(stats)
    }

    pub fn guard_state(&self, symbol: &str) -> SymbolGuardStateV1 {
        self.guards.get(symbol).cloned().unwrap_or_default()
    }

    pub fn guard_states(&self) -> Vec<(String, SymbolGuardStateV1)> {
        let mut rows: Vec<(String, SymbolGuardStateV1)> = self
            .guards
            .iter()
            .map(|(symbol, state)| (symbol.clone(), state.clone()))
            .collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0));
        rows
    }

    pub fn replace_guard_states(&mut self, rows: Vec<(String, SymbolGuardStateV1)>) {
        self.guards.clear();
        self.guards.extend(rows);
    }

    fn build_shortlist_pool(
        &self,
        candidates: &[SymbolStatsV1],
        now_ms: i64,
    ) -> Vec<SymbolStatsV1> {
        rank_candidates(candidates)
            .into_iter()
            .filter(|stats| self.can_reenter(&stats.symbol, stats, now_ms))
            .collect()
    }

    fn build_shortlists_no_overlap(
        &self,
        ranked_pool: &[SymbolStatsV1],
    ) -> BTreeMap<PortfolioId, Vec<String>> {
        let mut shortlist_by_id: BTreeMap<PortfolioId, Vec<String>> = self
            .portfolio_ids
            .iter()
            .map(|id| (id.clone(), Vec::new()))
            .collect();

        if ranked_pool.is_empty() {
            return shortlist_by_id;
        }

        let mut cursor = 0usize;
        for _round in 0..SHORTLIST_SIZE {
            let mut progressed = false;
            for portfolio_id in &self.portfolio_ids {
                let shortlist = shortlist_by_id
                    .get_mut(portfolio_id)
                    .expect("portfolio id should be initialized");
                if shortlist.len() >= SHORTLIST_SIZE {
                    continue;
                }
                if cursor >= ranked_pool.len() {
                    break;
                }

                shortlist.push(ranked_pool[cursor].symbol.clone());
                cursor = cursor.saturating_add(1);
                progressed = true;
            }
            if !progressed {
                break;
            }
        }

        shortlist_by_id
    }

    fn assign_active_symbols_no_overlap(
        &self,
        shortlist_by_id: &BTreeMap<PortfolioId, Vec<String>>,
    ) -> BTreeMap<PortfolioId, Vec<String>> {
        let mut used_symbols: HashSet<String> = HashSet::new();
        let mut active_by_id: BTreeMap<PortfolioId, Vec<String>> = self
            .portfolio_ids
            .iter()
            .map(|id| (id.clone(), Vec::new()))
            .collect();

        loop {
            let mut progressed = false;
            for portfolio_id in &self.portfolio_ids {
                let shortlist = shortlist_by_id.get(portfolio_id);
                let active_symbols = active_by_id
                    .get_mut(portfolio_id)
                    .expect("portfolio id should be initialized");
                if active_symbols.len() >= MAX_ACTIVE_SYMBOLS {
                    continue;
                }
                let Some(shortlist) = shortlist else {
                    continue;
                };
                let maybe_next = shortlist
                    .iter()
                    .find(|symbol| !used_symbols.contains(*symbol));
                if let Some(symbol) = maybe_next {
                    active_symbols.push(symbol.clone());
                    used_symbols.insert(symbol.clone());
                    progressed = true;
                }
            }
            if !progressed {
                break;
            }
        }

        active_by_id
    }
}
