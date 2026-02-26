use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

pub const SHORTLIST_SIZE: usize = 5;
pub const MAX_ACTIVE_SYMBOLS: usize = 4;
pub const FAST_STREAK_WINDOW_MS: i64 = 120_000;
pub const COOLDOWN_MS: i64 = 300_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PortfolioId {
    A,
    B,
}

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

#[derive(Debug, Default)]
pub struct PortfolioEngineV1 {
    guards: HashMap<String, SymbolGuardStateV1>,
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

    pub fn assign_without_overlap(
        &self,
        portfolio_a_candidates: &[SymbolStatsV1],
        portfolio_b_candidates: &[SymbolStatsV1],
        now_ms: i64,
    ) -> BTreeMap<PortfolioId, PortfolioStateV1> {
        let shortlist_a = self.build_shortlist(portfolio_a_candidates, now_ms);
        let shortlist_b = self.build_shortlist(portfolio_b_candidates, now_ms);

        let mut ownership: HashMap<String, (PortfolioId, SymbolStatsV1)> = HashMap::new();
        let mut owner_counts: HashMap<PortfolioId, usize> =
            HashMap::from([(PortfolioId::A, 0_usize), (PortfolioId::B, 0_usize)]);

        for (portfolio_id, shortlist) in [
            (PortfolioId::A, &shortlist_a),
            (PortfolioId::B, &shortlist_b),
        ] {
            for stats in shortlist {
                match ownership.get(&stats.symbol).cloned() {
                    None => {
                        ownership.insert(stats.symbol.clone(), (portfolio_id, stats.clone()));
                        *owner_counts.entry(portfolio_id).or_default() += 1;
                    }
                    Some((current_owner, current)) => {
                        let should_take = match rank_tuple_cmp(stats, &current) {
                            Ordering::Less => true,
                            Ordering::Equal => {
                                let candidate_count =
                                    owner_counts.get(&portfolio_id).copied().unwrap_or(0);
                                let current_count =
                                    owner_counts.get(&current_owner).copied().unwrap_or(0);
                                // Deterministic tie-break: keep active sets balanced.
                                candidate_count < current_count
                            }
                            Ordering::Greater => false,
                        };

                        if should_take {
                            ownership.insert(stats.symbol.clone(), (portfolio_id, stats.clone()));
                            if current_owner != portfolio_id {
                                if let Some(count) = owner_counts.get_mut(&current_owner) {
                                    *count = count.saturating_sub(1);
                                }
                                *owner_counts.entry(portfolio_id).or_default() += 1;
                            }
                        }
                    }
                }
            }
        }

        let state_a = PortfolioStateV1 {
            shortlist: shortlist_a.iter().map(|s| s.symbol.clone()).collect(),
            active_symbols: shortlist_a
                .iter()
                .filter(|s| {
                    ownership
                        .get(&s.symbol)
                        .map(|(owner, _)| *owner == PortfolioId::A)
                        .unwrap_or(false)
                })
                .map(|s| s.symbol.clone())
                .take(MAX_ACTIVE_SYMBOLS)
                .collect(),
        };

        let state_b = PortfolioStateV1 {
            shortlist: shortlist_b.iter().map(|s| s.symbol.clone()).collect(),
            active_symbols: shortlist_b
                .iter()
                .filter(|s| {
                    ownership
                        .get(&s.symbol)
                        .map(|(owner, _)| *owner == PortfolioId::B)
                        .unwrap_or(false)
                })
                .map(|s| s.symbol.clone())
                .take(MAX_ACTIVE_SYMBOLS)
                .collect(),
        };

        let mut states = BTreeMap::new();
        states.insert(PortfolioId::A, state_a);
        states.insert(PortfolioId::B, state_b);
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

    fn build_shortlist(&self, candidates: &[SymbolStatsV1], now_ms: i64) -> Vec<SymbolStatsV1> {
        rank_candidates(candidates)
            .into_iter()
            .filter(|stats| self.can_reenter(&stats.symbol, stats, now_ms))
            .take(SHORTLIST_SIZE)
            .collect()
    }
}
