use super::portfolio_runtime::{
    default_portfolio_ids, eligible, rank_candidates, PortfolioEngineV1, SymbolGuardStateV1,
    SymbolStatsV1,
};
use std::collections::HashSet;

fn stats(
    symbol: &str,
    age_minutes_from_first_tick: u64,
    closed_trades: u32,
    profitable_trades: u32,
    losing_trades: u32,
    avg_pnl_pct: f64,
) -> SymbolStatsV1 {
    SymbolStatsV1 {
        symbol: symbol.to_string(),
        age_minutes_from_first_tick,
        closed_trades,
        profitable_trades,
        losing_trades,
        avg_pnl_pct,
    }
}

#[test]
fn portfolio_runtime_eligible_requires_all_v1_thresholds() {
    let ok = stats("OK", 6, 6, 2, 3, 0.0);
    assert!(eligible(&ok));

    let too_young = stats("YOUNG", 5, 6, 2, 3, 0.0);
    assert!(!eligible(&too_young));

    let too_few_trades = stats("FEW", 6, 5, 2, 2, 0.0);
    assert!(!eligible(&too_few_trades));

    let low_winrate = stats("LOW_WR", 6, 10, 2, 8, 0.0);
    assert!(!eligible(&low_winrate));

    let negative_avg = stats("NEG", 6, 8, 4, 4, -0.01);
    assert!(!eligible(&negative_avg));
}

#[test]
fn portfolio_runtime_ranking_uses_v1_tuple_priority() {
    let ranked = rank_candidates(&[
        stats("wr_top", 6, 10, 6, 4, 0.1),
        stats("pm_top", 6, 10, 5, 1, 0.0),
        stats("avg_then_closed_high", 6, 12, 4, 2, 0.2),
        stats("avg_then_closed_low", 6, 9, 3, 1, 0.2),
        stats("avg_lower", 6, 30, 10, 8, 0.1),
    ]);

    let symbols: Vec<&str> = ranked.iter().map(|s| s.symbol.as_str()).collect();
    assert_eq!(
        symbols,
        vec![
            "wr_top",
            "pm_top",
            "avg_then_closed_high",
            "avg_then_closed_low",
            "avg_lower",
        ]
    );
}

#[test]
fn portfolio_runtime_assign_without_overlap_enforces_top5_and_max4() {
    let engine = PortfolioEngineV1::new();
    let pool = vec![
        stats("X", 6, 20, 13, 6, 0.12),
        stats("A1", 6, 18, 11, 5, 0.10),
        stats("A2", 6, 16, 10, 5, 0.09),
        stats("A3", 6, 14, 9, 4, 0.08),
        stats("A4", 6, 12, 8, 4, 0.07),
        stats("A6", 6, 10, 6, 4, 0.01),
        stats("A7", 6, 9, 6, 3, 0.01),
        stats("A8", 6, 8, 5, 3, 0.01),
        stats("A9", 6, 7, 4, 3, 0.01),
        stats("A10", 6, 7, 4, 3, 0.00),
        stats("A11", 6, 6, 3, 3, 0.00),
        stats("A12", 6, 6, 3, 3, 0.00),
    ];

    let assigned = engine.assign_without_overlap(&pool, 0);
    let a_state = assigned.get("A").expect("state A");
    let b_state = assigned.get("B").expect("state B");

    assert_eq!(a_state.shortlist.len(), 5);
    assert_eq!(b_state.shortlist.len(), 5);
    assert!(a_state.active_symbols.len() <= 4);
    assert!(b_state.active_symbols.len() <= 4);

    let shortlist_overlap: Vec<&String> = a_state
        .shortlist
        .iter()
        .filter(|sym| b_state.shortlist.contains(*sym))
        .collect();
    assert!(
        shortlist_overlap.is_empty(),
        "shortlists must not overlap across portfolios"
    );

    let overlap: Vec<&String> = a_state
        .active_symbols
        .iter()
        .filter(|sym| b_state.active_symbols.contains(*sym))
        .collect();
    assert!(overlap.is_empty(), "active symbols must not overlap");
}

#[test]
fn portfolio_runtime_assign_without_overlap_balances_identical_candidate_pool() {
    let engine = PortfolioEngineV1::new();
    let pool = vec![
        stats("S1", 6, 10, 6, 4, 0.10),
        stats("S2", 6, 10, 6, 4, 0.10),
        stats("S3", 6, 10, 6, 4, 0.10),
        stats("S4", 6, 10, 6, 4, 0.10),
        stats("S5", 6, 10, 6, 4, 0.10),
        stats("S6", 6, 10, 6, 4, 0.10),
    ];

    let assigned = engine.assign_without_overlap(&pool, 0);
    let a_state = assigned.get("A").expect("state A");
    let b_state = assigned.get("B").expect("state B");
    assert!(a_state.shortlist.len() <= 5);
    assert!(b_state.shortlist.len() <= 5);
    assert!(
        !a_state.active_symbols.is_empty() && !b_state.active_symbols.is_empty(),
        "identical pools must not starve one portfolio"
    );

    let shortlist_overlap: Vec<&String> = a_state
        .shortlist
        .iter()
        .filter(|sym| b_state.shortlist.contains(*sym))
        .collect();
    assert!(
        shortlist_overlap.is_empty(),
        "shortlists must not overlap across portfolios"
    );

    let overlap: Vec<&String> = a_state
        .active_symbols
        .iter()
        .filter(|sym| b_state.active_symbols.contains(*sym))
        .collect();
    assert!(overlap.is_empty(), "active symbols must not overlap");

    let union: HashSet<String> = a_state
        .shortlist
        .iter()
        .chain(b_state.shortlist.iter())
        .cloned()
        .collect();
    assert_eq!(
        union.len(),
        6,
        "all unique symbols should be distributed across shortlists without overlap"
    );
    assert_eq!(
        a_state.shortlist.len() + b_state.shortlist.len(),
        6,
        "with no-overlap shortlists total allocation cannot exceed pool size"
    );
}

#[test]
fn portfolio_runtime_with_portfolio_ids_supports_dynamic_count_and_independent_shortlists() {
    let engine = PortfolioEngineV1::with_portfolio_ids(vec![
        "A".to_string(),
        "B".to_string(),
        "C".to_string(),
    ]);
    assert_eq!(
        engine.portfolio_ids(),
        &vec!["A".to_string(), "B".to_string(), "C".to_string()]
    );

    let pool = vec![
        stats("S1", 6, 10, 6, 4, 0.10),
        stats("S2", 6, 10, 6, 4, 0.10),
        stats("S3", 6, 10, 6, 4, 0.10),
        stats("S4", 6, 10, 6, 4, 0.10),
        stats("S5", 6, 10, 6, 4, 0.10),
        stats("S6", 6, 10, 6, 4, 0.10),
        stats("S7", 6, 10, 6, 4, 0.10),
    ];
    let assigned = engine.assign_without_overlap(&pool, 0);

    assert_eq!(assigned.len(), 3);
    assert!(assigned.get("A").expect("A").shortlist.len() <= 5);
    assert!(assigned.get("B").expect("B").shortlist.len() <= 5);
    assert!(assigned.get("C").expect("C").shortlist.len() <= 5);

    let a_shortlist = assigned.get("A").expect("A").shortlist.clone();
    let b_shortlist = assigned.get("B").expect("B").shortlist.clone();
    let c_shortlist = assigned.get("C").expect("C").shortlist.clone();

    assert_ne!(
        a_shortlist, b_shortlist,
        "portfolios should not receive identical shortlist"
    );
    assert_ne!(
        b_shortlist, c_shortlist,
        "portfolios should not receive identical shortlist"
    );

    let mut shortlist_union: HashSet<String> = HashSet::new();
    for symbol in &a_shortlist {
        assert!(shortlist_union.insert(symbol.clone()));
    }
    for symbol in &b_shortlist {
        assert!(shortlist_union.insert(symbol.clone()));
    }
    for symbol in &c_shortlist {
        assert!(shortlist_union.insert(symbol.clone()));
    }
    assert_eq!(
        shortlist_union.len(),
        7,
        "all symbols should be allocated across 3 shortlists without overlap"
    );
}

#[test]
fn portfolio_runtime_default_portfolio_ids_fallback_for_invalid_input() {
    let engine = PortfolioEngineV1::with_portfolio_ids(vec!["".to_string(), " ".to_string()]);
    assert_eq!(engine.portfolio_ids(), default_portfolio_ids().as_slice());
}

#[test]
fn portfolio_runtime_stop_loss_fast_trigger_at_5_within_2m() {
    let mut engine = PortfolioEngineV1::new();
    let symbol = "FAST";
    for ts in [0_i64, 20_000, 40_000, 80_000] {
        assert!(!engine.record_closed_trade(symbol, -0.01, true, ts));
    }
    assert!(engine.record_closed_trade(symbol, -0.01, true, 100_000));
    let guard = engine.guard_state(symbol);
    assert_eq!(guard.cooldown_until_ms, Some(400_000));
}

#[test]
fn portfolio_runtime_stop_loss_persistent_trigger_on_6th_if_fast_missed() {
    let mut engine = PortfolioEngineV1::new();
    let symbol = "PERSIST";
    for ts in [0_i64, 40_000, 80_000, 120_000] {
        assert!(!engine.record_closed_trade(symbol, -0.01, true, ts));
    }
    assert!(!engine.record_closed_trade(symbol, -0.01, true, 200_000));
    assert!(engine.record_closed_trade(symbol, -0.01, true, 400_000));
}

#[test]
fn portfolio_runtime_stop_loss_streak_resets_on_positive_pnl() {
    let mut engine = PortfolioEngineV1::new();
    let symbol = "RESET";

    assert!(!engine.record_closed_trade(symbol, -0.01, true, 0));
    assert!(!engine.record_closed_trade(symbol, -0.01, true, 30_000));
    assert!(!engine.record_closed_trade(symbol, 0.02, false, 40_000));

    for ts in [60_000_i64, 80_000, 100_000, 120_000] {
        assert!(!engine.record_closed_trade(symbol, -0.01, true, ts));
    }
    assert!(engine.record_closed_trade(symbol, -0.01, true, 140_000));
}

#[test]
fn portfolio_runtime_cooldown_blocks_and_reentry_requires_eligible_again() {
    let mut engine = PortfolioEngineV1::new();
    let symbol = "REENTRY";
    let eligible_stats = stats(symbol, 8, 10, 5, 4, 0.02);
    let ineligible_stats = stats(symbol, 4, 3, 1, 2, -0.01);

    for ts in [0_i64, 10_000, 20_000, 30_000] {
        assert!(!engine.record_closed_trade(symbol, -0.02, true, ts));
    }
    assert!(engine.record_closed_trade(symbol, -0.02, true, 40_000));

    assert!(!engine.can_reenter(symbol, &eligible_stats, 339_999));
    assert!(!engine.can_reenter(symbol, &ineligible_stats, 340_001));
    assert!(engine.can_reenter(symbol, &eligible_stats, 340_001));
}

#[test]
fn portfolio_runtime_replace_guard_states_replaces_existing_map() {
    let mut engine = PortfolioEngineV1::new();
    assert!(!engine.record_closed_trade("BTCUSDT", -0.01, true, 1_000));
    assert_ne!(engine.guard_state("BTCUSDT"), SymbolGuardStateV1::default());

    engine.replace_guard_states(vec![(
        "ETHUSDT".to_string(),
        SymbolGuardStateV1 {
            streak_count: 3,
            first_streak_ts_ms: Some(42),
            cooldown_until_ms: Some(99),
        },
    )]);

    assert_eq!(engine.guard_state("BTCUSDT"), SymbolGuardStateV1::default());
    assert_eq!(
        engine.guard_state("ETHUSDT"),
        SymbolGuardStateV1 {
            streak_count: 3,
            first_streak_ts_ms: Some(42),
            cooldown_until_ms: Some(99),
        }
    );
}
