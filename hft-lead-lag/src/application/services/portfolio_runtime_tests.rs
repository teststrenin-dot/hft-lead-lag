use super::portfolio_runtime::{
    eligible, rank_candidates, PortfolioEngineV1, PortfolioId, SymbolGuardStateV1, SymbolStatsV1,
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
    ];

    let assigned = engine.assign_without_overlap(&pool, 0);
    let a_state = assigned.get(&PortfolioId::A).expect("state A");
    let b_state = assigned.get(&PortfolioId::B).expect("state B");

    assert_eq!(a_state.shortlist.len(), 5);
    assert_eq!(b_state.shortlist.len(), 5);
    assert!(a_state.active_symbols.len() <= 4);
    assert!(b_state.active_symbols.len() <= 4);

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
    let a_state = assigned.get(&PortfolioId::A).expect("state A");
    let b_state = assigned.get(&PortfolioId::B).expect("state B");
    assert_eq!(a_state.shortlist.len(), 5);
    assert_eq!(b_state.shortlist.len(), 5);
    assert!(
        !a_state.active_symbols.is_empty() && !b_state.active_symbols.is_empty(),
        "identical pools must not starve one portfolio"
    );

    let overlap: Vec<&String> = a_state
        .active_symbols
        .iter()
        .filter(|sym| b_state.active_symbols.contains(*sym))
        .collect();
    assert!(overlap.is_empty(), "active symbols must not overlap");

    let union: HashSet<String> = a_state
        .active_symbols
        .iter()
        .chain(b_state.active_symbols.iter())
        .cloned()
        .collect();
    assert_eq!(
        union.len(),
        5,
        "all shortlisted symbols must be assigned to exactly one portfolio"
    );
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
