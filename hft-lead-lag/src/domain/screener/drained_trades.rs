use std::collections::BTreeMap;

use super::shadow_fleet::FleetTrade;

#[derive(Debug, Clone, PartialEq)]
pub(super) struct CandidateEvent {
    pub symbol: String,
    pub event_ts_ms: i64,
    pub pnl_pct: f64,
    pub first_observed_ts_ms: i64,
}

pub(super) fn sort_drained_trades_in_place(drained_trades: &mut [FleetTrade]) {
    drained_trades.sort_by(|left, right| {
        left.symbol
            .cmp(&right.symbol)
            .then_with(|| left.trade.ts_ms.cmp(&right.trade.ts_ms))
            .then_with(|| left.trade.entry_ts_ms.cmp(&right.trade.entry_ts_ms))
            .then_with(|| left.config_id.cmp(&right.config_id))
    });
}

pub(super) fn filter_active_run_trades(
    drained_trades: &[FleetTrade],
    active_run_id: Option<&str>,
) -> Vec<FleetTrade> {
    drained_trades
        .iter()
        .filter(|ft| match active_run_id {
            Some(active) => ft.run_id.as_deref() == Some(active),
            None => true,
        })
        .cloned()
        .collect()
}

pub(super) fn collapse_candidate_events(active_trades: &[FleetTrade]) -> Vec<CandidateEvent> {
    let mut events: BTreeMap<(String, i64), (f64, usize, i64)> = BTreeMap::new();
    for ft in active_trades {
        let key = (ft.symbol.clone(), ft.trade.ts_ms);
        let entry = events
            .entry(key)
            .or_insert((0.0, 0usize, ft.trade.entry_ts_ms));
        entry.0 += ft.trade.pnl_pct;
        entry.1 = entry.1.saturating_add(1);
        entry.2 = entry.2.min(ft.trade.entry_ts_ms);
    }

    events
        .into_iter()
        .filter_map(
            |((symbol, event_ts_ms), (pnl_sum, count, first_observed_ts_ms))| {
                if count == 0 {
                    None
                } else {
                    Some(CandidateEvent {
                        symbol,
                        event_ts_ms,
                        pnl_pct: pnl_sum / count as f64,
                        first_observed_ts_ms,
                    })
                }
            },
        )
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::screener::shadow_fleet::FleetTrade;
    use crate::domain::screener::shadow_trader::{ClosedTrade, Direction, ExitReason};

    fn sample_trade(
        symbol: &str,
        config_id: u64,
        entry_ts_ms: i64,
        ts_ms: i64,
        pnl_pct: f64,
    ) -> FleetTrade {
        FleetTrade {
            config_id,
            symbol: symbol.to_string(),
            run_id: None,
            trade: ClosedTrade {
                pnl_pct,
                ts_ms,
                direction: Direction::Long,
                entry_ts_ms,
                entry_price: 100.0,
                exit_price: 100.1,
                exit_reason: ExitReason::TrailingTake,
                spike_bps: 10.0,
                catchup_pct: 0.1,
                catchup_ms: ts_ms.saturating_sub(entry_ts_ms),
                gate_spread_at_entry_bps: 1.0,
                gate_natr_30m_pct_at_entry: 0.0,
                hold_ms: ts_ms.saturating_sub(entry_ts_ms),
                early_stop_churn: false,
            },
        }
    }

    #[test]
    fn sort_drained_trades_orders_by_symbol_ts_entry_config() {
        let mut rows = vec![
            sample_trade("ETHUSDT", 2, 200, 1_000, 0.1),
            sample_trade("BTCUSDT", 9, 120, 900, 0.2),
            sample_trade("BTCUSDT", 3, 100, 900, 0.1),
            sample_trade("BTCUSDT", 1, 100, 800, 0.3),
        ];

        sort_drained_trades_in_place(&mut rows);

        let order: Vec<(String, i64, i64, u64)> = rows
            .iter()
            .map(|t| {
                (
                    t.symbol.clone(),
                    t.trade.ts_ms,
                    t.trade.entry_ts_ms,
                    t.config_id,
                )
            })
            .collect();

        assert_eq!(
            order,
            vec![
                ("BTCUSDT".to_string(), 800, 100, 1),
                ("BTCUSDT".to_string(), 900, 100, 3),
                ("BTCUSDT".to_string(), 900, 120, 9),
                ("ETHUSDT".to_string(), 1_000, 200, 2),
            ]
        );
    }

    #[test]
    fn filter_active_run_trades_keeps_only_matching_run_when_set() {
        let mut a = sample_trade("BTCUSDT", 1, 10, 20, 0.1);
        a.run_id = Some("run-a".to_string());
        let mut b = sample_trade("BTCUSDT", 2, 30, 40, 0.2);
        b.run_id = Some("run-b".to_string());
        let rows = vec![a.clone(), b.clone()];

        let active = filter_active_run_trades(&rows, Some("run-a"));
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].run_id.as_deref(), Some("run-a"));

        let global = filter_active_run_trades(&rows, None);
        assert_eq!(global.len(), 2);
    }

    #[test]
    fn collapse_candidate_events_groups_same_symbol_and_ts() {
        let rows = vec![
            sample_trade("BTCUSDT", 1, 100, 1_000, 0.4),
            sample_trade("BTCUSDT", 2, 90, 1_000, -0.2),
            sample_trade("BTCUSDT", 3, 200, 1_500, 0.1),
            sample_trade("ETHUSDT", 4, 300, 1_500, 0.3),
        ];

        let collapsed = collapse_candidate_events(&rows);
        assert_eq!(collapsed.len(), 3);

        let btc_1 = collapsed
            .iter()
            .find(|e| e.symbol == "BTCUSDT" && e.event_ts_ms == 1_000)
            .expect("collapsed BTC event");
        assert!((btc_1.pnl_pct - 0.1).abs() < 1e-12);
        assert_eq!(btc_1.first_observed_ts_ms, 90);
    }
}
