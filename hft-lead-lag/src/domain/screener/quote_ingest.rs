use super::shadow_fleet::{FleetTickMeta, ShadowFleet};
use super::state::Quote;
use super::utils::TimeDomainSample;
use super::{LAG_WINDOW_MS, ScreenerStore, now_ms};

pub(super) fn update(
    store: &ScreenerStore,
    symbol: &str,
    exchange: &'static str,
    bid: f64,
    ask: f64,
    timestamp_ns: i64,
    local_receive_ts_ns: i64,
) {
    if !bid.is_finite() || !ask.is_finite() || bid <= 0.0 || ask <= 0.0 {
        return;
    }

    let clocks = TimeDomainSample::from_raw(timestamp_ns, local_receive_ts_ns, now_ms());
    store.prune_symbol_catalog_if_needed(clocks.decision_ts_ms);

    let mut state = store.symbols.entry(symbol.to_string()).or_default();

    let state = state.value_mut();
    let ws_drift = clocks.decision_ws_drift_ms();
    let ingress_ws_drift = clocks.ingress_ws_drift_ms();
    let quote = Quote {
        bid,
        ask,
        ts_ms: clocks.exchange_event_ts_ms,
    };

    if !state.ingest_quote(exchange, quote, ws_drift, ingress_ws_drift) {
        return;
    }

    if state.binance.is_none() || state.gate.is_none() {
        state.updated_at_ms = clocks.exchange_event_ts_ms;
        state.leader_exchange = exchange;
        state.lag_ms = 0.0;
        store.mark_rows_cache_dirty();
        return;
    }

    state.updated_at_ms = clocks.exchange_event_ts_ms;
    state.update_lag(clocks.exchange_event_ts_ms, LAG_WINDOW_MS);
    state.update_cycles(clocks.exchange_event_ts_ms, store.window_ms);
    state.tick_shadow(clocks.exchange_event_ts_ms, store.window_ms);

    // Fleet: lazy-init on first tick, then tick all + drain trades to db.
    let (binance_ref, gate_ref) = match (state.binance.as_ref(), state.gate.as_ref()) {
        (Some(b), Some(g)) => (b, g),
        _ => return,
    };
    let fleet_configs = store.fleet_configs.load_full();
    let fleet = state
        .fleet
        .get_or_insert_with(|| ShadowFleet::new(fleet_configs.as_ref()));
    let run_id_arc = store.current_run_id.load();
    let run_id_ref = run_id_arc.as_deref();
    fleet.tick_all(
        clocks.exchange_event_ts_ms,
        binance_ref,
        gate_ref,
        &state.price_samples,
        store.window_ms,
        FleetTickMeta {
            symbol,
            gate_natr_30m_pct_at_entry: state.gate_natr_30m_pct,
            run_id: run_id_ref,
        },
    );
    let trades = fleet.drain_trades();
    if !trades.is_empty() {
        if let Some(ref writer) = store.db_writer {
            writer.send(trades);
        }
        // Without a writer attached, drop drained trades to keep fleet queue bounded.
    }
    store.mark_rows_cache_dirty();
}
