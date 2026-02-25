use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::{
    now_ms, ScreenerRow, ScreenerStore, ROWS_CACHE_MIN_REBUILD_INTERVAL_MS,
    SYMBOL_CATALOG_MAX_SIZE, SYMBOL_CATALOG_PRUNE_INTERVAL_MS, SYMBOL_STALE_TTL_MS,
};

pub(super) fn prune_symbol_catalog_if_needed(store: &ScreenerStore, now_ms: i64) {
    let last_prune_ms = store.last_catalog_prune_ms.load(Ordering::Relaxed);
    if now_ms.saturating_sub(last_prune_ms) < SYMBOL_CATALOG_PRUNE_INTERVAL_MS {
        return;
    }
    if store
        .last_catalog_prune_ms
        .compare_exchange(last_prune_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    let removed = prune_symbol_catalog_with_limits(
        store,
        now_ms,
        SYMBOL_STALE_TTL_MS,
        SYMBOL_CATALOG_MAX_SIZE,
    );
    if removed > 0 {
        store.mark_rows_cache_dirty();
    }
}

pub(super) fn prune_symbol_catalog_with_limits(
    store: &ScreenerStore,
    now_ms: i64,
    stale_ttl_ms: i64,
    max_symbols: usize,
) -> usize {
    let max_symbols = max_symbols.max(1);
    let stale_ttl_ms = stale_ttl_ms.max(1);
    let stale_keys: Vec<String> = store
        .symbols
        .iter()
        .filter_map(|entry| {
            let updated_at_ms = entry.value().updated_at_ms;
            if updated_at_ms > 0 && now_ms.saturating_sub(updated_at_ms) > stale_ttl_ms {
                Some(entry.key().clone())
            } else {
                None
            }
        })
        .collect();

    let mut removed = 0usize;
    for key in stale_keys {
        if store.symbols.remove(&key).is_some() {
            removed += 1;
        }
    }

    let overflow = store.symbols.len().saturating_sub(max_symbols);
    if overflow == 0 {
        return removed;
    }
    let mut oldest: Vec<(i64, String)> = store
        .symbols
        .iter()
        .map(|entry| (entry.value().updated_at_ms, entry.key().clone()))
        .collect();
    oldest.sort_by(|(left_ts, left_key), (right_ts, right_key)| {
        left_ts.cmp(right_ts).then_with(|| left_key.cmp(right_key))
    });
    for (_, key) in oldest.into_iter().take(overflow) {
        if store.symbols.remove(&key).is_some() {
            removed += 1;
        }
    }
    removed
}

pub(super) fn rows_sorted(store: &ScreenerStore) -> Vec<ScreenerRow> {
    let now = now_ms();
    prune_symbol_catalog_if_needed(store, now);
    if let Some(cached) = rows_snapshot_from_cache(store, now) {
        return cached;
    }
    let rows = Arc::new(build_rows_sorted(store));
    store.rows_cache.store(rows.clone());
    store
        .rows_cache_last_rebuild_ms
        .store(now, Ordering::Relaxed);
    store.rows_cache_dirty.store(false, Ordering::Relaxed);
    rows.as_ref().clone()
}

pub(super) fn rows_snapshot_from_cache(
    store: &ScreenerStore,
    now_ms: i64,
) -> Option<Vec<ScreenerRow>> {
    let cached = store.rows_cache.load_full();
    if cached.is_empty() {
        return None;
    }
    let dirty = store.rows_cache_dirty.load(Ordering::Relaxed);
    let last_rebuild_ms = store.rows_cache_last_rebuild_ms.load(Ordering::Relaxed);
    if !dirty || now_ms.saturating_sub(last_rebuild_ms) < ROWS_CACHE_MIN_REBUILD_INTERVAL_MS {
        return Some(cached.as_ref().clone());
    }
    None
}

pub(super) fn build_rows_sorted(store: &ScreenerStore) -> Vec<ScreenerRow> {
    let mut rows: Vec<ScreenerRow> = store
        .symbols
        .iter()
        .filter(|item| !item.value().leader_exchange.is_empty())
        .map(|item| {
            let shadow = &item.value().shadow;
            let stats = shadow.stats();
            ScreenerRow {
                symbol: item.key().clone(),
                leader_exchange: item.value().leader_exchange,
                data_source: "ws_live",
                is_fallback: false,
                last_update_ms: item.value().updated_at_ms,
                lag_ms: item.value().lag_ms,
                ws_drift_ms: item.value().drifts.combined,
                ws_drift_binance_ms: item.value().drifts.binance.unwrap_or(0.0),
                ws_drift_gate_ms: item.value().drifts.gate.unwrap_or(0.0),
                ws_drift_ingress_binance_ms: item.value().drifts.binance_ingress.unwrap_or(0.0),
                ws_drift_ingress_gate_ms: item.value().drifts.gate_ingress.unwrap_or(0.0),
                entry_half_life_ms: item.value().entry_half_life_ms,
                avg_gt_p90_ms: item.value().avg_gt_p90_ms,
                gate_natr_30m_pct: item.value().gate_natr_30m_pct,
                volume_24h_usd: item.value().volume_24h_usd,
                shadow_session_pnl_pct: stats.session_pnl_pct,
                shadow_session_trades: stats.session_trades,
                shadow_avg_trade_pct: stats.avg_trade_pct,
                shadow_win_rate_pct: stats.win_rate_pct,
                shadow_position: stats.position,
                shadow_spikes_detected: stats.spikes_detected,
                shadow_avg_catchup_pct: stats.avg_catchup_pct,
                shadow_avg_lag_ms: stats.avg_catchup_lag_ms,
            }
        })
        .collect();

    rows.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    rows
}
