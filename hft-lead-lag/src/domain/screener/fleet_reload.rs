use std::collections::HashSet;
use std::sync::Arc;

use super::fleet_patch::{should_reset_symbol, FleetPatchMode, FleetPatchPlan};
use super::{
    FleetPatchApplyError, FleetPatchMatchStats, FleetReloadReport, ScreenerStore, TraderConfig,
};

pub(super) fn try_apply_fleet_patch(
    store: &ScreenerStore,
    new_configs: Vec<TraderConfig>,
    plan: FleetPatchPlan,
) -> Result<FleetReloadReport, FleetPatchApplyError> {
    let old_config_count = store.fleet_configs.load().len();
    let new_config_count = new_configs.len();
    let match_stats = validate_patch(store, &new_configs, &plan)?;
    store.fleet_configs.store(Arc::new(new_configs));

    let mut symbols_reset = 0usize;
    let mut drained_trades = 0usize;
    let mut scope_symbols_matched = 0usize;
    let allow_incremental_fallback_reset =
        matches!(plan.mode, FleetPatchMode::Incremental) && match_stats.has_new_only_changed_ids;
    for mut entry in store.symbols.iter_mut() {
        let symbol = entry.key().clone();
        let state = entry.value_mut();
        let symbol_in_scope = plan.symbol_in_scope(&symbol);
        if plan.has_symbol_scope() && symbol_in_scope {
            scope_symbols_matched += 1;
        }
        let symbol_has_touched_configs = if matches!(plan.mode, FleetPatchMode::FullReplace) {
            true
        } else if !plan.has_changed_configs() {
            false
        } else if allow_incremental_fallback_reset {
            true
        } else {
            state
                .fleet
                .as_ref()
                .map(|fleet| fleet.contains_any_config_ids(&plan.changed_config_ids))
                .unwrap_or(false)
        };
        if !should_reset_symbol(&plan, &symbol, symbol_has_touched_configs) {
            continue;
        }
        let Some(mut fleet) = state.fleet.take() else {
            continue;
        };
        symbols_reset += 1;
        let trades = fleet.drain_trades();
        drained_trades += store.handle_drained_fleet_trades(trades);
    }

    Ok(FleetReloadReport {
        old_config_count,
        new_config_count,
        symbols_reset,
        drained_trades,
        changed_ids_requested: match_stats.changed_ids_requested,
        matched_changed_ids_old: match_stats.matched_changed_ids_old,
        matched_changed_ids_new: match_stats.matched_changed_ids_new,
        unmatched_changed_ids: match_stats.unmatched_changed_ids,
        scope_symbols_requested: match_stats.scope_symbols_requested,
        scope_symbols_matched,
    })
}

pub(super) fn validate_fleet_patch(
    store: &ScreenerStore,
    new_configs: &[TraderConfig],
    plan: &FleetPatchPlan,
) -> Result<(), FleetPatchApplyError> {
    validate_patch(store, new_configs, plan).map(|_| ())
}

fn validate_patch(
    store: &ScreenerStore,
    new_configs: &[TraderConfig],
    plan: &FleetPatchPlan,
) -> Result<FleetPatchMatchStats, FleetPatchApplyError> {
    validate_new_configs(new_configs)?;
    let match_stats = collect_patch_match_stats(store, plan, new_configs);
    if matches!(plan.mode, FleetPatchMode::Incremental) {
        if !plan.has_changed_configs() {
            return Err(FleetPatchApplyError::IncrementalMissingChangedConfigIds);
        }
        if !match_stats.has_any_match() {
            return Err(FleetPatchApplyError::IncrementalNoMatchedChangedConfigIds {
                changed_ids_requested: match_stats.changed_ids_requested,
                scope_symbols_requested: match_stats.scope_symbols_requested,
            });
        }
        if match_stats.has_new_only_changed_ids && !plan.has_symbol_scope() {
            return Err(
                FleetPatchApplyError::IncrementalNewConfigIdsRequireSymbolScope {
                    changed_ids_requested: match_stats.changed_ids_requested,
                },
            );
        }
    }
    Ok(match_stats)
}

fn validate_new_configs(new_configs: &[TraderConfig]) -> Result<(), FleetPatchApplyError> {
    let mut ids = HashSet::with_capacity(new_configs.len());
    for (index, cfg) in new_configs.iter().enumerate() {
        if let Err(err) = cfg.validate() {
            return Err(FleetPatchApplyError::InvalidConfig {
                index,
                field: err.field,
                reason: err.reason,
            });
        }
        let config_id = cfg.config_id();
        if !ids.insert(config_id) {
            return Err(FleetPatchApplyError::DuplicateConfigId { config_id });
        }
    }
    Ok(())
}

fn collect_patch_match_stats(
    store: &ScreenerStore,
    plan: &FleetPatchPlan,
    new_configs: &[TraderConfig],
) -> FleetPatchMatchStats {
    let mut old_ids = HashSet::new();
    for entry in store.symbols.iter() {
        if let Some(fleet) = entry.value().fleet.as_ref() {
            fleet.collect_config_ids(&mut old_ids);
        }
    }
    let new_ids: HashSet<u64> = new_configs.iter().map(TraderConfig::config_id).collect();
    let mut matched_changed_ids_old = 0usize;
    let mut matched_changed_ids_new = 0usize;
    let mut matched_changed_ids_any = 0usize;
    let mut has_new_only_changed_ids = false;
    for id in &plan.changed_config_ids {
        let in_old = old_ids.contains(id);
        let in_new = new_ids.contains(id);
        if in_old {
            matched_changed_ids_old += 1;
        }
        if in_new {
            matched_changed_ids_new += 1;
        }
        if in_old || in_new {
            matched_changed_ids_any += 1;
        }
        if in_new && !in_old {
            has_new_only_changed_ids = true;
        }
    }
    let unmatched_changed_ids = plan
        .changed_config_ids
        .len()
        .saturating_sub(matched_changed_ids_any);

    FleetPatchMatchStats {
        changed_ids_requested: plan.changed_config_ids.len(),
        matched_changed_ids_old,
        matched_changed_ids_new,
        matched_changed_ids_any,
        unmatched_changed_ids,
        has_new_only_changed_ids,
        scope_symbols_requested: plan.symbol_scope_len(),
    }
}
