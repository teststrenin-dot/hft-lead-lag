use super::{PolicyConfigSnapshot, ScreenerStore};

pub(super) fn top_policy_configs(
    store: &ScreenerStore,
    symbol: &str,
    top_k: usize,
) -> Option<Vec<PolicyConfigSnapshot>> {
    let top_k = top_k.max(1);
    if let Some(state) = store.symbols.get(symbol) {
        return state
            .fleet
            .as_ref()
            .map(|fleet| fleet.top_policy_configs(top_k));
    }
    let normalized = symbol.trim().to_ascii_uppercase();
    store.symbols.get(&normalized).and_then(|state| {
        state
            .fleet
            .as_ref()
            .map(|fleet| fleet.top_policy_configs(top_k))
    })
}

pub(super) fn fleet_policy_overview(
    store: &ScreenerStore,
    top_k: usize,
    max_symbols: usize,
) -> Vec<(String, Vec<PolicyConfigSnapshot>)> {
    let top_k = top_k.max(1);
    let max_symbols = max_symbols.max(1);
    let mut symbols: Vec<String> = store
        .symbols
        .iter()
        .filter(|entry| entry.fleet.is_some())
        .map(|entry| entry.key().clone())
        .collect();
    symbols.sort_unstable();
    symbols.truncate(max_symbols);

    symbols
        .into_iter()
        .filter_map(|symbol| {
            let state = store.symbols.get(&symbol)?;
            let fleet = state.fleet.as_ref()?;
            Some((symbol, fleet.top_policy_configs(top_k)))
        })
        .collect()
}
