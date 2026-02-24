use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetPatchMode {
    FullReplace,
    Incremental,
}

impl FleetPatchMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::FullReplace => "full_replace",
            Self::Incremental => "incremental",
        }
    }
}

#[derive(Debug, Clone)]
pub struct FleetPatchPlan {
    pub mode: FleetPatchMode,
    pub changed_config_ids: HashSet<u64>,
    pub symbol_scope: Option<HashSet<String>>,
}

impl FleetPatchPlan {
    pub fn new(
        mode: FleetPatchMode,
        changed_config_ids: impl IntoIterator<Item = u64>,
        symbol_scope: Option<impl IntoIterator<Item = String>>,
    ) -> Self {
        Self {
            mode,
            changed_config_ids: changed_config_ids.into_iter().collect(),
            symbol_scope: symbol_scope.map(|scope| scope.into_iter().collect()),
        }
    }

    pub fn has_changed_configs(&self) -> bool {
        !self.changed_config_ids.is_empty()
    }

    pub fn symbol_in_scope(&self, symbol: &str) -> bool {
        self.symbol_scope
            .as_ref()
            .map(|scope| scope.contains(symbol))
            .unwrap_or(true)
    }
}

pub fn should_reset_symbol(
    plan: &FleetPatchPlan,
    symbol: &str,
    symbol_has_touched_configs: bool,
) -> bool {
    match plan.mode {
        FleetPatchMode::FullReplace => true,
        FleetPatchMode::Incremental => symbol_has_touched_configs && plan.symbol_in_scope(symbol),
    }
}

#[cfg(test)]
mod tests {
    use super::{should_reset_symbol, FleetPatchMode, FleetPatchPlan};

    #[test]
    fn full_replace_marks_all_symbols_for_reset() {
        let plan = FleetPatchPlan::new(FleetPatchMode::FullReplace, Vec::<u64>::new(), None::<
            Vec<String>,
        >);
        assert!(should_reset_symbol(&plan, "BTCUSDT", false));
        assert!(should_reset_symbol(&plan, "ETHUSDT", true));
    }

    #[test]
    fn incremental_only_resets_symbols_with_touched_configs() {
        let plan = FleetPatchPlan::new(FleetPatchMode::Incremental, [10_u64, 20_u64], None::<
            Vec<String>,
        >);
        assert!(should_reset_symbol(&plan, "BTCUSDT", true));
        assert!(!should_reset_symbol(&plan, "BTCUSDT", false));
    }

    #[test]
    fn incremental_with_symbol_scope_limits_resets() {
        let plan = FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [10_u64],
            Some(vec!["BTCUSDT".to_string()]),
        );
        assert!(should_reset_symbol(&plan, "BTCUSDT", true));
        assert!(!should_reset_symbol(&plan, "ETHUSDT", true));
        assert!(!should_reset_symbol(&plan, "BTCUSDT", false));
    }
}
