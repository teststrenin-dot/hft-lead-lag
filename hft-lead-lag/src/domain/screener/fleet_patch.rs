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

fn normalize_symbol_key(symbol: &str) -> Option<String> {
    let normalized = symbol.trim().to_uppercase();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

impl FleetPatchPlan {
    pub fn new(
        mode: FleetPatchMode,
        changed_config_ids: impl IntoIterator<Item = u64>,
        symbol_scope: Option<impl IntoIterator<Item = String>>,
    ) -> Self {
        let normalized_scope = symbol_scope.map(|scope| {
            scope
                .into_iter()
                .filter_map(|symbol| normalize_symbol_key(&symbol))
                .collect::<HashSet<String>>()
        });
        Self {
            mode,
            changed_config_ids: changed_config_ids.into_iter().collect(),
            symbol_scope: normalized_scope,
        }
    }

    pub fn has_changed_configs(&self) -> bool {
        !self.changed_config_ids.is_empty()
    }

    pub fn has_symbol_scope(&self) -> bool {
        self.symbol_scope
            .as_ref()
            .is_some_and(|scope| !scope.is_empty())
    }

    pub fn symbol_scope_len(&self) -> usize {
        self.symbol_scope.as_ref().map_or(0, HashSet::len)
    }

    pub fn symbol_in_scope(&self, symbol: &str) -> bool {
        let Some(normalized_symbol) = normalize_symbol_key(symbol) else {
            return false;
        };
        self.symbol_scope
            .as_ref()
            .map(|scope| scope.contains(&normalized_symbol))
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
        let plan = FleetPatchPlan::new(
            FleetPatchMode::FullReplace,
            Vec::<u64>::new(),
            None::<Vec<String>>,
        );
        assert!(should_reset_symbol(&plan, "BTCUSDT", false));
        assert!(should_reset_symbol(&plan, "ETHUSDT", true));
    }

    #[test]
    fn incremental_only_resets_symbols_with_touched_configs() {
        let plan = FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [10_u64, 20_u64],
            None::<Vec<String>>,
        );
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

    #[test]
    fn plan_new_normalizes_symbol_scope_to_uppercase_trimmed() {
        let plan = FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [42_u64],
            Some(vec![
                " btcusdt ".to_string(),
                "BTCUSDT".to_string(),
                "".to_string(),
                "   ".to_string(),
            ]),
        );
        assert!(plan.has_symbol_scope());
        assert_eq!(plan.symbol_scope_len(), 1);
        assert_eq!(
            plan.symbol_scope.expect("scope"),
            std::collections::HashSet::from(["BTCUSDT".to_string()])
        );
    }

    #[test]
    fn symbol_in_scope_is_case_insensitive_after_normalization() {
        let plan = FleetPatchPlan::new(
            FleetPatchMode::Incremental,
            [1_u64],
            Some(vec!["btcusdt".to_string(), "ethusdt".to_string()]),
        );
        assert!(plan.symbol_in_scope("BTCUSDT"));
        assert!(plan.symbol_in_scope("ethusdt"));
        assert!(!plan.symbol_in_scope("SOLUSDT"));
    }
}
