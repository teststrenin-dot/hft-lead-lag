use super::messages::SymbolId;
use bytes::Bytes;
use std::collections::HashMap;

pub const MAX_STRATEGY_SYMBOLS: usize = SymbolId::MAX as usize + 1;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[error(
    "strategy symbol universe exceeds SymbolId capacity: unique={unique_symbols}, max={max_symbols}"
)]
pub struct StrategySymbolIdCapacityError {
    pub unique_symbols: usize,
    pub max_symbols: usize,
}

pub fn build_strategy_symbol_id_pairs(
    strategy_symbols: &[String],
) -> Result<Vec<(Bytes, SymbolId)>, StrategySymbolIdCapacityError> {
    let mut pairs: Vec<(Bytes, SymbolId)> = Vec::with_capacity(strategy_symbols.len());
    let mut seen: HashMap<Bytes, SymbolId> = HashMap::with_capacity(strategy_symbols.len());
    let mut next_symbol_id: usize = 0;

    for symbol in strategy_symbols {
        let key = Bytes::copy_from_slice(symbol.as_bytes());
        if seen.contains_key(&key) {
            continue;
        }
        let Ok(symbol_id) = SymbolId::try_from(next_symbol_id) else {
            return Err(StrategySymbolIdCapacityError {
                unique_symbols: next_symbol_id.saturating_add(1),
                max_symbols: MAX_STRATEGY_SYMBOLS,
            });
        };
        seen.insert(key.clone(), symbol_id);
        pairs.push((key, symbol_id));
        next_symbol_id = next_symbol_id.saturating_add(1);
    }

    Ok(pairs)
}

pub fn build_strategy_symbol_id_map(
    strategy_symbols: &[String],
) -> Result<HashMap<Bytes, SymbolId>, StrategySymbolIdCapacityError> {
    let pairs = build_strategy_symbol_id_pairs(strategy_symbols)?;
    let mut map = HashMap::with_capacity(pairs.len());
    for (symbol, symbol_id) in pairs {
        map.insert(symbol, symbol_id);
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strategy_symbol_pairs_deduplicate_and_preserve_first_seen_order() {
        let symbols = vec![
            "BTCUSDT".to_string(),
            "ETHUSDT".to_string(),
            "BTCUSDT".to_string(),
            "SOLUSDT".to_string(),
        ];
        let pairs = build_strategy_symbol_id_pairs(&symbols).expect("pairs");
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (Bytes::from_static(b"BTCUSDT"), 0));
        assert_eq!(pairs[1], (Bytes::from_static(b"ETHUSDT"), 1));
        assert_eq!(pairs[2], (Bytes::from_static(b"SOLUSDT"), 2));
    }

    #[test]
    fn strategy_symbol_pairs_fail_when_capacity_exceeded() {
        let total = MAX_STRATEGY_SYMBOLS.saturating_add(1);
        let symbols: Vec<String> = (0..total).map(|idx| format!("S{idx:05}")).collect();
        let err = build_strategy_symbol_id_pairs(&symbols).expect_err("capacity error");
        assert_eq!(
            err,
            StrategySymbolIdCapacityError {
                unique_symbols: MAX_STRATEGY_SYMBOLS.saturating_add(1),
                max_symbols: MAX_STRATEGY_SYMBOLS,
            }
        );
    }
}
