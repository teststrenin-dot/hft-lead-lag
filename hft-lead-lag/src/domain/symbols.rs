//! Symbol handling with interning for zero-copy hot path
//!
//! Uses Arc<str> for symbol interning to avoid repeated allocations
//! when processing messages for the same symbol.

use bytes::Bytes;
use std::sync::Arc;

const MAX_SYMBOL_CACHE_ENTRIES: usize = 8192;
const MAX_GATE_CONTRACT_CACHE_ENTRIES: usize = 8192;
const MAX_SYMBOL_LEN_BYTES: usize = 32;
const MIN_SYMBOL_LEN_BYTES: usize = 3;

/// Symbol cache for interning
/// Prevents repeated allocations for the same symbol string
#[derive(Debug, Default, Clone)]
pub struct SymbolCache {
    // Using dashmap for concurrent access without locks
    // In production, consider string-interner crate for better performance
    cache: std::sync::Arc<dashmap::DashMap<String, Arc<str>>>,
    bytes_cache: std::sync::Arc<dashmap::DashMap<Vec<u8>, Bytes>>,
    gate_contract_cache: std::sync::Arc<dashmap::DashMap<Vec<u8>, Bytes>>,
}

impl SymbolCache {
    pub fn new() -> Self {
        Self {
            cache: std::sync::Arc::new(dashmap::DashMap::new()),
            bytes_cache: std::sync::Arc::new(dashmap::DashMap::new()),
            gate_contract_cache: std::sync::Arc::new(dashmap::DashMap::new()),
        }
    }

    /// Get or create interned symbol
    pub fn intern(&self, symbol: &str) -> Arc<str> {
        // Fast path: check if exists
        if let Some(existing) = self.cache.get(symbol) {
            return existing.clone();
        }

        // Slow path: insert new
        let arc_str: Arc<str> = symbol.into();
        self.cache.insert(symbol.to_string(), arc_str.clone());
        arc_str
    }

    /// Get or create as Bytes (for zero-copy parsing)
    pub fn intern_bytes(&self, symbol: &[u8]) -> Bytes {
        if !is_cacheable_symbol_bytes(symbol) {
            return Bytes::copy_from_slice(symbol);
        }
        if let Some(existing) = self.bytes_cache.get(symbol) {
            return existing.clone();
        }

        if self.bytes_cache.len() >= MAX_SYMBOL_CACHE_ENTRIES {
            return Bytes::copy_from_slice(symbol);
        }

        let bytes = Bytes::copy_from_slice(symbol);
        self.bytes_cache.insert(symbol.to_vec(), bytes.clone());
        bytes
    }

    /// Get or create canonical symbol from Gate contract bytes (e.g. BTC_USDT -> BTCUSDT).
    pub fn intern_gate_contract(&self, contract: &[u8]) -> Bytes {
        if is_cacheable_contract_bytes(contract) {
            if let Some(existing) = self.gate_contract_cache.get(contract) {
                return existing.clone();
            }
        }

        let normalized = normalize_gate_contract(contract);
        let symbol = self.intern_bytes(&normalized);
        if is_cacheable_contract_bytes(contract)
            && self.gate_contract_cache.len() < MAX_GATE_CONTRACT_CACHE_ENTRIES
        {
            self.gate_contract_cache
                .insert(contract.to_vec(), symbol.clone());
        }
        symbol
    }

    #[cfg(test)]
    fn bytes_cache_len(&self) -> usize {
        self.bytes_cache.len()
    }
}

fn normalize_gate_contract(contract: &[u8]) -> Vec<u8> {
    if let Some(base) = contract.strip_suffix(b"_USDT") {
        let mut normalized = Vec::with_capacity(base.len() + 4);
        normalized.extend_from_slice(base);
        normalized.extend_from_slice(b"USDT");
        return normalized;
    }
    if let Some(base) = contract.strip_suffix(b"_USD") {
        let mut normalized = Vec::with_capacity(base.len() + 4);
        normalized.extend_from_slice(base);
        normalized.extend_from_slice(b"USDT");
        return normalized;
    }
    contract.to_vec()
}

fn is_cacheable_symbol_bytes(symbol: &[u8]) -> bool {
    let len = symbol.len();
    if !(MIN_SYMBOL_LEN_BYTES..=MAX_SYMBOL_LEN_BYTES).contains(&len) {
        return false;
    }
    symbol
        .iter()
        .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
}

fn is_cacheable_contract_bytes(contract: &[u8]) -> bool {
    is_cacheable_symbol_bytes(contract)
}

/// Predefined symbol constants for common pairs.
pub mod pairs {
    pub const BTC_USDT: &str = "BTCUSDT";
    pub const ETH_USDT: &str = "ETHUSDT";
    pub const SOL_USDT: &str = "SOLUSDT";
    pub const BNB_USDT: &str = "BNBUSDT";
    pub const XRP_USDT: &str = "XRPUSDT";
}

/// Backward-compatible alias for callers using `domain::symbols::symbols::*`.
pub use pairs as symbols;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_symbol_interning() {
        let cache = SymbolCache::new();

        let s1 = cache.intern("BTCUSDT");
        let s2 = cache.intern("BTCUSDT");

        // Should be same Arc
        assert!(Arc::ptr_eq(&s1, &s2));

        let s3 = cache.intern("ETHUSDT");
        assert!(!Arc::ptr_eq(&s1, &s3));
    }

    #[test]
    fn test_symbol_bytes_reuse_same_allocation() {
        let cache = SymbolCache::new();

        let s1 = cache.intern_bytes(b"BTCUSDT");
        let s2 = cache.intern_bytes(b"BTCUSDT");

        assert_eq!(s1, s2);
        assert_eq!(s1.as_ptr(), s2.as_ptr());
    }

    #[test]
    fn test_symbol_bytes_preserves_non_utf8_payload() {
        let cache = SymbolCache::new();
        let raw = b"BTC\xffUSDT";

        let s1 = cache.intern_bytes(raw);
        let s2 = cache.intern_bytes(raw);

        assert_eq!(s1.as_ref(), raw);
        assert_eq!(s2.as_ref(), raw);
        assert_ne!(s1.as_ptr(), s2.as_ptr());
        assert_eq!(cache.bytes_cache_len(), 0);
    }

    #[test]
    fn test_gate_contract_normalization() {
        let cache = SymbolCache::new();

        let s1 = cache.intern_gate_contract(b"BTC_USDT");
        let s2 = cache.intern_gate_contract(b"BTC_USD");
        let s3 = cache.intern_gate_contract(b"BTCUSDT");

        assert_eq!(s1.as_ref(), b"BTCUSDT");
        assert_eq!(s2.as_ref(), b"BTCUSDT");
        assert_eq!(s3.as_ref(), b"BTCUSDT");
    }

    #[test]
    fn test_symbol_bytes_cache_is_capped() {
        let cache = SymbolCache::new();
        let total = MAX_SYMBOL_CACHE_ENTRIES.saturating_add(128);
        for idx in 0..total {
            let symbol = format!("SYM{idx:05}USDT");
            let _ = cache.intern_bytes(symbol.as_bytes());
        }
        assert_eq!(cache.bytes_cache_len(), MAX_SYMBOL_CACHE_ENTRIES);
    }
}
