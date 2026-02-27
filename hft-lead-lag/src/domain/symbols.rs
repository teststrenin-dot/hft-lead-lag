//! Symbol handling with interning for zero-copy hot path
//!
//! Uses Arc<str> for symbol interning to avoid repeated allocations
//! when processing messages for the same symbol.

use bytes::Bytes;
use std::sync::Arc;

/// Symbol cache for interning
/// Prevents repeated allocations for the same symbol string
#[derive(Debug, Default, Clone)]
pub struct SymbolCache {
    // Using dashmap for concurrent access without locks
    // In production, consider string-interner crate for better performance
    cache: std::sync::Arc<dashmap::DashMap<String, Arc<str>>>,
    bytes_cache: std::sync::Arc<dashmap::DashMap<String, Bytes>>,
    gate_contract_cache: std::sync::Arc<dashmap::DashMap<String, Bytes>>,
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
        let symbol_str = std::str::from_utf8(symbol).unwrap_or("UNKNOWN");
        if let Some(existing) = self.bytes_cache.get(symbol_str) {
            return existing.clone();
        }

        let bytes = Bytes::copy_from_slice(symbol_str.as_bytes());
        self.bytes_cache
            .insert(symbol_str.to_string(), bytes.clone());
        bytes
    }

    /// Get or create canonical symbol from Gate contract bytes (e.g. BTC_USDT -> BTCUSDT).
    pub fn intern_gate_contract(&self, contract: &[u8]) -> Bytes {
        let contract_str = std::str::from_utf8(contract).unwrap_or("UNKNOWN");
        if let Some(existing) = self.gate_contract_cache.get(contract_str) {
            return existing.clone();
        }

        let normalized = normalize_gate_contract(contract_str);
        let symbol = self.intern_bytes(normalized.as_bytes());
        self.gate_contract_cache
            .insert(contract_str.to_string(), symbol.clone());
        symbol
    }
}

fn normalize_gate_contract(contract: &str) -> String {
    if let Some(base) = contract.strip_suffix("_USDT") {
        return format!("{base}USDT");
    }
    if let Some(base) = contract.strip_suffix("_USD") {
        return format!("{base}USDT");
    }
    contract.to_string()
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
    fn test_gate_contract_normalization() {
        let cache = SymbolCache::new();

        let s1 = cache.intern_gate_contract(b"BTC_USDT");
        let s2 = cache.intern_gate_contract(b"BTC_USD");
        let s3 = cache.intern_gate_contract(b"BTCUSDT");

        assert_eq!(s1.as_ref(), b"BTCUSDT");
        assert_eq!(s2.as_ref(), b"BTCUSDT");
        assert_eq!(s3.as_ref(), b"BTCUSDT");
    }
}
