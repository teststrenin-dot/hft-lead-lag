//! Symbol handling with interning for zero-copy hot path
//! 
//! Uses Arc<str> for symbol interning to avoid repeated allocations
//! when processing messages for the same symbol.

use std::sync::Arc;
use bytes::Bytes;

/// Symbol cache for interning
/// Prevents repeated allocations for the same symbol string
#[derive(Debug, Default, Clone)]
pub struct SymbolCache {
    // Using dashmap for concurrent access without locks
    // In production, consider string-interner crate for better performance
    cache: std::sync::Arc<dashmap::DashMap<String, Arc<str>>>,
}

impl SymbolCache {
    pub fn new() -> Self {
        Self {
            cache: std::sync::Arc::new(dashmap::DashMap::new()),
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
        let arc_str = self.intern(symbol_str);
        Bytes::copy_from_slice(arc_str.as_bytes())
    }
}

/// Predefined symbol constants for common pairs
pub mod symbols {
    pub const BTC_USDT: &str = "BTCUSDT";
    pub const ETH_USDT: &str = "ETHUSDT";
    pub const SOL_USDT: &str = "SOLUSDT";
    pub const BNB_USDT: &str = "BNBUSDT";
    pub const XRP_USDT: &str = "XRPUSDT";
}

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
}
