//! Common utilities for exchange connectors

use bytes::Bytes;
use hmac::{Hmac, Mac};
use memchr::memmem;
use sha2::{Sha256, Sha512};
use std::time::{SystemTime, UNIX_EPOCH};

/// HMAC-SHA256 signer (used by Binance, Bybit)
#[derive(Clone)]
pub struct HmacSha256 {
    key: Vec<u8>,
}

impl HmacSha256 {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            key: secret.to_vec(),
        }
    }

    pub fn sign(&self, data: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    pub fn sign_static(secret: &[u8], data: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha256>::new_from_slice(secret).expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }
}

/// HMAC-SHA512 signer (used by Gate.io)
#[derive(Clone)]
pub struct HmacSha512 {
    key: Vec<u8>,
}

impl HmacSha512 {
    pub fn new(secret: &[u8]) -> Self {
        Self {
            key: secret.to_vec(),
        }
    }

    pub fn sign(&self, data: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha512>::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    pub fn sign_static(secret: &[u8], data: &[u8]) -> String {
        let mut mac =
            Hmac::<Sha512>::new_from_slice(secret).expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }
}

/// Timestamped raw WebSocket payload: `(data, receive_ts_ns)`.
/// The nanosecond timestamp is captured at the moment the WS frame
/// arrives in the reader task, before it enters the mpsc channel.
pub type StampedBytes = (Vec<u8>, i64);

/// Get current timestamp in nanoseconds since UNIX epoch.
/// Use this to stamp incoming WS frames at receive time.
#[inline]
pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as i64
}

/// Get current timestamp in milliseconds
#[inline]
pub fn timestamp_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Get current timestamp in seconds
#[inline]
pub fn timestamp_sec() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Parse float from bytes efficiently
#[inline]
pub fn parse_float(bytes: &[u8]) -> Option<f64> {
    fast_float::parse(bytes).ok()
}

/// Parse i64 from bytes efficiently
#[inline]
pub fn parse_i64(bytes: &[u8]) -> Option<i64> {
    let f: f64 = fast_float::parse(bytes).ok()?;
    Some(f as i64)
}

#[inline]
pub fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    memmem::find(haystack, needle).is_some()
}

/// Convert price string to ticks (1e-8 precision)
#[inline]
pub fn price_to_ticks(price_str: &[u8]) -> Option<i64> {
    let price = parse_float(price_str)?;
    Some((price * 100_000_000.0) as i64)
}

/// Convert quantity string to ticks
#[inline]
pub fn qty_to_ticks(qty_str: &[u8]) -> Option<i64> {
    let qty = parse_float(qty_str)?;
    Some((qty * 100_000_000.0) as i64)
}

/// Extract string value from JSON field using simd-json style parsing
/// Returns the value as Bytes for zero-copy processing
fn find_json_field_value_start(
    json: &[u8],
    field_bytes: &[u8],
    search_from: usize,
) -> Option<usize> {
    let idx = memmem::find(&json[search_from..], field_bytes)?;
    let mut pos = search_from + idx + field_bytes.len();
    while pos < json.len()
        && (json[pos] == b' ' || json[pos] == b':' || json[pos] == b'\n' || json[pos] == b'\r')
    {
        pos += 1;
    }
    (pos < json.len()).then_some(pos)
}

pub fn extract_json_string_field(json: &[u8], field: &str) -> Option<Bytes> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();

    extract_json_string_field_ref_by_pattern(json, field_bytes).map(Bytes::copy_from_slice)
}

pub fn extract_json_string_field_ref<'a>(json: &'a [u8], field: &str) -> Option<&'a [u8]> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();
    extract_json_string_field_ref_by_pattern(json, field_bytes)
}

pub fn extract_json_string_field_ref_by_pattern<'a>(
    json: &'a [u8],
    field_bytes: &[u8],
) -> Option<&'a [u8]> {
    let mut pos = 0;
    while let Some(mut value_pos) = find_json_field_value_start(json, field_bytes, pos) {
        if json[value_pos] == b'"' {
            value_pos += 1;
            let start = value_pos;
            while value_pos < json.len() && json[value_pos] != b'"' {
                if json[value_pos] == b'\\' {
                    value_pos += 2;
                } else {
                    value_pos += 1;
                }
            }
            return Some(&json[start..value_pos]);
        }
        pos = value_pos.saturating_add(1);
    }
    None
}

/// Extract bool value from JSON field.
/// Supports native booleans (`true`/`false`) and numeric fallback (`0`/`1`).
pub fn extract_json_bool_field(json: &[u8], field: &str) -> Option<bool> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();
    extract_json_bool_field_by_pattern(json, field_bytes)
}

pub fn extract_json_bool_field_by_pattern(json: &[u8], field_bytes: &[u8]) -> Option<bool> {
    let mut pos = 0;
    while let Some(value_pos) = find_json_field_value_start(json, field_bytes, pos) {
        let tail = &json[value_pos..];
        if tail.starts_with(b"true") {
            return Some(true);
        }
        if tail.starts_with(b"false") {
            return Some(false);
        }
        if json[value_pos] == b'-' || json[value_pos].is_ascii_digit() {
            let mut end = value_pos;
            while end < json.len()
                && (json[end].is_ascii_digit()
                    || json[end] == b'-'
                    || json[end] == b'+'
                    || json[end] == b'.'
                    || json[end] == b'e'
                    || json[end] == b'E')
            {
                end += 1;
            }
            if let Some(v) = parse_float(&json[value_pos..end]) {
                return Some(v != 0.0);
            }
        }
        pos = value_pos.saturating_add(1);
    }
    None
}

/// Extract i64 value from JSON field
pub fn extract_json_i64_field(json: &[u8], field: &str) -> Option<i64> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();
    extract_json_i64_field_by_pattern(json, field_bytes)
}

pub fn extract_json_i64_field_by_pattern(json: &[u8], field_bytes: &[u8]) -> Option<i64> {
    let mut pos = 0;
    while let Some(value_pos) = find_json_field_value_start(json, field_bytes, pos) {
        if json[value_pos] == b'-' || json[value_pos].is_ascii_digit() {
            let start = value_pos;
            let mut end = value_pos;
            while end < json.len()
                && (json[end].is_ascii_digit()
                    || json[end] == b'-'
                    || json[end] == b'+'
                    || json[end] == b'.'
                    || json[end] == b'e'
                    || json[end] == b'E')
            {
                end += 1;
            }
            return parse_i64(&json[start..end]);
        }
        pos = value_pos.saturating_add(1);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_hmac_sha256() {
        let secret = b"test_secret";
        let data = b"test_data";

        let result = HmacSha256::sign_static(secret, data);
        assert!(!result.is_empty());
    }

    #[test]
    fn test_extract_json_string() {
        let json = br#"{"s":"BTCUSDT","p":"50000.00"}"#;
        let symbol = extract_json_string_field(json, "s").unwrap();
        assert_eq!(&symbol[..], b"BTCUSDT");
    }

    #[test]
    fn test_extract_json_string_ref() {
        let json = br#"{"s":"BTCUSDT","p":"50000.00"}"#;
        let symbol = extract_json_string_field_ref(json, "s").unwrap();
        assert_eq!(symbol, b"BTCUSDT");
    }

    #[test]
    fn test_extract_json_i64() {
        let json = br#"{"T":1234567890,"p":50000}"#;
        let ts = extract_json_i64_field(json, "T").unwrap();
        assert_eq!(ts, 1234567890);
    }

    #[test]
    fn test_extract_json_i64_supports_scientific_notation() {
        let json = br#"{"T":1e3,"x":-2.5e2}"#;
        assert_eq!(extract_json_i64_field(json, "T"), Some(1000));
        assert_eq!(extract_json_i64_field(json, "x"), Some(-250));
    }

    #[test]
    fn test_extract_json_bool_true_false() {
        let json = br#"{"m":true,"x":false}"#;
        assert_eq!(extract_json_bool_field(json, "m"), Some(true));
        assert_eq!(extract_json_bool_field(json, "x"), Some(false));
    }

    #[test]
    fn test_extract_json_bool_numeric_fallback() {
        let json = br#"{"m":1,"x":0}"#;
        assert_eq!(extract_json_bool_field(json, "m"), Some(true));
        assert_eq!(extract_json_bool_field(json, "x"), Some(false));
    }

    #[test]
    fn test_extract_json_bool_numeric_decimal_fallback() {
        let json = br#"{"m":-0.5,"x":0.0,"y":0.25}"#;
        assert_eq!(extract_json_bool_field(json, "m"), Some(true));
        assert_eq!(extract_json_bool_field(json, "x"), Some(false));
        assert_eq!(extract_json_bool_field(json, "y"), Some(true));
    }

    #[test]
    fn test_find_json_field_value_start_skips_delimiters() {
        let json = br#"{"s" : "BTCUSDT"}"#;
        let pos = find_json_field_value_start(json, br#""s""#, 0).unwrap();
        assert_eq!(json[pos], b'"');
    }

    #[test]
    fn test_find_json_field_value_start_can_resume_search() {
        let json = br#"{"s":"BTCUSDT","s":"ETHUSDT"}"#;
        let first = find_json_field_value_start(json, br#""s""#, 0).unwrap();
        let second = find_json_field_value_start(json, br#""s""#, first + 1).unwrap();
        assert!(second > first);
    }

    #[test]
    fn test_contains_bytes_detects_subslice_without_utf8_conversion() {
        let data = br#"{"e":"bookTicker","s":"BTCUSDT"}"#;
        assert!(contains_bytes(data, b"bookTicker"));
        assert!(!contains_bytes(data, b"aggTrade"));
    }

    #[test]
    #[ignore = "profiling-only benchmark harness"]
    fn bench_common_extractors_profile() {
        let payload = br#"{"e":"bookTicker","s":"BTCUSDT","b":"50000.1","B":"1.5","a":"50000.2","A":"2.0","T":1700000000000,"m":1}"#;
        let iterations: usize = 250_000;
        let start = Instant::now();
        let mut guard = 0i64;
        for _ in 0..iterations {
            let s = extract_json_string_field_ref_by_pattern(payload, b"\"s\"").unwrap();
            let b = extract_json_string_field_ref_by_pattern(payload, b"\"b\"").unwrap();
            let a = extract_json_string_field_ref_by_pattern(payload, b"\"a\"").unwrap();
            let ts = extract_json_i64_field_by_pattern(payload, b"\"T\"").unwrap();
            let m = extract_json_bool_field_by_pattern(payload, b"\"m\"").unwrap();
            guard = guard
                .wrapping_add(i64::from(s.len() as i32))
                .wrapping_add(i64::from(b.len() as i32))
                .wrapping_add(i64::from(a.len() as i32))
                .wrapping_add(ts)
                .wrapping_add(i64::from(m as i32));
        }
        let elapsed = start.elapsed();
        let nanos_per_iter = elapsed.as_nanos() / iterations as u128;
        eprintln!(
            "bench_common_extractors_profile: iters={} elapsed_ms={} ns_per_iter={} guard={}",
            iterations,
            elapsed.as_millis(),
            nanos_per_iter,
            guard
        );
        assert_ne!(guard, 0);
    }
}
