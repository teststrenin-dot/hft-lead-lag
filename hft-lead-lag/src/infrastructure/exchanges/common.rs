//! Common utilities for exchange connectors

use bytes::Bytes;
use hmac::{Hmac, Mac};
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
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
            .expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    pub fn sign_static(secret: &[u8], data: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret)
            .expect("HMAC can take key of any size");
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
        let mut mac = Hmac::<Sha512>::new_from_slice(&self.key)
            .expect("HMAC can take key of any size");
        mac.update(data);
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    pub fn sign_static(secret: &[u8], data: &[u8]) -> String {
        let mut mac = Hmac::<Sha512>::new_from_slice(secret)
            .expect("HMAC can take key of any size");
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
pub fn extract_json_string_field(json: &[u8], field: &str) -> Option<Bytes> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();
    
    // Find the field
    let mut pos = 0;
    while pos < json.len() {
        if let Some(idx) = json[pos..].windows(field_bytes.len()).position(|w| w == field_bytes) {
            pos += idx + field_bytes.len();
            
            // Skip whitespace and colon
            while pos < json.len() && (json[pos] == b' ' || json[pos] == b':' || json[pos] == b'\n' || json[pos] == b'\r') {
                pos += 1;
            }
            
            if pos >= json.len() {
                return None;
            }
            
            // Check if string value
            if json[pos] == b'"' {
                pos += 1;
                let start = pos;
                while pos < json.len() && json[pos] != b'"' {
                    if json[pos] == b'\\' {
                        pos += 2; // Skip escaped char
                    } else {
                        pos += 1;
                    }
                }
                return Some(Bytes::copy_from_slice(&json[start..pos]));
            }
        } else {
            break;
        }
    }
    None
}

/// Extract i64 value from JSON field
pub fn extract_json_i64_field(json: &[u8], field: &str) -> Option<i64> {
    let field_pattern = format!("\"{}\"", field);
    let field_bytes = field_pattern.as_bytes();
    
    let mut pos = 0;
    while pos < json.len() {
        if let Some(idx) = json[pos..].windows(field_bytes.len()).position(|w| w == field_bytes) {
            pos += idx + field_bytes.len();
            
            while pos < json.len() && (json[pos] == b' ' || json[pos] == b':' || json[pos] == b'\n' || json[pos] == b'\r') {
                pos += 1;
            }
            
            if pos >= json.len() {
                return None;
            }
            
            // Check if numeric value
            if json[pos] == b'-' || json[pos].is_ascii_digit() {
                let start = pos;
                while pos < json.len() && (json[pos].is_ascii_digit() || json[pos] == b'-' || json[pos] == b'.') {
                    pos += 1;
                }
                return parse_i64(&json[start..pos]);
            }
        } else {
            break;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_extract_json_i64() {
        let json = br#"{"T":1234567890,"p":50000}"#;
        let ts = extract_json_i64_field(json, "T").unwrap();
        assert_eq!(ts, 1234567890);
    }
}
