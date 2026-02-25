//! REST clients for exchange data (cold path only)
//!
//! Used for:
//! - Getting 24h volume for symbol filtering
//! - Authentication (listen keys, etc.)
//! - Order placement (fallback)

use crate::domain::ExchangeResult;
use crate::infrastructure::exchanges::common::HmacSha256;
use reqwest::{Client, header::HeaderMap};
use serde::Deserialize;
use tracing::debug;

/// 24h ticker data
#[derive(Debug, Clone, Deserialize)]
pub struct Ticker24h {
    pub symbol: String,
    pub quote_volume: f64, // 24h USD volume
    pub last_price: Option<f64>,
    pub price_change_24h_pct: Option<f64>,
}

/// Binance REST API client
pub struct BinanceRestClient {
    client: Client,
    base_url: String,
}

fn parse_json_f64_field(value: &serde_json::Value, key: &str) -> Option<f64> {
    value
        .get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| value.get(key).and_then(|v| v.as_f64()))
}

fn filter_tickers_by_volume(tickers: Vec<Ticker24h>, min_volume_usd: f64) -> Vec<Ticker24h> {
    tickers
        .into_iter()
        .filter(|ticker| ticker.quote_volume >= min_volume_usd)
        .collect()
}

fn ticker_symbols(tickers: Vec<Ticker24h>) -> Vec<String> {
    tickers.into_iter().map(|ticker| ticker.symbol).collect()
}

fn parse_binance_ticker(value: serde_json::Value) -> Option<Ticker24h> {
    let symbol = value.get("symbol")?.as_str()?.to_string();

    // Skip non-USDT pairs and invalid symbols
    if !symbol.ends_with("USDT") {
        return None;
    }
    if !symbol.chars().all(|c| c.is_ascii_alphanumeric()) {
        return None;
    }

    let quote_volume = parse_json_f64_field(&value, "quoteVolume")?;
    let last_price =
        parse_json_f64_field(&value, "lastPrice").or_else(|| parse_json_f64_field(&value, "last"));
    let price_change_24h_pct = parse_json_f64_field(&value, "priceChangePercent");

    Some(Ticker24h {
        symbol,
        quote_volume,
        last_price,
        price_change_24h_pct,
    })
}

impl BinanceRestClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(10000))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url: "https://fapi.binance.com".to_string(),
        }
    }

    /// Get 24h tickers for all symbols
    pub async fn get_24h_tickers(&self) -> ExchangeResult<Vec<Ticker24h>> {
        let url = format!("{}/fapi/v1/ticker/24hr", self.base_url);
        debug!("GET {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;

        let tickers_raw: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;

        debug!("Received {} raw tickers from Binance", tickers_raw.len());

        let tickers: Vec<Ticker24h> = tickers_raw
            .into_iter()
            .filter_map(parse_binance_ticker)
            .collect();

        debug!("Filtered to {} valid tickers", tickers.len());
        Ok(tickers)
    }

    /// Get symbols with 24h volume > min_volume_usd
    pub async fn get_symbols_with_volume(
        &self,
        min_volume_usd: f64,
    ) -> ExchangeResult<Vec<String>> {
        let tickers = self.get_tickers_with_volume(min_volume_usd).await?;
        let symbols = ticker_symbols(tickers);

        debug!(
            "Found {} symbols with volume >= {} USD",
            symbols.len(),
            min_volume_usd
        );
        Ok(symbols)
    }

    /// Get full ticker snapshots for symbols with 24h volume > min_volume_usd
    pub async fn get_tickers_with_volume(
        &self,
        min_volume_usd: f64,
    ) -> ExchangeResult<Vec<Ticker24h>> {
        let tickers = self.get_24h_tickers().await?;
        Ok(filter_tickers_by_volume(tickers, min_volume_usd))
    }
}

impl Default for BinanceRestClient {
    fn default() -> Self {
        Self::new()
    }
}

/// Gate REST API client
pub struct GateRestClient {
    client: Client,
    api_key: Option<String>,
    api_secret: Option<String>,
}

impl GateRestClient {
    pub fn new() -> Self {
        let client = Client::builder()
            .timeout(std::time::Duration::from_millis(10000))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            api_key: None,
            api_secret: None,
        }
    }

    pub fn set_credentials(&mut self, api_key: String, api_secret: String) {
        self.api_key = Some(api_key);
        self.api_secret = Some(api_secret);
    }

    /// Get 24h tickers for all contracts
    pub async fn get_24h_tickers(&self) -> ExchangeResult<Vec<Ticker24h>> {
        let url = "https://api.gateio.ws/api/v4/futures/usdt/tickers";
        debug!("GET {}", url);

        let mut headers = HeaderMap::new();

        // Add auth headers if credentials available
        if let (Some(key), Some(secret)) = (&self.api_key, &self.api_secret) {
            use sha2::{Digest, Sha512};
            use std::time::{SystemTime, UNIX_EPOCH};

            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string();

            let body_hash = hex::encode(Sha512::digest("".as_bytes()));
            let sign_payload = format!(
                "GET\n/api/v4/futures/usdt/tickers\n\n{}\n{}",
                body_hash, timestamp
            );
            let signature = HmacSha256::sign_static(secret.as_bytes(), sign_payload.as_bytes());

            headers.insert("KEY", key.parse().unwrap());
            headers.insert("SIGN", signature.parse().unwrap());
            headers.insert("Timestamp", timestamp.parse().unwrap());
        }

        let response = self
            .client
            .get(url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;

        let tickers_raw: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;

        let tickers: Vec<Ticker24h> = tickers_raw
            .into_iter()
            .filter_map(|t| {
                let contract = t.get("contract")?.as_str()?.to_string();

                // Convert Gate format to standard format
                // Gate uses: BTC_USD → BTCUSDT, ETH_USD → ETHUSDT
                // Some symbols: BTR_USDTT → BTRUSDT
                let symbol = contract.replace("_USD", "USDT").replace("USDTT", "USDT");

                // Gate uses volume_24h_quote for USD volume
                let quote_volume = t.get("volume_24h_quote")?.as_str()?.parse::<f64>().ok()?;
                let last_price = t.get("last")?.as_str()?.parse::<f64>().ok();
                let price_change_24h_pct = t
                    .get("change_percentage")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| t.get("change_percentage").and_then(|v| v.as_f64()));

                Some(Ticker24h {
                    symbol,
                    quote_volume,
                    last_price,
                    price_change_24h_pct,
                })
            })
            .collect();

        Ok(tickers)
    }

    /// Get symbols with 24h volume > min_volume_usd
    pub async fn get_symbols_with_volume(
        &self,
        min_volume_usd: f64,
    ) -> ExchangeResult<Vec<String>> {
        let tickers = self.get_tickers_with_volume(min_volume_usd).await?;
        let symbols = ticker_symbols(tickers);

        debug!(
            "Found {} symbols with volume >= {} USD",
            symbols.len(),
            min_volume_usd
        );
        Ok(symbols)
    }

    /// Get full ticker snapshots for symbols with 24h volume > min_volume_usd
    pub async fn get_tickers_with_volume(
        &self,
        min_volume_usd: f64,
    ) -> ExchangeResult<Vec<Ticker24h>> {
        let tickers = self.get_24h_tickers().await?;
        Ok(filter_tickers_by_volume(tickers, min_volume_usd))
    }

    /// Get Gate NATR (%) on 30m candles for a symbol.
    pub async fn get_natr_30m(&self, symbol: &str, period: usize) -> ExchangeResult<Option<f64>> {
        if period == 0 || !symbol.ends_with("USDT") {
            return Ok(None);
        }

        let base = symbol.trim_end_matches("USDT");
        let primary_contract = format!("{}_USDT", base);
        if let Some(v) = self
            .get_natr_30m_by_contract(&primary_contract, period)
            .await?
        {
            return Ok(Some(v));
        }

        let alt_contract = format!("{}_USDTT", base);
        if alt_contract != primary_contract {
            return self.get_natr_30m_by_contract(&alt_contract, period).await;
        }
        Ok(None)
    }

    async fn get_natr_30m_by_contract(
        &self,
        contract: &str,
        period: usize,
    ) -> ExchangeResult<Option<f64>> {
        let limit = period.saturating_add(1).max(2);
        let url = format!(
            "https://api.gateio.ws/api/v4/futures/usdt/candlesticks?contract={contract}&interval=30m&limit={limit}"
        );
        debug!("GET {}", url);

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;
        if !response.status().is_success() {
            return Ok(None);
        }

        let mut candles: Vec<GateCandle> = response
            .json()
            .await
            .map_err(|e| crate::domain::ExchangeError::Internal(e.to_string()))?;
        if candles.len() < 2 {
            return Ok(None);
        }

        candles.sort_by_key(|c| c.t);
        let sample_count = period.min(candles.len().saturating_sub(1));
        if sample_count == 0 {
            return Ok(None);
        }

        let start = candles.len().saturating_sub(sample_count);
        let mut tr_sum = 0.0;
        for i in start..candles.len() {
            let candle = &candles[i];
            let Some(high) = value_to_f64(&candle.h) else {
                return Ok(None);
            };
            let Some(low) = value_to_f64(&candle.l) else {
                return Ok(None);
            };
            let Some(close) = value_to_f64(&candle.c) else {
                return Ok(None);
            };
            let prev_close = value_to_f64(&candles[i - 1].c).unwrap_or(close);

            let tr = (high - low)
                .max((high - prev_close).abs())
                .max((low - prev_close).abs());
            tr_sum += tr;
        }

        let atr = tr_sum / sample_count as f64;
        let Some(last) = candles.last() else {
            return Ok(None);
        };
        let Some(last_close) = value_to_f64(&last.c) else {
            return Ok(None);
        };
        if last_close <= 0.0 {
            return Ok(None);
        }

        Ok(Some((atr / last_close) * 100.0))
    }
}

impl Default for GateRestClient {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Deserialize)]
struct GateCandle {
    t: i64,
    h: serde_json::Value,
    l: serde_json::Value,
    c: serde_json::Value,
}

fn value_to_f64(value: &serde_json::Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .or_else(|| value.as_f64())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filter_tickers_by_volume_keeps_only_threshold_matches() {
        let tickers = vec![
            Ticker24h {
                symbol: "A".to_string(),
                quote_volume: 99.0,
                last_price: None,
                price_change_24h_pct: None,
            },
            Ticker24h {
                symbol: "B".to_string(),
                quote_volume: 100.0,
                last_price: None,
                price_change_24h_pct: None,
            },
            Ticker24h {
                symbol: "C".to_string(),
                quote_volume: 101.0,
                last_price: None,
                price_change_24h_pct: None,
            },
        ];

        let filtered = filter_tickers_by_volume(tickers, 100.0);
        let symbols: Vec<String> = filtered.into_iter().map(|t| t.symbol).collect();
        assert_eq!(symbols, vec!["B".to_string(), "C".to_string()]);
    }

    #[test]
    fn ticker_symbols_collects_symbol_column() {
        let tickers = vec![
            Ticker24h {
                symbol: "BTCUSDT".to_string(),
                quote_volume: 0.0,
                last_price: None,
                price_change_24h_pct: None,
            },
            Ticker24h {
                symbol: "ETHUSDT".to_string(),
                quote_volume: 0.0,
                last_price: None,
                price_change_24h_pct: None,
            },
        ];
        assert_eq!(
            ticker_symbols(tickers),
            vec!["BTCUSDT".to_string(), "ETHUSDT".to_string()]
        );
    }

    #[test]
    fn parse_binance_ticker_reads_last_price_field() {
        let raw = serde_json::json!({
            "symbol": "BTCUSDT",
            "quoteVolume": "123.45",
            "lastPrice": "98765.43",
            "priceChangePercent": "1.5"
        });
        let ticker = parse_binance_ticker(raw).expect("ticker should parse");
        assert_eq!(ticker.symbol, "BTCUSDT");
        assert_eq!(ticker.quote_volume, 123.45);
        assert_eq!(ticker.last_price, Some(98765.43));
        assert_eq!(ticker.price_change_24h_pct, Some(1.5));
    }

    #[tokio::test]
    #[ignore = "requires live Binance REST access"]
    async fn test_binance_tickers() {
        let client = BinanceRestClient::new();
        let tickers = client.get_24h_tickers().await.unwrap();
        assert!(!tickers.is_empty());

        // Check BTCUSDT exists
        let btc = tickers.iter().find(|t| t.symbol == "BTCUSDT");
        assert!(btc.is_some());
    }

    #[tokio::test]
    #[ignore = "requires live Binance REST access"]
    async fn test_binance_volume_filter() {
        let client = BinanceRestClient::new();
        let symbols = client.get_symbols_with_volume(1_000_000.0).await.unwrap();
        assert!(!symbols.is_empty());

        // All symbols should have volume >= 1M
        println!("Symbols with volume >= 1M: {}", symbols.len());
        for symbol in symbols.iter().take(10) {
            println!("  - {}", symbol);
        }
    }
}
