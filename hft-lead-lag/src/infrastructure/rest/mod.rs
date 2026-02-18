//! REST clients for exchange data (cold path only)
//! 
//! Used for:
//! - Getting 24h volume for symbol filtering
//! - Authentication (listen keys, etc.)
//! - Order placement (fallback)

use reqwest::{Client, header::HeaderMap};
use serde::Deserialize;
use tracing::debug;
use crate::domain::ExchangeResult;
use crate::infrastructure::exchanges::common::HmacSha256;

/// REST client configuration
#[derive(Debug, Clone)]
pub struct RestConfig {
    pub base_url: String,
    pub timeout_ms: u64,
    pub max_retries: u32,
}

impl Default for RestConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            timeout_ms: 10000,
            max_retries: 3,
        }
    }
}

/// 24h ticker data
#[derive(Debug, Clone, Deserialize)]
pub struct Ticker24h {
    pub symbol: String,
    pub quote_volume: f64,  // 24h USD volume
    pub last_price: Option<f64>,
    pub price_change_24h_pct: Option<f64>,
}

/// Binance REST API client
pub struct BinanceRestClient {
    client: Client,
    base_url: String,
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

        let response = self.client
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
            .filter_map(|t| {
                let symbol = t.get("symbol")?.as_str()?.to_string();
                
                // Skip non-USDT pairs and invalid symbols
                if !symbol.ends_with("USDT") {
                    return None;
                }
                
                // Skip symbols with non-ASCII characters
                if !symbol.chars().all(|c| c.is_ascii_alphanumeric()) {
                    return None;
                }
                
                // Parse quoteVolume - can be string or number
                let quote_volume = t.get("quoteVolume")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| t.get("quoteVolume").and_then(|v| v.as_f64()))?;
                
                // Parse last price - optional, can be string or number
                let last_price = t.get("last")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| t.get("last").and_then(|v| v.as_f64()));
                let price_change_24h_pct = t.get("priceChangePercent")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .or_else(|| t.get("priceChangePercent").and_then(|v| v.as_f64()));

                Some(Ticker24h {
                    symbol,
                    quote_volume,
                    last_price,
                    price_change_24h_pct,
                })
            })
            .collect();

        debug!("Filtered to {} valid tickers", tickers.len());
        Ok(tickers)
    }

    /// Get symbols with 24h volume > min_volume_usd
    pub async fn get_symbols_with_volume(&self, min_volume_usd: f64) -> ExchangeResult<Vec<String>> {
        let tickers = self.get_tickers_with_volume(min_volume_usd).await?;
        
        let symbols: Vec<String> = tickers
            .into_iter()
            .map(|t| t.symbol)
            .collect();

        debug!("Found {} symbols with volume >= {} USD", symbols.len(), min_volume_usd);
        Ok(symbols)
    }

    /// Get full ticker snapshots for symbols with 24h volume > min_volume_usd
    pub async fn get_tickers_with_volume(&self, min_volume_usd: f64) -> ExchangeResult<Vec<Ticker24h>> {
        let tickers = self.get_24h_tickers().await?;
        Ok(tickers
            .into_iter()
            .filter(|t| t.quote_volume >= min_volume_usd)
            .collect())
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
            use sha2::{Sha512, Digest};
            use std::time::{SystemTime, UNIX_EPOCH};
            
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .to_string();
            
            let body_hash = hex::encode(Sha512::digest("".as_bytes()));
            let sign_payload = format!("GET\n/api/v4/futures/usdt/tickers\n\n{}\n{}", body_hash, timestamp);
            let signature = HmacSha256::sign_static(secret.as_bytes(), sign_payload.as_bytes());
            
            headers.insert("KEY", key.parse().unwrap());
            headers.insert("SIGN", signature.parse().unwrap());
            headers.insert("Timestamp", timestamp.parse().unwrap());
        }

        let response = self.client
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
                let symbol = contract
                    .replace("_USD", "USDT")
                    .replace("USDTT", "USDT");
                
                // Gate uses volume_24h_quote for USD volume
                let quote_volume = t.get("volume_24h_quote")?.as_str()?.parse::<f64>().ok()?;
                let last_price = t.get("last")?.as_str()?.parse::<f64>().ok();
                let price_change_24h_pct = t.get("change_percentage")
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
    pub async fn get_symbols_with_volume(&self, min_volume_usd: f64) -> ExchangeResult<Vec<String>> {
        let tickers = self.get_tickers_with_volume(min_volume_usd).await?;
        
        let symbols: Vec<String> = tickers
            .into_iter()
            .map(|t| t.symbol)
            .collect();

        debug!("Found {} symbols with volume >= {} USD", symbols.len(), min_volume_usd);
        Ok(symbols)
    }

    /// Get full ticker snapshots for symbols with 24h volume > min_volume_usd
    pub async fn get_tickers_with_volume(&self, min_volume_usd: f64) -> ExchangeResult<Vec<Ticker24h>> {
        let tickers = self.get_24h_tickers().await?;
        Ok(tickers
            .into_iter()
            .filter(|t| t.quote_volume >= min_volume_usd)
            .collect())
    }
}

impl Default for GateRestClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_binance_tickers() {
        let client = BinanceRestClient::new();
        let tickers = client.get_24h_tickers().await.unwrap();
        assert!(!tickers.is_empty());
        
        // Check BTCUSDT exists
        let btc = tickers.iter().find(|t| t.symbol == "BTCUSDT");
        assert!(btc.is_some());
    }

    #[tokio::test]
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
