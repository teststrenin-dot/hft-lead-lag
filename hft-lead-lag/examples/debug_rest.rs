// Debug REST client
use reqwest::Client;

#[tokio::main]
async fn main() {
    let client = Client::new();
    
    println!("Fetching Binance 24h tickers...");
    let response = client
        .get("https://fapi.binance.com/fapi/v1/ticker/24hr")
        .send()
        .await
        .unwrap();
    
    let tickers_raw: Vec<serde_json::Value> = response.json().await.unwrap();
    println!("Raw tickers count: {}", tickers_raw.len());
    
    if let Some(first) = tickers_raw.first() {
        println!("\nFirst ticker:");
        println!("  symbol: {:?}", first.get("symbol").and_then(|v| v.as_str()));
        println!("  quoteVolume: {:?}", first.get("quoteVolume"));
        println!("  quoteVolume type: {}", 
            match first.get("quoteVolume") {
                Some(v) => if v.is_string() { "string" } 
                          else if v.is_number() { "number" } 
                          else { "other" },
                None => "null"
            }
        );
        println!("  last: {:?}", first.get("last"));
    }
    
    // Check how many have valid symbol
    let with_symbol: Vec<_> = tickers_raw.iter()
        .filter(|t| t.get("symbol").and_then(|v| v.as_str()).is_some())
        .collect();
    println!("\nTickers with symbol: {}", with_symbol.len());
    
    // Check how many end with USDT
    let with_usdt: Vec<_> = with_symbol.iter()
        .filter(|t| t.get("symbol")
            .and_then(|v| v.as_str())
            .map(|s| s.ends_with("USDT"))
            .unwrap_or(false))
        .collect();
    println!("Tickers ending with USDT: {}", with_usdt.len());
}
