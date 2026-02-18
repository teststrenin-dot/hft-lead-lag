// Test REST client directly
use hft_lead_lag::infrastructure::rest::BinanceRestClient;

#[tokio::main]
async fn main() {
    let client = BinanceRestClient::new();
    
    println!("Fetching Binance 24h tickers...");
    match client.get_24h_tickers().await {
        Ok(tickers) => {
            println!("✅ Got {} tickers", tickers.len());
            
            // Show top 10 by volume
            let mut sorted = tickers.clone();
            sorted.sort_by(|a, b| b.quote_volume.partial_cmp(&a.quote_volume).unwrap());
            
            println!("\nTop 10 by 24h volume:");
            for t in sorted.iter().take(10) {
                println!("  {:12} ${:>15.2}", t.symbol, t.quote_volume);
            }
            
            // Filter by 1M
            let filtered: Vec<_> = tickers.iter().filter(|t| t.quote_volume >= 1_000_000.0).collect();
            println!("\nSymbols with volume >= $1M: {}", filtered.len());
        }
        Err(e) => println!("❌ Error: {}", e),
    }
}
