# R7 — Dead Code Review

Date: 2026-02-26

## Findings

### P1
1. Dead auth branch in Gate REST ticker call (plus latent `unwrap`).
- Evidence: `src/infrastructure/rest/mod.rs:168`, `:181`, `:198`.
- Note: no in-tree callsites set credentials before this endpoint.

### P2
1. Unused field: `GateMarketData::is_authenticated`.
- Evidence: `src/infrastructure/exchanges/gate/mod.rs:93`, `:105`, `:423`.

2. Unused field: `BinanceMarketData::api_key`.
- Evidence: `src/infrastructure/exchanges/binance/mod.rs:85`, `:96`, `:121`.

3. Unused config field: `WsServerConfig::max_clients`.
- Evidence: `src/api/ws_server.rs:46`, `:54`.

4. Unused method in-tree: `MarketDataServer::publish`.
- Evidence: `src/api/ws_server.rs:78`.

5. Legacy constructors in-tree unused (`HttpServer::new`, `with_min_volume`, `start`, `MarketDataServer::start`).
- Evidence: `src/api/http_server.rs:76`, `:80`, `:108`; `src/api/ws_server.rs:88`.

### P3
1. Empty/legacy doc directories cleanup opportunity (`archieve` naming and empty dirs).
