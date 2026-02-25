# Review: История Коммитов (main)

## Scope

Линейная история `main` (от `471a602` до `d213761`) с фокусом на регрессии устойчивости и незакрытые риски.

## Findings

### P1

1. WS-задача может аварийно завершиться из-за `Mutex` poisoning при `lock().unwrap()`.
   - Commit lineage: заметно после `a73b5f7` (объединение read/write-петель).
   - Paths:
     - `src/infrastructure/exchanges/binance/mod.rs:249`
     - `src/infrastructure/exchanges/gate/mod.rs:325`
   - Риск: единичная panic может перевести источник market-data в перманентный outage.

### P2

1. Реигра подписок при reconnect растет без dedup/trim.
   - Paths:
     - `src/infrastructure/exchanges/binance/mod.rs:289`
     - `src/infrastructure/exchanges/gate/mod.rs:372`
   - Риск: повторные `SUBSCRIBE` могут уткнуться в rate limit и сорвать handshake.

## Verdict

История демонстрирует системное улучшение качества, но transport-layer регрессии в WS остаются и должны идти в первый remediation-пакет.
