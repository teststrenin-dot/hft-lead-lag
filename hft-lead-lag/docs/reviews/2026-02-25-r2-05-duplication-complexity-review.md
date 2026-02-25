# Review: Дублирование, Избыточность, Переусложнение (Round 2)

## Findings

### P2

1. Существенный дублирующийся контур в Binance/Gate connectors (subscription registry, channel-capacity handling, draining).
   - Paths:
     - `src/infrastructure/exchanges/binance/mod.rs:45`
     - `src/infrastructure/exchanges/gate/mod.rs:48`
   - Риск: drift между биржевыми ветками и разъезжающееся поведение при правках.

### P3

1. Gate nested-field extractor остаётся сложным ручным парсером байтового JSON-куска.
   - Path:
     - `src/infrastructure/exchanges/gate/mod.rs:166`
   - Риск: хрупкость и высокая стоимость сопровождения.

2. Дублирование обёрток `get_symbols_with_volume/get_tickers_with_volume` между REST-клиентами сохраняется (частично снижено helper-ами).
   - Paths:
     - `src/infrastructure/rest/mod.rs:103`
     - `src/infrastructure/rest/mod.rs:189`

## Verdict

Локальные P3-упрощения сделаны, но крупные точки дублирования в exchange-коннекторах остаются.
