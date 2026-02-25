# Review: Дизайн Screener (Round 2)

## Findings

### P2

1. Хотя `data_source/is_fallback/last_update_ms` теперь есть в API-модели, UI screener их не визуализирует.
   - Paths:
     - `src/api/handlers.rs:186`
     - `src/api/templates/screener.html:100`
   - Риск: оператор не видит происхождение и свежесть данных на экране.

2. `symbols` map в screener растёт без TTL/eviction, а `rows_sorted` делает полный scan+sort на каждый запрос.
   - Path:
     - `src/domain/screener/mod.rs:366`
   - Риск: рост latency/memory при расширении universe.

## Сильные стороны

- Богатая symbol-state модель и аккуратная связка с shadow/fleet.
- Инкрементальный patch-контур и метрики матчей/дренажа выглядят зрелыми.

## Verdict

Screener по доменной части сильный, но operator UX и масштабируемость выдачи требуют доработки.
