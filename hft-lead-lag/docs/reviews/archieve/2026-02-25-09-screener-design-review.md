# Review: Дизайн Screener

## Findings

### P2

1. API не отдает per-symbol свежесть (`last_update_ms`), хотя state это хранит.
   - Paths:
     - `src/domain/screener/state.rs:46`
     - `src/domain/screener/mod.rs:397`

2. При fallback на REST данные источник не маркируется явно (`is_fallback`/`data_source` отсутствуют).
   - Paths:
     - `src/api/handlers.rs:186`
     - `src/infrastructure/enrichment.rs:83`

## Сильные стороны

- Цепочка обновлений и DTO-консистентность хорошо организованы.
- NATR enrichment и hot path в целом аккуратно интегрированы.

## Verdict

Screener в целом зрелый, но операторская прозрачность «свежести» и происхождения данных недостаточна для уверенного live-использования.
