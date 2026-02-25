# Review: Дизайн Shadow Fleet (Round 2)

## Findings

### P2

1. Policy ranking (`policy_snapshots/top_policy_configs`) практически не экспонируется вовне (кроме тестов).
   - Path:
     - `src/domain/screener/shadow_fleet.rs:504`
   - Риск: оператор не видит, почему config gate-enabled/disabled.

2. Порядок trial queue не FIFO: сортировка по filename лексикографическая, а `submission_id` начинается с `run_id`.
   - Paths:
     - `src/main.rs:416`
     - `ray_driver/ipc.py:62`
   - Риск: более поздний submit может обработаться раньше по алфавитному приоритету run_id.

## Сильные стороны

- Patch/apply-механика строгая, fail-closed и наблюдаемая.
- Контракт `config_id` versioned, incremental path устойчивый.

## Verdict

Ядро shadow-fleet стало значительно надёжнее; оставшиеся риски в основном в observability ranking-а и predictability порядка queue.
