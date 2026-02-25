# Review: Превентивная Архитектура

## Findings

### P1

1. Нет структурированного failure-ack для rejected patch; оператор получает timeout без причины.
   - Paths:
     - `ray_driver/ipc.py:46`
     - `src/main.rs:615`

2. `run_id` коллизии подрывают invariant уникальности run-трассировки.
   - Paths:
     - `ray_driver/scout.py:56`
     - `ray_driver/expand.py:76`
     - `ray_driver/cli.py:187`

### P2

3. Patch-level метаданные не экспонируются в trial API/UI (drained/matched/unmatched/scope).
   - Path:
     - `src/api/handlers.rs:509`

4. Backpressure в DB writer приводит к data-drop вместо controlled degradation.
   - Path:
     - `src/infrastructure/db.rs:261`

## Verdict

Есть сильный fail-closed apply-слой, но наблюдаемость и операторская диагностика отказов patch-процесса требуют доработки.
