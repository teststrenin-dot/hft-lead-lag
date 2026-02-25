# Review: Когнитивная Нагрузка и God Objects

## Findings

### P2

1. `spawn_runtime_grid_hot_reload` совмещает несколько state-machines (trial control, patch apply, runtime-grid, ack, db updates).
   - Path:
     - `src/main.rs:550`

2. `EventLoopState` + `run_event_loop` концентрируют ingestion, агрегации, routing, ws-broadcast и logging.
   - Paths:
     - `src/main.rs:822`
     - `src/main.rs:1450`

3. Exchange-ветки содержат дубли с риском drift при правках.
   - Paths:
     - `src/main.rs:900`
     - `src/main.rs:1030`

## Verdict

Когнитивная нагрузка в `main.rs` выше безопасного порога для быстрого безрегрессионного изменения.
