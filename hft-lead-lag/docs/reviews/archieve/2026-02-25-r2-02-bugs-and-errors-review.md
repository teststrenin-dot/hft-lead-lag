# Review: Баги и Ошибки (Round 2)

## Findings

### P1

1. Watcher trial-batch может пропустить быстрые перезаписи при одинаковых `(mtime, len)`.
   - Paths:
     - `src/main.rs:330`
     - `src/main.rs:884`
   - Деталь: fingerprint-сравнение не учитывает содержимое.
   - Риск: драйвер ждёт ack до таймаута, фактически новый payload не применён.

### P2

1. Watcher runtime-grid игнорирует обновления с тем же `mtime`.
   - Path:
     - `src/main.rs:940`
   - Риск: операторские изменения `runtime-grid.toml` могут молча не применяться.

2. `DbWriter` продолжает дропать батчи при насыщении primary+overflow очередей.
   - Path:
     - `src/infrastructure/db.rs:367`
   - Риск: потеря трейдов и искажение trial analytics в пике.

## Что улучшилось с прошлого раунда

- `run_id`-коллизии закрыты.
- Структурированный `error` ack для apply reject закрыт.

## Verdict

Качество фиксов высокое, но в watcher-контуре и контролируемой деградации DB writer остаются практические reliability-риски.
