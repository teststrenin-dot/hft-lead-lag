# Review: Архитектура и Дизайн (Round 2)

## Findings

### P2

1. Invalid JSON в `trial-batches/` удаляется без `error` ack и без quarantine.
   - Path:
     - `src/main.rs:907`
   - Деталь: при parse error runtime логирует ошибку и удаляет файл, но не возвращает структурированный `error` ответ драйверу.
   - Риск: оператор видит timeout/пропуск без явной причины и теряет диагностический артефакт payload-а.

2. В DB writer нет жёсткого механизма backpressure propagation вверх по стеку.
   - Path:
     - `src/infrastructure/db.rs:367`
   - Риск: silent erosion качества данных при перегрузе writer.

### P3

1. Gate subscription запускается строго последовательно.
   - Path:
     - `src/main.rs:1481`
   - Риск: линейный рост времени старта при расширении universe.

## Плюсы текущей архитектуры

- Fail-closed incremental apply и детальные patch-метрики сохранены.
- Блокирующие DB операции на apply-пути выведены в `spawn_blocking`.
- Version guard для `config_id` стабилизировал межъязыковой контракт.

## Verdict

Архитектура стала существенно устойчивее, но queue poison-handling и нагрузочный путь DB persistence ещё требуют доукрепления.
