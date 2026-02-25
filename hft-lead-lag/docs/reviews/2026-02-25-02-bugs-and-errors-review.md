# Review: Баги и Ошибки

## Findings

### P1

1. `run_id` формируется с секундной гранулярностью (`int(time.time())`), возможны коллизии и смешивание аналитики разных прогонов.
   - Paths:
     - `ray_driver/scout.py:56`
     - `ray_driver/expand.py:76`
     - `ray_driver/cli.py:187`
     - `src/infrastructure/db.rs:65`

2. Trial-batch watcher детектит изменение только по `mtime > prev`; быстрые перезаписи могут пропускаться.
   - Path:
     - `src/main.rs:615`
   - Эффект: драйвер ждет ack до таймаута, хотя файл уже перезаписан.

3. При reject некорректного incremental-патча runtime не пишет структурированный failure-ack.
   - Paths:
     - `ray_driver/ipc.py:46`
     - `src/main.rs:615`
   - Эффект: оператор видит только timeout и ищет причину в логах вручную.

### P2

1. В `DbWriter::send` при backpressure батчи могут дропаться (`try_send`), что ведет к потере данных трейдов при пике.
   - Path:
     - `src/infrastructure/db.rs:261`

## Repro/Signal

- Коллизии `run_id`: два запуска одной фазы в пределах секунды.
- Пропуск watcher: быстрые перезаписи `config/trial-batch.json` с одинаковым coarse `mtime`.
- Failure-ack gap: отправка invalid incremental payload.
