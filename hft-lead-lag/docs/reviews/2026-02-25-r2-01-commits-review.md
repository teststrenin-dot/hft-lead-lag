# Review: История Коммитов (main, Round 2)

## Scope

Проверка эволюции `main` после remediation-коммитов `033b47e`, `e71d825`, `e1af2ee`.

## Findings

### P3

1. Queue-ack файлы не очищаются после успешного чтения драйвером.
   - Paths:
     - `src/main.rs:437`
     - `ray_driver/ipc.py:75`
     - `ray_driver/ipc.py:145`
   - Деталь: для `submission_id` ack пишется в `config/trial-acks/<submission_id>.json`, но Python удаляет только legacy `.trial-ack`.
   - Риск: рост `config/trial-acks` в долгоживущем окружении и деградация операционного обслуживания.

## Что улучшилось с прошлого раунда

- `run_id` теперь collision-resistant (`time_ns + uuid`) и закрывает прошлую P1 коллизию.
- Failure-ack на reject в apply-пути реализован.
- Контракт `config_id` стал versioned.

## Verdict

Предыдущий remediation-блок качественно закрывает критичные проблемы; остался операционный cleanup-хвост для ack-очереди.
