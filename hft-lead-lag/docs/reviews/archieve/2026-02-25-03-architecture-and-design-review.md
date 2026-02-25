# Review: Архитектура и Дизайн

## Findings

### P2

1. Single-path file IPC (`trial-batch.json` + `.trial-ack`) ограничивает многодрайверный сценарий и масштабирование оркестрации.
   - Paths:
     - `src/main.rs:615`
     - `docs/ray-asha-deep-dive.md:286`

2. В watcher hot-reload используются повторные `open_db` в синхронном контексте для apply-пути.
   - Paths:
     - `src/main.rs:514`
     - `src/main.rs:550`
   - Риск: блокирующий I/O в чувствительном контуре при частых батчах.

3. Контракт `config_id` завязан на hash всех полей без version guard.
   - Paths:
     - `src/domain/screener/trader_config.rs:60`
     - `src/main.rs:289`
   - Риск: эволюция `TraderConfig` может ломать incremental-семантику молча.

## Плюсы текущей архитектуры

- Fail-closed валидация incremental patch до применения.
  - `src/domain/screener/mod.rs:220`
- Четкое разделение домен/инфра/API в основной структуре `src/`.

## Verdict

Базовая слоистость хорошая, но контур trial IPC и evolve-контракты требуют укрепления для масштабирования и безопасных изменений схемы.
