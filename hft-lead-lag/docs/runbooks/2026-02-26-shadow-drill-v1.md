# Shadow Drill v1 (2026-02-26)

## Goal

Проверить, что полный pipeline `scout -> expand -> forward -> promote` даёт валидные данные, а портфельный runtime (`active/candidates/guards`) работает операционно.

## Preconditions

- Сервисы market data поднимаются без ошибок.
- БД: `data/optimizer.db` доступна на запись.
- Конфиг runtime-grid валиден.

## 1. Start Runtime

```bash
cargo run --manifest-path hft-lead-lag/Cargo.toml
```

Проверка health:

```bash
curl -s http://127.0.0.1:5000/health | jq
```

## 2. Run Trial Pipeline

Запустить фазы в обычном порядке (через runner UI/API или CLI скрипты проекта):

1. `scout`
2. `expand`
3. `forward`
4. `promote`

Минимально убедиться, что есть `forward-*`:

```bash
sqlite3 data/optimizer.db \
"SELECT DISTINCT run_id FROM trades WHERE run_id LIKE 'forward-%' ORDER BY run_id DESC LIMIT 20;"
```

## 3. Portfolio API Checks

```bash
curl -s http://127.0.0.1:5000/api/v1/portfolio/active | jq
curl -s http://127.0.0.1:5000/api/v1/portfolio/candidates | jq '.total_candidates'
curl -s http://127.0.0.1:5000/api/v1/portfolio/guards | jq '.total_symbols'
```

Проверки:

- В `active` есть оба слота `A` и `B`.
- В `active` нет overlap между `A.active_symbols` и `B.active_symbols`.
- В `guards` при стоп-сериях появляются `cooldown_until_ms`.

## 4. SQL Sanity

Forward-метрики:

```bash
sqlite3 data/optimizer.db \
"SELECT run_id, COUNT(*) total, SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) wins, \
        ROUND(AVG(pnl_pct), 6) avg_pnl, ROUND(SUM(pnl_pct), 6) total_pnl \
 FROM trades \
 WHERE run_id LIKE 'forward-%' \
 GROUP BY run_id \
 ORDER BY MAX(exit_ts_ms) DESC;"
```

Портфельный snapshot в БД:

```bash
sqlite3 data/optimizer.db "SELECT portfolio_id, shortlist_json, active_symbols_json, updated_at_ms FROM portfolio_state_v1 ORDER BY portfolio_id;"
sqlite3 data/optimizer.db "SELECT symbol, streak_count, first_streak_ts_ms, cooldown_until_ms, updated_at_ms FROM portfolio_symbol_guard_v1 ORDER BY symbol;"
```

## 5. Acceptance Criteria

- В БД присутствуют `forward-*` run_id.
- `portfolio_state_v1` и `portfolio_symbol_guard_v1` обновляются в ходе работы.
- API возвращает валидные snapshot’ы портфелей и guard-state.
- Нет пересечений активных символов между `A` и `B`.
