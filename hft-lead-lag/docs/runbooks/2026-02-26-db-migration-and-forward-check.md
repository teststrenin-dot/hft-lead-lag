# DB Migration and Forward Check (2026-02-26)

## Goal

Проверить, что при старте runtime БД автоматически домигрируется до актуальной схемы, и что данные `forward-*` доступны для аналитики и API.

## 1. Startup Migration Check

1. Запустить бинарь (любым стандартным способом), чтобы выполнился `open_db(...)`.
2. Убедиться, что `data/optimizer.db` создан/обновлен.

Проверка ключевых колонок `trial_runs_meta`:

```bash
sqlite3 data/optimizer.db "PRAGMA table_info('trial_runs_meta');"
```

Ожидаемые колонки:

- `run_id`
- `submitted_config_count`
- `applied_at_ms`
- `drained_trades`
- `apply_mode`
- `symbols_reset`
- `changed_ids_requested`
- `matched_changed_ids_old`
- `matched_changed_ids_new`
- `unmatched_changed_ids`
- `scope_symbols_requested`
- `scope_symbols_matched`
- `closed_at_ms`

Проверка портфельных таблиц:

```bash
sqlite3 data/optimizer.db \
"SELECT name FROM sqlite_master WHERE type='table' AND name IN ('portfolio_state_v1','portfolio_symbol_guard_v1');"
```

## 2. Forward Runs Presence

Проверка, есть ли `forward-*` раны вообще:

```bash
sqlite3 data/optimizer.db \
"SELECT run_id, COUNT(*) AS trades, MIN(entry_ts_ms) AS first_ts, MAX(exit_ts_ms) AS last_ts \
 FROM trades \
 WHERE run_id LIKE 'forward-%' \
 GROUP BY run_id \
 ORDER BY last_ts DESC;"
```

Если строк нет, forward-фаза не запускалась в этой БД.

## 3. Forward Metrics Sanity

Сводка по каждому `forward-*`:

```bash
sqlite3 data/optimizer.db \
"SELECT run_id, \
        COUNT(*) AS total_trades, \
        SUM(CASE WHEN pnl_pct > 0 THEN 1 ELSE 0 END) AS wins, \
        ROUND(AVG(pnl_pct), 6) AS avg_pnl_pct, \
        ROUND(SUM(pnl_pct), 6) AS total_pnl_pct \
 FROM trades \
 WHERE run_id LIKE 'forward-%' \
 GROUP BY run_id \
 ORDER BY MAX(exit_ts_ms) DESC;"
```

Сверка patch-метаданных по forward-run:

```bash
sqlite3 data/optimizer.db \
"SELECT run_id, apply_mode, symbols_reset, changed_ids_requested, \
        matched_changed_ids_old, matched_changed_ids_new, unmatched_changed_ids, \
        scope_symbols_requested, scope_symbols_matched, closed_at_ms \
 FROM trial_runs_meta \
 WHERE run_id LIKE 'forward-%' \
 ORDER BY applied_at_ms DESC;"
```

## 4. Portfolio Runtime Snapshot Sanity

Активные портфели:

```bash
sqlite3 data/optimizer.db \
"SELECT portfolio_id, shortlist_json, active_symbols_json, updated_at_ms \
 FROM portfolio_state_v1 \
 ORDER BY portfolio_id;"
```

Guard/cooldown состояния:

```bash
sqlite3 data/optimizer.db \
"SELECT symbol, streak_count, first_streak_ts_ms, cooldown_until_ms, updated_at_ms \
 FROM portfolio_symbol_guard_v1 \
 ORDER BY symbol;"
```

## 5. API Sanity

При работающем runtime:

```bash
curl -s http://127.0.0.1:5000/api/v1/portfolio/active | jq
curl -s http://127.0.0.1:5000/api/v1/portfolio/candidates | jq '.total_candidates'
curl -s http://127.0.0.1:5000/api/v1/portfolio/guards | jq '.total_symbols'
```

## 6. Acceptance Criteria

- `open_db` не падает на старой БД и добавляет отсутствующие колонки.
- `trial_runs_meta` содержит полный набор колонок patch-метаданных.
- Таблицы `portfolio_state_v1` и `portfolio_symbol_guard_v1` существуют.
- SQL запросы по `forward-*` возвращают консистентные метрики (или корректно пустой результат, если forward не запускался).
- API endpoints портфеля отвечают валидным JSON.
