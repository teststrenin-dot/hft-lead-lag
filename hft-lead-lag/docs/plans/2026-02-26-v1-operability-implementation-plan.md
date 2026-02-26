# V1 Operability Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Довести систему до реально работоспособного состояния по зафиксированной `v1` бизнес-спецификации (теневой флот -> кандидатный котел -> 2 портфеля -> eject/reset/cooldown), с проверяемыми метриками и наблюдаемостью.

**Architecture:** Добавляем отдельный `portfolio runtime` слой в `application/services`, который принимает агрегированные данные флота, строит shortlist на каждый портфель, разрешает конфликты по символам и применяет eject/reset правила. Интегрируем его в существующий runtime цикл (ребаланс каждые 2 минуты) и API read-model. Состояние портфелей и guard-метрики сохраняем в SQLite для перезапуска и дебага.

**Tech Stack:** Rust (tokio, axum, rusqlite), existing ScreenerStore/ShadowFleet, SQLite WAL, cargo test, pytest (ray_driver).

---

### Task 1: Зафиксировать `v1` правила в тестах (RED)

**Files:**
- Create: `src/application/services/portfolio_runtime.rs`
- Create: `src/application/services/portfolio_runtime_tests.rs`
- Modify: `src/application/services/mod.rs`

1. Добавить unit tests на `eligible(symbol)`:
   - `age_minutes_from_first_tick > 5`
   - `closed_trades > 5`
   - `useful_winrate >= 0.30`
   - `avg_pnl_pct >= 0`
2. Добавить unit tests на ranking tuple:
   - `(useful_winrate desc, pm_raw desc, avg_pnl_pct desc, closed_trades desc)`
3. Добавить unit tests на conflict rule:
   - один символ не может быть активен сразу в 2 портфелях
   - победитель конфликта выбирается по tuple.
4. Run: `cargo test portfolio_runtime -- --nocapture`
   - Expected: FAIL (модуля пока нет / неполная реализация).

### Task 2: Реализовать portfolio runtime ядро (GREEN)

**Files:**
- Modify: `src/application/services/portfolio_runtime.rs`
- Modify: `src/application/services/mod.rs`

1. Реализовать структуры:
   - `SymbolStatsV1`
   - `PortfolioId` (`A`, `B`)
   - `PortfolioStateV1`
   - `PortfolioEngineV1`
2. Реализовать функции:
   - `compute_useful_winrate()`
   - `compute_pm_raw()`
   - `eligible()`
   - `rank_candidates()`
   - `assign_without_overlap()`
3. Ограничения:
   - `0..=4` символа на портфель
   - отдельный top-5 shortlist на каждый портфель
   - запрет overlap символов.
4. Run: `cargo test portfolio_runtime -- --nocapture`
   - Expected: PASS.

### Task 3: Реализовать eject/reset/cooldown правила (RED -> GREEN)

**Files:**
- Modify: `src/application/services/portfolio_runtime.rs`
- Modify: `src/application/services/portfolio_runtime_tests.rs`

1. Добавить failing tests на stop-loss streak:
   - fast trigger: 5 SL подряд за <= 2 минуты
   - persistent trigger: 6-й SL подряд (если fast не сработал)
   - сброс streak на `pnl > 0`.
2. Реализовать per-symbol guard state:
   - `streak_count`
   - `first_streak_ts_ms`
   - `cooldown_until_ms`
3. Реализовать re-entry:
   - после eject символ возвращается в общий котел только после cooldown 5 минут
   - затем повторно проходит `eligible(symbol)`.
4. Run: `cargo test portfolio_runtime -- --nocapture`
   - Expected: PASS.

### Task 4: Интегрировать runtime в основной цикл (RED -> GREEN)

**Files:**
- Modify: `src/main.rs`
- Modify: `src/event_loop_runtime.rs`
- Modify: `src/event_loop_core.rs`
- Modify: `src/domain/screener/mod.rs`
- Modify: `src/domain/screener/quote_ingest.rs`
- Test: `src/main_tests.rs`

1. Добавить failing integration tests:
   - rebalance tick каждые 2 минуты вызывает обновление портфелей
   - после серии stop-loss символ eject-ится и уходит в cooldown
   - symbol overlap между портфелями не допускается.
2. Прокинуть в runtime контекст shared `PortfolioEngineV1` (Arc/RwLock).
3. На каждом rebalance tick:
   - читать агрегированные symbol stats из флота
   - строить shortlist A/B
   - применять конфликтное распределение.
4. На каждом закрытом трейде:
   - обновлять guard state (streak/cooldown).
5. Run: `cargo test -- --nocapture`
   - Expected: PASS.

### Task 5: Persistence + schema для портфелей (RED -> GREEN)

**Files:**
- Modify: `src/infrastructure/db.rs`
- Modify: `src/api/handlers/helpers.rs`
- Modify: `src/api/handlers.rs`
- Test: `src/api/handlers/tests.rs`

1. Добавить failing tests на DB schema и read-model:
   - наличие `portfolio_state_v1`
   - наличие `portfolio_symbol_guard_v1`
   - корректная загрузка активных портфелей.
2. Добавить schema/migrations в `open_db` через `CREATE TABLE IF NOT EXISTS` + `add_column_if_missing` где нужно.
3. Реализовать upsert/read helper’ы:
   - текущее состояние портфелей
   - cooldown/guard per symbol.
4. Добавить API endpoints:
   - `GET /api/v1/portfolio/active`
   - `GET /api/v1/portfolio/candidates`
   - `GET /api/v1/portfolio/guards`
5. Run:
   - `cargo test api::handlers::tests -- --nocapture`
   - `cargo test infrastructure::db::tests -- --nocapture`
   - Expected: PASS.

### Task 6: Миграция существующей БД и проверка trial метрик

**Files:**
- Modify: `src/infrastructure/db.rs`
- Modify: `src/main.rs`
- Create: `docs/runbooks/2026-02-26-db-migration-and-forward-check.md`

1. Добавить startup self-check на `trial_runs_meta` колонки:
   - `apply_mode`, `symbols_reset`, `changed_ids_requested`, `matched_changed_ids_old`, `matched_changed_ids_new`, `unmatched_changed_ids`, `scope_symbols_requested`, `scope_symbols_matched`.
2. Если колонок нет:
   - применить safe migration через `add_column_if_missing`.
3. Добавить runbook команд проверки:
   - состояние таблиц
   - наличие `forward-*` раннов
   - sanity SQL для метрик forward.
4. Run:
   - `cargo test infrastructure::db::tests -- --nocapture`
   - локальный запуск бинаря для auto-migration.

### Task 7: E2E shadow drill (операционная работоспособность)

**Files:**
- Modify: `docs/runbooks/2026-02-26-shadow-drill-v1.md`
- Optional modify: `config/runtime-grid.toml`, `config/trial-batch.json` (runtime artifacts)

1. Подготовить сценарий прогона:
   - запуск runtime
   - запуск scout/expand/forward/promote
   - проверка portfolio endpoints.
2. Acceptance checks:
   - в БД появляются `forward-*` run_id
   - портфели A/B формируются без overlap
   - eject/reset/cooldown фиксируются в state.
3. Verification commands:
   - `cargo test`
   - `pytest -q ray_driver/tests`
   - SQL sanity queries по `trades`/`trial_runs_meta`/portfolio tables.

### Task 8: Go-Live gate (после shadow стабильности)

**Files:**
- Create: `docs/runbooks/2026-02-26-live-gate-checklist.md`
- Modify (if needed): `src/domain/exchange.rs`, `src/infrastructure/exchanges/gate/mod.rs`, `src/main.rs`

1. Явно определить режимы:
   - `paper` (default)
   - `live` (feature-flag + env kill-switch)
2. Перед live включить mandatory guards:
   - max loss/day
   - max open positions
   - emergency stop
   - order throttle.
3. Без прохождения чеклиста `live` блокируется на старте.

### Task 9: Финальная верификация и фиксация версии

1. Run: `cargo test`
2. Run: `pytest -q ray_driver/tests`
3. Run: shadow drill checklist
4. Обновить `docs/plans/2026-02-26-shadow-fleet-portfolio-target-state-v1.md` со статусом `Implementing` -> `Implemented`.
5. Подготовить changelog по:
   - что сделано
   - что отложено на `v2` (dynamic policy, capital rebalance).
