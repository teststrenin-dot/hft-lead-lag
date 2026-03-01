Глянул **`hft-lead-lag-main (3).zip`** так же, как ты просил: **сначала `docs/status` (core важнее всего), потом код**, и в конце — **сквозной бизнес-процесс “вход → выход”** + **статус**.

---

## 1) Аудит `docs/status/core` — бизнес-задача и бизнес-процессы

### Что стало сильно лучше (и это уже уровень “на весь проект”)

В `docs/status/core/` сейчас ровно 3 канонических документа, и они реально держат проект:

* `2026-02-27-business-objective-economic-control-map.md`
  → появился **numeric KPI envelope** + **fail rule** (“breach 3 окна → degraded/non-HFT”). Это очень правильно.
* `2026-02-27-operating-model-spec-v1.md` (по факту v2)
  → есть **operating model**, state machine, и SLO-контракт.
* `2026-02-26-business-logic-roadmap.md`
  → введены `HFT-CP0..CP7` + `HFT-RM1..RM5` и **Rule→Code→Test matrix**. Это прям анти-эволюция.

### Что нужно “раскидать правильно” (P0 в core)

#### P0.1 — В core не хватает *явного разделения бизнес-процесса по режимам* (`mixed` vs `hft_core`)

Сейчас **V1 Business Process Contract** (про shadow→gate→портфели→ребаланс→paper) описан так, будто это “всегда так работает”.
Но у тебя уже есть режимы:

* `RUNTIME_PLANE_MODE=mixed` — полный бизнес-процесс (shadow/портфели/DB/UI)
* `RUNTIME_PLANE_MODE=hft_core` — **HFT-ядро**, где:

  * runtime-grid выключен,
  * портфельный scheduler выключен,
  * control-plane worker выключен,
  * persistence выключена.

**Рекомендация:** прямо в core (лучше всего в `operating-model-spec…`) добавить раздел:

* **“Business Process — mixed mode”** (как сейчас V1 контракт)
* **“Business Process — hft_core mode”** (Signal→Execution→Health, без Validation/Competition/Risk в виде портфелей)

И в `roadmap.md` в секции “End-to-End Business Process (current stage)” подписать:
✅ *“Это процесс для `mixed` (paper/scout). Для `hft_core` см. отдельный поток.”*

#### P0.2 — Приведи в порядок версионирование файла operating model

Файл называется `…operating-model-spec-v1.md`, но в шапке **“Spec v2”**. Это не смертельно, но это источник дрейфа.
✅ либо переименуй файл в v2, либо верни заголовок в v1.

#### P0.3 — Уточнить смысл узла “Competition” (чтобы не было самообмана)

В core у тебя закреплено:

* shortlist **без overlap** между портфелями
* active **без overlap** между портфелями

Это больше похоже на **allocation/diversification policy**, чем на “гонку за один и тот же символ”.

✅ В `economic-control-map` дописать одной строкой:

* `Competition (v1) = disjoint allocation policy (diversification); not a competitive overlap model.`
  Это уберёт когнитивный конфликт “я думал гонка, а оно распределение”.

---

## 2) Аудит кодовой базы — направление “HFT на слабом сервере (2 ядра)”

### Вердикт

**Направление в целом правильное**: ты реально сделал ключевые “HFT-шаги”:

* `RUNTIME_PLANE_MODE` (`mixed` / `hft_core`) — есть
* decouple ingest → control-plane handoff — есть (direct ingest только test)
* control-plane вынесен в отдельный OS thread + coalescing — есть
* 2-core caps (`MAX_STRATEGY_SYMBOLS`, `MAX_SCREENER_SYMBOLS`, `MAX_RUNTIME_GRID_CONFIGS`) — есть
* updated-only обработка (`PendingSymbolSet`) — есть
* replay harness, exec queue, SLA telemetry — есть

Но есть **2 P0 проблемы**, которые сейчас мешают назвать это “HFT-forward продуктом” на 2 ядрах.

---

### P0 проблема №1: В `hft_core` всё ещё поднимается HTTP API, где есть эндпоинты, способные запустить Python/Ray

Сейчас `start_api_servers(...)` вызывается **всегда**, даже при `RUNTIME_PLANE_MODE=hft_core` (см. `src/main.rs`, блок где это вызывается без условий).
А HTTP server включает `/api/v1/trials/runner/*`, который внутри умеет запускать `python3 -m ray_driver ...` (см. `src/api/runner.rs`).

Это прямое нарушение твоего же закона из core:

> Python не должен иметь путь в runtime-контур на trading host.

Да, это “по кнопке”, не автоматически — но на прод-хосте это **футган**.

✅ Как исправить (лучший вариант):

1. В `start_api_servers(...)` добавить параметр `RuntimePlaneMode`.
2. В `hft_core`:

   * либо **не стартовать HTTP вообще**, оставить только stdout/metrics,
   * либо стартовать **урезанный health-only сервер**, без trial runner и без “операторских” ручек.

Минимальный патч:

* В `api/http_server.rs` не регистрировать маршруты runner’а, если `plane_mode == hft_core`.

---

### P0 проблема №2: “Numeric HFT SLO” зафиксирован, но нет доказательства, что `hft_core` реально попадает в эти числа

В `docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md` у тебя p99 порядка **600–700ms**, backlog сотни сообщений. Это очевидно **не HFT** по твоему же контракту (2ms).

Я понимаю: этот замер почти наверняка был сделан **до полного RM2/RM5 отделения**, и/или в `mixed` режиме под нагрузкой screener’а.
Но сейчас по core документам ты говоришь “SLO freeze”, значит нужен **новый perf-снимок именно в `hft_core`**.

✅ Что нужно как acceptance для “HFT core реально готов”:

* запустить `RUNTIME_PLANE_MODE=hft_core` на целевом 2-core хосте
* снять 3 consecutive health окна
* показать:

  * `end_to_end.p99 <= 2000us`
  * backlog в конвертах (binance/gate/signal/execution)
  * drops/timeouts = 0 в стабильном окне

Без этого статус “HFT-готов” пока **архитектурный**, но не доказанный.

---

### P1 (не блокер, но даст большой прирост) — чистка hot path в `hft_core`

Даже в `hft_core` у тебя есть “лишние” операции:

1. `event_loop_ingest::ingest_exchange_batch` дедупит latest внутри batch через `HashMap<Bytes, usize>` и делает `ticker.symbol.clone()`
   → лучше дедупить по `strategy_symbol_id` (у тебя он уже проставляется в parse).

2. `ingest_ticker` всегда делает `from_utf8` для symbol, даже когда `control_plane/screener/ws_tx` отсутствуют
   → в `hft_core` это лишнее.

3. `StrategySignal` содержит `symbol: String` и `context: String`
   → при частых сигналах это лишние аллокации.

4. `ExecutionQueueTx` overflow lane держит `HashMap<String, OrderIntent>` под `Mutex`
   → на signal-бурстах это будет давать хвосты.

✅ Правильная эволюция:

* `StrategySignal { symbol_id, ... }` + строка только для UI/логов в control-plane
* overflow в execution keyed by `SymbolId`, не `String`
* batch dedupe keyed by `SymbolId`, не `Bytes`

---

## 3) Корректный бизнес-процесс “от входа до выхода” (с учётом core + реального кода)

Я дам **две версии**, потому что это единственно честно при наличии `mixed` и `hft_core`.

---

### 3.1 `RUNTIME_PLANE_MODE=mixed` — полный бизнес-процесс (paper/scout)

**Вход:** WS котировки Binance/Gate + локальные таймстампы
**Выход:**

* paper аналитика (портфели/shortlist/active, equity/pnl)
* UI/API read model
* артефакты/история (DB, если включено)

**Старт:**

1. строится universe (volume filter)
2. применяется cap профиля 2-core (strategy/screener symbols, max configs)
3. (если не hft_core) грузится runtime-grid и ставится fleet configs
4. стартует DB persistence
5. стартует HTTP server + (опционально) WS chart pipeline
6. стартует control-plane worker (отдельный thread)
7. подписка на символы (strategy + screener symbols)
8. старт стратегии + execution runtime
9. основной event loop

**Runtime loop:**

1. получаем batch тиков с биржи
2. `EventLoopState::process_exchange_result`:

   * считает drift метрики
   * **пушит ControlUpdate в bounded queue** (try_send + overflow latest-by-symbol)
   * строит `updated_strategy_symbol_ids` + `strategy_updates`
   * фиксирует stage timestamps
3. обновлённые symbol_id попадают в `PendingSymbolSet`
4. flush strategy updates → `strategy.on_*_book`
5. signal tick: `check_signal(symbol_id)` по pending-битсету → если сигнал → `OrderIntent` в execution queue
6. отдельный execution worker симулирует/шлёт intent, пишет SLA/kill-switch метрики
7. раз в 2 минуты (если enabled) `portfolio_scheduler_tick_v1` обновляет shortlist/active и guards

**Control-plane worker:**

* coalesce updates (latest wins per symbol/exchange)
* вызывает `screener.update(...)`
* публикует UI snapshots

---

### 3.2 `RUNTIME_PLANE_MODE=hft_core` — HFT forward ядро (на trading host)

**Вход:** только strategy symbols WS котировки
**Выход:** только:

* сигналы/интенты исполнения
* health SLO (latency/backlog/drop)
* (опционально) raw feed запись / replay-артефакты

**Старт:**

1. universe → cap strategy symbols
2. runtime-grid **не грузится**
3. persistence **не инициализируется**
4. control-plane worker **не стартует**
5. portfolio scheduler **выключен**
6. подписка **только на strategy symbols**
7. стратегия + execution runtime + event loop

**Runtime:**

1. receive ticks
2. updated-only signal check
3. `OrderIntent` → execution queue
4. SLA/kill-switch + health

**Важно:** сейчас у тебя в hft_core всё равно поднимается HTTP сервер, и там остаётся путь к Python runner — это я бы фикснул как P0.

---

## 4) Статус проекта по этому репо (как есть сейчас)

### Статус по `docs/status/core/2026-02-26-business-logic-roadmap.md`

**HFT checkpoints:**

* `HFT-CP0 … HFT-CP6`: **Completed**
* `HFT-CP7`: **In progress** (health enforcement есть, watchdog/recovery ещё нет)

**Remediation:**

* `HFT-RM1 … HFT-RM5`: **Completed**

### Мой “аудиторский статус” (честный)

* **Стратегия/процессы в core стали сильными** (есть SLO, state machine, mapping to code/tests).
* **Архитектура реально двигается в HFT-формат** (planes, caps, isolation).
* Но **HFT-ready по числам пока не доказано**:

  * в evidences есть старые p99 сотни миллисекунд,
  * нет “3 окна подряд” health-снимка в `hft_core`, который попадает в SLO.

### P0 блокеры перед тем, как считать “HFT forward продукт готов”

1. В `hft_core` убрать/отрезать API-путь, который может запускать `python3 -m ray_driver` (runner endpoints).
2. Сделать **новый perf evidence**: `hft_core` на 2-core host → 3 health окна внутри SLO.

---

## Что я бы делал следующим PR’ом (самый выгодный)

1. **Health-only API в `hft_core`** (или вообще no API): отрезать runner, trials, любые “оркестрации”.
2. **Batch dedupe по `SymbolId`**, не по `Bytes`, в `ingest_exchange_batch`.
3. **StrategySignal без String** (symbol_id), и execution overflow keyed by SymbolId.

Если скажешь “ок, делаем”, я могу расписать **точечный patch-plan по файлам** именно для этих трёх PR (с какими структурами/сигнатурами и где резать роуты API в `hft_core`).
