Принял. Я распаковал **`hft-lead-lag-main (1).zip`** и сделал аудит в том порядке, который ты просил: **status/core → затем код → затем “правильный” сквозной бизнес-процесс**.

Ниже — без воды: что у тебя уже хорошо, где именно “не HFT”, почему на **2 ядрах** это ломает p99, и как довести до **HFT-forward продукта**.

---

## 1) Аудит `docs/status/core` (бизнес-задача + бизнес-процессы)

### Что хорошо (и это реально “высокоуровневое”)

В `core/` у тебя сейчас ровно то, что должно быть каноном:

* **`Business Objective and Economic Control Map v2`** — чётко фиксирует цель и цепочку управления:
  `Signal → Validation → Competition → Risk → Capital → Feedback`
* **`Operating Model Spec v2 (Rust-only target)`** — описывает единый “исполняемый” operating model + state machine
* **`Business Logic Roadmap — HFT Runtime Track`** — связывает цель/operating model с HFT-CP лестницей

Это уже сильно выше, чем “тактика”.

### Где я бы “расставил правильно”, чтобы не было самообмана

Сейчас в core всё логично, но есть **две критические правки**, без которых проект будет считать себя HFT, не будучи HFT:

#### (A) Переименовать смысл узла `Competition` (или зафиксировать как policy)

В `Economic Control Map v2` и `Operating Model Spec` у тебя прописано:

* **shortlist строится *без overlap* между портфелями**
* **active тоже без overlap**

Это **не “соревнование портфелей за ресурсы”**, это скорее **allocation policy / diversification slicing**.

✅ Рекомендация в core:

* либо прямо назвать это `Allocation` (внутри Competition узла)
* либо оставить `Competition`, но добавить фразу:
  **“Shortlist disjoint = intentional diversification policy; portfolios do not compete for the same symbol at shortlist level.”**

Иначе мозг будет думать “гонка”, а код будет делать “раскидывание”.

#### (B) Добавить числовые HFT-гейты в core (иначе “Completed” = иллюзия)

У тебя в динамике есть доказательства CP2 и там **p99 сотни миллисекунд** и backlog большой (см. `docs/status/dynamics/2026-02-28-cp2-lock-free-p99-evidence.md`). Это означает: **hot path перегружен**.

✅ Рекомендация в core:
в `Economic Control Map` (KPI envelope) и/или в `Roadmap` добавить минимум:

* `internal_e2e_p99_us <= 2000` (пример — 2ms) **на целевом хосте**
* backlog по входящим очередям ~= 0 (или строго ограничен)
* если backlog растёт → это **не HFT режим**, а деградация/перегруз

Без этих чисел ты можешь “выполнить CP1–CP4 архитектурно”, но по факту быть в 400–700ms p99.

---

## 2) Аудит кодовой базы: направление “HFT на слабом сервере” — частично правильное, но сейчас **НЕ доведено**

### Что реально стало лучше (это правильное направление)

Я вижу, что ты продвинулся по HFT-лестнице не на словах:

* **SymbolId/updated-only**: `PendingSymbolSet` (битсет) в `src/event_loop_core.rs`
* **queue-fed стратегия**: `enqueue_strategy_updates → flush_strategy_updates`, без RwLock/Mutex в runtime path
* **replay harness**: `src/infrastructure/replay/raw_feed.rs` + режим `REPLAY_RAW_FEED_PATH`
* **execution fast path**: `src/event_loop_execution.rs` с bounded queue + kill switch
* коннекторы проставляют `strategy_symbol_id` уже на parse (`binance/mod.rs` и `gate/mod.rs`)

Это всё — **правильная архитектурная траектория**.

### Главная проблема: ты всё ещё тащишь “не-HFT фабрику” прямо в data-plane

Сейчас **основной runtime loop** в `src/event_loop_runtime.rs` делает:

* получает тик
* **тут же вызывает** `ingest_exchange_batch(...)`
* а внутри `ingest_ticker(...)` вызывается `screener.update(...)`

А вот **`screener.update`** (см. `src/domain/screener/quote_ingest.rs` + `mod.rs`) делает штуки, которые на **2 ядрах** убьют любой p99:

1. `DashMap<String, SymbolState>` + `symbol.to_string()` на апдейте
   → постоянные аллокации/локи на hot path.

2. Внутри обновления символа ты вызываешь:

   * `update_lag`, `update_cycles`, `tick_shadow`
   * и самое тяжёлое: **`ShadowFleet::tick_all()`** (см. `shadow_fleet.rs`)
     который **проходит по всем конфигам на каждом тике**.

3. У тебя в `config/runtime-grid.toml` стоит `max_configs = 1500`
   → на 2 ядрах это физически превращает runtime в “оптимизатор”, а не HFT engine.

**Итог:** ты можешь быть “lock-free” и “SymbolId”, но всё равно получишь p99 в сотни миллисекунд, потому что выполняешь слишком много работы на тик.

И это подтверждается твоими же evidences: p99 сотни ms + backlog (в CP2 evidence).

---

## 3) Корректный бизнес-процесс “от входа до выхода” для HFT-forward продукта на 2 ядрах

Тут важный момент: на таком железе **обязательно** разделять процесс на 2 плоскости (но можно в одном Rust процессе).

### 3.1 Data Plane (HFT hot path) — “делает деньги / решения”

**Вход:** raw WS frames (Binance/Gate)
**Выход:** `OrderIntent` (paper/live) + минимальные телеметрические метрики

1. WS receive → фиксируем `recv_ts_ns`
2. parse → минимальные поля → `BookUpdate { symbol_id, bid_ticks, ask_ticks, exch_ts, recv_ts }`
3. dedupe latest-per-symbol внутри батча (по `symbol_id`)
4. update strategy state (без локов)
5. updated-only signal evaluation
6. если сигнал → `OrderIntent` → bounded queue (try_send, без ожиданий)
7. execution worker → отправка/эмуляция → `sent_ts` + SLA метрики
8. health endpoint отдаёт p50/p95/p99 + backlog + kill-switch

**Жёсткое правило:** data plane **не вызывает** `ScreenerStore::update()` и **не пишет** в SQLite.

---

### 3.2 Control Plane (warm path) — “выбор/валидация/аналитика”

**Вход:** *семплированный* поток market updates или raw feed файл
**Выход:** candidate stats, portfolio assignment, UI, config promotion

1. получает market updates **через bounded канал** (если канал переполнен — дропаем старое, держим latest)
2. обновляет lag/cycles/shadow модель
3. генерит closed trades → на их основе обновляет candidate history
4. применяет gate/ranking
5. каждые 2 минуты делает portfolio assignment (disjoint shortlist + active)
6. пишет агрегаты в DB (в отдельном blocking thread)
7. UI читает read-model

---

### 3.3 Offline / Cold (исследования / ASHA / grid search)

**Вход:** raw feed / DB / архивы
**Выход:** новый `runtime-grid.toml` / `trial-batch.json` / promoted configs

На твоём сервере с 2 ядрами это **не должно жить**.

---

## 4) Если хочешь HFT-forward на 2 ядрах — что именно переделать (очень детально)

### Шаг 1 — Ввести run-mode и реально отделить planes

Добавь режим (env/cli), например:

* `RUN_MODE=hft_core`
* `RUN_MODE=scout` (текущий)

**В `hft_core`:**

* не запускать:

  * `spawn_runtime_grid_hot_reload(...)`
  * `spawn_gate_natr_refresher(...)`
  * API серверы (или оставить только `/health`)
  * SQLite writer
  * `screener.update()` в ingest path

**В `scout`:**

* всё как сейчас (но тогда ты не HFT, ты “discovery node”)

Где менять: `src/main.rs` (места где ты стартуешь hot reload, refresher, api, db, screener).

---

### Шаг 2 — Убрать `screener.update` из `EventLoopState::process_exchange_result`

Сейчас в `src/event_loop_core.rs`:

* `ingest_exchange_batch(...)` → внутри `ingest_ticker` → `screener.update(...)`

Нужно:

* вместо прямого вызова `screener.update` отправлять в канал **`ControlUpdate`**:

  * `{ symbol_id, exchange, bid_ticks, ask_ticks, exch_ts_ns, recv_ts_ns }`

И этот канал **читает control-plane worker**.

---

### Шаг 3 — Ограничить universe и configs на trading host

Сейчас `build_runtime_universe` может дать много символов.

Для 2 ядер тебе нужен cap, например:

* `MAX_STRATEGY_SYMBOLS=50` (или даже 20)
* `MAX_SCREENER_SYMBOLS=200` (если scout)

И главный cap:

* `MAX_FLEET_CONFIGS_ON_HOST <= 50` (а не 1500)

Если хочешь много — делай это оффлайн.

---

### Шаг 4 — ShadowFleet tick_all сделать не “на каждый тик”

Даже в scout режиме:

* делай `tick_all` **не чаще, чем раз в N миллисекунд** на символ
  (например 50–100ms), либо только при “значимом изменении” (gap/vol trigger).

Иначе ты сам строишь CPU-пилу.

---

### Шаг 5 — DB writer только в отдельном blocking thread

Сейчас у тебя DB writer уже есть, но важно: **никаких rusqlite операций в tokio воркерах**.

Лучший вариант на 2 ядрах:

* один OS thread = DB
* один OS thread = control-plane
* engine thread = отдельно

---

## 5) Мой вердикт “правильное ли направление”

**Траектория правильная**, потому что ты реально сделал важные HFT-шаги (SymbolId/updated-only/lock-free/replay/exec-queue).
Но **в текущем виде это всё ещё не HFT-forward продукт**, потому что ты оставил тяжёлый screener+fleet pipeline на пути тика.

Если ты сделаешь **разделение planes** (хотя бы в одном процессе на 2 threads) и ограничишь workload (symbols/configs), у тебя появится реальный шанс выйти на нормальный p99 на твоём хосте.

---

Если хочешь, следующим сообщением я сделаю тебе прям **“patch-plan по файлам”** (как чеклист PRов):

* что именно вырезать/перенести из `event_loop_ingest.rs` и `event_loop_core.rs`
* как назвать тип `ControlUpdate`
* где завести bounded канал
* как стартовать control-plane worker в `main.rs`
* и какие env-переменные добавить для режима `hft_core` (без новых процессов, только Rust).
