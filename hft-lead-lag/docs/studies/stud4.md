Ниже — **аудит именно этого zip (4)**: я посмотрел `docs/status/*` (в первую очередь `core`), потом ключевые узлы runtime’а (main → setup → event loop → ingest/control/execution → API surface), и сверил это с тем, что ты называешь “HFT-based-only на слабом сервере (2 core)”.

---

## 1) Аудит `docs/status`: бизнес‑задача и бизнес‑процессы разложены правильно?

### Что хорошо (и почему это уже “правильно разложено”)

У тебя сейчас **очень правильная структура**:

* `docs/status/core/` — **канонические** документы (стратегия/контракты/роадмеп).
* `docs/status/dynamics/` — **доказательства** (evidence), экспериментальные отчёты, runbook’и, валидации SLO, итд.

Это ровно то, что нужно, чтобы не возвращаться в “эволюционную кашу”:
core = *что строим и по каким законам*, dynamics = *почему мы верим, что это работает*.

### Конкретно по `core` (что я увидел)

1. `2026-02-27-business-objective-economic-control-map.md`

   * Есть **Locked Business Objective**, **Economic Control Map**, **Architecture Law**, **Numeric KPI envelope**, **Mode boundary**.
   * Важный плюс: явно написано, что **UI observer-first**, и **hft_core не должен тащить UI/runner**.

2. `2026-02-27-operating-model-spec-v1.md`

   * Очень ценная штука: “**mode-specific process boundary**” — то, чего обычно не хватает и из-за чего всё превращается в монолит.

3. `2026-02-26-business-logic-roadmap.md`

   * Сейчас это фактически “**HFT runtime track roadmap**” (HFT-CP0…HFT-CP7 + RM1…RM5) + формальный E2E бизнес‑процесс для `mixed` и ссылка на редуцированный `hft_core`.

### Где я бы “доточил” core (не потому что плохо, а потому что станет ещё жёстче)

**(A) Добавь короткий `docs/status/core/README.md`** на 30–50 строк:

* что считается “каноном”;
* какие файлы менять при изменении поведения;
* 2 режима (`mixed`/`hft_core`) в двух абзацах;
* 1 таблица: “какой документ отвечает за что”.

**(B) Уточни в Control Map пункт про `Competition` одним предложением**
Сейчас ты уже сделал “v1 semantics = disjoint allocation/diversification”. Отлично. Я бы просто добавил в одном месте формулировку:

* “в v1 portfolios делят **один** ranked pool (shadow stats), а не имеют независимые ранкеры/метрики”.

Это снимет будущие споры “а почему не независимая гонка”.

---

## 2) Аудит кодовой базы: направление на HFT при 2 ядрах — правильное?

**Да, направление правильное.**
Причём не на уровне слов, а на уровне *реальных инженерных границ*:

### Что уже сделано правильно (ключевые “засечки”)

#### 2.1. Ты ввёл строгий split режимов (`RUNTIME_PLANE_MODE`)

В `main.rs` есть `RuntimePlaneMode { Mixed, HftCore }`, и дальше режим реально режет функциональность:

* `hft_core`:

  * **нет** persistence (`db_writer=None`)
  * **нет** runtime grid hot reload
  * **нет** NATR refresher
  * **нет** control-plane worker
  * **нет** portfolio scheduler
  * API surface = **HealthOnly** (только `/health`), то есть **нет** trial runner/orchestration routes
  * подписки на биржи = **только strategy symbols** (а не screener universe)

Это прям “правильная HFT-инженерия”: *режим задаёт физику процесса*.

#### 2.2. Ты отделил control-plane от data-plane (RM2/RM5)

* В `mixed` режиме runtime не делает `screener.update` в hot path.
* `ControlPlaneTx` + dedicated OS thread + coalescing по `(symbol, exchange)` — это уже близко к промышленной схеме.

#### 2.3. Ты вернул event-driven signal loop (CP7 block2)

Сейчас сигнал‑тик запускается **по приходу** маркет‑события, а не таймером.
В `docs/status/dynamics/2026-02-28-cp7-block2-event-driven-signal-loop-evidence.md` у тебя зафиксирована SLO‑валидность.

#### 2.4. Есть операционная “жёсткость”: RM4 enforcement в `/health`

`/health` не просто “ok”, а реально:

* считает p99 latency,
* считает backlog,
* смотрит drop/timeout counters,
* и после 3 окон делает `degraded_non_hft`.

Это круто, потому что “HFT” у тебя теперь не мнение, а **контракт**.

---

## 3) Что в коде всё ещё “не идеально” для HFT‑ядра (и как переделать)

Ниже — **не философия**, а конкретные технические места, которые сейчас (в zip4) всё ещё могут тянуть ненужный overhead в `hft_core` или мешать “идеальному трейлу”.

### P0. В `hft_core` ты всё ещё делаешь работу ingest’а, которая там не нужна

Файл: `src/event_loop_ingest.rs`
Функция: `ingest_exchange_batch(...)`

Даже когда:

* `ctx.screener.is_none()`
* `ctx.ws_tx.is_none()`
* `ctx.control_plane.is_none()`

…функция всё равно:

* строит `HashMap<Bytes, usize>` (positions),
* клонирует Bytes-ключи,
* делает `from_utf8(symbol)` для каждого тикера (пусть и без `String` аллокации),
* гоняет dedupe‑логику, которая нужна для screener/ws/control-plane.

**Почему это важно:**
`hft_core` у тебя концептуально “Signal→Execution→Health”. Если ingest занимается “подготовкой для screener”, ты обратно загрязняешь hot path.

**Как исправить (лучшее/дешёвое):**
Добавь быстрый early-return:

* если нет screener/ws/control-plane — делай только то, что реально нужно для health (например: tick_count + drift sampling), и **не строить HashMap**.

Пример (идея, не дословный копипаст):

```rust
pub fn ingest_exchange_batch(..., ctx: &mut BatchIngestContext) {
    let no_fanout = ctx.screener.is_none() && ctx.ws_tx.is_none() && ctx.control_plane.is_none();
    if no_fanout {
        // только health counters / drift, без symbol parsing и dedupe
        for t in drained.iter().chain(tickers.iter()) {
            ctx.ticker_count += 1;
            if let Some(ts) = t.exchange_ts_ms {
                ctx.record_tick_drift(exchange, ts);
            }
        }
        return;
    }

    // текущий “полный” путь остаётся для mixed
    ...
}
```

Это даст:

* меньше аллокаций/хеширования,
* меньше CPU на batch,
* более “чистое” соответствие operating model: `hft_core` = минимум.

### P0. Логирование каждого сигнала в hot path

Файл: `src/event_loop_core.rs`
Функция: `handle_signal_tick`

Там сейчас на каждый сигнал:

```rust
tracing::info!(... "Signal detected: ...")
```

В HFT это почти всегда убийца p99:

* форматирование,
* сериализация полей,
* синхронизация лог-саба.

**Что сделать:**

* либо опустить до `debug!` и включать только при отладке,
* либо “sampling” (раз в N секунд),
* либо логировать агрегат: `signals_detected_per_window`, `max_spread_seen`, etc.

### P1. `StrategySignal` содержит `String` и `context: String`

Файл: `src/application/strategies/mod.rs`

Сейчас это ок для mixed/UI, но для hft_core идеал:

* внутренний сигнал = `symbol_id + direction + spread_bps + ts`,
* а строковые поля и context — только на boundary (UI/logging).

**Минимальная эволюция без боли:**

* оставить `StrategySignal` как DTO,
* но внутри data-plane (hft_core) иметь `StrategySignalLite` без аллокаций,
* конвертировать в `StrategySignal` только когда реально надо (в mixed, в UI, или при debug).

### P1. Health-only API всё равно поднимает “лишнее состояние”

Файл: `src/api/http_server.rs`

`HttpServer::serve` даже в `HealthOnly`:

* открывает DB (`open_db`),
* создаёт `TrialRunnerManager`,
* держит `ScreenerStore` (DashMap + Mutex + ArcSwap и т.п.).

Это **не ломает** SLO, потому что это “в основном на старте”, но для “идеального трейла hft_core” это грязно.

**Правильная архитектура (когда будет время):**

* отдельный `HealthOnlyState { health: Arc<HealthState>, ... }`
* отдельный router без DB/trial/screener.

### P1. RM4 enforcement применяется ко всем режимам

`/health` всегда применяет строгие RM4 пороги и может вернуть `503` даже в `mixed`.

Это двояко:

* как “жёсткий quality gate” — полезно,
* как “сервис жив/мертв” для mixed/UI — может быть раздражающе.

**Компромисс:**

* в `mixed` делать RM4 breach = warning (не issue),
* а `degraded_non_hft` строго для `hft_core`.

Для этого нужно просто добавить в `HttpState` поле `runtime_plane_mode` (или bool `hft_core`) и ветку в `health_response`.

### P2. Control-plane queue capacity (default 8192) не соответствует идее “freshness > backlog”

В `event_loop_control.rs` дефолт capacity огромный, хотя у тебя есть coalescing map “latest wins”.

На слабом сервере правильнее:

* держать маленькую очередь (256–1024),
* а “latest-by-key overflow lane” пусть работает.

Иначе очередь просто становится буфером устаревших апдейтов.

---

## 4) “Корректный бизнес‑процесс от входа до выхода” (как ты просил) — в двух режимах

Ниже — именно **процесс программы**, не “теория стратегии”.

---

### 4.1. Процесс `mixed` mode — полный “бизнес контур” (race + UI)

**Вход**: WS market data + (опционально) запуск scout/forward через API
**Выход**: paper-race состояния, снапшоты, метрики, UI наблюдение, run artifacts

#### Startup

1. Load config:

   * `ConfigManager::from_env()`
   * читается `TRADING_MODE` (сейчас фактически `paper`)
   * читается `RUNTIME_PLANE_MODE=mixed`

2. Build runtime universe:

   * `fetch_volume_tickers(MIN_VOLUME_USD)`
   * строится `common_symbols / strategy_symbols / screener_symbols`
   * применяются 2-core caps (RM3): `MAX_STRATEGY_SYMBOLS`, `MAX_SCREENER_SYMBOLS`, `MAX_RUNTIME_GRID_CONFIGS`

3. Init runtime services:

   * создаётся `ScreenerStore`
   * выставляются `portfolio_ids` (через `PORTFOLIO_IDS`), window size, configs
   * поднимается `DbWriter` (persistence включена)
   * поднимается `ControlPlane` worker (dedicated thread)

4. Start HTTP API (Full surface):

   * `/health`
   * screener/portfolio endpoints
   * runner endpoints (`scout`, `forward`), но **в рамках ограничений** (server-side guards)

5. Start market data connectors:

   * Binance WS
   * Gate WS
   * (опционально) raw feed recorder если задан `RAW_FEED_RECORD_PATH`

#### Runtime loop (основной цикл)

6. На каждый WS frame:

   * parse → `BookTicker` (минимальные копии, SymbolId уже прикручен)
   * stage timestamps пишутся в `HealthState`

7. Data-plane update:

   * обновляется strategy state (через очередь strategy_updates → flush)
   * формируются `updated_strategy_symbol_ids`

8. Signal loop:

   * берём pending symbol ids (битсет)
   * `check_signal(symbol_id)` до budget
   * если сигнал есть → enqueue `OrderIntent` в execution queue (non-blocking)

9. Control-plane fanout (в отдельном thread/runtime):

   * `ControlUpdate` coalescing by `(symbol, exchange)`
   * `screener.update(...)`
   * WS/UI fanout (если включено)

10. Portfolio scheduler (каждые 120_000ms):

* строит ranked pool из shadow stats
* раздаёт shortlist без overlap
* выбирает active без overlap (`<=4`)
* применяет guards/cooldown правила
* пишет snapshot в DB

#### Exit / failure handling

11. `/health`:

* показывает SLO, backlog, drop/timeout, stall signals, alert_level
* RM4 breach windows → может переводить run в degraded

12. Остановка процесса сейчас по сути внешняя (kill/systemd).
    **Рекомендация**: позже добавить graceful shutdown:

* stop WS,
* flush DB writer,
* финальный snapshot.

---

### 4.2. Процесс `hft_core` mode — минимальное execution ядро (HFT‑контур)

**Вход**: WS market data
**Выход**: execution intents (пока симулированный send) + health SLO gate

#### Startup

1. Load config + `RUNTIME_PLANE_MODE=hft_core`

2. Build runtime universe:

   * берёшь symbols (после caps)
   * подписки = только `strategy_symbols`

3. Init runtime:

   * **не стартуешь** DbWriter
   * **не стартуешь** control-plane worker
   * **не стартуешь** portfolio scheduler
   * API = HealthOnly (`/health`)

4. Start connectors + event loop + execution worker.

#### Runtime loop

5. На каждый WS frame:

   * parse → apply strategy book update
   * pending symbol ids → check_signal → enqueue intent
   * execution worker пытается “send” с timeout/kill-switch
   * health counters обновляются (latency/backlog/drop)

#### SLO gate

6. `/health`:

   * если 3 окна подряд breach — status = `degraded_non_hft` и HTTP 503.

---

## 5) Текущий статус проекта (по факту репо + status docs)

### Статус по HFT checkpoint’ам (из core + dynamics)

* `HFT-CP0` ✅ Completed (observability: timestamps + latency/backlog)
* `HFT-CP1` ✅ Completed (SymbolId, снятие аллокаций, dedupe by id)
* `HFT-CP2` ✅ Completed (lock-free single-owner strategy)
* `HFT-CP3` ✅ Completed (updated-only + pending bitset)
* `HFT-CP4` ✅ Completed (minimal-copy parse path)
* `HFT-CP5` ✅ Completed (record/replay harness)
* `HFT-CP6` ✅ Completed (execution queue + SLA telemetry + kill-switch)
* `HFT-CP7` 🟡 In progress (ops automation tail: “scheduler integration policy”)

### Статус по remediation (RM*)

* `RM1..RM5` ✅ Completed (plane split, decoupling, caps, SLO freeze, control-plane isolation)

### Самое важное: HFT‑SLO валидирован

У тебя есть evidence, что после block2 (event-driven signal loop) `hft_core` на целевом хосте снова укладывается в RM4 envelope.

---

## 6) Мой короткий “план доведения до идеального HFT forward продукта” (на твоих ресурсах)

Если цель — **не “красиво”, а “работает и не жрёт время”**, я бы сделал так:

### Priority 0 (сразу)

1. **No-op ingest в hft_core** (early return) — чтобы `hft_core` реально был “clean kernel”.
2. **Убрать `info!` per signal** (или sampling).
3. (Опционально) **уменьшить control-plane queue capacity** (mixed) до 256–1024.

### Priority 1 (чтобы стало “как у взрослых”)

4. `StrategySignalLite` (SymbolId, без String) в hot path.
5. Health RM4 evaluation учитывать режим (`mixed` ≠ `hft_core`).

### Priority 2 (полировка)

6. HealthOnly server без DB/trial/screener init.
7. Монотонное время для внутренних latency stats (чтобы NTP не делал странностей).

---

Если хочешь — я могу в следующем сообщении накидать **конкретный diff-план** (какие файлы/функции трогаем и какие тесты добавляем) ровно для P0/P1, чтобы ты это сделал за 1–2 коротких сессии без расползания scope.



/////////////////////////
////////////////////////
///////////////////////
//////////////////////


Ок — даю **ровно P0/P1** в формате “какие файлы/функции трогаем + какие тесты добавляем”, и сразу прикладываю **готовые диффы** (на основе твоего текущего `hft-lead-lag-main (4).zip`).

---

## P0 — “снять тупой CPU/alloc налог” (2-core friendly, HFT hot-path)

### P0.1 — Fast-path в ingest: когда нет observer/control-plane, не делаем dedupe+clone вообще

**Проблема сейчас:** даже в `hft_core` (где `screener/ws/control_plane` отключены) `ingest_exchange_batch()` всё равно:

* создаёт `HashMap`,
* клонирует `Bytes` для ключей,
* строит `latest`-вектор,
* потом вызывает `ingest_ticker()` (в котором есть UTF-8 decode).

Это **пустая работа** в `hft_core`: сигнал-ядро и так живёт по стратегии/обновлениям, а ingest-ветка для screener/ws/control-plane.

**Решение:** добавить **ранний выход**:

* если `ctx.screener/ws_tx/control_plane == None`, то:

  * один раз берём `local_ms`,
  * просто инкрементим `ticker_count`,
  * пишем `record_tick_drift()` для телеметрии,
  * `return`.

**Файл/функция:**

* `src/event_loop_ingest.rs`
* `ingest_exchange_batch()`

**Тест:**

* `src/main_tests.rs`
* новый тест доказывает, что в fast-path нет UTF-8 decode (кладём `symbol=b"\xFF"` — невалидный UTF-8, но счётчик и drift обновляются).

#### DIFF (P0.1)

```diff
diff --git a/src/event_loop_ingest.rs b/src/event_loop_ingest.rs
@@
 pub(super) fn ingest_exchange_batch<F: Fn() -> i64>(
     first: &hft_lead_lag::domain::BookTicker,
     drained: &[hft_lead_lag::domain::BookTicker],
     ctx: &mut BatchIngestContext<'_, F>,
 ) {
+    // Ultra-hot fast path: when no observer/control-plane surfaces are attached
+    // (typical `hft_core` mode), avoid symbol decoding + per-batch dedupe allocations.
+    // We only maintain lightweight counters/telemetry.
+    if ctx.screener.is_none() && ctx.ws_tx.is_none() && ctx.control_plane.is_none() {
+        let local_ms = (ctx.now_ms)();
+        for ticker in std::iter::once(first).chain(drained.iter()) {
+            *ctx.ticker_count += 1;
+            ctx.metrics.record_tick_drift(local_ms, ticker.exchange_ts_ns);
+        }
+        return;
+    }
+
     let mut positions: HashMap<bytes::Bytes, usize> = HashMap::with_capacity(drained.len() + 1);
     let mut latest: Vec<&hft_lead_lag::domain::BookTicker> = Vec::with_capacity(drained.len() + 1);
```

```diff
diff --git a/src/main_tests.rs b/src/main_tests.rs
@@
 #[test]
+fn ingest_exchange_batch_fast_path_updates_counters_without_utf8_decode() {
+    // `hft_core`-style path: no screener, no ws, no control-plane.
+    // Use an invalid UTF-8 symbol to prove we do not decode symbols on this path.
+    let first = hft_lead_lag::domain::BookTicker::new(
+        bytes::Bytes::from_static(b"\xFF"),
+        1,
+        2,
+        1,
+        1,
+        100_000_000,
+        100_000_001,
+    );
+    let drained: Vec<hft_lead_lag::domain::BookTicker> = Vec::new();
+    let mut ticker_count = 0usize;
+    let mut metrics = EventLoopMetrics::new();
+    let now_ms = || 150i64;
+    let mut ctx = BatchIngestContext {
+        exchange: "binance",
+        ticker_count: &mut ticker_count,
+        metrics: &mut metrics,
+        now_ms: &now_ms,
+        screener: None,
+        ws_tx: None,
+        control_plane: None,
+    };
+
+    ingest_exchange_batch(&first, &drained, &mut ctx);
+
+    assert_eq!(ticker_count, 1);
+    assert_eq!(
+        metrics.drift_stats_string_and_reset(),
+        "n=1 avg=50ms p50=50ms p95=50ms p99=50ms max=50ms"
+    );
+}
```

**Acceptance (P0.1):**

* В `RUNTIME_PLANE_MODE=hft_core` пропадает аллокационный мусор на batch-ingest.
* `/health.runtime_latency_us.ingest.p99` должен стать заметно ровнее на bursts.

---

### P0.2 — Убрать `signal.clone()` из hot path + сделать логирование сигналов управляемым (sampling/disable)

**Проблема сейчас:** в `handle_signal_tick()` делается:

* `signal.clone()` (тащит `String`-поля, `context`, etc),
* `info!` на **каждый** сигнал (I/O + форматирование).

На 2 ядрах это убивает jitter и latency.

**Решение:**

1. Логировать сигнал **до** передачи в `OrderIntent`, но **не двигая** его поля (используем `.as_str()`).
2. Убрать `clone()` — просто **move** `signal` в `OrderIntent`.
3. Добавить `SIGNAL_LOG_EVERY`:

   * `0` = выключить,
   * `N` = логировать каждый N-й сигнал.
   * default:

     * `mixed` -> `1` (чтобы поведение по умолчанию не ломать),
     * `hft_core` -> `0` (чтобы не загрязнять hot path).

**Файлы/функции:**

* `src/event_loop_core.rs`

  * `EventLoopState { signal_log_every }`
  * `set_signal_log_every()`
  * `handle_signal_tick()`
* `src/event_loop_runtime.rs`

  * `EventLoopRuntimeContext { signal_log_every }`
  * установка в state
* `src/main.rs`

  * env `SIGNAL_LOG_EVERY`
  * `signal_log_every_from_env()`
  * проброс в `EventLoopRuntimeContext`

**Тесты:**

* `src/main_tests.rs`

  * `signal_log_every_defaults_to_verbose_in_mixed_and_disabled_in_hft_core`
  * `signal_log_every_env_override_allows_zero_and_positive_values`

#### DIFF (P0.2)

```diff
diff --git a/src/event_loop_core.rs b/src/event_loop_core.rs
@@
 pub(super) struct EventLoopState {
     pub(super) ticker_count: usize,
     pub(super) signal_count: usize,
+    signal_log_every: u64,
@@
 impl EventLoopState {
     pub(super) fn new() -> Self {
         Self {
             ticker_count: 0,
             signal_count: 0,
+            signal_log_every: 1,
@@
     pub(super) fn set_screener_ingest_enabled(&mut self, enabled: bool) {
         self.screener_ingest_enabled = enabled;
     }
+
+    pub(super) fn set_signal_log_every(&mut self, every: u64) {
+        self.signal_log_every = every;
+    }
@@
             if let Some(signal) = signal {
                 self.signal_count += 1;
+                let should_log_signal = self.signal_log_every > 0
+                    && (self.signal_count as u64 % self.signal_log_every == 0);
+                if should_log_signal {
+                    info!(
+                        "{} signal #{}: {} | spread={:.2}bps | dir={} | bid_ask={:.2}bps ask_bid={:.2}bps | {}",
+                        signal.strategy,
+                        self.signal_count,
+                        signal.symbol.as_str(),
+                        signal.spread_bps,
+                        signal.direction.as_str(),
+                        signal.bid_ask_bps,
+                        signal.ask_bid_bps,
+                        signal.context.as_str()
+                    );
+                }
                 let enqueued_ts_ns = Self::now_ns();
                 let intent = OrderIntent {
-                    signal: signal.clone(),
+                    signal,
                     signal_decided_ts_ns,
                     enqueued_ts_ns,
                 };
@@
-                info!( ... per-signal log ... );
             }
```

```diff
diff --git a/src/event_loop_runtime.rs b/src/event_loop_runtime.rs
@@
 pub(super) struct EventLoopRuntimeContext<'a> {
@@
     pub(super) portfolio_scheduler_enabled: bool,
+    pub(super) signal_log_every: u64,
@@
     let mut state = EventLoopState::new();
     state.set_screener_ingest_enabled(runtime_context.screener_ingest_enabled);
+    state.set_signal_log_every(runtime_context.signal_log_every);
```

```diff
diff --git a/src/main.rs b/src/main.rs
@@
 const SIGNAL_CHECK_BUDGET_PER_TICK: usize = 256;
+/// Per-signal info log sampling. `0` disables signal logs.
+/// In `hft_core` mode default is `0` to protect the hot path.
+/// In `mixed` mode default is `1` to preserve existing verbose debugging.
+const SIGNAL_LOG_EVERY_ENV: &str = "SIGNAL_LOG_EVERY";
@@
+fn parse_env_u64(name: &str) -> Option<u64> { ... }
+
+fn signal_log_every_from_env(runtime_plane_mode: RuntimePlaneMode) -> u64 {
+    if let Some(v) = parse_env_u64(SIGNAL_LOG_EVERY_ENV) {
+        return v;
+    }
+    if runtime_plane_mode.hft_core() { 0 } else { 1 }
+}
@@
         EventLoopRuntimeContext {
@@
             portfolio_scheduler_enabled: !runtime_plane_mode.hft_core(),
+            signal_log_every: signal_log_every_from_env(runtime_plane_mode),
             health_state: health_state.as_ref(),
```

```diff
diff --git a/src/main_tests.rs b/src/main_tests.rs
@@
+#[test]
+fn signal_log_every_defaults_to_verbose_in_mixed_and_disabled_in_hft_core() {
+    let _lock = env_test_lock();
+    std::env::remove_var(SIGNAL_LOG_EVERY_ENV);
+    assert_eq!(signal_log_every_from_env(RuntimePlaneMode::Mixed), 1);
+    assert_eq!(signal_log_every_from_env(RuntimePlaneMode::HftCore), 0);
+}
+
+#[test]
+fn signal_log_every_env_override_allows_zero_and_positive_values() {
+    let _lock = env_test_lock();
+    std::env::set_var(SIGNAL_LOG_EVERY_ENV, "0");
+    assert_eq!(signal_log_every_from_env(RuntimePlaneMode::Mixed), 0);
+    std::env::set_var(SIGNAL_LOG_EVERY_ENV, "1000");
+    assert_eq!(signal_log_every_from_env(RuntimePlaneMode::HftCore), 1000);
+    std::env::remove_var(SIGNAL_LOG_EVERY_ENV);
+}
```

**Acceptance (P0.2):**

* В `hft_core` по умолчанию **нет per-signal logs**.
* В hot-path больше нет `signal.clone()` → меньше alloc/jitter.
* Можно включить дебаг: `SIGNAL_LOG_EVERY=1` (или `50/100/1000`).

---

## P1 — “почистить контрольную плоскость, чтобы `hft_core` был реально минимальным”

### P1.1 — HealthOnly API не должен трогать filesystem и DB

**Проблема сейчас:** даже когда ты запускаешь `hft_core` (и HTTP surface = `HealthOnly`), сервер всё равно:

* делает `create_dir_all(data/)`,
* открывает/инициализирует sqlite (`open_db`).

Это **лишний I/O**, и на слабом сервере это просто мусор в модели “kernel only”.

**Решение:** в `HttpServer::serve()` оборачиваем DB init в `if surface == Full`.

**Файл/функция:**

* `src/api/http_server.rs`
* `HttpServer::serve()`

**Тесты:**

* (Минимально достаточно) — это ветка на старте.
  Если хочешь прям unit-тест: можно вынести `init_db_if_needed(surface, db_path)` в отдельную функцию и тестировать “не создаёт data/ при HealthOnly”. Но это уже “P1.1a”.

#### DIFF (P1.1)

```diff
diff --git a/src/api/http_server.rs b/src/api/http_server.rs
@@
     pub async fn serve(
         &self,
         listener: tokio::net::TcpListener,
     ) -> Result<(), Box<dyn std::error::Error>> {
         let db_path = PathBuf::from("data/optimizer.db");
-        if let Some(parent) = db_path.parent() {
-            std::fs::create_dir_all(parent)?;
-        }
-        // Run schema init/migrations once on server boot; handlers use read-only opens.
-        let _ = crate::infrastructure::db::open_db(&db_path)?;
+        // In `HealthOnly` surface we intentionally avoid touching the filesystem
+        // (DB init/migrations) to keep `hft_core` deployments minimal.
+        if matches!(self.surface, HttpServerSurface::Full) {
+            if let Some(parent) = db_path.parent() {
+                std::fs::create_dir_all(parent)?;
+            }
+            // Run schema init/migrations once on server boot; handlers use read-only opens.
+            let _ = crate::infrastructure::db::open_db(&db_path)?;
+        }
```

**Acceptance (P1.1):**

* В `hft_core` запуск `/health` не создаёт `data/optimizer.db` и не инициирует sqlite.
* В `mixed` ничего не ломается: DB по-прежнему поднимается на старте.

---

## Как это запускать и чем мерить эффект

### Рекомендуемые env для “реально HFT kernel” на 2 ядра

```bash
RUNTIME_PLANE_MODE=hft_core
SIGNAL_LOG_EVERY=0
```

### Что смотреть в `/health` после P0

* `runtime_latency_us.ingest/decision/end_to_end` — особенно `p99`
* `runtime_backlog_depth.*` — не должно “ползти вверх”
* `execution_dropped_intents`, `execution_send_timeouts` — должны быть 0 на стабильном окне
* `hft_mode_status` — должен оставаться `hft`, без `degraded_non_hft`

---

## TL;DR: что именно ты получаешь после этих P0/P1

* **P0**: убираем **alloc+hashmap+utf8** из `hft_core` ingest, убираем `signal.clone()`, и убираем **инфолог на каждый сигнал** (с управляемым семплингом).
* **P1**: `HealthOnly` API больше не трогает DB/FS вообще — `hft_core` становится реально “тонким ядром”.

Если хочешь — следующим шагом (уже **P2**, не в этом ответе) я бы делал **StrategySignalLite + SymbolId-keyed execution overflow** (убрать String clone из `ExecutionQueueTx::overflow_latest_by_symbol`), потому что это будет следующим ощутимым источником аллокаций при частых сигналах.
