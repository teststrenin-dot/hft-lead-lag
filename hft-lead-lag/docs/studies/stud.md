Ок, понял задачу: **не “улучшить текущий прототип”, а довести до HFT‑формата, где есть настоящий hot‑path, понятные latency budgets, детерминированный replay и отделение control plane от data plane.**

Я посмотрел твой репо (внутри реально Rust‑runtime, но есть Python `ray_driver` для оркестрации/ASHA). Самое важное: **ощущение “всё медленно и каша” у тебя сейчас рождается не из того, что “Rust плохой”, а из того, что в горячем пути всё ещё есть:**

* `String` аллокации на каждом тике,
* `RwLock/Mutex` вокруг book state,
* клонирование тиков пачками (`collect().cloned()`),
* и работа со `HashMap<String, ...>` в местах, которые должны быть “array by symbol_id”.

Это убивает как latency, так и стабильность p99.

Ниже — **план вывода в HFT forward‑product** (без философии, с конкретикой “что менять” и “как принять”).

---

# 0) Реалистичная рамка “HFT” для крипты

В крипте (WS + публичный интернет) “настоящий HFT” в микросекундах редко имеет смысл: **внешняя задержка** (exchange → ты → exchange) обычно миллисекунды и доминирует.

Поэтому правильная цель для forward‑HFT продукта:

**A. Внутренняя задержка (ingest → decision → enqueue order intent)**

* p50: десятки–сотни микросекунд
* p99: < 1–2 мс (и главное — без хвостов 10–100мс из-за аллокаций/локов)

**B. Отсутствие джиттера**

* никаких случайных spikes из-за GC/lock contention/alloc/sorting

**C. Детерминированность**

* одинаковый input stream → одинаковые решения

И это достижимо.

---

# 1) Архитектура: разнести проект на 3 “плоскости”

Это главный шаг, который превращает “массу” в продукт.

## 1.1 Hot Path (Data Plane)

Только:

* приём WS кадров,
* парсинг минимально нужных полей,
* обновление in‑memory market state,
* расчёт signal/risk gates,
* генерация order intent,
* отправка в execution pipe.

**Правило:** в hot path не должно быть:

* SQLite
* Axum/UI
* JSON pretty / serde больших структур
* аллокаций строк
* локов между потоками

## 1.2 Warm Path (Control Plane)

* агрегаты статистики (твои candidate metrics, portfolio race),
* 2‑минутные ребалансы портфелей,
* API/UI read‑model,
* snapshot/restore,
* логирование/метрики.

Warm path может быть async, может писать в DB, может быть “удобным”.

## 1.3 Cold Path (Research / Orchestration)

* Ray/ASHA, grid search, офлайн аналитика,
* тяжёлые вычисления, “вектора”, таблички.

Python тут **уместен**. Но он **не должен участвовать** в runtime.

---

# 2) HFT‑пайплайн (checkpoints), который реально выведет в forward‑product

Я дам “чекпоинты” в твоём стиле: каждый — измеримый, тестируемый.

---

## HFT‑CP0: Latency & Allocation Observatory

**Цель:** перестать “чувствовать”, начать измерять.

### Что сделать

1. Поставить таймстампы на этапах:

* `recv_ws_frame_ts`
* `parsed_ts`
* `state_updated_ts`
* `signal_decided_ts`
* `order_intent_enqueued_ts`

2. Добавить гистограммы (p50/p95/p99/max) **внутренней** задержки:

* ingest latency
* decision latency
* end‑to‑end internal

3. Добавить счётчики:

* dropped WS messages (у тебя уже есть на коннекторе)
* dropped batches DB
* backlog depth каналов

### Acceptance

* ты можешь открыть одну страницу/эндпойнт и увидеть **p99 internal** + drop rates
* ты можешь сравнить “до/после” каждого следующих CP

---

## HFT‑CP1: Убить `String`/аллоцирование символов в hot path

Сейчас у тебя в нескольких местах в горячем цикле идёт:

* `String::from_utf8_lossy(...).to_string()`
* `Vec<String>` + `sort_unstable()` + `dedup()`
* `HashMap<String, BookTicker>`

**Это HFT‑убийца №1.**

### Точка атаки (по твоему коду)

* `src/application/services/lead_lag.rs`

  * `update_primary_book/update_hedge_book`: сейчас конвертит символ в `String` и пишет в `RwLock<HashMap<String,...>>`
* `src/event_loop_ingest.rs`

  * `updated_symbols_from_batch`: собирает `Vec<String>`, сортит, дедупит
* `src/event_loop_core.rs`

  * собираешь `ticks: Vec<_> = ... .cloned().collect()` по всем `strategy_symbols`

### Как правильно (минимальная архитектурная версия)

Ввести **SymbolId** (u16/u32) и universe mapping.

**Universe (cold/warm):**

* `Vec<Arc<str>> id_to_symbol`
* `HashMap<Arc<str>, SymbolId>` или лучше `hashbrown`/`fxhash` (cold path ok)

**Hot path:** таскает только `SymbolId`.

**Market state:** `Vec<BookState>` по `SymbolId`, а не HashMap.

### Acceptance

* в профиле на hot path исчезают `alloc::string::String`, `from_utf8_lossy`, `sort_unstable`
* количество аллокаций на тик ≈ 0

---

## HFT‑CP2: Убрать `RwLock/Mutex` из стратегии и книги

Сейчас `LeadLagStrategy` держит books и clock offsets в `Arc<RwLock<...>>` и `Arc<Mutex<...>>`. Даже если contention маленький — это **джиттер**.

### Правильная модель для HFT

**Один thread = один владелец market state.**
Feed‑таски только пушат события в lock‑free очередь.

Схема:

* Thread A (Binance feed) → `SPSC ringbuffer`
* Thread B (Gate feed) → `SPSC ringbuffer`
* Thread C (Strategy engine) читает из обоих ringbuffers, обновляет state, решает сигналы

Execution может быть:

* в том же thread (если не блокирует),
* или отдельный thread D через SPSC.

### Acceptance

* в hot path нет `RwLock::write().await` и `Mutex::lock()`
* p99 internal резко стабилизируется (обычно именно locks дают хвосты)

---

## HFT‑CP3: Перестать “каждый тик прогонять весь universe”

Сейчас ты местами делаешь:

* собрал список `strategy_symbols`
* по нему прошёлся
* `collect().cloned()`
* и по каждому вызвал async обработку

Это не HFT. HFT — это **event‑driven**: пришёл апдейт по символу → обработай только его.

### Как сделать

1. На ingest выставляй флаг “symbol updated”:

* `updated_bitset[symbol_id] = 1`

2. В strategy tick ты берёшь **только** updated ids (и сбрасываешь)
3. Обрабатываешь их

Bitset можно реализовать как:

* `Vec<u64>` (супер быстро),
* или готовый `fixedbitset`.

### Acceptance

* CPU загрузка ≈ пропорциональна реальному числу апдейтов, а не размеру вселенной
* уменьшается tail latency при больших universes

---

## HFT‑CP4: Парсинг WS: “минимум полей, минимум копий”

Ты уже используешь быстрые экстракторы и `fast-float`. Это хорошо.
Но сейчас местами ты копируешь строки (`Bytes::copy_from_slice`) и гоняешь лишнее.

### Принцип

* В hot path парсишь только то, что нужно: bid/ask/ts/symbol
* Symbol не копируешь → маппишь в `SymbolId` и забываешь строку
* Цены сразу в ticks (у тебя уже)

### Acceptance

* на профиле парсинг не доминирует
* нет “копируем символ bytes” в горячем пути

---

## HFT‑CP5: Детеминированный Replay Harness

Это *must*, иначе ты будешь бесконечно “эволюционно дебажить”.

### Что сделать

1. Пишешь “raw feed recorder”: сохраняешь входные WS кадры + recv_ts
2. Делаешь офлайн “replay mode”:

* читает лог
* прогоняет через тот же engine
* сравнивает решения/сделки

### Acceptance

* любой баг воспроизводим локально
* любой performance regression ловится на replay бенчмарке

---

## HFT‑CP6: Execution fast path (forward product)

Если ты реально хочешь “HFT‑форвард”, execution тоже должен быть отдельным контуром:

* очередь `OrderIntent` из стратегии
* минимальная сериализация/подпись
* connection reuse
* строгие таймауты
* kill‑switch

**Важно:** даже если order placement через REST, ты можешь:

* держать keep‑alive,
* убрать лишние JSON roundtrips,
* делать “fire-and-track” асинхронно,
* не блокировать strategy thread.

### Acceptance

* генерация intent не блокируется сетевыми вызовами
* есть чёткие SLA по “intent enqueue → sent” (внутренние)

---

## HFT‑CP7: Продуктовая эксплуатация (что делает это “системой”)

Это то, что превращает твой CP6 в “forward product”, а не в игрушку:

* watchdog per component (feed/engine/execution/dbwriter)
* snapshot/restore **идемпотентный** (без double count и без silent loss)
* алерты на:

  * drift spikes
  * message drop rate
  * stalled engine loop
  * backlog depth
* runbook: “что делать, если …”

---

# 3) Самые сильные “быстрые победы” прямо по твоему текущему коду

Если делать приоритетами, то вот тройка, которая даст максимальный эффект:

## (1) Убрать `String` из `LeadLagStrategy`

В `lead_lag.rs`:

* сейчас: `HashMap<String, BookTicker>` + `String::from_utf8_lossy`
* должно стать: `Vec<BookTicker>` или `Vec<Option<BookTicker>>` по `SymbolId`

## (2) Убрать `collect().cloned()` на каждом tick

В `event_loop_core.rs` ты собираешь `Vec` тиков пачкой и клонируешь.
HFT‑версия: обработка только `updated_symbol_ids`.

## (3) Убрать сортировки `Vec<String>` в ingest

`updated_symbols_from_batch` — это заметная нагрузка и джиттер.
Замена: `SmallVec<SymbolId>` + линейный дедуп (или bitset).

---

# 4) Про “питон и вектора”: как правильно, чтобы не мешал HFT

Ты хочешь скорость — но **не надо убивать Python везде**.

Правильная схема:

* **Python/Ray** остаётся как “офлайн тренажёр/оркестратор”
  → генерит конфиги/пороговые значения/портфельные правила
  → пишет артефакты (json/toml)
* **Rust runtime** читает эти артефакты и торгует
  (без участия Python процесса)

“Вектора” (numpy/polars) нужны:

* для бектеста/пересчётов/аналитики
* для построения гиперпараметров
* для исследований

Но HFT‑online чаще всего **event-driven**, не vectorized.

---

# 5) Что бы я сделал “первым PR”, чтобы ты сразу почувствовал, что это становится HFT

Не переписывать всё. Один “сквозной” PR:

1. Ввести `SymbolId` и `Universe`
2. Перевести market books в `Vec` по `SymbolId`
3. Перевести updated symbols на bitset/vec ids
4. Убрать `RwLock/Mutex` из `LeadLagStrategy`: сделать один owner thread
5. Добавить latency histograms (CP0)

**Эффект:** ты сразу увидишь, что “масса” превращается в engine.

---

Если хочешь — я могу прямо следующим сообщением:

* накидать **конкретную целевую структуру модулей/крейтов** (hft-core / connectors / engine / control-plane),
* и выписать **точный список замен по файлам** (типа: `lead_lag.rs` -> убрать locks, `event_loop_*` -> перейти на ids, `symbols.rs` -> новая модель),
* плюс предложить формат “replay log” и как его встроить так, чтобы потом это стало твоим главным тестовым стендом.
