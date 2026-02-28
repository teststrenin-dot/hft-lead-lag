Да — **для HFT‑рантайма на сервере с 2 ядрами Python (и любые “вторые процессы”) надо выносить из прод‑контура**, иначе ты сам себе создаёшь джиттер/хвосты/конкуренцию за CPU.
Но важная оговорка: **“убить питон” само по себе не сделает систему HFT**, потому что основная “тормозящая масса” у тебя сейчас *уже внутри Rust* (строки/локи/HashMap/BTreeSet/SQLite в tokio‑потоке).

Я распаковал твой zip и посмотрел hot‑path места. Прямые источники “не‑HFT” поведения:

* `src/event_loop_core.rs`: `HashMap<String, BookTicker>`, `BTreeSet<String>`, `.cloned().collect()` на каждый батч.
* `src/event_loop_ingest.rs`: `updated_symbols_from_batch()` делает `String::from_utf8_lossy(...).to_string()` + `sort_unstable()` + `dedup()` **на каждый батч**.
* `src/application/services/lead_lag.rs`: `RwLock<HashMap<String, BookTicker>>` + `Mutex` + `now_utc()` в `check_signal()`.
* `src/domain/screener/quote_ingest.rs`: `store.symbols.entry(symbol.to_string())` — **аллоцирует `String` на каждый тик**.
* `src/infrastructure/exchanges/common.rs`: `extract_json_*` делает `format!()` для паттерна поля **каждый раз**, а `Bytes::copy_from_slice` — копию поля (это не zero‑copy).
* `src/infrastructure/db.rs`: SQLite flush в `tokio::spawn` (блокирующие операции внутри tokio‑воркера) → хвосты p99 на 2 ядрах гарантированы.

Ниже — **конкретный план “вывести в HFT forward‑product” под ограничение 2 CPU**, с тем, какие файлы и подход менять, и какой результат принимать.

---

## 1) Ответ на “надо ли убивать питон?”

**Да, в прод‑рантайме.** На 2 ядрах:

* **Ray/Python‑оркестратор (папка `ray_driver`) не должен жить на том же боксе**, где крутится engine.
* Python оставь как **офлайн‑инструмент** (локально / отдельный сервер): генерит конфиги → ты копируешь `runtime-grid.toml` / `trial-batch.json` на прод (или вообще фиксируешь конфиг руками).

Если ты сейчас реально запускаешь рядом `ray_driver`/Ray — это почти гарантированно и есть “почему всё каша”.

---

## 2) Минимальная целевая архитектура под 2 ядра

Один процесс **Rust**, но внутри 2 “домена” по CPU (2 потока/2 рантайма):

### Вариант A (самый практичный): два токио‑рантайма, два потока

* **Thread 0 (Engine / Hot):** WS ingest + market state + signal + order intent
  *Никаких SQLite, никаких API, никаких file-watch.*
* **Thread 1 (Control / Warm):** API/UI + DB writer + hot reload + telemetry + всё “раз в секунды/минуты”.

Коммуникация: каналы (`crossbeam`/`flume`/tokio mpsc), но главное — **hot поток не ждёт warm**.

### Вариант B (проще внедрить, хуже по хвостам): один токио runtime на 2 worker threads

Тогда ОБЯЗАТЕЛЬНО:

* весь SQLite (rusqlite) — только через `spawn_blocking` или отдельный std::thread
* API/templating/logging — не должны лить нагрузку в те же воркеры, где engine

Я бы для “HFT‑форвард” выбирал **Вариант A**.

---

## 3) Roadmap по рефактору: 6 PR’ов с максимальным ROI

Сделаю в твоём стиле: checkpoint’ы с acceptance. Это можно реально “тащить” без ресурсов на отдельный сервер.

---

### HFT‑CP0 — Метрики задержки и джиттера (чтобы перестать гадать)

**Цель:** видеть p50/p95/p99 internal latency.

**Сделать:**

* добавить в event loop измерения:

  * `ws_recv_ts_ns` (у тебя уже есть `now_ns()` в reader)
  * `parsed_ts_ns`
  * `state_update_ts_ns`
  * `signal_ts_ns`
* вывести гистограмму/percentiles в `/health` или в лог раз в 5с.

**Acceptance:**

* ты видишь `p50/p99 internal (recv→signal)` и `max`, плюс drop counts.

---

### HFT‑CP1 — Убрать `String` из hot‑path вообще (самый большой выигрыш)

Сейчас у тебя на каждом тике/батче создаются строки (`from_utf8_lossy → to_string`) и гоняются по HashMap/BTreeSet.

**Решение:** ввести `SymbolId` и работать по ID.

**Что добавить:**

* `type SymbolId = u16;` (у тебя максимум ~2000 символов — хватит)
* `Universe`:

  * `id_to_symbol: Vec<Arc<str>>`
  * `symbol_key_to_id: HashMap<SymbolKey, SymbolId>`
* `SymbolKey`: без аллокаций, например `u128` (упаковать bytes + длину)

**Какие файлы менять сначала:**

* `src/domain/messages.rs`: добавить `BookTickerLite { symbol_id, bid_ticks, ask_ticks, exchange_ts_ns, local_ts_ns }`
* `src/infrastructure/exchanges/binance/mod.rs` и `gate/mod.rs`: парсить и возвращать `BookTickerLite`, а не `BookTicker(Bytes)`
* `src/event_loop_core.rs`: убрать `HashMap<String, BookTicker>` и `BTreeSet<String>`, заменить на массивы/битсеты (см. CP2)

**Acceptance:**

* в `perf`/логах исчезают массовые аллокации `String`, `sort_unstable`, `dedup`
* internal latency p99 падает/стабилизируется

---

### HFT‑CP2 — Переписать `EventLoopState` на массивы + bitset (вместо HashMap/BTreeSet)

Текущее:

* `latest_bn: HashMap<String, BookTicker>`
* `pending_signal_symbols: BTreeSet<String>`
* `strategy_ticks_in_order().cloned().collect()`

**Должно стать:**

* `latest_bn: Vec<Option<BookTickerLite>>` (индекс = symbol_id)
* `latest_gt: Vec<Option<BookTickerLite>>`
* `pending: Vec<u64>` bitset (или `fixedbitset`)
* `updated: SmallVec<[SymbolId; 64]>` или просто “установить бит и всё”

**Точки изменения:**

* `src/event_loop_core.rs::EventLoopState`

  * `process_exchange_result()` возвращает не `Vec<String>`, а `Vec<SymbolId>` или bitset‑дельту
  * `mark_pending_signal_symbols()` становится `pending.set(id)`
  * `handle_signal_tick()` итерирует по битсету (и сбрасывает)
* `src/event_loop_ingest.rs`:

  * убрать `updated_symbols_from_batch()` со строками и сортировкой
  * `strategy_ticks_in_order` больше не нужен (или становится прямым индекс‑доступом)

**Acceptance:**

* обработка батча = O(k) где k — реально обновлённые символы, без сортировок/клонов
* p99 стабилен, CPU падает

---

### HFT‑CP3 — `LeadLagStrategy` превратить в sync “engine” без локов и без `now_utc()`

Сейчас `LeadLagStrategy`:

* хранит `RwLock<HashMap<String, BookTicker>>` и `Mutex`
* `check_signal()` дергает системное время через `time::OffsetDateTime::now_utc()`

**Нужно:**

* `LeadLagEngine`:

  * поля: `primary: Vec<Option<BookTickerLite>>`, `hedge: Vec<Option<BookTickerLite>>`
  * `clock_offset_primary`, `clock_offset_hedge` (без Mutex, потому что один поток‑владелец)
* `on_primary_book(tick)` и `on_hedge_book(tick)` — **обычные функции**, без async
* `check_signal(symbol_id, now_ns)` — pure function без аллокаций

**Файлы:**

* переписать `src/application/services/lead_lag.rs` (или сделать новый модуль `application/engine/lead_lag_engine.rs`, а старый оставить для совместимости)

**Acceptance:**

* в hot path нет `RwLock::read/write().await`, нет `Mutex::lock()`
* no `String::to_string()` при формировании сигнала (в лог берёшь `&id_to_symbol[id]`)

---

### HFT‑CP4 — Парсер WS: убрать `format!()` и `Bytes::copy_from_slice` в hot parse

Сейчас в `common.rs`:

* `format!("\"{}\"", field)` на каждый вызов
* `Bytes::copy_from_slice` для каждого найденного поля

Это прямой источник джиттера.

**Как проще всего:**

* Для bookTicker и gate book_ticker сделать **специализированный парсер**, который:

  * ищет `b"\"s\":\""` / `b"\"b\":\""` / `b"\"a\":\""` и т.д. (константы)
  * возвращает **срез** `&[u8]` (не копию) внутри текущего `Vec<u8>`
  * сразу парсит `bid/ask` в ticks
  * `symbol` сразу превращает в `SymbolId` через `SymbolKey`

**Файлы:**

* `src/infrastructure/exchanges/common.rs` (добавить “fast path парсеры”)
* `binance/mod.rs`, `gate/mod.rs` (использовать их)

**Acceptance:**

* “нулевые” копии строк/bytes на тик
* падение CPU в коннекторах и меньше хвостов

---

### HFT‑CP5 — SQLite/DB writer: вынести из tokio worker threads

Сейчас `spawn_writer()` в `infrastructure/db.rs` делает rusqlite операции внутри `tokio::spawn`, т.е. **блокирует воркер**.

На 2 ядрах это почти гарантированно даёт пиковые задержки в engine.

**Правильно:**

* DB writer должен жить в отдельном `std::thread::spawn` (или хотя бы `spawn_blocking`)
* каналы остаются, но flush/transactions — только на DB‑потоке

**Acceptance:**

* при интенсивной записи trades p99 engine не деградирует

---

## 4) Что делать со Screener/Portfolio (твоя бизнес‑логика) в контексте HFT

Тут ключевое: **сейчас Screener обновляется на каждом тике и при этом аллоцирует `String`** (`symbol.to_string()`), плюс DashMap и прочее.

Если ты хочешь HFT‑ядро — тебе надо решить:

### Путь 1 (быстро в HFT): отделить Screener от hot path

* Hot engine торгует выбранные символы (например, top‑N заранее)
* Screener либо:

  * получает **семплированные** данные (например, раз в 50–100мс на символ),
  * либо вообще живёт офлайн/replay

Это самый дешёвый путь к реальному “форвард‑HFT”.

### Путь 2 (правильный системный): Screener тоже переводить на `SymbolId` и массивы

То есть:

* `ScreenerStore` перестаёт быть `DashMap<String, SymbolState>`
* становится `Vec<SymbolState>` (индекс = SymbolId)
* API read‑model уже сам маппит `SymbolId -> String` только на отдаче

Это много работы, но это “протянуть весь бизнес‑процесс” без деградации.

**Компромисс (я бы так сделал на 2 ядрах):**

* сначала внедрить SymbolId+arrays только в hot engine (CP1–CP3),
* затем Screener перевести в warm‑режим: обновлять не каждый тик, а “последний тик за интервал” (например, 20–50мс) → и уже потом оптимизировать его структуры.

---

## 5) Режимы запуска на твоём сервере (2 ядра) — чтобы реально стало лучше уже сейчас

Даже до рефактора можно “не убить себя”:

1. **Не запускай Ray/Python на этом же боксе.**
2. Уменьши `*_MSG_CHANNEL_CAPACITY` (Binance/Gate) до 2–8k, потому что:

   * для bookTicker тебе важен latest, а не очередь на 25k (это только увеличивает задержку “старых” сообщений).
3. Логи:

   * перестань логировать каждый сигнал на `info!` в проде (это I/O + форматирование).
   * логируй агрегаты раз в 1–5 секунд.
4. DB:

   * увеличить flush interval или batch size
   * и/или временно отключить запись “всё подряд” (пока не вынесешь в отдельный поток)

---

## 6) Если хочешь — я дам тебе прямо “каркас целевых модулей” под твой код

Вот минимальная структура, которая прямо ложится на твой репо:

* `src/hot/universe.rs` — SymbolId/SymbolKey/Universe
* `src/hot/tick.rs` — BookTickerLite / Side enum
* `src/hot/engine.rs` — LeadLagEngine (sync)
* `src/hot/event_loop.rs` — EventLoopState на Vec/bitset
* `src/control/` — API/DB/hotreload (как есть, но вынесено на другой поток)
* коннекторы (`infrastructure/exchanges/*`) возвращают `BookTickerLite`

---

### Главное резюме

**Да, на 2 ядрах ты должен “сжать” прод‑контур до одного Rust процесса и убрать Python/Ray из runtime.**
Но чтобы это стало HFT, тебе важнее сделать три вещи:

1. **SymbolId вместо String везде в hot path**
2. **Vec/bitset вместо HashMap/BTreeSet/сортировок**
3. **SQLite и API на другом потоке (warm), не на воркере engine**

Если хочешь, следующим сообщением я напишу **очень конкретный “порядок правок по файлам” (как чек‑лист PR’ов)** с тем, какие типы вводить и какие функции переписать в твоих:
`event_loop_core.rs`, `event_loop_ingest.rs`, `lead_lag.rs`, `binance/mod.rs`, `gate/mod.rs`, `common.rs`, `db.rs`.
