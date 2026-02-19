# HFT Lead-Lag Documentation

Документация проекта `hft-lead-lag` — обновлена по фактическому коду после серии P0-фиксов и архитектурных рефакторингов.

---

## 1) Статус документации

Для полного технического аудита:

- **`docs/review-2026-02-19-deep-dive.md`** — сверх-детальный deep-dive обзор с привязкой к серверу, включая ревью всех 50 коммитов.

---

## 2) Верифицированный серверный контекст

| Параметр | Значение |
|----------|----------|
| OS | Linux 5.15 (KVM VM) |
| CPU | 2 vCPU Intel Xeon Skylake |
| RAM | 3.8 GiB, swap 9 GiB |
| Location | Tokyo, Japan (Kaopu Cloud / AS138915) |
| Rust | 1.95.0-nightly |
| TCP latency | Binance 5ms, Gate 3ms |

---

## 3) Текущее состояние проекта

| Компонент | Статус | Примечание |
|---|---|---|
| Exchange connectors (Binance/Gate) | ✅ Работает | WS reconnect + exponential backoff + subscription replay |
| Bounded channels | ✅ Работает | 10K capacity + try_send drop policy |
| Fail-fast startup | ✅ Работает | HTTP/WS bind проверяется до запуска event loop |
| Health endpoint | ✅ Работает | Отражает реальный статус Binance/Gate через AtomicBool |
| Drift metrics | ✅ Работает | P50/P95/P99/max в status log каждые 5с |
| Runtime API | ✅ Работает | 6 endpoints (см. ниже) |
| Runtime UI (`/screener`) | ✅ Работает | Таблица + uPlot chart + shadow trade zones |
| Screener lag/drift метрики | ✅ Работает | Ingress timestamps, rolling median, percentiles |
| Shadow Trader (paper mode) | ✅ Работает | Spike-follow модель, paper PnL tracking |
| Order execution (real orders) | ⚠️ Не завершено | Executor-заглушки присутствуют, не подключены |
| Secrets management | ✅ Исправлено | `.env` (gitignored) + `dotenvy` auto-load |
| Codebase | 35 файлов, 5469 LOC | Декомпозированы screener (950→5 файлов), http_server (793→3 файла), executors в отдельные модули |

---

## 4) Архитектура (актуальная)

```text
src/                          5469 LOC, 35 files
├── main.rs                   417 LOC  — event loop, drift metrics, orchestration
├── lib.rs                           — crate root
├── config/mod.rs             198 LOC  — AppConfig from env
├── api/                             — HTTP/WS external interfaces
│   ├── http_server.rs        123 LOC  — server config, routing, HealthState
│   ├── handlers.rs           159 LOC  — request handlers, DTOs
│   ├── templates.rs          321 LOC  — screener dashboard HTML/CSS/JS
│   ├── ws_server.rs          188 LOC  — WebSocket broadcast server
│   ├── health.rs             146 LOC  — HealthChecker (legacy, #[allow(dead_code)])
│   └── mod.rs
├── domain/                          — business logic, no I/O deps
│   ├── screener/
│   │   ├── mod.rs            231 LOC  — ScreenerStore facade + ScreenerRow DTO
│   │   ├── shadow_trader.rs  470 LOC  — spike-follow paper trading engine
│   │   ├── cycle_tracker.rs   90 LOC  — divergence/convergence cycle detection
│   │   ├── state.rs           50 LOC  — SymbolState, Quote, refresh_ws_drift
│   │   └── utils.rs           58 LOC  — percentile, now_ms, timestamp helpers
│   ├── messages.rs           178 LOC  — BookTicker, Trade message types
│   ├── models.rs                    — OrderSide, TimeInForce etc.
│   ├── exchange.rs                  — ExchangeError
│   └── symbols.rs                   — SymbolInfo
├── application/
│   ├── services/
│   │   ├── lead_lag.rs       207 LOC  — LeadLagAnalyzer (unused in main loop)
│   │   └── risk.rs                  — RiskManager skeleton
│   └── ports/mod.rs                 — trait ports (documented, not wired)
└── infrastructure/
    ├── enrichment.rs         139 LOC  — NATR enrichment + fallback screener rows
    ├── exchanges/
    │   ├── binance/
    │   │   ├── mod.rs        398 LOC  — WS connect + reconnect
    │   │   └── executor.rs    29 LOC  — order executor stub
    │   ├── gate/
    │   │   ├── mod.rs        487 LOC  — WS connect + reconnect + parsing
    │   │   └── executor.rs    63 LOC  — order executor stub
    │   ├── common.rs         228 LOC  — shared exchange utilities
    │   └── mod.rs                   — explicit re-exports
    ├── rest/mod.rs           411 LOC  — BinanceRestClient, GateRestClient
    ├── websocket/mod.rs      170 LOC  — low-level WS helpers
    └── logging.rs                   — tracing setup
```

---

## 5) Быстрый старт

```bash
cd /root/turbo/hft-lead-lag

# Создать .env (gitignored) с ключами
cat > .env << 'EOF'
BINANCE_API_KEY=...
BINANCE_API_SECRET=...
GATE_API_KEY=...
GATE_API_SECRET=...
EOF

# Debug build + run
cargo run --quiet

# Или release (рекомендуется для production)
cargo build --release
./target/release/hft-lead-lag
```

Проверка:

```bash
curl http://localhost:5000/health
curl http://localhost:5000/api/v1/screener | python3 -m json.tool
```

- UI: `http://<host>:5000/screener`
- WS broadcast: `ws://<host>:8181/ws`

---

## 6) Фактические API endpoints (из router)

| Метод | Путь | Описание |
|-------|------|----------|
| GET | `/health` | Live health: `{"status":"ok","binance":true,"gate":true}` или `503 degraded` |
| GET | `/api/v1/symbols` | Символы с объёмами и 24h динамикой (REST → Binance + Gate) |
| GET | `/api/v1/screener` | JSON-данные скринера (lag, drift, shadow PnL, NATR) |
| GET | `/screener` | HTML-страница дашборда |
| GET | `/api/v1/shadow/:symbol` | Shadow trader debug info для символа |
| GET | `/api/v1/chart/:symbol` | Chart data: bid/ask история + shadow trades |

---

## 7) Модель Shadow Trader (по коду)

Текущая реализация: **spike-follow** (не premium-percentile P90/P10/P50).

Расположение: `src/domain/screener/shadow_trader.rs` (470 LOC).

Опорные константы:

| Константа | Значение | Описание |
|-----------|----------|----------|
| `FILL_DELAY_MS` | 7 | Задержка исполнения (paper fill) |
| `COOLDOWN_MS` | 3000 | Пауза между сделками |
| `WARMUP_MS` | 30000 | Прогрев перед первой сделкой |
| `QUOTE_FRESHNESS_MS` | 1000 | Максимальный возраст котировки |
| `SPIKE_THRESHOLD_BPS` | 30.0 | Порог спайка для входа |

Логика:
1. Обнаружение spike-движения Gate→Binance.
2. Paper-вход с задержкой fill.
3. Выход по target / timeout / stop-loss.
4. Метрики (PnL/hr, win rate, avg trade) в screener API и дашборд.

Типы (очищены от dead fields):
- `Quote` — без `bid_qty`/`ask_qty` (никогда не читались)
- `OpenPosition` (бывш. ShadowPosition) — без `binance_mid_at_entry`
- `spike_timestamps: VecDeque<i64>` вместо `spike_history: VecDeque<SpikeEvent>`

---

## 8) Benchmark: WS drift vs socket count

Эмпирический тест (30с на конфиг, release build):

| SYMS_PER_WS | Сокетов | P50 | P95 | P99 | Max |
|-------------|---------|-----|-----|-----|------|
| 2 | 94 | 3ms | 4ms | 4ms | 13ms |
| 5 | 38 | 3ms | 5ms | 9ms | 236ms |
| 10 | 20 | 3ms | 4ms | 7ms | 42ms |
| **20 (default)** | **10** | **3ms** | **4ms** | **7ms** | **31ms** |
| 47 | 4 | 3ms | 4ms | 5ms | 33ms |

**Вывод:** количество сокетов не влияет на P50 drift. Default=20 даёт оптимальный баланс (10 сокетов, P99=7ms).

Root cause исторического drift 177,995ms: unbounded queue death spiral при CPU starvation. Исправлено bounded channels (10K capacity + try_send drop).

---

## 9) Проверка качества

```bash
cargo build    # 0 warnings
cargo test     # 15 pass (14 unit + 1 doctest)
```

Все warnings устранены в P1 рефакторинге:
- `#[allow(dead_code)]` с doc-комментариями для executor-заглушек, WsManager, HealthChecker, ports
- Удалён мёртвый `Gate::parse_trade` instance method
- Бизнес-логика NATR вынесена из API-слоя в `infrastructure/enrichment.rs`

---

## 10) P0-фиксы (выполнено)

Все критические проблемы из deep-dive ревью исправлены:

| P0 | Проблема | Коммит | Статус |
|----|----------|--------|--------|
| P0-1 | Секреты в репо | `3b1ff68` | ✅ Удалены, `.env` + dotenvy |
| P0-2 | Fail-open startup | `3b1ff68` | ✅ Fail-fast bind |
| P0-3 | Unbounded queues | `3b1ff68` | ✅ Bounded 10K + try_send |
| P0-4 | WS reconnect | `1563433` | ✅ Exponential backoff + replay |
| P0-5 | Fake health | `3b1ff68` | ✅ Live AtomicBool health |
| P0-6 | subscribe_trades | `1563433` | ✅ Правильный builder |
| P0-7 | Fallback universe | `1563433` | ✅ BTC/ETH fallback |
| P0-8 | Gate parser dedup | `1563433` | ✅ Удалён dead instance method |

---

## 11) Архитектурные рефакторинги (выполнено)

| Рефакторинг | До | После | Коммит |
|-------------|-----|-------|--------|
| screener.rs | 950 LOC god object в api/ | 5 файлов в domain/screener/ (899 LOC) | `c0aaf0c` |
| http_server.rs | 793 LOC с inline HTML | 3 файла: server 123 + handlers 159 + templates 321 | `89c7583` |
| NATR enrichment | Inline в handlers.rs (283 LOC) | enrichment.rs (139 LOC) + handlers (159 LOC) | `031f5b7` |
| Exchange executors | Inline в binance/gate mod.rs | Отдельные executor.rs (29 + 63 LOC) | `031f5b7` |
| Типы screener | Dead fields (bid_qty, ask_qty, binance_mid_at_entry) | Очищены | `c0aaf0c` |
| Endpoint constants | 9 констант (5 dead) | 4 живые константы | `89c7583` |
| Dead code warnings | 6 warnings | 0 warnings (#[allow(dead_code)] + удаление) | `031f5b7` |
| SYMBOLS_PER_WS | 2 (94 сокета) | 20 (10 сокетов) | `bbe34fc` |

---

*Last updated: 2026-02-19 (post P0-fixes + P1 refactoring + architecture decomposition)*
