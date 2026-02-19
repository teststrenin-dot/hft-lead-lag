# HFT Lead-Lag Documentation

Документация проекта `hft-lead-lag` (обновлена после полного deep-dive ревью).

---

## 1) Статус документации

Этот README приведён в актуальное состояние по фактическому коду и логам.

Для полного технического аудита используйте:

- **`docs/review-2026-02-19-deep-dive.md`** — сверх-детальный обзор:
  - общее качество,
  - математика,
  - логика,
  - баги,
  - over-architecture,
  - god objects,
  - слои/модули,
  - дублирование,
  - deep-dive ревью всех 43 коммитов,
  - привязка к реальному серверу.

---

## 2) Верифицированный серверный контекст

Проверка выполнялась на реальном хосте:

- Linux `5.15` (KVM VM)
- `2 vCPU`
- RAM `3.8 GiB`, swap `9 GiB` (частично используется)
- Disk `50G`, свободно ~`11G`
- Load average ~`1.9`
- Rust `1.95.0-nightly`

Это важно для интерпретации runtime-поведения (fan-out сокетов, очереди, latency drift, backpressure).

---

## 3) Текущее состояние проекта (по факту)

| Компонент | Статус | Примечание |
|---|---|---|
| Exchange connectors (Binance/Gate) | ✅ Работает | Подключение и подписки подтверждены логами |
| Runtime API (`/health`, `/api/v1/symbols`, `/api/v1/screener`) | ✅ Работает | Есть операционные ограничения (см. deep-dive) |
| Runtime UI (`/screener`) | ✅ Работает | Polling + chart отображаются |
| Screener lag/drift метрики | ✅ Работает | Есть риски CPU/стат.смещений под нагрузкой |
| Shadow Trader (paper mode) | ✅ Работает | Текущая модель — spike-follow, не P90/P10 |
| Order execution (real orders) | ⚠️ Не завершено | Часть executor-веток пока заглушки |
| Production hardening | ⚠️ Частично | Нужны reconnect/backpressure/health improvements |
| Security hygiene | ❌ Критичный долг | Требуется ротация/очистка секретов из истории |

---

## 4) Навигация по документации

```text
docs/
├── README.md                                # этот файл
├── review-2026-02-19-deep-dive.md          # полный технический аудит
├── review-shadow-trader.md                  # исторический review-файл
├── manifest/
│   └── MANIFESTO.md
├── backlog/
│   └── README.md
├── sprints/
│   ├── sprint-001-connectors.md
│   ├── sprint-002-orders.md
│   └── sprint-003-production.md
├── TASK-001-connectors.md
└── TASK-002-screener-leadlag-checkpoints.md
```

---

## 5) Быстрый старт (безопасный)

```bash
cd /root/turbo/hft-lead-lag

# Никогда не коммитьте реальные ключи в репозиторий
export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
export GATE_API_KEY="..."
export GATE_API_SECRET="..."

cargo run --quiet
```

Проверка:

```bash
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5000/api/v1/symbols
curl http://127.0.0.1:5000/api/v1/screener
```

- UI: `http://127.0.0.1:5000/screener`
- WS broadcast: `ws://127.0.0.1:8181/ws`

---

## 6) Фактические runtime endpoint'ы (из router)

Согласно `src/api/http_server.rs`:

- `GET /health`
- `GET /api/v1/symbols`
- `GET /api/v1/screener`
- `GET /screener`
- `GET /api/v1/shadow/:symbol`
- `GET /api/v1/chart/:symbol`

Важно:

1. `/health` сейчас возвращает статический `{"status":"ok"}` и не отражает полноту системного здоровья.
2. В коде есть endpoint-константы, которые не все зарегистрированы в router — ориентируйтесь на фактические route выше.

---

## 7) Актуальная модель Shadow Trader (по коду)

Текущая реализация: **spike-follow** (а не premium-percentile P90/P10/P50).

Опорные константы:

- `FILL_DELAY_MS = 7`
- `COOLDOWN_MS = 3000`
- `WARMUP_MS = 30000`
- `QUOTE_FRESHNESS_MS = 1000`
- `SPIKE_THRESHOLD_BPS = 30.0`

Логика (упрощённо):

1. Сигнал строится на spike-движении.
2. Вход делается в paper-позицию с задержкой fill.
3. Выход основан на target/timeout/stop-loss правилах.
4. Метрики shadow выводятся в screener API/таблицу.

---

## 8) Ключевые технические риски (кратко)

Подробности и доказательства — в `review-2026-02-19-deep-dive.md`.

### P0

1. Секреты в shell-скриптах (требуется немедленная ротация/очистка истории).
2. Fail-open startup: при проблеме bind API процесс может продолжать runtime.
3. Unbounded очереди в WS-пайплайне (риск memory/latency blow-up).
4. Недостаточная reconnect/health зрелость для production-профиля.

### P1

1. Декомпозиция `screener.rs` и `http_server.rs`.
2. Устранение дублирования parser/lifecycle/symbol-universe логики.
3. Выравнивание слоёв: бизнес-логика из API в application services.

### P2

1. Полное выравнивание docs↔code через автоматические проверки.
2. Усиление guardrails для размера модулей и операционной надёжности.

---

## 9) Проверка качества

```bash
cargo build
cargo test
```

Текущий baseline:

- сборка успешна,
- тесты успешны (`14` unit + `1` doctest),
- есть warnings по dead/unused code в нескольких модулях (см. deep-dive отчёт).

---

## 10) Полезные логи

- `logs/runtime.log`
- `logs/launcher.log`
- `logs/summary.log`
- `test_connection_20260218_104355.log`
- `test_final_20260218_110029.log`

---

## 11) Что обновлено в этой ревизии документации

1. Добавлен полный deep-dive отчёт `review-2026-02-19-deep-dive.md`.
2. Исправлены устаревшие формулировки по модели Shadow Trader.
3. Синхронизирован список фактических endpoint'ов с router.
4. Добавлены server-grounded эксплуатационные риски и приоритеты P0/P1/P2.
5. Явно зафиксированы security-ограничения по ключам.

---

*Last updated: 2026-02-19 (deep-dive audit sync)*
