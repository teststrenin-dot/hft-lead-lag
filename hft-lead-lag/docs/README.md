# HFT Lead-Lag Documentation

Добро пожаловать в документацию HFT Lead-Lag торговой системы.

---

## Структура документации

```
docs/
├── README.md                 # Этот файл (навигация)
├── manifest/
│   └── MANIFESTO.md          # Миссия, принципы, архитектура
├── backlog/
│   └── README.md             # Product backlog
├── sprints/
│   ├── sprint-001-connectors.md    # Sprint 1: Connectors MVP ✅
│   ├── sprint-002-orders.md        # Sprint 2: Order Management
│   └── sprint-003-production.md    # Sprint 3: Production Ready
└── TASK-*.md                 # Спецификации задач
    └── TASK-001-connectors.md
    └── TASK-002-screener-leadlag-checkpoints.md
```

---

## Быстрая навигация

### Для разработчиков
- 📋 [Manifesto](manifest/MANIFESTO.md) — Принципы, архитектура, **инструменты (MCP, Skills, Subagents)**
- 🔧 [TASK-001](TASK-001-connectors.md) — Спецификация коннекторов
- 🧪 [TASK-002](TASK-002-screener-leadlag-checkpoints.md) — Screener + Lead-Lag test checkpoints
- 📦 [Backlog](backlog/README.md) — Бэклог задач

### Для менеджмента
- 📊 [Sprint 1](sprints/sprint-001-connectors.md) — Completed
- 📈 [Sprint 2](sprints/sprint-002-orders.md) — Planned
- 🎯 [Sprint 3](sprints/sprint-003-production.md) — Planned

### Для DevOps
- 📝 [Runbook](sprints/sprint-003-production.md#runbook) — Start/Stop/Troubleshooting
- 📊 [Metrics](sprints/sprint-003-production.md#метрики-для-реализации) — Prometheus metrics

---

## Статус проекта

| Компонент | Статус | Спринт |
|-----------|--------|--------|
| Exchange Connectors | ✅ Done | Sprint 1 |
| Lead-Lag Strategy | ✅ Done | Sprint 1 |
| Risk Management | ✅ Done | Sprint 1 |
| REST symbols API (24h vol/dynamics) | ✅ Done | Sprint 1 (post) |
| WS market broadcast API | ✅ Done | Sprint 1 (post) |
| Web Screener (`/screener`, `/api/v1/screener`) | ✅ Done (MVP) | Sprint 1 (post) |
| Screener live coverage (`common_symbols`) | ✅ Done | Sprint 1 (post) |
| Screener metrics (`ws_drift_binance_ms`, `ws_drift_gate_ms`, `entry_half_life_ms`, `avg_gt_p90_ms`, `gate_natr_30m_pct`) | ✅ Done | TASK-002 |
| Lead-Lag P90/P50 runtime logic | 🟡 In progress | TASK-002 |
| Centralized logs (`project/logs`) | ✅ Done | Sprint 1 (post) |
| Order Management | ⬜ Todo | Sprint 2 |
| Position Tracking | ⬜ Todo | Sprint 2 |
| Production Ready | ⬜ Todo | Sprint 3 |

## Инструменты разработки

| Инструмент | Статус | Описание |
|------------|--------|----------|
| MCP Sequential Thinking | ✅ Enabled | Многошаговое мышление с ревизией |
| Superpowers Skills | ✅ Enabled | 14 скиллов для разных задач |
| Subagents | ✅ Enabled | Делегирование для ускорения |

---

## Быстрый старт

### 1. Клонирование
```bash
cd /root/turbo/hft-lead-lag
```

### 2. Конфигурация
```bash
export BINANCE_API_KEY="..."
export BINANCE_API_SECRET="..."
export GATE_API_KEY="..."
export GATE_API_SECRET="..."
```

### 3. Запуск
```bash
cargo run
```

### 3.1 Checkpoint API
```bash
curl http://127.0.0.1:5000/health
curl http://127.0.0.1:5000/api/v1/symbols
curl http://127.0.0.1:5000/api/v1/screener
# UI: http://127.0.0.1:5000/screener
# WS stream: ws://127.0.0.1:8181/ws
```

### 4. Тесты
```bash
cargo test
./test_connection.sh
./test_final.sh
```

### 5. Логи
- Runtime: `logs/runtime.log`
- Тесты: `logs/test_connection_*.log`, `logs/test_final_*.log`
- Краткий итог: `logs/summary.log`

---

## Ключевые метрики

| Метрика | Значение |
|---------|----------|
| LOC | ~1300 |
| Тесты | 14 passing |
| Build time (debug) | ~15s |
| Test coverage | ~60% |

---

## Контакты

- **Проект**: `/root/turbo/hft-lead-lag`
- **Документация**: `/root/turbo/hft-lead-lag/docs`
- **Код**: `/root/turbo/hft-lead-lag/src`

---

*Last updated: 2026-02-18 (screener ws-drift + p90-time + Gate NATR metrics)*
