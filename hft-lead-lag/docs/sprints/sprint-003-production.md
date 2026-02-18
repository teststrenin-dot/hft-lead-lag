# Sprint 3: Production Readiness

**Даты**: 2026-03-04 — 2026-03-11 (1 неделя)

**Цель**: Подготовка к production — надёжность, мониторинг, observability.

---

## Sprint Goals

1. ⬜ Auto-reconnect с exponential backoff
2. 🟡 Health checks и monitoring (baseline `/health` готов)
3. ⬜ Prometheus metrics
4. ⬜ Data recording для анализа
5. ⬜ Graceful shutdown

---

## Задачи спринта

### P0 — Критично

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| CONN-008 | Reconnect logic | Auto-reconnect с backoff | 5 SP | ⬜ Todo |
| CONN-012 | Health checks | Connection monitoring | 3 SP | 🟡 In progress (baseline health endpoint) |
| INFRA-004 | Metrics | Prometheus metrics | 5 SP | ⬜ Todo |
| INFRA-006 | Data recording | Запись тиков в data lake | 5 SP | ⬜ Todo |

### P1 — Важно

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| INFRA-005 | Alerting | Alertmanager integration | 5 SP | ⬜ Todo |
| STRAT-004 | Position management | Lifecycle management | 5 SP | ⬜ Todo |
| STRAT-005 | Exit logic | Закрытие по target spread | 3 SP | ⬜ Todo |
| RISK-006 | Margin monitoring | Отслеживание маржи | 3 SP | ⬜ Todo |

### P2 — Средний приоритет

| ID | Задача | Описание | Оценка | Статус |
|----|--------|----------|--------|--------|
| INFRA-007 | Dashboard | Grafana dashboard | 3 SP | ⬜ Todo |
| TEST-003 | Load tests | Нагрузочное тестирование | 5 SP | ⬜ Todo |

---

## Метрики для реализации

### WebSocket Metrics
- `ws_messages_total{exchange, type}` — счётчик сообщений
- `ws_errors_total{exchange, error_type}` — счётчик ошибок
- `ws_reconnects_total{exchange}` — переподключения
- `ws_latency_seconds{exchange, quantile}` — latency распределения

### Trading Metrics
- `orders_placed_total{exchange, symbol, side}` — размещённые ордера
- `orders_filled_total{exchange, symbol}` — исполненные ордера
- `order_latency_seconds{exchange, type}` — latency ордеров
- `spread_basis_points{symbol}` — текущий spread
- `position_size_usd{exchange, symbol}` — размер позиции
- `pnl_usd{exchange, symbol}` — PnL

### System Metrics
- `build_info{version, commit}` — информация о билде
- `uptime_seconds` — время работы
- `memory_usage_bytes` — использование памяти

---

## Plan по надёжности

### Reconnect Logic
```rust
pub struct ReconnectConfig {
    pub initial_delay_ms: u64,      // 100ms
    pub max_delay_ms: u64,          // 30000ms
    pub multiplier: f64,            // 2.0
    pub max_retries: u32,           // unlimited
}
```

### Health Checks
```rust
pub struct HealthStatus {
    pub binance_connected: bool,
    pub gate_connected: bool,
    pub last_message_ts: i64,
    pub orders_enabled: bool,
}
```

### Graceful Shutdown
1. Получить SIGTERM/SIGINT
2. Остановить приём новых сигналов
3. Закрыть открытые позиции (опционально)
4. Отключиться от бирж
5. Записать финальные метрики
6. Выйти

---

## Definition of Done

- [ ] Код написан
- [ ] Тесты проходят
- [ ] Metrics экспортируются в Prometheus
- [ ] Dashboard создан
- [ ] Runbook написан

---

## Runbook (черновик)

### Start
```bash
cd /root/turbo/hft-lead-lag
cargo run --quiet
```

### Stop
```bash
# Graceful
kill -TERM <pid>

# Force
kill -KILL <pid>
```

### Troubleshooting

**Проблема**: Нет connection к Binance
```bash
# Проверить логи
tail -f logs/runtime.log

# Проверить network
curl https://fapi.binance.com

# Перезапустить
# остановить текущий PID и запустить снова
ps -eo pid,args | awk '$2 ~ /target\\/debug\\/hft-lead-lag$/ {print $1}'
```

**Проблема**: Высокий spread
```bash
# Проверить screener API
curl http://127.0.0.1:5000/api/v1/screener

# Проверить network
ping fapi.binance.com
ping fx-ws.gateio.ws
```

**Проблема**: Высокий lag при низком ingress drift  
Это часто рыночная асинхронность между биржами, а не локальная сетка/CPU.  
Смотрите одновременно `lag_ms` и `ws_drift_ingress_*`.

---

*Sprint planned: 2026-02-18 (runbook refreshed: 2026-02-18)*
