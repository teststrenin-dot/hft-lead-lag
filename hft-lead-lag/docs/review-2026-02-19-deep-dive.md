# Deep-Dive Review: hft-lead-lag

**Дата:** 2026-02-19  
**Формат:** сверх-детальный аудит (code + runtime + commits)  
**Статус:** актуальный обзор после серии deep-dive ревью сабагентами  
**Важно:** выводы привязаны к реальному серверу и фактическим логам, а не к абстрактной оценке.

---

## 0) Область проверки и источники фактов

Проверка включала:

1. **Общее качество проекта**
2. **Математика и статистика**
3. **Логика/состояния/runtime flow**
4. **Поиск багов**
5. **Избыточность архитектуры**
6. **God objects / oversized modules**
7. **Качество слоёв и модульных границ**
8. **Дублирование**
9. **Deep-dive ревью всей истории коммитов**

Использованные источники:

- Код: `src/**/*.rs`, `Cargo.toml`, `docs/*.md`
- Логи и runtime-артефакты:
  - `logs/runtime.log`
  - `logs/launcher.log`
  - `logs/summary.log`
  - `test_connection_20260218_104355.log`
  - `test_final_20260218_110029.log`
- Git history: `git log --oneline --reverse` (43 коммита)
- Базовая валидация:
  - `cargo build` — успешно
  - `cargo test` — успешно (`14` unit tests + `1` doctest)
  - При этом есть warnings по dead/unused code в ключевых модулях.

---

## 1) Реальный серверный профиль (ground truth)

- OS: `Linux 5.15.0-60-generic` (KVM VM)
- CPU: `2 vCPU` (`Intel Xeon Skylake`)
- RAM: `3.8 GiB` (free ~401 MiB на момент проверки), swap `9 GiB` (used ~2.2 GiB)
- Disk: `50G`, свободно ~`11G` (80% use)
- Load average: около `1.9`
- Runtime:
  - Python `3.10.12`
  - Node `v24.13.0` / npm `11.6.2`
  - Rust `1.95.0-nightly`

### Почему это критично для оценки

Для данного проекта именно этот профиль жёстко ограничивает:

- количество одновременно поддерживаемых WS-сокетов/тасков;
- допустимую сложность hot-path вычислений;
- размер очередей без backpressure;
- запас на деградацию при всплесках market data.

---

## 2) Сводный вердикт

| Область | Вердикт | Коротко |
|---|---|---|
| Общая инженерная оценка | **Mixed / условно good foundation** | Основа рабочая, но есть прод-критичные дыры |
| Математика | **Mixed** | Базовые формулы ок, но есть смещения и docs↔code drift |
| Логика runtime | **Высокий риск** | Часть fail/degraded сценариев обрабатывается опасно |
| Баги | **High/Critical risk** | Есть конкретные дефекты и security-риски |
| Архитектура | **Частично overbuilt** | Много неиспользуемых/дублированных слоёв |
| God objects | **Подтверждено** | Ключевые модули перегружены ответственностями |
| Слои/модули | **Средне** | Формально разделены, но границы протекают |
| Дублирование | **Высокое** | Влияет на reliability и скорость изменений |
| Коммит-дисциплина | **Улучшается, но с churn-кластерами** | Видны циклы `feat→fix→rework/revert` |

---

## 3) Что в проекте действительно сильное

1. **Рабочий MVP-поток подтверждён runtime-логами**  
   Есть успешные подключения Binance/Gate и подписки (`test_connection_20260218_104355.log:8-12`, `test_final_20260218_110029.log:17-20`).

2. **Базовая quality-gate дисциплина есть**  
   Проект компилируется и проходит тесты (`cargo build`, `cargo test`).

3. **Есть осознанное hardening-поведение при старте**  
   Дренирование stale startup ticks (`logs/runtime.log:20`).

4. **Слой domain в целом не загрязнён infrastructure-импортами**  
   Базовая структура слоёв сохранена.

5. **Есть движение в сторону perf-харденинга в последних коммитах**  
   Последний кластер коммитов сфокусирован на снижении hot-path overhead.

---

## 4) P0-критика (исправлять в первую очередь)

### P0-1) Секреты в репозитории (критично)

- **Доказательство:**  
  `test_connection.sh:9-12`, `test_final.sh:9-12` — реальные API keys/secrets.
- **Риск:** мгновенная компрометация доступа к биржам.
- **Server impact:** shared VM + открытый доступ к репо/логам увеличивает вероятность утечки.
- **Действие:** срочная ротация ключей + удаление из истории git + secret scanning в CI.

### P0-2) Процесс продолжает работать после падения API bind

- **Доказательство:**  
  `src/main.rs:167-178` — ошибка только логируется;  
  `logs/runtime.log:14-15` — `Address already in use`;  
  `logs/runtime.log:19` — `System initialized`.
- **Риск:** система "полужива" (данные/стратегия есть, control-plane недоступен).
- **Действие:** fail-fast или полноценный degraded mode (без торговли).

### P0-3) Unbounded очереди в горячем контуре

- **Доказательство:**  
  `src/infrastructure/exchanges/binance/mod.rs:136,244`  
  `src/infrastructure/exchanges/gate/mod.rs:238,239`
- **Риск:** memory growth и latency spikes при backpressure.
- **Server impact:** на 3.8 GiB RAM + swap usage это реальный OOM/lag риск.
- **Действие:** bounded channels + политика дропа/coalesce + метрики queue depth.

### P0-4) Нет полноценного reconnect state machine в реальном WS-потоке

- **Доказательство:** в reader при close/error — `break` и завершение task, без автоматической реинициализации (`binance/mod.rs:151-164`, `gate/mod.rs:255-260`).
- **Риск:** потеря потока данных до ручного рестарта.
- **Действие:** supervisor + backoff + resubscribe + heartbeat watchdog.

### P0-5) `/health` отражает не здоровье системы, а статический "ok"

- **Доказательство:** `src/api/http_server.rs:134-136`  
  при существующем, но не подключённом `HealthChecker` (`src/api/health.rs:24+`).
- **Риск:** ложноположительный health при деградации.
- **Действие:** health-агрегация от состояния коннекторов/очередей/API.

---

## 5) Deep-dive по каждому треку

## 5.1 Общее качество

### Ключевые findings

1. Security posture недостаточен (секреты в скриптах).
2. Operability риск: API bind failure не делает процесс unhealthy/failed.
3. Perf risk: слишком много WS-сокетов при `SYMBOLS_PER_WS=2`.
4. Reliability risk: возможна "тихая деградация" без правдивого health.
5. Testability gap: runtime-инциденты покрыты слабее, чем unit-контракты.

### Серверная корреляция

На `2 vCPU` ошибки orchestration и лишний fan-out быстро превращаются в задержки обработки market data и рост drift.

---

## 5.2 Математика и статистика

### Что корректно

- Корректный расчёт bps-метрик.
- Корректная базовая идея median lag.
- NATR и процентильные подходы реализованы как концепция.

### Что проблемно

1. **Пер-тик пересчёт percentiles с сортировкой** — CPU heavy.
2. **Drift-фильтрация и stale handling создают bias в наблюдаемости.**
3. **Docs↔code mismatch в модели Shadow Trader** (документация описывает premium/P90/P10/P50 модель, код — spike-follow).
4. **Часть метрик может давать оптимистичный сдвиг** при деградации канала.

### Фактические константы Shadow Trader в коде

- `FILL_DELAY_MS = 7` (`src/api/screener.rs:17`)
- `COOLDOWN_MS = 3000` (`src/api/screener.rs:389`)
- `WARMUP_MS = 30000` (`src/api/screener.rs:393`)
- `QUOTE_FRESHNESS_MS = 1000` (`src/api/screener.rs:395`)
- `SPIKE_THRESHOLD_BPS = 30.0` (`src/api/screener.rs:407`)

---

## 5.3 Логика и state transitions

### Ключевые проблемы

1. Fail-open при старте API/WS.
2. Нет end-to-end reconnect orchestration.
3. Возможны stale/live ambiguity сценарии.
4. Fallback режимы и symbol universe могут давать ложное ощущение корректного покрытия.

### Runtime evidence

- `logs/launcher.log:1145-1149` — Binance symbols не получены, но включается fallback на Gate universe, затем `Common symbols: 28`.
- Это operationally удобно, но логически рискованно для качества данных.

---

## 5.4 Баги (конкретные)

| ID | Severity | Confidence | Доказательство | Комментарий |
|---|---|---|---|---|
| B1 | Critical | High | `test_connection.sh:9-12`, `test_final.sh:9-12` | Секреты в репо |
| B2 | High | High | `main.rs:167-178`, `runtime.log:14-19` | Fail-open startup |
| B3 | High | High | `http_server.rs:134-136` | `health` всегда `ok` |
| B4 | High | High | `binance/gate mod.rs` unbounded channel lines | Риск очередей без границ |
| B5 | High | Medium-High | WS worker break без полного supervisor | Потеря потока после close/error |
| B6 | Medium | High | `binance/mod.rs:291` + `build_book_ticker_subscription` | trade-подписка использует не тот builder |
| B7 | High | High | `launcher.log:1145-1149` | рискованный fallback universe |
| B8 | Medium | High | docs заявляют endpoint'ы, которых нет в router | docs drift |
| B9 | Medium | High | dead/unused warnings в core модулях | технический долг |
| B10 | Medium | High | монолитные файлы >500 LOC | повышенная дефектность изменений |

---

## 5.5 Over-architecture

### Ненужная сложность (на текущем этапе)

1. Отдельные порты `application/ports` фактически не подключены в runtime.
2. Отдельная health-подсистема существует, но route не использует её.
3. Часть WS-абстракций инициализирована, но основной поток живёт в exchange-модулях напрямую.
4. Много "каркаса под будущее", при недостроенном критичном execution path.

### Что является "оправданной сложностью"

- Domain-модели и обменные типы.
- Разделение API / infrastructure каталогов как основа.
- Наличие risk/service контуров как направление развития (но их нужно довести до реального подключения).

---

## 5.6 God objects / oversized modules

| Модуль | Размер | Риск | Почему |
|---|---:|---|---|
| `src/api/screener.rs` | 950 LOC | High | В одном файле и ingest, и метрики, и shadow-sim, и debug API |
| `src/api/http_server.rs` | 748 LOC | High | Router + handlers + inline UI + fallback логика |
| `src/infrastructure/exchanges/gate/mod.rs` | 548 LOC | Medium-High | transport+parsing+auth+executor |
| `src/main.rs` | 376 LOC | Medium-High | orchestration, startup, loop, subscriptions в одном месте |

**Факт размера:** `wc -l` подтверждает 2622 строки на 4 ключевых файла.

---

## 5.7 Слои и модульные границы

### Плюсы

- Каталожное разделение `domain/application/infrastructure/api` есть.
- Domain не загрязнён внешними I/O деталями.

### Минусы

1. API слой напрямую использует REST-клиенты infrastructure (`http_server.rs:141-147`).
2. Business-heavy логика находится в API-модуле (`api/screener.rs`).
3. Runtime orchestration обходит application-порты.

### Вывод

Границы не разрушены полностью, но "протекают" в местах, где нагрузка и риски максимальны.

---

## 5.8 Дублирование

### Ключевые точки дублирования

1. Дубли parser-логики в `gate/mod.rs`.
2. Повторяющиеся WS lifecycle куски для Binance/Gate.
3. Дубли ответственности сборки symbol universe в `main`, `http_server`, `ws_server`.
4. Дубли и рассинхрон docs↔code по стратегии/endpoint'ам.

### Последствия

- Рост стоимости изменений.
- Риск расхождений в поведении между "почти одинаковыми" ветками.
- Больше шансов на регрессии под ограниченным CPU budget.

---

## 5.9 Deep-dive ревью всех коммитов (43)

### Итог по траектории

- Начало: крупные инфраструктурные/feature-коммиты с широким scope.
- Середина: высокий churn в screener/shadow/chart (несколько циклов `feat→fix→rework/revert`).
- Поздний этап: фокус на perf и hot-path оптимизациях.

### Кластеры риска

1. **Screener timestamp/drift cluster** — серия фикс-коммитов после feature-волн.
2. **Chart/UI churn cluster** — интенсивные итерации и один явный revert.
3. **Shadow model churn cluster** — смена модели/параметров в короткий период.

### Реестр всех коммитов (краткая классификация)

| SHA | Тип | Риск | Краткая оценка |
|---|---|---|---|
| 471a602 | init | High | Большой стартовый монокоммит |
| 07f99aa | chore | Low | Root tooling |
| bd1403f | feat | High | Большой runtime/API скоуп |
| aaea09c | fix | Medium-High | Логика lag/streams |
| 9ed50d9 | fix | Medium | Подписки полного сета |
| 3009433 | fix | Medium | Расширение feed |
| 80d077b | docs | Low | Doc update |
| f9203f5 | feat | High | Новые screener-метрики |
| 1080d04 | feat | Medium-High | Drift exposure |
| c4dd34d | fix | Medium | Startup/drift hardening |
| 5f9a1bf | feat | High | Разделение Binance WS |
| 9f4c1f0 | feat | Medium | Порог объёма 10M |
| 5a8ef87 | fix | Medium | Fallback scope |
| 293a07e | fix | Medium-High | Exchange timestamp basis |
| c732bdd | fix | High | Ingress ts capture shift |
| 26ae32e | docs | Low | Refresh docs |
| 0b1617d | docs | Low | Manifest update |
| 590d4ce | feat | High | Shadow trader introduction |
| 79698e3 | fix | High | Shadow race/stuck fixes |
| 0bd1d33 | fix | Low-Med | Shadow debug field corrections |
| 703d2f3 | feat | High | uPlot chart feature |
| 96770f8 | docs | Low | Docs sync |
| ba1d677 | feat | High | Embed real-time chart |
| 10ef7be | fix | Medium | Chart robustness |
| 7eff059 | feat | Medium-High | Blacklist + trade zones |
| 93d5c45 | feat | High | Spread chart model shift |
| 2c34b53 | revert | Medium | Revert previous chart choice |
| 262e959 | feat | High | Volume column + filter 1M |
| 784f26a | feat | Medium-High | Rolling median + sortable UI |
| 88c5aad | docs | Low | TASK doc updates |
| 96e4790 | docs | Low | Docs deep update |
| 2bf7145 | docs | Low-Med | Add review findings |
| a962770 | fix | High | Math/perf/signal safety patch |
| 6b70b07 | feat | High | Shadow model rewrite |
| 75b3dc4 | refactor | High | Slippage→fill delay |
| 2ea31ff | feat | Medium | Zones + markers |
| 1051dd9 | fix | Medium-High | Entry/exit marker corrections |
| c3d29e9 | perf | Medium | Hot-path clone reduction |
| d90ca89 | ui | Medium-Low | Chart cleanup |
| 4070682 | fix | Medium | Historical chart load fix |
| 18edde5 | refactor | Medium-High | bid/ask-only refactor |
| e1e0c5f | perf | Medium | Retention limit |
| 1ecdce3 | perf | Medium-High | zero-alloc hot-path pass |

### Главный вывод по коммитам

Процесс разработки живой и продуктивный, но качество стабилизации пока ниже темпа ввода фич — особенно в runtime-heavy сегментах.

---

## 6) Коррекция устаревших пунктов документации

Ниже — что обязательно синхронизировать в основной документации:

1. **Shadow Trader модель:** текущий код = spike-follow, а не premium P90/P10/P50 модель.
2. **Параметры Shadow:** значения в docs не должны расходиться с `src/api/screener.rs`.
3. **Endpoint inventory:** в docs должны быть только route, реально зарегистрированные в router (`http_server.rs:75-82`).
4. **Health semantics:** явно описать, что сейчас `/health` статический и требует доработки.
5. **Security раздел:** запрет хранения реальных ключей в репозитории.

---

## 7) Приоритетный roadmap (P0/P1/P2)

## P0 (немедленно)

1. Ротация/удаление скомпрометированных ключей.
2. Bounded queues + reconnect supervisor.
3. Fail-fast startup при невозможности поднять HTTP/WS API.
4. Правдивый health.
5. Синхронизация docs↔code по Shadow/endpoint'ам.

## P1 (ближайший спринт)

1. Декомпозиция `screener.rs` и `http_server.rs`.
2. Дедуп parser/lifecycle/symbol-universe логики.
3. Вывод бизнес-логики из API слоя в application services.
4. Усиление интеграционных тестов для деградационных runtime-сценариев.

## P2 (после стабилизации)

1. Полное выравнивание архитектурных границ через реально используемые порты.
2. Политика size-limit и quality gate для oversized modules.
3. Автоматические docs-consistency проверки (routes/params/metrics).

---

## 8) Приложение: ключевые evidence points

- `src/main.rs:167-178` — API/WS старт в spawn + логирование ошибок без fail-fast.
- `src/api/http_server.rs:134-136` — статический health.
- `src/api/http_server.rs:75-82` — фактические HTTP route.
- `src/api/http_server.rs:717-748` — список endpoint-констант шире фактической маршрутизации.
- `src/infrastructure/exchanges/binance/mod.rs:136,244` — unbounded channels.
- `src/infrastructure/exchanges/gate/mod.rs:238,239` — unbounded channels.
- `src/infrastructure/exchanges/binance/mod.rs:196` — `SYMBOLS_PER_WS = 2`.
- `src/infrastructure/exchanges/binance/mod.rs:291` — suspicious trade subscribe builder usage.
- `src/application/ports/mod.rs:4-13` — порты объявлены.
- `src/api/health.rs:24+` — отдельный health checker.
- `logs/runtime.log:14-19` — bind errors + system initialized.
- `logs/runtime.log:16-17` — 93 symbols => 47 sockets.
- `logs/launcher.log:389` — 104 symbols => 52 sockets.
- `logs/launcher.log:803-816` — drift spikes до ~177995ms.
- `logs/launcher.log:1145-1149` — fallback universe при ошибке Binance REST.
- `test_connection_20260218_104355.log:8-12` — успешные websocket connect.
- `test_final_20260218_110029.log:10-13` — runtime symbols/common.

---

## 9) Финальный итог

Проект **не плохой** и имеет рабочую основу, но в текущем состоянии это **не “production-safe HFT runtime”** на сервере 2 vCPU/3.8 GiB без срочных корректировок из P0.  
Главная ценность текущего шага — не просто список проблем, а связка "**код + коммиты + реальные логи + ограничения сервера**", которая даёт прозрачный порядок действий для стабилизации.
