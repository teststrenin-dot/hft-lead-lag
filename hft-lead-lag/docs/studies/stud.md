Ок — ниже я “прогоняю” **твій delivery‑процесс (контракты → изолированные вехи → тесты)** строго по твоей **Business Logic v1 roadmap** и по фактической **математике runtime (HFT lead‑lag)**.

Я сделаю это в двух слоях:

1. **Как реально должен течь процессинг данных/решений end‑to‑end** (с привязкой к формулам и тем, что публикуется в UI).
2. **Как этот поток “разрезается” на вехи CP1 → CP2 → CP3**, что именно менять в коде/контрактах/DB, и какие тесты = критерии выхода.

---

# 1) End-to-end процессинг CP0 (то, что уже “зафиксировано”)

Твой CP0 по факту означает: есть **устойчивый pipeline**, и математика “заморожена” как источник истины. Тогда “идеальная” схема процессинга выглядит так (сверху вниз):

## 1.1 Ingress → Time normalization → Clock offset

**Вход:** raw события бирж (ws), с exchange ts в разных единицах (sec/ms/us/ns) + локальный ingress ts.

**Нормализация времени (ms):**

* sec → `*1000`
* ms → как есть
* us → `/1000`
* ns → `/1_000_000`

**Оценка offset по бирже:**

* `offset_sample = ingress_ts - exchange_ts`
* `offset = median(last N offset_sample)`
* `corrected_exchange_ts = exchange_ts + offset`

Guard: выбросы (|sample| > 6h) игнорируются, медиана пересчитывается периодически.

**Публикуемые/контрольные метрики:**

* `drift_ms = local_ts_ms - exchange_ts_ms`
* outlier: если `|drift_ms| > 30_000` → `None` (не используем)

**Почему это критично бизнес‑логически:** весь lead‑lag, “кто лидер”, lag_ms, циклы — развалятся без стабильной коррекции времени.

---

## 1.2 Screener state: lag + лидер + циклы

На обновлениях по обоим эксчейнджам (условно Binance/Gate):

* **Instant lag:**
  `instant_lag_ms = |binance.ts_ms - gate.ts_ms|`
* **Публикуемый lag:** `lag_ms = p50(samples_window)`
* **Кто лидер:**
  `leader = argmax(corrected_exchange_ts_ms)`

**Циклы divergence/convergence** через `leader_mid`:

* `leader_mid = mid(fresher exchange)`

Дальше считаются бпс‑метрики дивергенции/конвергенции (как в доке), и **CycleTracker** строит:

* `p90_divergence`
* `p50_convergence`
* **half_life_ms**: вход в “зону” при divergence ≥ p90 и выход при convergence ≤ p50.

**Это твой “market microstructure telemetry”**, который потом должен стать частью safety‑guards (особенно в live).

---

## 1.3 Lead‑Lag signal (service level)

Основной расчёт:

**Спред:**
`spread_bps = ((leader_price - lagger_price) / lagger_price) * 10_000`

Проверяем 2 направления:

* `bid_ask_bps = spread(primary.bid, hedge.ask)`
* `ask_bid_bps = spread(hedge.bid, primary.ask)`
* `spread_bps = max(bid_ask_bps, ask_bid_bps)`

**Направление:**

* LONG lagger, если `bid_ask_bps >= ask_bid_bps`
* иначе SHORT lagger

**Гейт по лидерству (после offset‑коррекции):**
сигнал допустим только если
`primary_corrected_ts >= hedge_corrected_ts`

**Entry condition:**
`spread_bps >= min_entry_spread_bps`

---

## 1.4 ShadowTrader: вход/выход, pnl, причины

Это важно, потому что *вся гонка и портфели питаются именно shadow‑сделками*.

### Entry (baseline gap model)

**Baseline по окну:**

* `baseline_ask_gap_bps = mean((binance_ask - gate_ask)/gate_ask * 10_000)`
* `baseline_bid_gap_bps = mean((gate_bid - binance_bid)/gate_bid * 10_000)`

**Текущий сигнал (отклонение от baseline):**

* `long_signal_bps = current_ask_gap_bps - baseline_ask_gap_bps`
* `short_signal_bps = current_bid_gap_bps - baseline_bid_gap_bps`

**Entry trigger:**
`signal_bps >= spike_threshold_bps`

**Spread filter:**
`gate_spread_bps <= max_spread_bps`

### Exit

Unrealized bps:

* long: `((gate.bid - entry)/entry)*10_000`
* short: `((entry - gate.ask)/entry)*10_000`

Breakeven activation:

* `breakeven_threshold_bps = spike_bps * target_ratio`
* активировать если `unrealized >= threshold`

После breakeven:

* exit breakeven если `unrealized <= 0`
* trailing_take если `unrealized <= peak_unrealized * trailing_decay_ratio`
* timeout если `hold_ms > max_hold_ms`

До breakeven:

* stop_loss если `unrealized <= -stop_loss_bps`
* timeout иначе

**Closed pnl (важно: pnl_pct в процентах):**

* `raw_return = (exit-entry)/entry` (со знаком направления)
* `fees = 2 * taker_fee`
* `pnl_pct = (raw_return - fees) * 100`

---

## 1.5 ShadowFleet: окна + score + gate + prune

Окна: 1h / 6h / 24h, экспоненциальное затухание:

`decay = exp(-dt_ms / horizon_ms)`
`state *= decay`

Метрики окна:

* `avg_pnl_pct = pnl_sum_pct / trades`
* `win_rate_pct = wins / trades * 100`
* `stop_loss_share_pct = stop_loss_trades / trades * 100`

**Score (frozen phase‑0):**

```
score =
  1.0 * (avg_pnl_6h / 100)
  + 0.20 * (win_rate_6h / 100)
  - 0.50 * (stop_loss_share_6h / 100)
```

**Gate:**

* `trades_6h >= 5`
* `avg_pnl_6h > 0`
* `stop_loss_share_6h <= 55%`

**Prune:**

* если `session_trades >= 30` и `avg_pnl_pct < -0.05` → disable
* если `session_trades == 0` и прошло ≥ 10 минут → disable

---

## 1.6 Portfolio runtime: кандидаты → shortlist → ranking → active + cooldown

**Кандидатная история** агрегируется из trades:

* `closed_trades = COUNT(*)`
* `profitable_trades = SUM(pnl_pct > 0)`
* `losing_trades = SUM(pnl_pct < 0)`
* `pnl_sum_pct = SUM(pnl_pct)`
* `first_trade_ts_ms = MIN(entry_ts_ms)`

**Производные:**

* `useful_winrate = profitable_trades / closed_trades`
* `pm_raw = profitable_trades - losing_trades`
* `avg_pnl_pct = pnl_sum_pct / closed_trades`

**Eligibility gate:**

* возраст > 5 минут
* `closed_trades > 5`
* `useful_winrate >= 0.30`
* `avg_pnl_pct >= 0`

**Ranking tuple (desc):**

1. `useful_winrate`
2. `pm_raw`
3. `avg_pnl_pct`
4. `closed_trades`
5. `symbol` (tie‑break)

**Cooldown/reset:**

* fast trigger: `stop_loss_streak >= 5 within 120_000 ms`
* persistent: `stop_loss_streak >= 6`
* `cooldown_until = ts_ms + 300_000`

**Scheduler cadence:**

* tick every 120_000 ms
* rebalance allowed если `now - last_rebalance >= 120_000`

---

## 1.7 API/UI read-model

* `/health` ok, feeds alive
* `/api/v1/portfolio/active` — отдаёт состояния (сейчас A/B, надо сделать динамически)
* UI читает портфели и их метрики

---

# 2) Теперь “прогоняем” delivery‑подход по твоему Roadmap

Твой Roadmap уже хорошо сформулирован как milestone‑план. Я добавлю то, чего обычно не хватает, чтобы резко сократить время:
**контракты + инварианты из математики + тест‑матрица.**

---

# Checkpoint 1 — Race‑Ready Portfolios (major)

Цель CP1 в терминах процессинга:

> Мы хотим, чтобы стадия **1.6 Portfolio runtime** стала **N‑портфельной**, где каждый портфель строит свой shortlist/ranking, но стадия “выбор active” соблюдает глобальный constraint **no‑overlap** между портфелями.

И при этом **ничего не меняем** в математике сигналов/пнл/скоринга (CP0 baseline lock).

---

## CP1.1 — Dynamic Portfolio Count (backend contract)

### Что реально нужно сделать (по слоям)

#### A) Контракт конфигурации (источник истины)

Сделай явную структуру:

* `portfolios: [ { id, params… } ]`

Даже если сейчас у портфелей одинаковые параметры, это важно, потому что:

* количество портфелей меняется без перекомпиляции;
* появляется “ключ” для сегментации данных и состояния.

#### B) Модель состояния runtime

Заменить A/B на:

* `HashMap<PortfolioId, PortfolioState>` или `Vec<PortfolioState>` с явным `id`.

**Состав PortfolioState минимально:**

* `id`
* `shortlist: Vec<Candidate>`
* `active: Vec<ActiveSymbol>` (или `Option<ActiveSymbol>`)
* `guards/cooldown`
* `last_rebalance_ts`
* `metrics snapshot`

#### C) API контракт

`/api/v1/portfolio/active` должен возвращать список, а не фиксированные поля.

**Хороший признак “контракт‑фёрст”:**
если фронт может отрендерить 1/2/7 портфелей **без правок**, значит контракт норм.

#### D) Persistence snapshot compatibility

Тебе нужен **версионированный snapshot**:

* `SnapshotV1 { A:…, B:… }`
* `SnapshotV2 { portfolios: [...] }`

С миграцией V1 → V2:

* A → portfolios[0], B → portfolios[1]
* остальные пустые/дефолт

### Exit criteria (как у тебя) + инженерные инварианты

* меняем число портфелей в конфиге → без перекомпиляции → runtime поднимается
* `/api/v1/portfolio/active` возвращает **ровно N** портфелей

**Инварианты (тестируемые):**

* `portfolio_id` уникален
* snapshot restore не теряет cooldown/active/last_rebalance_ts

### Тест‑набор

1. **Config test:** 1,2,5 портфелей → корректный парсинг, уникальность id.
2. **API contract test:** ответ — массив портфелей, schema стабильна.
3. **Snapshot migration test:** V1 → V2 сохраняет эквивалентность A/B.

---

## CP1.2 — Independent Coin Race per Portfolio + no-overlap active

Это самый “математически насыщенный” кусок CP1, потому что тут важно **не разломать ranking/gates/cooldown**, и сделать алгоритм активов строго воспроизводимым.

### Ключевой вопрос: “откуда берётся независимость?”

Независимость shortlist/ranking по портфелям означает:

> Метрики кандидата `(useful_winrate, pm_raw, avg_pnl_pct, closed_trades, age)` должны считаться **в разрезе портфеля**, а не глобально.

То есть агрегатор из п.1.6 должен стать:

* было: `GROUP BY symbol`
* стало: `GROUP BY (portfolio_id, symbol)`
  (или эквивалентно через mapping config→portfolio)

### Практический способ сегментации (без усложнения)

Нужно обеспечить одно из двух (любой вариант ок, выбирай по реальности кода):

**Вариант 1 (чище):**
в момент записи trade добавляешь `portfolio_id` в trade record.
Тогда DB‑агрегация простая: `GROUP BY portfolio_id, symbol`.

**Вариант 2 (минимально инвазивный):**
если trade уже содержит `config_id`, и есть таблица/маппинг `config_id → portfolio_id`, то агрегацию можно делать join’ом.

> Важно: для CP1 тебе не нужно идеальное хранилище — тебе нужно, чтобы **ranking per portfolio** реально считался отдельно.

### Как формируется shortlist per portfolio (по твоей математике)

Для каждого портфеля `p`:

1. берём кандидатов `(symbol)` с агрегированной историей портфеля
2. считаем:

   * `useful_winrate = profitable/closed`
   * `pm_raw = profitable - losing`
   * `avg_pnl_pct = pnl_sum/closed`
   * `age_minutes = (now - first_trade_ts)/60_000`
3. применяем eligibility gate:

   * age > 5
   * closed > 5
   * useful_winrate ≥ 0.30
   * avg_pnl_pct ≥ 0
4. сортируем по ranking tuple (desc)

Shortlist = top‑K (K из конфига), **но** shortlist может содержать пересечения по символам между портфелями — это не запрещено твоими acceptance.

### Как выбрать active без overlap (детерминированно)

В CP1 лучше не изобретать оптимизационные алгоритмы, а сделать **простой, воспроизводимый allocator**, который:

* гарантирует disjoint active sets,
* стабильный по результатам (important для дебага),
* легко тестируется.

**Детерминированный greedy allocator:**

1. задаём порядок портфелей (например, `sort by portfolio_id` — стабильный)
2. идём по портфелям, каждому выдаём первые N символов из shortlist, которые:

   * не на cooldown у этого портфеля
   * ещё не заняты другим портфелем в этом rebalance‑цикле

Псевдологика:

* `taken_symbols = {}`
* для каждого `p`:

  * `active[p] = []`
  * for cand in shortlist[p]:

    * если `cand.symbol ∉ taken_symbols` и `cand` не на cooldown → берем
    * добавляем в `taken_symbols`
    * пока не набрали лимит слотов

**Пограничные случаи (обязательно прописать заранее):**

* если портфель не может набрать ни одного уникального символа → `active пуст` (и это ок)
* если N портфелей > уникальных символов → неизбежны пустые портфели, это ожидаемо
* tie‑break уже есть (`symbol` lexicographic), плюс порядок портфелей фиксирован → воспроизводимость полная

### Exit criteria CP1.2

* каждый портфель формирует **свой** shortlist (на своих данных)
* active symbols не пересекаются между портфелями

### Тест‑матрица (сильная, но недорогая)

1. **Unit test ranking tuple:** на заранее заданных метриках сортировка совпадает с правилами.
2. **Eligibility gate tests:** границы age/closed/winrate/avg_pnl.
3. **Allocator property test:**

   * вход: случайные shortlists
   * утверждение: `intersection(active[p_i], active[p_j]) == ∅` для любых i≠j
4. **Determinism test:** одинаковые входы → одинаковые active.

---

## CP1.3 — UI & Endpoint Adaptation

Ты уже правильно обозначил: UI должен “просто отрисовать список”.

### Контракт, который реально удобен UI

Отдавай на портфель:

* `id`
* `shortlist: [{symbol, useful_winrate, pm_raw, avg_pnl_pct, closed_trades, cooldown_until?}]`
* `active: [symbol...]`
* `guards: {cooldown, stop_loss_streak, ...}`
* `last_rebalance_ts`
* плюс “KPI set” из текущего runtime (как у тебя в 11)

### Exit criteria

* UI отображает все портфели из ответа
* ничего не надо фиксить руками при смене N

---

## CP1.4 — Stabilization & Regression Sweep

Тут важно покрыть именно то, что чаще всего ломается при переходе A/B → N:

**Регрессионные зоны:**

* scheduler cadence (120s) не деградировал
* cooldown/reset логика не “переехала” случайно на общий уровень
* restore после рестарта возвращает N портфелей с корректными active/shortlist/cooldown
* perf-smoke: сортировки/агрегации на N портфелей не съели цикл

**Exit criteria:** как у тебя.

---

# Checkpoint 2 — Promotion & Bot Runtime (операционный режим без денег)

Теперь мы используем твою математику так, чтобы “гонка” стала источником правды для запуска execution loops.

## CP2.0 Ключевая формализация: что такое “winner”

Требование: “формально и воспроизводимо”.

Я бы зафиксировал так (в рамках твоих текущих формул):

### Winner per portfolio = (symbol, config_id)

1. `symbol` выбирается из `active[p]` (результат CP1, уже no‑overlap)
2. `config_id` выбирается по ShadowFleet внутри этого портфеля/символа:

   * кандидатные конфиги: те, что проходят gate:

     * trades_6h ≥ 5
     * avg_pnl_6h > 0
     * stop_loss_share ≤ 55%
   * выбираем max `score` (твой frozen score)
   * tie‑break: стабильный (например, `config_id`)

Это полностью соответствует твоему математическому стеку и не добавляет новую “философию”.

## CP2.1 Portfolio → Bot runtime (1:1 loop)

* на каждый портфель создаётся отдельный execution loop
* режим пока paper (или shadow→paper), но с реальными order intents и обработкой ошибок/рестартов

## CP2.2 Winner switch без поломки ingestion/метрик

Требование “переключение winner не ломает shadow ingestion и метрики” = строго:

* shadow ingestion продолжает писать trades/metrics в те же структуры
* execution loop “подписан” на winner state и меняет активный (symbol, config) атомарно

**Тесты:**

* переключение winner во время работы цикла не приводит к панике, не ломает API
* метрики до/после переключения остаются консистентны

## CP2.3 Health/restart policy per bot

В API:

* `/api/v1/portfolio/{id}/health` (или в общем списке)
* состояние loop: last_tick, last_feed_event, errors, reconnects

**Инварианты:**

* падение одного портфельного loop не роняет остальные
* restart не сбрасывает глобальные состояния shadow fleet

---

# Checkpoint 3 — Capital Rebalance + Live (финал)

Здесь “математика + бизнес‑логика” становится про деньги и безопасность. Ты уже правильно ставишь “live поверх проверенного paper/shadow”.

## CP3.1 Allocation/reallocation policy (минимально жизнеспособная, но строгая)

Тебе нужна политика, которая:

* использует уже имеющиеся метрики (не требует новой науки)
* сглаживает шум (иначе будет churn)
* имеет hard limits

### Простой вариант (под твои метрики)

Для каждого портфеля считаем **portfolio_score** на основе его текущего winner/config:

Например:

* берём тот же `score` ShadowFleet (6h) победителя
* если gate не пройден → score=0

Дальше:

* `w_i = max(0, score_i)`
* если `sum(w)=0` → всё в cash/или равномерно минимумами
* иначе `alloc_i_target = w_i / sum(w)`

Сглаживание (чтобы не дергаться):

* `alloc_i_new = (1-α)*alloc_i_old + α*alloc_i_target`
  где α маленький (0.05–0.2)

Hard bounds:

* `alloc_i_min`, `alloc_i_max`
* turnover limit per rebalance

## CP3.2 Live safety-guards (встроенные в твой pipeline)

У тебя уже есть сильные зачатки safety:

* drift outlier guard
* leadership gate
* stop_loss_streak triggers + cooldown
* max_hold_ms/timeouts

Для live их надо поднять на уровень “kill conditions”:

**Примеры условий стопа, напрямую из твоих метрик:**

* если `lag_ms` (p50) выше порога → выключить входы
* если offset/drift часто None/outlier → пауза
* если `stop_loss_streak` триггерит fast/persistent → disable symbol/portfolio
* если `early_stop_churn` слишком высок → disable config

## CP3.3 Runbook rollback/disable

Требование “до уровня портфеля и символа” означает:

* флаг disable portfolio
* флаг disable symbol per portfolio
* флаг disable config_id (у тебя уже есть prune/disable configs)

И это должно работать без redeploy (через конфиг/DB‑флаг/админ‑endpoint).

---

# 3) Суперкоротко: “идеальный” порядок реализации по твоему CP1 (чтобы не жрало время)

1. **CP1.1:** вводишь `PortfolioId` как первоклассную сущность в runtime/API/snapshot
2. **CP1.2:** сегментируешь candidate history по `portfolio_id` + вводишь allocator no-overlap
3. **CP1.3:** UI читает список портфелей, фронт становится “data-driven”
4. **CP1.4:** регрессия: scheduler/cooldown/restore/perf-smoke

И только потом CP2/CP3.

---

Если хочешь, я могу на следующем шаге сделать максимально прикладно и “как для PR”:

* выписать **точные контракты JSON** для `/api/v1/portfolio/active` (schema + пример payload),
* описать **миграцию snapshot V1→V2**,
* и дать **детерминированный алгоритм no-overlap** в виде псевдокода + набор тестов (Given/When/Then) под твои gate/ranking/cooldown.
