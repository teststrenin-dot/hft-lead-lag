# Strategy Runtime Modularity (2026-02-20)

## Цель

Сделать подключение второй стратегии дешёвым по времени разработки: без правок hot-loop в `main`, с явным выбором стратегии через конфиг.

## Что изменено

1. Введён единый runtime-контракт стратегии:
   - `src/application/strategies/mod.rs`
   - `RuntimeStrategy` (`on_primary_book`, `on_hedge_book`, `check_signal`)
2. Добавлен builder выбора стратегии:
   - `build_runtime_strategy(...)`
   - текущая реализация: `lead_lag_classic`
   - `dislocation_reversion` заведён в enum как следующий слот и сейчас даёт корректный fail-fast
3. `main` отвязан от `LeadLagStrategy`:
   - event loop теперь работает через `&dyn RuntimeStrategy`
   - логирование сигналов унифицировано через `StrategySignal`
4. В конфиг добавлен селектор:
   - `config/config.toml`:
     - `[strategy]`
     - `active = "lead_lag_classic"`
   - `src/config/mod.rs`: `StrategyKind`, `strategy_kind()`

## Почему это ускоряет добавление второй стратегии

Для новой стратегии больше не нужно переписывать event loop.

Достаточно:
1. Реализовать `RuntimeStrategy` в новом модуле.
2. Добавить одну ветку в `build_runtime_strategy`.
3. Включить её через `[strategy].active`.

## Проверка работоспособности

Запущено и прошло:

1. `cargo check --all-targets`
2. `cargo build`
3. `cargo test`

Дополнительные тесты на новый контур:

1. `config::tests::config_defaults_to_lead_lag_strategy_when_field_is_missing`
2. `config::tests::config_reads_explicit_strategy_selection`
3. `application::strategies::tests::lead_lag_runtime_emits_normalized_signal`
4. `tests::runtime_strategy_builder_loads_lead_lag_classic`
5. `tests::runtime_strategy_builder_rejects_unimplemented_strategy`

## Критика и ответы

1. Критика: "trait object добавит latency".
   - Ответ: один virtual-call на обработку книги/сигнала, без аллокаций в hot path. Сетевой и парсинг-слой по-прежнему доминируют по стоимости.

2. Критика: "сломается обратная совместимость конфигов".
   - Ответ: `StrategyKind` имеет default (`lead_lag_classic`), что покрыто тестом на отсутствие поля.

3. Критика: "оператор выставит неготовую стратегию и система тихо продолжит".
   - Ответ: не тихо. Для неимплементированной стратегии возвращается ошибка и процесс падает на старте (fail-fast), это покрыто тестом.
