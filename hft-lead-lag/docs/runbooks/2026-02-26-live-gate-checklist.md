# Live Gate Checklist (2026-02-26)

## Scope

Чеклист перед любым включением режима реальной торговли. Текущий единственный рабочий режим исполнения: `paper`.

## 1. Runtime Mode

- Подтверждён единый режим `paper`.
- `TRADING_MODE` принимает только `paper`; legacy значение `shadow_only` больше не поддерживается.
- `live` не включается без прохождения всех пунктов ниже.

## 2. Data and Pipeline

- Последний `forward-*` run подтверждён и не пустой.
- Метрики по `forward-*` не деградировали относительно предыдущего валидного прогона.
- API `/api/v1/portfolio/*` отвечает стабильно.

## 3. Mandatory Risk Guards

Перед `live` должны быть включены и проверены:

- max loss/day
- max open positions
- emergency stop
- order throttle

## 4. Operational Readiness

- Health endpoint без критических issues.
- DB не показывает saturation проблемы (`db_dropped_batches`, `db_overflowed_batches` в норме).
- Логи не содержат повторяющихся критичных ошибок коннекторов/ордера.

## 5. Go / No-Go

`Go` только если:

- все пункты выше зелёные,
- есть ручное подтверждение оператора,
- есть план быстрого отката в `paper`.

Иначе: `No-Go`, остаёмся в `paper`.
