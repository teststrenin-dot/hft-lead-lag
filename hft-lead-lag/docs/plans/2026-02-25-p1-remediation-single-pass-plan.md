# P1 Remediation Single Pass Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Закрыть все подтвержденные P1 находки одним прогоном без расширения scope.

**Architecture:** Исправления разделены на 3 независимых контура: (1) WS worker robustness + replay dedup, (2) trial run_id uniqueness в Python driver, (3) надежный trial-batch detect + failure ack в Rust/Python IPC. Каждый контур закрывается через TDD (RED->GREEN) и затем проверяется интеграционным прогоном тестов Rust+Python.

**Tech Stack:** Rust (tokio, tungstenite), Python 3, pytest, cargo test.

---

### Task 1: WS Robustness + Replay Dedup

**Files:**
- Modify: `src/infrastructure/exchanges/binance/mod.rs`
- Modify: `src/infrastructure/exchanges/gate/mod.rs`
- Test: локальные unit tests в этих же файлах

1. Добавить/расширить unit tests на хранение replay-подписок (dedup поведение).
2. Запустить таргетные тесты и зафиксировать RED.
3. Реализовать безопасные helper’ы без `lock().unwrap()` и с dedup/идемпотентным replay snapshot.
4. Запустить таргетные тесты и затем весь rust test suite.

### Task 2: Unique run_id generation

**Files:**
- Modify: `ray_driver/scout.py`
- Modify: `ray_driver/expand.py`
- Modify: `ray_driver/cli.py`
- Create/Modify: `ray_driver/tests/test_run_id.py`

1. Написать failing tests на уникальность/формат run_id и отсутствие second-based collision.
2. Запустить `pytest` на новый тест и зафиксировать RED.
3. Вынести генератор run_id в общий helper и подключить во все фазы (`scout`, `expand`, `forward`).
4. Запустить `pytest ray_driver/tests`.

### Task 3: trial-batch detect + explicit failure ack

**Files:**
- Modify: `src/main.rs`
- Modify: `ray_driver/ipc.py`
- Add tests: `src/main_tests.rs` (новые unit tests на change fingerprint / ack parsing)
- Add tests: `ray_driver/tests/test_ipc_ack_failure.py`

1. Написать failing tests:
   - detect изменения trial-batch по fingerprint (mtime+size), а не только по `mtime`.
   - parsing explicit failure ack в Python IPC.
2. Запустить таргетные тесты и зафиксировать RED.
3. Реализовать:
   - fingerprint-based detect в watcher;
   - explicit failure ack payload на invalid/rejected patch;
   - обработку failure ack в `FleetIPC._wait_ack` с осмысленным exception.
4. Запустить rust+python тесты.

### Task 4: Full verification

1. Run: `cargo test`
2. Run: `python3 -m pytest ray_driver/tests -q`
3. Проверить, что P1 findings закрыты в коде и тестах.
