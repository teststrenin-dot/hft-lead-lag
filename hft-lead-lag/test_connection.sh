#!/bin/bash
# Тест подключения к биржам HFT Lead-Lag

set -e

cd /root/turbo/hft-lead-lag

# API ключи
export BINANCE_API_KEY="TnczkCaMuCYSvkLBYbiRXkAPDXIexso3jdIKu3TBA8aSiRwGlOTnSspstBcdpZrp"
export BINANCE_API_SECRET="cYkg26J3WqiMyPZMKA87tgbPJmRo1ybghVyeh52s2JaLQrTNDolmAc6V66rAGPxj"
export GATE_API_KEY="f9dd727fd86d14c064971e59e0c88e3f"
export GATE_API_SECRET="534d0d582a0fa23faf378cf2b0b68cc4c56212b47f1293b93fa335fdf326dfb1"
export RUST_LOG=hft_lead_lag=info

LOG_DIR="/root/turbo/hft-lead-lag/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/test_connection.log"
SUMMARY_FILE="$LOG_DIR/summary.log"

echo "=== HFT Lead-Lag Connection Test ===" | tee "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Запуск на 10 секунд
timeout 10 ./target/debug/hft-lead-lag 2>&1 | tee -a "$LOG_FILE" || true

echo "" | tee -a "$LOG_FILE"
echo "=== Test Summary ===" | tee -a "$LOG_FILE"
echo "Finished: $(date)" | tee -a "$LOG_FILE"

# Проверка результатов
echo "" | tee -a "$LOG_FILE"
echo "Results:" | tee -a "$LOG_FILE"

if grep -q "Connected to Binance Futures WebSocket" "$LOG_FILE"; then
    echo "✅ Binance: CONNECTED" | tee -a "$LOG_FILE"
else
    echo "❌ Binance: FAILED" | tee -a "$LOG_FILE"
fi

if grep -q "Connected to Gate.io Futures WebSocket" "$LOG_FILE"; then
    echo "✅ Gate.io: CONNECTED" | tee -a "$LOG_FILE"
else
    echo "❌ Gate.io: FAILED" | tee -a "$LOG_FILE"
fi

if grep -q "Subscribing to" "$LOG_FILE"; then
    echo "✅ Subscriptions: SENT" | tee -a "$LOG_FILE"
else
    echo "❌ Subscriptions: FAILED" | tee -a "$LOG_FILE"
fi

echo "" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] connection-test"
    tail -n 8 "$LOG_FILE"
    echo "----"
} > "$SUMMARY_FILE"
echo "Summary saved to: $SUMMARY_FILE" | tee -a "$LOG_FILE"
