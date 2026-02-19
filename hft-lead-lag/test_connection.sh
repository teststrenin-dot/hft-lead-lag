#!/bin/bash
# Тест подключения к биржам HFT Lead-Lag

set -e

cd /root/turbo/hft-lead-lag

# API ключи — задать в окружении или в .env
if [ -f .env ]; then
    set -a; source .env; set +a
fi

for var in BINANCE_API_KEY BINANCE_API_SECRET GATE_API_KEY GATE_API_SECRET; do
    if [ -z "${!var}" ]; then
        echo "ERROR: $var is not set. Export it or add to .env" >&2
        exit 1
    fi
done
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
