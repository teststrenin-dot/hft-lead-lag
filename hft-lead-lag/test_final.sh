#!/bin/bash
# Финальный тест HFT Lead-Lag с volume filter и тикерами

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
LOG_FILE="$LOG_DIR/test_final.log"
SUMMARY_FILE="$LOG_DIR/summary.log"

echo "╔═══════════════════════════════════════════════════════════╗" | tee "$LOG_FILE"
echo "║     HFT Lead-Lag Final Connection Test                    ║" | tee -a "$LOG_FILE"
echo "╚═══════════════════════════════════════════════════════════╝" | tee -a "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Запуск на 60 секунд с выводом тикеров
timeout 60 ./target/debug/hft-lead-lag 2>&1 | tee -a "$LOG_FILE" || true

echo "" | tee -a "$LOG_FILE"
echo "╔═══════════════════════════════════════════════════════════╗" | tee -a "$LOG_FILE"
echo "║                    Test Summary                           ║" | tee -a "$LOG_FILE"
echo "╚═══════════════════════════════════════════════════════════╝" | tee -a "$LOG_FILE"
echo "Finished: $(date)" | tee -a "$LOG_FILE"

# Проверка результатов
echo "" | tee -a "$LOG_FILE"
echo "Results:" | tee -a "$LOG_FILE"

if grep -q "Binance: [1-9]" "$LOG_FILE"; then
    BINANCE_COUNT=$(grep "Binance:" "$LOG_FILE" | head -1 | grep -oP '\d+' | head -1)
    echo "✅ Binance: $BINANCE_COUNT symbols with volume >= \$1M" | tee -a "$LOG_FILE"
else
    echo "❌ Binance: FAILED" | tee -a "$LOG_FILE"
fi

if grep -q "Gate: [1-9]" "$LOG_FILE"; then
    GATE_COUNT=$(grep "Gate:" "$LOG_FILE" | head -1 | grep -oP '\d+' | head -1)
    echo "✅ Gate: $GATE_COUNT symbols with volume >= \$1M" | tee -a "$LOG_FILE"
else
    echo "❌ Gate: FAILED" | tee -a "$LOG_FILE"
fi

if grep -q "Common symbols: [1-9]" "$LOG_FILE"; then
    COMMON_COUNT=$(grep "Common symbols:" "$LOG_FILE" | grep -oP '\d+' | head -1)
    echo "✅ Common symbols: $COMMON_COUNT" | tee -a "$LOG_FILE"
else
    echo "❌ Common symbols: NONE" | tee -a "$LOG_FILE"
fi

if grep -q "Connected to Binance Futures WebSocket" "$LOG_FILE"; then
    echo "✅ Binance WebSocket: CONNECTED" | tee -a "$LOG_FILE"
else
    echo "❌ Binance WebSocket: FAILED" | tee -a "$LOG_FILE"
fi

if grep -q "Connected to Gate.io Futures WebSocket" "$LOG_FILE"; then
    echo "✅ Gate WebSocket: CONNECTED" | tee -a "$LOG_FILE"
else
    echo "❌ Gate WebSocket: FAILED" | tee -a "$LOG_FILE"
fi

echo "" | tee -a "$LOG_FILE"
echo "Log saved to: $LOG_FILE" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"
echo "═══════════════════════════════════════════════════════════" | tee -a "$LOG_FILE"
{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] final-test"
    tail -n 16 "$LOG_FILE"
    echo "----"
} > "$SUMMARY_FILE"
echo "Summary saved to: $SUMMARY_FILE" | tee -a "$LOG_FILE"
