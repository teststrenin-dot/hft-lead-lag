#!/bin/bash
# Финальный тест HFT Lead-Lag с volume filter и тикерами

set -e

cd /root/turbo/hft-lead-lag

# API ключи
export BINANCE_API_KEY="TnczkCaMuCYSvkLBYbiRXkAPDXIexso3jdIKu3TBA8aSiRwGlOTnSspstBcdpZrp"
export BINANCE_API_SECRET="cYkg26J3WqiMyPZMKA87tgbPJmRo1ybghVyeh52s2JaLQrTNDolmAc6V66rAGPxj"
export GATE_API_KEY="f9dd727fd86d14c064971e59e0c88e3f"
export GATE_API_SECRET="534d0d582a0fa23faf378cf2b0b68cc4c56212b47f1293b93fa335fdf326dfb1"
export RUST_LOG=hft_lead_lag=info

LOG_FILE="/root/turbo/hft-lead-lag/test_final_$(date +%Y%m%d_%H%M%S).log"

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
