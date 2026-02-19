#!/usr/bin/env bash
# Benchmark: measure drift percentiles at different SYMBOLS_PER_WS values.
# Each configuration runs for DURATION seconds, then the process is stopped
# and drift stats are extracted from logs.
#
# Usage: ./benchmark_sockets.sh [duration_per_run]
#   duration_per_run  – seconds each config runs (default: 30)

set -euo pipefail
cd "$(dirname "$0")"

# Load credentials
if [ -f .env ]; then
  set -a; source .env; set +a
fi

DURATION=${1:-30}
LOG_DIR="logs"
RESULT_FILE="benchmark_results.txt"
CONFIGS=(2 5 10 20 47)  # SYMBOLS_PER_WS values to test (47 = all 93 symbols in 2 sockets)

# Build release binary once
echo "=== Building release binary ==="
cargo build --release --quiet 2>/dev/null

printf "%-12s %-8s %-10s %-10s %-10s %-10s %-10s\n" \
  "SYMS_PER_WS" "SOCKETS" "DRIFT_AVG" "DRIFT_P50" "DRIFT_P95" "DRIFT_P99" "DRIFT_MAX" | tee "$RESULT_FILE"
printf "%s\n" "$(printf '=%.0s' {1..82})" | tee -a "$RESULT_FILE"

for SYM_PER_WS in "${CONFIGS[@]}"; do
  # Calculate expected sockets: ceil(93 / SYM_PER_WS) * 2 exchanges
  SOCKETS=$(( ( (93 + SYM_PER_WS - 1) / SYM_PER_WS ) * 2 ))

  echo ""
  echo "--- Testing SYMBOLS_PER_WS=${SYM_PER_WS} (≈${SOCKETS} sockets) for ${DURATION}s ---"

  # Clear old runtime log
  rm -f "${LOG_DIR}/runtime.log"

  # Run the system
  export SYMBOLS_PER_WS="${SYM_PER_WS}"
  timeout "${DURATION}" ./target/release/hft-lead-lag 2>/dev/null &
  PID=$!

  # Wait for the run duration
  sleep "${DURATION}"

  # Stop the process
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  sleep 1

  # Extract drift stats from log
  # Format: drift=[n=NNNN avg=Xms p50=Xms p95=Xms p99=Xms max=Xms]
  LAST_DRIFT=$(grep -oP 'drift=\[.*?\]' "${LOG_DIR}/runtime.log" 2>/dev/null | tail -1 || echo "")

  if [ -z "$LAST_DRIFT" ] || echo "$LAST_DRIFT" | grep -q "no_data"; then
    printf "%-12s %-8s %-10s %-10s %-10s %-10s %-10s\n" \
      "$SYM_PER_WS" "$SOCKETS" "N/A" "N/A" "N/A" "N/A" "N/A" | tee -a "$RESULT_FILE"
  else
    # Aggregate all drift reports from the run
    AVG_ALL=$(grep -oP 'avg=\K-?[0-9]+' "${LOG_DIR}/runtime.log" | awk '{s+=$1;n++} END{printf "%d", s/n}')
    P50_ALL=$(grep -oP 'p50=\K-?[0-9]+' "${LOG_DIR}/runtime.log" | awk '{s+=$1;n++} END{printf "%d", s/n}')
    P95_ALL=$(grep -oP 'p95=\K-?[0-9]+' "${LOG_DIR}/runtime.log" | awk '{s+=$1;n++} END{printf "%d", s/n}')
    P99_ALL=$(grep -oP 'p99=\K-?[0-9]+' "${LOG_DIR}/runtime.log" | awk '{s+=$1;n++} END{printf "%d", s/n}')
    MAX_ALL=$(grep -oP 'max=\K-?[0-9]+' "${LOG_DIR}/runtime.log" | sort -n | tail -1)

    printf "%-12s %-8s %-10s %-10s %-10s %-10s %-10s\n" \
      "$SYM_PER_WS" "$SOCKETS" "${AVG_ALL}ms" "${P50_ALL}ms" "${P95_ALL}ms" "${P99_ALL}ms" "${MAX_ALL}ms" | tee -a "$RESULT_FILE"
  fi
done

echo ""
echo "=== Benchmark complete. Results saved to ${RESULT_FILE} ==="
