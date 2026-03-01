#!/usr/bin/env bash
set -euo pipefail

URL="http://127.0.0.1:5000/health"
SAMPLES=6
INTERVAL_SEC=5
WARMUP_SAMPLES=1
OUT_FILE="/tmp/hft_health_recovery_drill_$(date +%s).jsonl"

usage() {
  cat <<'EOF'
Usage: health_recovery_drill.sh [options]

Options:
  --url <http-url>           Health endpoint URL (default: http://127.0.0.1:5000/health)
  --samples <n>              Number of samples to collect (default: 6)
  --interval-sec <n>         Delay between samples in seconds (default: 5)
  --warmup-samples <n>       Initial samples ignored for pass/fail (default: 1)
  --out <path>               JSONL output path (default: /tmp/hft_health_recovery_drill_<ts>.jsonl)
  -h, --help                 Show this help

Pass criteria (after warmup):
  - status == "ok"
  - hft_mode_status == "hft"
  - issues does NOT contain:
    hft_slo_degraded_non_hft
    engine_state_stall
    signal_loop_stall
    execution_loop_stall
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      URL="${2:-}"
      shift 2
      ;;
    --samples)
      SAMPLES="${2:-}"
      shift 2
      ;;
    --interval-sec)
      INTERVAL_SEC="${2:-}"
      shift 2
      ;;
    --warmup-samples)
      WARMUP_SAMPLES="${2:-}"
      shift 2
      ;;
    --out)
      OUT_FILE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if ! [[ "$SAMPLES" =~ ^[0-9]+$ ]] || ! [[ "$INTERVAL_SEC" =~ ^[0-9]+$ ]] || ! [[ "$WARMUP_SAMPLES" =~ ^[0-9]+$ ]]; then
  echo "samples/interval-sec/warmup-samples must be non-negative integers" >&2
  exit 2
fi

if [[ "$SAMPLES" -eq 0 ]]; then
  echo "--samples must be > 0" >&2
  exit 2
fi

mkdir -p "$(dirname "$OUT_FILE")"
: > "$OUT_FILE"

echo "health_recovery_drill: url=$URL samples=$SAMPLES interval=${INTERVAL_SEC}s warmup=$WARMUP_SAMPLES"
echo "health_recovery_drill: writing JSONL samples to $OUT_FILE"

FAIL_COUNT=0
WATCHDOG_KEYS_JSON='["hft_slo_degraded_non_hft","engine_state_stall","signal_loop_stall","execution_loop_stall"]'

for ((i=1; i<=SAMPLES; i++)); do
  payload="$(curl -fsS "$URL")"
  echo "$payload" >> "$OUT_FILE"

  analysis="$(python3 - "$i" "$WARMUP_SAMPLES" "$WATCHDOG_KEYS_JSON" <<'PY'
import json
import sys

sample_idx = int(sys.argv[1])
warmup = int(sys.argv[2])
watchdog_keys = set(json.loads(sys.argv[3]))

obj = json.loads(sys.stdin.read() or "{}")
status = obj.get("status", "unknown")
hft_mode = obj.get("hft_mode_status", "unknown")
issues = obj.get("issues", []) or []
warnings = obj.get("warnings", []) or []

watchdog_hits = [k for k in issues if k in watchdog_keys]
in_eval = sample_idx > warmup
failed = in_eval and (status != "ok" or hft_mode != "hft" or bool(watchdog_hits))

print(
    json.dumps(
        {
            "sample": sample_idx,
            "in_eval_window": in_eval,
            "failed": failed,
            "status": status,
            "hft_mode_status": hft_mode,
            "issues": issues,
            "warnings": warnings,
            "watchdog_hits": watchdog_hits,
        },
        ensure_ascii=True,
    )
)
PY
<<<"$payload")"

  sample_failed="$(python3 - <<'PY'
import json
import sys
obj = json.loads(sys.stdin.read() or "{}")
print("1" if obj.get("failed") else "0")
PY
<<<"$analysis")"

  summary_line="$(python3 - <<'PY'
import json
import sys
obj = json.loads(sys.stdin.read() or "{}")
print(
    f"sample={obj['sample']} eval={obj['in_eval_window']} failed={obj['failed']} "
    f"status={obj['status']} hft_mode={obj['hft_mode_status']} watchdog_hits={obj['watchdog_hits']}"
)
PY
<<<"$analysis")"

  echo "$summary_line"
  if [[ "$sample_failed" == "1" ]]; then
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi

  if [[ "$i" -lt "$SAMPLES" ]]; then
    sleep "$INTERVAL_SEC"
  fi
done

if [[ "$FAIL_COUNT" -gt 0 ]]; then
  echo "health_recovery_drill: FAIL ($FAIL_COUNT failing samples after warmup). See $OUT_FILE" >&2
  exit 1
fi

echo "health_recovery_drill: PASS (all evaluated samples satisfy recovery criteria)."
echo "health_recovery_drill: samples saved to $OUT_FILE"
