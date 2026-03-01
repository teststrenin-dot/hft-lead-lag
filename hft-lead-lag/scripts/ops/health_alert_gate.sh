#!/usr/bin/env bash
set -euo pipefail

URL="http://127.0.0.1:5000/health"
ON_WARN_CMD=""
ON_CRITICAL_CMD=""

usage() {
  cat <<'EOF'
Usage: health_alert_gate.sh [options]

Options:
  --url <http-url>           Health endpoint URL (default: http://127.0.0.1:5000/health)
  --on-warn <shell-cmd>      Command to run when alert_level == warn
  --on-critical <shell-cmd>  Command to run when alert_level == critical
  -h, --help                 Show help

Exit codes:
  0   alert_level=ok
  10  alert_level=warn
  20  alert_level=critical
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)
      URL="${2:-}"
      shift 2
      ;;
    --on-warn)
      ON_WARN_CMD="${2:-}"
      shift 2
      ;;
    --on-critical)
      ON_CRITICAL_CMD="${2:-}"
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

payload="$(curl -fsS "$URL")"
analysis="$(python3 - <<'PY'
import json
import sys

obj = json.loads(sys.stdin.read() or "{}")
level = obj.get("alert_level", "unknown")
status = obj.get("status", "unknown")
issues = obj.get("issues", []) or []
warnings = obj.get("warnings", []) or []
print(json.dumps({
    "alert_level": level,
    "status": status,
    "issues": issues,
    "warnings": warnings,
}, ensure_ascii=True))
PY
<<<"$payload")"

level="$(python3 - <<'PY'
import json
import sys
obj = json.loads(sys.stdin.read() or "{}")
print(obj.get("alert_level", "unknown"))
PY
<<<"$analysis")"

echo "health_alert_gate: level=$level details=$analysis"

case "$level" in
  ok)
    exit 0
    ;;
  warn)
    if [[ -n "$ON_WARN_CMD" ]]; then
      bash -lc "$ON_WARN_CMD"
    fi
    exit 10
    ;;
  critical)
    if [[ -n "$ON_CRITICAL_CMD" ]]; then
      bash -lc "$ON_CRITICAL_CMD"
    fi
    exit 20
    ;;
  *)
    echo "health_alert_gate: unknown alert level: $level" >&2
    exit 2
    ;;
esac
