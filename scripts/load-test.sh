#!/usr/bin/env bash
# Fire load at order-service. Uses `hey` if available, else falls back to ApacheBench.
#   ./scripts/load-test.sh [TOTAL_REQUESTS] [CONCURRENCY]
set -euo pipefail

TOTAL="${1:-100000}"
CONC="${2:-200}"
URL="${ORDER_API:-http://localhost:8081}/api/orders"
BODY='{"customerId":"c1","productId":"p1","quantity":1,"amount":9.99}'

echo "Target: $URL"
echo "Total: $TOTAL  Concurrency: $CONC"

if command -v hey >/dev/null 2>&1; then
  hey -n "$TOTAL" -c "$CONC" -m POST \
    -H 'Content-Type: application/json' -d "$BODY" "$URL"
elif command -v ab >/dev/null 2>&1; then
  TMP=$(mktemp); printf '%s' "$BODY" > "$TMP"
  ab -n "$TOTAL" -c "$CONC" -p "$TMP" -T 'application/json' "$URL"
  rm -f "$TMP"
else
  echo "Install a load tool first:  brew install hey"
  exit 1
fi

echo
echo "Inspect latency/throughput in Grafana (http://localhost:3000) and"
echo "consumer lag / DLQ in Kafka-UI (http://localhost:8080)."
