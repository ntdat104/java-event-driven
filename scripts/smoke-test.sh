#!/usr/bin/env bash
# End-to-end smoke test: create an order, then poll until it is CONFIRMED.
set -euo pipefail

ORDER_API="${ORDER_API:-http://localhost:8081}"
INV_API="${INV_API:-http://localhost:8082}"

echo "→ Creating order..."
RESP=$(curl -s -XPOST "$ORDER_API/api/orders" \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","productId":"p1","quantity":2,"amount":59.90}')
echo "  $RESP"

ORDER_ID=$(echo "$RESP" | sed -n 's/.*"orderId":"\([^"]*\)".*/\1/p')
[ -n "$ORDER_ID" ] || { echo "✗ no orderId returned"; exit 1; }

echo "→ Waiting for event round-trip (orders.created → reserve → inventory.reserved)..."
for i in $(seq 1 20); do
  STATUS=$(curl -s "$ORDER_API/api/orders/$ORDER_ID" | sed -n 's/.*"status":"\([^"]*\)".*/\1/p')
  echo "  attempt $i: status=$STATUS"
  if [ "$STATUS" = "CONFIRMED" ] || [ "$STATUS" = "CANCELLED" ]; then
    echo "✓ Final status: $STATUS"
    echo "→ Inventory for p1:"
    curl -s "$INV_API/api/inventory/p1"; echo
    exit 0
  fi
  sleep 0.5
done

echo "✗ Order did not reach a final status in time"; exit 1
