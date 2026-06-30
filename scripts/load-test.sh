#!/usr/bin/env bash
# Load-test the order pipeline at ~10k req/s with k6 (through nginx → 5 order
# instances), then VERIFY the run: throughput, latency, message loss, consumer
# lag drain and DLQ. Uses native k6 if installed, else the grafana/k6 container.
#
#   ./scripts/load-test.sh                 # 10k req/s for 60s (defaults)
#   RATE=5000 DURATION=120s ./scripts/load-test.sh
#
# Env: RATE, DURATION, TARGET, VUS, MAX_VUS, PRODUCTS  (see scripts/load-test.js)
#
# NOTE: no `set -e` — k6 returns non-zero when a threshold is breached, and a
# reporting script must still run its verification after that.
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

RATE="${RATE:-10000}"
DURATION="${DURATION:-60s}"
TARGET="${TARGET:-http://localhost:8088/api/orders}"
K6LOG="$(pwd)/load-test-summary.json"   # we capture k6's text output here
KAFKA_CT=edd-kafka1
PG_CT=edd-postgres
CMD_CFG=/etc/kafka/secrets/client-ssl.properties
BS=kafka1:19092

# Optional knobs → k6 -e args (only when set, to avoid empty args)
k6env=(-e RATE="$RATE" -e DURATION="$DURATION" -e TARGET="$TARGET")
[ -n "${VUS:-}" ]      && k6env+=(-e VUS="$VUS")
[ -n "${MAX_VUS:-}" ]  && k6env+=(-e MAX_VUS="$MAX_VUS")
[ -n "${PRODUCTS:-}" ] && k6env+=(-e PRODUCTS="$PRODUCTS")

echo "================================================================"
echo " Load test: $RATE req/s for $DURATION  →  $TARGET"
echo "================================================================"

# ---- 1. Run k6 (tee output so we can parse it; don't abort on threshold fail) -
if command -v k6 >/dev/null 2>&1; then
  k6 run "${k6env[@]}" scripts/load-test.js 2>&1 | tee "$K6LOG" || true
else
  echo "(k6 not found on host — running via the grafana/k6 container)"
  docker run --rm --network host "${k6env[@]}" \
    -v "$(pwd)/scripts/load-test.js:/load-test.js:ro" \
    grafana/k6 run /load-test.js 2>&1 | tee "$K6LOG" || true
fi

# ---- helpers ---------------------------------------------------------------
kafka_count() {  # total messages across all partitions of a topic (0 if absent)
  docker exec "$KAFKA_CT" /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$BS" --command-config "$CMD_CFG" --topic "$1" 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}
group_lag() {    # total consumer lag for a group
  docker exec "$KAFKA_CT" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server "$BS" --command-config "$CMD_CFG" --describe --group "$1" 2>/dev/null \
    | awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END{print s+0}'
}
outbox_pending() {  # events written but not yet relayed to Kafka (send-side backlog)
  docker exec "$PG_CT" psql -U postgres -d order_db -t -A \
    -c "select count(*) from outbox_event where status <> 'PUBLISHED';" 2>/dev/null | tr -d '[:space:]'
}

# ---- 2. Wait for the pipeline to fully drain ------------------------------
# Consumer lag → 0 AND outbox backlog → 0. Lag alone isn't enough: the outbox
# relay may still hold un-published rows even when topic lag is zero. We key off
# the outbox (not order PENDING count) so a pre-existing stale order can't hang us.
echo
echo "Draining pipeline (consumer lag → 0 AND outbox backlog → 0, max 180s)..."
for i in $(seq 1 90); do
  lag_inv=$(group_lag inventory-service)
  lag_ord=$(group_lag order-service)
  obx=$(outbox_pending)
  printf "\r  lag inv=%s ord=%s   outbox backlog=%s        " "${lag_inv:-?}" "${lag_ord:-?}" "${obx:-?}"
  [ "${lag_inv:-1}" = "0" ] && [ "${lag_ord:-1}" = "0" ] && [ "${obx:-1}" = "0" ] && break
  sleep 2
done
echo

# ---- 3. Report -------------------------------------------------------------
accepted=$(grep 'orders_accepted' "$K6LOG" 2>/dev/null | grep -oE '[0-9]+' | head -1)
created=$(kafka_count orders.created)
reserved=$(kafka_count inventory.reserved)
dlt_oc=$(kafka_count orders.created.DLT)
dlt_ir=$(kafka_count inventory.reserved.DLT)

obx=$(outbox_pending)
echo "================================================================"
echo " RESULTS  (topic counts are cumulative — see note below)"
echo "================================================================"
echo "HTTP 202 accepted (k6)      : ${accepted:-n/a}"
echo "orders.created   (produced) : $created"
echo "inventory.reserved          : $reserved"
echo "outbox backlog (unpublished): ${obx:-?}"
echo "DLQ orders.created.DLT      : $dlt_oc"
echo "DLQ inventory.reserved.DLT  : $dlt_ir"
echo
echo "Order status in Postgres:"
docker exec "$PG_CT" psql -U postgres -d order_db -t -A -F' : ' \
  -c "select status, count(*) from orders group by status order by status;" 2>/dev/null \
  | sed 's/^/  /'

echo
echo "---- verdict ----"
# No producer-side loss: every accepted order shows up on the topic.
if [ -n "${accepted:-}" ] && [ "${created:-0}" -ge "${accepted:-0}" ]; then
  echo "  ✓ no producer loss      (orders.created ≥ accepted)"
else
  echo "  ✗ producer loss?        (accepted=${accepted:-?} > orders.created=$created)"
fi
# Each order got an inventory decision: reserved count tracks created count.
if [ "${reserved:-0}" -ge "${created:-0}" ]; then
  echo "  ✓ every event consumed  (inventory.reserved ≥ orders.created)"
else
  echo "  ✗ consumer behind        (inventory.reserved=$reserved < orders.created=$created)"
fi
[ "${dlt_oc:-0}" = "0" ] && [ "${dlt_ir:-0}" = "0" ] \
  && echo "  ✓ DLQ empty             (no poison messages)" \
  || echo "  ⚠ DLQ non-empty         (inspect in Kafka-UI :8080)"
[ "${obx:-1}" = "0" ] \
  && echo "  ✓ outbox drained        (every event published — send side lost nothing)" \
  || echo "  ⚠ outbox backlog=$obx   (relay still draining — re-check in a moment)"

echo
echo "NOTE: topic counts are cumulative across runs (topics persist), and Postgres"
echo "uses the ./data/postgres bind mount — 'docker compose down -v' does NOT reset it."
echo "For a clean slate:  docker compose down -v && sudo rm -rf data/  (then re-up)."

echo
echo "Dashboards: Grafana http://localhost:3000  ·  Kafka-UI http://localhost:8080"
echo
cat <<'EOF'
Fault-tolerance test: re-run this and, mid-flight, kill a broker or an app:
  docker compose stop kafka3          # broker down → RF=3 keeps producing (min.insync=2)
  docker compose stop order-3         # instance down → nginx routes around it
Then watch the verdict: DLQ stays empty, lag + outbox drain ⇒ no loss.
EOF
