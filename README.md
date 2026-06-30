# Event-Driven Microservices — Spring Boot 21 · Kafka · Redis · H2

Two services communicating via Kafka with a reliability-first design: **no event
loss**, **exactly-once *effect*** under at-least-once delivery, automatic **retry +
DLQ**, end-to-end **distributed tracing**, and producer/consumer tuning for **~10k
req/s** with low latency.

```
                 ┌──────────────────────────────────────────────┐
   POST /orders  │              order-service  :8081            │
 ───────────────►│  REST → @Transactional { Order + Outbox } ──┐ │
                 │                          (H2, one local TX)  │ │
                 │   OutboxRelay (@Scheduled) ◄──── poll ───────┘ │
                 └───────────┬──────────────────────────▲─────────┘
                             │ publish (idempotent)      │ consume (manual ack)
                       orders.created            inventory.reserved
                             │                           │
                 ┌───────────▼──────────────────────────┴─────────┐
                 │            inventory-service  :8082             │
                 │  @KafkaListener (manual ack)                    │
                 │  TX { inbox(eventId) + reserve stock + outbox } │
                 │  OutboxRelay → publish inventory.reserved       │
                 │  retry: ExponentialBackOff → <topic>.DLT        │
                 └────────────────────────────────────────────────┘

   Kafka (KRaft) · Redis · OpenTelemetry + Grafana LGTM (Loki·Tempo·Prometheus) · Kafka-UI
```

## Modules

| Module             | Role                                                                 |
|--------------------|----------------------------------------------------------------------|
| `common-events`    | Shared event contracts (`OrderCreatedEvent`, `InventoryReservedEvent`, `Topics`) |
| `order-service`    | REST ingress → transactional outbox → publishes `orders.created`; consumes `inventory.reserved` to finalise the order |
| `inventory-service`| Consumes `orders.created`, reserves stock, publishes `inventory.reserved` via its own outbox |

---

## The reliability design (the important part)

### 1. No event loss — Transactional Outbox

A service must change its DB **and** publish an event. Doing both directly is the
*dual-write problem*: if the process dies between the DB commit and the Kafka send,
the event is lost forever (or vice-versa).

Instead, each service writes the event into an `outbox_event` table **in the same
local transaction** as the business change. A `@Scheduled` **OutboxRelay** then polls
pending rows and publishes them. A row stays `PENDING` until the broker acks, so a
crash mid-publish simply retries on the next tick. **The event can never be lost.**

> Scaling note: the polling relay is fine to tens of thousands/sec. Beyond that, swap
> it for **Debezium CDC** reading the DB log — same `outbox_event` contract, zero
> polling latency. Documented, not silently capped.

### 2. Why we DON'T use auto-commit offsets

`enable.auto.commit=true` commits offsets on a **timer**, decoupled from whether your
code actually finished. That gives two bad failure modes:

- commit fires *before* processing finishes → crash → **event silently lost**;
- you have no control point to align the offset with your side effects.

We set **`enable.auto.commit=false`** + **`AckMode.MANUAL_IMMEDIATE`** and call
`ack.acknowledge()` **only after** the transaction succeeds. This is true
**at-least-once**: a crash before ack just redelivers the record.

### 3. Exactly-once *effect* — idempotent consumers (DB inbox + Redis)

At-least-once means duplicates are normal, so every consumer must be idempotent.

- **DB inbox** (`processed_event`) is the source of truth: the `eventId` is inserted
  in the *same transaction* as the work. PK uniqueness makes dedup correct across
  crashes and concurrent consumers.
- **Redis** is the fast path: it is written **only after commit**, so a hit always
  means "really processed" — it safely short-circuits the common duplicate case
  without a DB round-trip. (Writing Redis *before* commit would be a bug: a crash
  would leave it marked-but-not-done and the redelivery would be wrongly skipped →
  lost. We avoid that with an after-commit hook.)

### 4. Retry + Dead Letter Queue

`DefaultErrorHandler` retries failed records with **exponential backoff**
(500ms → ×2 → cap 10s, give up after 60s), then a `DeadLetterPublishingRecoverer`
routes the record to **`<topic>.DLT`** (e.g. `orders.created.DLT`). Poison messages
(deserialization failures, caught by `ErrorHandlingDeserializer`) are **not** retried
— they go straight to the DLT so one bad record never blocks a partition. Inspect /
replay DLT contents in Kafka-UI.

### 5. Observability — OpenTelemetry + Grafana LGTM

The full **metrics + traces + logs** triad, correlated in Grafana:

```
order/inventory (host)
  ├─ /actuator/prometheus  ◄── Prometheus      (metrics, pull + exemplars)
  └─ OTLP (traces + logs)  ──► OTel Collector ──┬─► Tempo  (traces)
                                                └─► Loki   (logs)
              Grafana ◄── Prometheus · Tempo · Loki  (one click: metric → trace → log)
```

- **Metrics** — Micrometer → Prometheus registry, scraped at `/actuator/prometheus`.
  Histograms expose **exemplars** (a `traceId` per bucket) so a latency spike in a
  Grafana panel links straight to the trace that caused it.
- **Traces** — Micrometer Tracing + OpenTelemetry, exported over **OTLP** to the
  collector → **Tempo**. Kafka listener/template observation propagates the W3C
  `traceparent` across hops. Because the outbox relay runs on a *different* thread
  than the original request, we **capture the traceparent at enqueue time, store it
  in the outbox row, and replay it as a Kafka header** — so a single trace spans
  `HTTP → outbox → Kafka → consumer → outbox → Kafka → consumer`. Tempo's
  metrics-generator also derives a **service graph** + RED metrics from spans.
- **Logs** — exported over **OTLP** (Spring Boot 3.4 OTLP log appender) to the
  collector → **Loki**. Each line carries `trace_id` as structured metadata, so
  Loki ↔ Tempo links work both ways. `traceId`/`spanId` stay in the console pattern too.

The **OpenTelemetry Collector** is the single ingestion seam: swap a backend without
touching the apps. Grafana ships with **provisioned datasources + dashboards** (see
`monitoring/grafana/`) — no manual setup.

### 6. Tuned for ~10k req/s

**Producer:** `acks=all` + idempotence (durable, no dup on broker) with `lz4`
compression, `linger.ms=5`, `batch.size=64KB`, `max.in.flight=5` — batching amortises
per-record cost without losing ordering or durability.
**Topics:** 12 partitions → up to 12 parallel consumers per group.
**Consumer:** `concurrency = partitions`, `max.poll.records=500`.
**Ingress:** Tomcat 400 worker threads; each `createOrder` is a short local TX.
**JPA:** Hikari pool 50, JDBC batch inserts.
Horizontal scale: run N instances per service in the same consumer group — Kafka
rebalances partitions across them; the outbox relay uses `PESSIMISTIC_WRITE` so
multiple relays don't double-publish.

---

## Running it

### 1. Start infrastructure
```bash
docker compose up -d        # Kafka, Redis, OTel Collector, Tempo, Loki, Prometheus, Grafana, Kafka-UI
```

### 2. Build
```bash
mvn -s settings-local.xml clean package          # -s only needed if behind the CodeArtifact mirror
```
> `settings-local.xml` resolves from Maven Central directly. If your global
> `~/.m2/settings.xml` CodeArtifact token is valid, you can omit `-s`.

### 3. Run the services (two terminals)
```bash
mvn -s settings-local.xml -pl order-service     spring-boot:run
mvn -s settings-local.xml -pl inventory-service spring-boot:run
```

### 4. Smoke test
```bash
./scripts/smoke-test.sh
```
or manually:
```bash
# create an order
curl -s -XPOST localhost:8081/api/orders -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","productId":"p1","quantity":2,"amount":59.90}'
# → {"orderId":"...","status":"PENDING"}

# a moment later the order is CONFIRMED (inventory reserved + event consumed back)
curl -s localhost:8081/api/orders/<orderId>
curl -s localhost:8082/api/inventory/p1
```

### 5. Load test (~10k req/s)
```bash
./scripts/load-test.sh 100000 200      # 100k requests, 200 concurrent (needs `hey` or falls back to ab)
```

## Consoles

| What              | URL                                            |
|-------------------|------------------------------------------------|
| **Grafana**       | http://localhost:3000  (anonymous admin) — dashboards under the *Event-Driven* folder |
| Metrics (Prom)    | http://localhost:9090                          |
| Traces (Tempo)    | via Grafana → Explore → Tempo (API on :3200)   |
| Logs (Loki)       | via Grafana → Explore → Loki (API on :3100)    |
| OTLP ingest       | localhost:4318 (HTTP) / localhost:4317 (gRPC)  |
| Kafka / DLQ       | http://localhost:8080                          |
| Prometheus scrape | http://localhost:8081/actuator/prometheus      |

Open Grafana → **Dashboards → Event-Driven**: two ready-made boards,
*Service Overview (RED + JVM)* and *Kafka & Event Pipeline*. From any latency
panel, click an exemplar dot to jump to the exact trace; from a trace span, jump
to its logs — all wired via provisioning.

## Failure scenarios to try
- **Kill inventory-service mid-load**, restart it → no orders stuck; events redeliver, dedup prevents double reservation.
- **Stop Kafka**, create orders, restart Kafka → outbox drains, nothing lost.
- **Send a malformed record** to `orders.created` → lands in `orders.created.DLT`, partition keeps flowing.

## Production hardening (out of scope here, noted for honesty)
- Kafka replication factor 3, `min.insync.replicas=2` (here RF=1, single broker).
- Replace H2 with Postgres; replace the polling relay with Debezium CDC.
- Schema Registry + Avro/Protobuf instead of JSON strings for contract evolution.
- Lower trace sampling (e.g. 10%) and add an outbox-lag / consumer-lag alert.
- A scheduled purge of old `processed_event` / published `outbox_event` rows.
