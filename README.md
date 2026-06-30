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

### 0. Generate Kafka TLS certs (once)
The Kafka cluster runs over **SSL/mTLS**, so generate the keystores first:
```bash
./scripts/generate-kafka-certs.sh        # writes ./certs/* (CA + broker & client stores)
```

### 1. Start infrastructure
```bash
docker compose up -d        # 5-broker Kafka (SSL), Redis, OTel Collector, Tempo, Loki, Prometheus, Grafana, Kafka-UI
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

### 5. Load test (~10k req/s, clustered)
Run **5 order + 5 inventory** instances behind nginx and fire k6 at them — see
[Scale-out & load testing](#scale-out--load-testing-5-order--5-inventory--nginx) below.
```bash
docker compose up -d --build          # brings up nginx + 5+5 app instances too
./scripts/load-test.sh                # k6 @ 10k req/s for 60s, then a loss/lag/DLQ report
```

## Production-like Kafka: 5-broker cluster over SSL/mTLS

`docker-compose.yml` runs a **5-broker KRaft cluster** with TLS + mutual auth on
every data listener — built for realistic benchmarking, not a single dev broker.

| Aspect            | Setting                                                                 |
|-------------------|-------------------------------------------------------------------------|
| Brokers           | 5 (KRaft combined broker+controller), host ports `9092 / 9192 / 9292 / 9392 / 9492` |
| Security          | `HOST` + inter-broker `DOCKER` listeners = **SSL, client-auth required (mTLS)**; internal `CONTROLLER` = PLAINTEXT |
| Topics            | **5 partitions**, **RF=3**, `min.insync.replicas=2` (tolerate one broker down at `acks=all`) |
| Cert SAN          | `localhost`, `kafka1`..`kafka5`, `127.0.0.1`                             |

SSL flows through Spring's `KafkaProperties` into both the producer and the
consumer, so the SSL block lives directly under `spring.kafka` in each
`application.yml` — **no `KafkaConfig` code change**. Everything is env-overridable:

| Property / env var                                            | Default                              |
|---------------------------------------------------------------|--------------------------------------|
| `KAFKA_BOOTSTRAP`                                             | `localhost:9092,…:9192,…:9292,…:9392,…:9492` |
| `KAFKA_SECURITY_PROTOCOL`                                    | `SSL`                                |
| `KAFKA_SSL_TRUSTSTORE_LOCATION` / `..._KEYSTORE_LOCATION`    | `file:certs/kafka.client.{trust,key}store.jks` |
| `KAFKA_SSL_TRUSTSTORE_PASSWORD` / `..._KEYSTORE_..` / `..._KEY_..` | `changeit`                      |
| `app.topics.partitions` / `app.topics.replicas`             | `5` / `3`                            |

Regenerate certs for a real deployment (strong password + real broker hostnames):
```bash
KAFKA_CERT_PASSWORD=<strong> KAFKA_BROKER_SAN="DNS:broker1.prod,DNS:broker2.prod,IP:10.0.0.5" \
  ./scripts/generate-kafka-certs.sh
```

> ⚠️ The committed certs use the throwaway password `changeit` for convenience.
> For real production regenerate with a strong password and keep the key material
> in a secret manager — never in git. See `certs/README.md`.

## Scale-out & load testing (5 order + 5 inventory + nginx)

`docker-compose.yml` also runs both services **containerised and scaled to 5
instances each**, load-balanced by nginx — the topology you'd benchmark 10k req/s
against. The apps reach the cluster over the internal SSL listeners (`kafkaN:19092`).

```
                 ┌─────────┐   /api/orders     ┌─ order-1 ─┐
   k6  ─10k/s──► │  nginx  │ ─ least_conn ───► │   ...     │ ─┐
                 │  :8088  │   /api/inventory   └─ order-5 ─┘  │  orders.created
                 └─────────┘ ─────────────────►(inventory-1..5)│  inventory.reserved
                                                               ▼
                                        5-broker Kafka (SSL) · Postgres · Redis
```

| Piece            | Detail |
|------------------|--------|
| order-1..5       | nginx upstream `order_backend` (keepalive), each an OutboxRelay sharing the table via `SELECT … FOR UPDATE SKIP LOCKED` |
| inventory-1..5   | one consumer per partition (5 partitions ⇒ 5 active across the group) |
| nginx            | `localhost:8088`, round-robins + `proxy_next_upstream` so a dead instance is skipped |
| Postgres         | `max_connections=300`; each app caps its Hikari pool at 20 (10×20 = 200) |

```bash
docker compose up -d --build          # 5 brokers + Postgres + Redis + 5+5 apps + nginx
docker compose ps                     # wait until order-* / inventory-* are healthy
./scripts/load-test.sh                # 10k req/s, 60s          (override: RATE, DURATION)
RATE=5000 DURATION=120s ./scripts/load-test.sh
```

The script uses **k6** (native, or the `grafana/k6` container if k6 isn't
installed) and after the run reports — so you can answer "ổn định không, mất
message không, chịu lỗi ra sao":

- **Throughput / latency / errors** — k6 summary (`http_reqs`, `p95`/`p99`, `http_req_failed`)
- **No message loss** — `orders.created` count ≥ HTTP-202 accepted (producer), and
  `inventory.reserved` ≥ `orders.created` (every event consumed)
- **DLQ** — `*.DLT` topics should be empty (no poison messages)
- **Drain / lag** — waits for both consumer groups' lag → 0
- **Final state** — order status distribution in Postgres; **0 PENDING** ⇒ nothing stuck

**Fault tolerance** — re-run the test and mid-flight:
```bash
docker compose stop kafka3     # broker down → RF=3 + min.insync=2 keeps producing
docker compose stop order-3    # instance down → nginx routes around it
```
The verdict should still show an empty DLQ, lag draining, and 0 PENDING — i.e. no loss.

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
