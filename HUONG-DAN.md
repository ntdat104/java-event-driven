# Hướng dẫn chạy từ A → Z

Hệ thống event-driven: **order-service** ↔ **inventory-service** qua Kafka, với
cụm **5 broker chạy SSL/mTLS**, **5 instance mỗi service** sau **nginx**, và bộ
**load-test k6 ~10k req/s** có kiểm tra mất message / độ trễ / chịu lỗi.

> Toàn bộ lệnh chạy ở thư mục gốc repo: `java-event-driven/`.

---

## 0. Yêu cầu

| Cần có | Dùng để | Bắt buộc? |
|--------|---------|-----------|
| **Docker** + Docker Compose v2 | chạy cụm Kafka, Postgres, Redis, 5+5 app, nginx | ✅ |
| **JDK 21** + Maven | chỉ khi muốn chạy app trên host (chế độ dev, Phần 9) | tuỳ chọn |
| **k6** | load-test (không có cũng được — script tự dùng container `grafana/k6`) | tuỳ chọn |
| `keytool`, `openssl` | sinh chứng chỉ (có sẵn trong JDK + hệ điều hành) | ✅ |

⚠️ **Tài nguyên:** chạy **5 broker + 10 JVM app** cùng lúc rất nặng CPU. Để đạt
**10k req/s thật** cần máy **nhiều core** (khuyến nghị ≥ 16 vCPU, ≥ 16GB RAM).
Máy ít core (vd 8) vẫn chạy được nhưng throughput thực tế sẽ thấp do nghẽn CPU.

---

## 1. Sơ đồ & cổng

```
   k6 ─10k/s─► nginx :8088 ─┬─ /api/orders ───► order-1..5  ─┐
                            └─ /api/inventory ─► inventory-1..5│
                                                              ▼
                        Kafka (5 broker, SSL/mTLS) · Postgres · Redis
```

| Cổng (host) | Dịch vụ |
|-------------|---------|
| **8088** | **nginx** — đích bắn tải: `http://localhost:8088/api/orders` |
| 9092 / 9192 / 9292 / 9392 / 9492 | 5 Kafka broker (SSL) — chỉ cần khi chạy app trên host |
| 8080 | Kafka-UI (xem topic, consumer lag, DLQ) |
| 3000 | Grafana (dashboard) — user ẩn danh, quyền admin |
| 9090 | Prometheus |
| 5432 / 6379 | Postgres / Redis |

---

## 2. Sinh chứng chỉ Kafka SSL/mTLS (chạy 1 lần)

Cụm Kafka chạy SSL nên phải tạo keystore/truststore trước:

```bash
./scripts/generate-kafka-certs.sh
```

Lệnh này tạo thư mục `certs/` (CA + keystore/truststore cho broker và client,
mật khẩu demo `changeit`). SAN của cert đã phủ `localhost`, `kafka1`..`kafka5`,
`127.0.0.1`.

> 🔐 **Production:** sinh lại với mật khẩu mạnh và hostname thật, rồi cất key vào
> secret manager — **đừng commit vào git**:
> ```bash
> KAFKA_CERT_PASSWORD='matkhau-manh' \
> KAFKA_BROKER_SAN="DNS:broker1.prod,DNS:broker2.prod,IP:10.0.0.5" \
>   ./scripts/generate-kafka-certs.sh
> ```

---

## 3. Dựng toàn bộ hệ thống

```bash
docker compose up -d --build
```

- Lần đầu sẽ **build 2 image** (Maven build bên trong Docker) — mất vài phút.
- Khởi động: 5 broker → Postgres/Redis → 5 order + 5 inventory → nginx +
  observability.

Kiểm tra trạng thái (đợi tới khi `order-*` / `inventory-*` đều **healthy**):

```bash
docker compose ps
```

> ⏳ Trên máy ít core, app có thể mất **1–3 phút** để `healthy` (do 10 JVM khởi
> động cùng lúc tranh CPU). Đợi tới khi cột STATUS hiện `(healthy)` rồi mới test.

Xem log một instance nếu cần:
```bash
docker compose logs -f order-1
```

---

## 4. Smoke test (kiểm tra nhanh 1 đơn hàng)

```bash
# Tạo 1 order qua nginx
curl -s -XPOST localhost:8088/api/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"c1","productId":"p1","quantity":1,"amount":9.99}'
# → {"orderId":"<uuid>","status":"PENDING"}
```

Lấy `orderId` ở trên rồi kiểm tra — sau ~1s phải chuyển sang **CONFIRMED**
(order → orders.created → inventory reserve → inventory.reserved → order xác nhận):

```bash
curl -s localhost:8088/api/orders/<orderId>      # status: CONFIRMED
curl -s localhost:8088/api/inventory/p1          # tồn kho đã giảm
```

---

## 5. Load test ~10k req/s + báo cáo

```bash
./scripts/load-test.sh                       # mặc định: 10000 req/s trong 60s
```

Tuỳ chỉnh bằng biến môi trường:
```bash
RATE=5000 DURATION=120s ./scripts/load-test.sh      # 5k req/s trong 2 phút
RATE=10000 DURATION=30s PRODUCTS=200 ./scripts/load-test.sh
```

| Biến | Ý nghĩa | Mặc định |
|------|---------|----------|
| `RATE` | số request/giây mục tiêu | `10000` |
| `DURATION` | thời lượng bắn tải | `60s` |
| `PRODUCTS` | số mã sản phẩm (trải key để không cạn tồn kho) | `200` |
| `TARGET` | URL đích | `http://localhost:8088/api/orders` |
| `VUS` / `MAX_VUS` | số virtual user k6 cấp phát | `800` / `4000` |

Script dùng **k6** (native nếu có, không thì container `grafana/k6`), rồi **chờ
pipeline drain** (consumer lag → 0 **và** outbox backlog → 0) và in báo cáo:

```
HTTP 202 accepted (k6)      : 60000
orders.created   (produced) : 60000     ← so với accepted ⇒ producer không mất
inventory.reserved          : 60000     ← so với orders.created ⇒ consumer xử lý hết
outbox backlog (unpublished): 0
DLQ orders.created.DLT      : 0
DLQ inventory.reserved.DLT  : 0
---- verdict ----
  ✓ no producer loss        ✓ every event consumed
  ✓ DLQ empty               ✓ outbox drained
```

### Đọc kết quả trả lời câu hỏi gì
- **Ổn định / tốc độ / độ trễ** → phần k6 summary: `http_reqs` (req/s thực tế),
  `http_req_duration` (avg, p95, p99), `http_req_failed` (tỷ lệ lỗi).
- **Có mất message không** → `orders.created` ≥ accepted (không mất phía gửi) và
  `inventory.reserved` ≥ `orders.created` (consumer xử lý hết); `outbox backlog`
  và 2 topic `*.DLT` đều = 0.
- **Chịu lỗi** → xem Phần 6.

---

## 6. Test khả năng chịu lỗi (fault tolerance)

Mở 1 terminal bắn tải dài, terminal khác gây sự cố giữa chừng:

```bash
# Terminal 1
RATE=3000 DURATION=120s ./scripts/load-test.sh

# Terminal 2 — chọn 1 trong 2:
docker compose stop kafka3      # 1 broker chết → RF=3 + min.insync=2 vẫn ghi được
docker compose stop order-3     # 1 instance app chết → nginx tự định tuyến vòng qua
```

Sau đó bật lại và để pipeline hồi phục:
```bash
docker compose start kafka3 order-3
```

**Kỳ vọng:** báo cáo cuối vẫn `DLQ empty`, `outbox drained`, lag về 0 ⇒ **không
mất message** dù có sự cố (nhờ acks=all, RF=3, outbox + dedup theo eventId).

---

## 7. Quan sát (observability)

| Công cụ | URL | Xem gì |
|---------|-----|--------|
| **Grafana** | http://localhost:3000 | Dashboard *Event-Driven*: RED metrics, JVM, Kafka pipeline; click exemplar để nhảy sang trace |
| **Kafka-UI** | http://localhost:8080 | Topic, **consumer lag**, nội dung **DLQ** |
| Prometheus | http://localhost:9090 | Query metric thô |

---

## 8. Tinh chỉnh hiệu năng

| Muốn gì | Sửa ở đâu |
|---------|-----------|
| Tăng song song consumer | `app.topics.partitions` trong cả 2 `application.yml` (vd 12) **và** tăng partition topic trên broker. Consumer chạy 1-event-một-lần, song song tối đa = số partition. |
| Throughput relay (gửi) | `app.outbox.batch-size` (mặc định 500) + `app.outbox.poll-interval-ms` (mặc định 50ms) |
| Số instance | Thêm/bớt service `order-N` / `inventory-N` trong `docker-compose.yml` (nhớ cập nhật upstream trong `nginx/nginx.conf`) |
| Bộ nhớ mỗi app | `JAVA_OPTS` trong anchor `x-app-env` (mặc định `-Xms256m -Xmx512m`) |
| SSL keystore/mật khẩu | biến `KAFKA_SSL_*` trong `x-app-env` (mặc định trỏ `certs/`, mật khẩu `changeit`) |

Chi tiết best-practice 10k req/s (vì sao `AckMode.BATCH`, `max.in.flight=5`,
idempotence…) xem `README.md`.

---

## 9. (Tuỳ chọn) Chạy app trên host bằng Maven — chế độ dev

Nếu muốn debug/sửa code nhanh thay vì container hoá:

```bash
# 1. Chỉ dựng hạ tầng (5 broker SSL + Postgres + Redis + observability), KHÔNG bật app/nginx
docker compose up -d kafka1 kafka2 kafka3 kafka4 kafka5 postgres redis \
  otel-collector tempo loki prometheus grafana kafka-ui

# 2. Chạy từng service trên host (mỗi cái 1 terminal). App tự đọc cert ở ./certs
#    và kết nối các broker qua cổng SSL trên localhost (9092..9492).
mvn -s settings-local.xml -pl order-service     spring-boot:run     # :8081
mvn -s settings-local.xml -pl inventory-service spring-boot:run     # :8082
```

Khi đó bắn tải thẳng vào order-service (không qua nginx):
```bash
TARGET=http://localhost:8081/api/orders RATE=2000 DURATION=30s ./scripts/load-test.sh
```

> `pom.xml` đã đặt `workingDirectory` của `spring-boot:run` về thư mục gốc repo
> nên `file:certs/...` luôn resolve đúng dù chạy module nào.

---

## 10. Dọn dẹp & reset

```bash
docker compose down              # dừng & xoá container (giữ dữ liệu)
docker compose down -v           # xoá luôn volume Kafka
```

> ⚠️ Postgres dùng **bind-mount** `./data/postgres` nên `down -v` **KHÔNG** xoá
> dữ liệu đơn hàng. Muốn sạch hoàn toàn (đếm message từ 0, không còn order cũ):
> ```bash
> docker compose down -v && sudo rm -rf data/
> ```

---

## 11. Khắc phục sự cố

| Triệu chứng | Nguyên nhân & cách xử lý |
|-------------|--------------------------|
| nginx trả **502 Bad Gateway** | App chưa `healthy` (đang khởi động). Đợi `docker compose ps` báo healthy rồi thử lại. |
| App khởi động **rất chậm** / `unhealthy` chập chờn | Thiếu CPU (10 JVM + 5 broker). Giảm số instance, hoặc tắt observability: `docker compose stop grafana tempo loki prometheus kafka-ui otel-collector`. |
| Throughput thấp hơn nhiều so với `RATE` | Server nghẽn (thường là CPU). Xem `docker stats`. Cần máy mạnh hơn cho 10k thật. |
| k6 báo `permission denied` khi ghi summary | Đã xử lý — script ghi `load-test-summary.json` qua stdout của host, không mount vào container k6. |
| Còn vài order **PENDING** dù lag = 0 | Có thể là order cũ còn sót trong `./data/postgres` từ lần test trước. Reset bằng lệnh ở Phần 10. |
| App không kết nối được Kafka (SSL) | Chưa sinh cert (Phần 2), hoặc cert SAN không khớp hostname broker. Sinh lại cert. |
| Build image lỗi mạng (Maven) | Cần mạng để tải dependency lần đầu. Thử lại `docker compose build`. |

---

## Tóm tắt nhanh (TL;DR)

```bash
./scripts/generate-kafka-certs.sh     # 1. sinh cert (1 lần)
docker compose up -d --build          # 2. dựng cụm + 5+5 app + nginx
docker compose ps                     # 3. đợi tất cả healthy
./scripts/load-test.sh                # 4. bắn 10k req/s + báo cáo mất message/độ trễ/DLQ
```
