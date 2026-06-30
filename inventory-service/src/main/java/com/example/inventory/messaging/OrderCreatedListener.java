package com.example.inventory.messaging;

import com.example.events.InventoryReservedEvent;
import com.example.events.OrderCreatedEvent;
import com.example.events.Topics;
import com.example.inventory.domain.Inventory;
import com.example.inventory.domain.InventoryRepository;
import com.example.inventory.outbox.OutboxEvent;
import com.example.inventory.outbox.OutboxRepository;
import com.example.inventory.service.IdempotencyService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Exactly-once processing in ONE local transaction:
 *   inbox(eventId) + reserve stock + outbox(inventory.reserved).
 * The relay publishes the outcome afterwards. Manual ack commits the offset only
 * after the transaction succeeds; otherwise DefaultErrorHandler retries → DLT.
 */
@Component
public class OrderCreatedListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedListener.class);
    private static final int DEFAULT_STOCK = 100000;

    private final InventoryRepository inventoryRepository;
    private final OutboxRepository outboxRepository;
    private final IdempotencyService idempotency;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final TransactionTemplate transactionTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrderCreatedListener(InventoryRepository inventoryRepository, OutboxRepository outboxRepository,
                                IdempotencyService idempotency, ObjectMapper objectMapper, Tracer tracer,
                                TransactionTemplate transactionTemplate, KafkaTemplate<String, String> kafkaTemplate) {
        this.inventoryRepository = inventoryRepository;
        this.outboxRepository = outboxRepository;
        this.idempotency = idempotency;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.transactionTemplate = transactionTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

//    @KafkaListener(
//            topics = Topics.ORDERS_CREATED,
//            groupId = "inventory-service",
//            containerFactory = "kafkaBatchListenerContainerFactory"
//    )
//    public void onOrdersCreatedBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
//        log.info("Nhận một batch gồm {} tin nhắn từ topic {}", records.size(), Topics.ORDERS_CREATED);
//
//        try {
//            // STEP 1: Xử lý ghi DB toàn bộ Lô (Kho + Outbox) tập trung trong ĐÚNG 1 Local Transaction duy nhất
//            transactionTemplate.executeWithoutResult(status -> {
//                List<OutboxEvent> outboxEventsToSave = new ArrayList<>();
//
//                for (ConsumerRecord<String, String> record : records) {
//                    try {
//                        OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
//
//                        // Kiểm tra trùng lặp tin nhắn (Idempotency)
//                        if (!idempotency.shouldProcess(event.eventId())) {
//                            log.debug("Phát hiện event trùng lặp {}, bỏ qua không xử lý", event.eventId());
//                            continue;
//                        }
//
//                        // Cập nhật kho hàng
//                        Inventory inventory = inventoryRepository.findById(event.productId())
//                                .orElseGet(() -> new Inventory(event.productId(), DEFAULT_STOCK));
//                        boolean reserved = inventory.tryReserve(event.quantity());
//                        inventoryRepository.save(inventory);
//
//                        // Khởi tạo sự kiện phản hồi
//                        InventoryReservedEvent result = new InventoryReservedEvent(
//                                UUID.randomUUID().toString(),
//                                event.orderId(),
//                                event.productId(),
//                                event.quantity(),
//                                reserved,
//                                reserved ? null : "INSUFFICIENT_STOCK",
//                                Instant.now());
//
//                        // Trích xuất traceparent từ Kafka Header nếu có để duy trì Distributed Tracing
//                        String traceParentStr = null;
//                        var traceHeader = record.headers().lastHeader("traceparent");
//                        if (traceHeader != null) {
//                            traceParentStr = new String(traceHeader.value(), java.nio.charset.StandardCharsets.UTF_8);
//                        }
//
//                        // Gom vào danh sách để thực hiện batch insert outbox xuống DB
//                        outboxEventsToSave.add(new OutboxEvent(
//                                result.eventId(),
//                                "Inventory",
//                                event.orderId(), // partition key
//                                Topics.INVENTORY_RESERVED,
//                                InventoryReservedEvent.class.getSimpleName(),
//                                serialize(result),
//                                traceParentStr != null ? traceParentStr : currentTraceParent()
//                        ));
//
//                    } catch (Exception e) {
//                        // Tùy chọn Production: Ghi nhận log lỗi của dòng dữ liệu sai định dạng
//                        // nhưng không quăng RuntimeException ra ngoài để tránh làm hỏng việc xử lý các dòng đúng khác trong lô
//                        log.error("Lỗi phân tích cú pháp hoặc xử lý dòng record: {}", record.offset(), e);
//                    }
//                }
//
//                // Thực hiện Batch Insert xuống PostgreSQL (Sẽ kích hoạt jdbc.batch_size=50 như cấu hình của bạn)
//                if (!outboxEventsToSave.isEmpty()) {
//                    outboxRepository.saveAll(outboxEventsToSave);
//                }
//            }); // KẾT THÚC TRANSACTION CHÍNH ĐỂ GIẢI PHÓNG DB CONNECTION NHANH NHẤT
//
//            // STEP 2: Khi cả lô đã lưu DB thành công, tiến hành Ack cho Kafka chuyển Offset lên.
//            // Việc đẩy ngược sang Kafka topic INVENTORY_RESERVED sẽ do Worker ngầm OutboxRelay đảm nhiệm một cách mượt mà độc lập.
//            ack.acknowledge();
//            log.debug("Đã xử lý và Ack thành công cho batch gồm {} bản ghi", records.size());
//
//        } catch (Exception e) {
//            log.error("Lỗi nghiêm trọng khi xử lý Batch Transaction. Toàn bộ lô sẽ bị rollback và thử lại.", e);
//            // Để mặc định cho ErrorHandler của Spring Kafka lo liệu (Retries -> DLT)
//            throw e;
//        }
//    }

    @KafkaListener(topics = Topics.ORDERS_CREATED, groupId = "inventory-service")
    public void onOrderCreated(@Payload String payload, Acknowledgment ack) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // STEP 1: Xử lý ghi DB (Kho + Outbox) tập trung trong 1 transaction chính
        transactionTemplate.execute(status -> {
            if (!idempotency.shouldProcess(event.eventId())) {
                log.debug("Duplicate event {} detected", event.eventId());
                return null;
            }

            Inventory inventory = inventoryRepository.findById(event.productId())
                    .orElseGet(() -> new Inventory(event.productId(), DEFAULT_STOCK));
            boolean reserved = inventory.tryReserve(event.quantity());
            inventoryRepository.save(inventory);

            InventoryReservedEvent result = new InventoryReservedEvent(
                    UUID.randomUUID().toString(),
                    event.orderId(),
                    event.productId(),
                    event.quantity(),
                    reserved,
                    reserved ? null : "INSUFFICIENT_STOCK",
                    Instant.now());

            return outboxRepository.save(new OutboxEvent(
                    result.eventId(),
                    "Inventory",                         // Thêm aggregateType nếu constructor yêu cầu
                    event.orderId(),                     // partition key (aggregateId)
                    Topics.INVENTORY_RESERVED,
                    InventoryReservedEvent.class.getSimpleName(),
                    serialize(result),
                    currentTraceParent()));
        });

        ack.acknowledge();
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize event", e);
        }
    }

    private String currentTraceParent() {
        var span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        var ctx = span.context();
        return "00-" + ctx.traceId() + "-" + ctx.spanId() + "-01";
    }
}
