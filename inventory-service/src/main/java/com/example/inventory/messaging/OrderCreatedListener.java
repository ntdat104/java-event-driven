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
import java.time.Instant;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Exactly-once processing in ONE local transaction:
 *   inbox(eventId) + reserve stock + outbox(inventory.reserved).
 * The relay publishes the outcome afterwards.
 *
 * <p>One record at a time. The container commits the offset once per poll
 * (AckMode.BATCH) after this method returns — no manual ack. On any exception we
 * let it propagate so DefaultErrorHandler retries the record and finally routes
 * it to the DLT; redelivery is safe because we dedupe on {@code eventId}.
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

    public OrderCreatedListener(InventoryRepository inventoryRepository, OutboxRepository outboxRepository,
                                IdempotencyService idempotency, ObjectMapper objectMapper, Tracer tracer,
                                TransactionTemplate transactionTemplate) {
        this.inventoryRepository = inventoryRepository;
        this.outboxRepository = outboxRepository;
        this.idempotency = idempotency;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.transactionTemplate = transactionTemplate;
    }

    @KafkaListener(topics = Topics.ORDERS_CREATED, groupId = "inventory-service")
    public void onOrderCreated(@Payload String payload) throws Exception {
        OrderCreatedEvent event = objectMapper.readValue(payload, OrderCreatedEvent.class);

        // All DB writes (inbox + stock + outbox) in one short local transaction.
        transactionTemplate.executeWithoutResult(status -> {
            if (!idempotency.shouldProcess(event.eventId())) {
                log.debug("Duplicate event {} detected", event.eventId());
                return;
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

            outboxRepository.save(new OutboxEvent(
                    result.eventId(),
                    "Inventory",
                    event.orderId(),                     // partition key (aggregateId)
                    Topics.INVENTORY_RESERVED,
                    InventoryReservedEvent.class.getSimpleName(),
                    serialize(result),
                    currentTraceParent()));
        });
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
