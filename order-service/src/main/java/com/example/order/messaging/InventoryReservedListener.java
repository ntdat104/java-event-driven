package com.example.order.messaging;

import com.example.events.InventoryReservedEvent;
import com.example.events.Topics;
import com.example.order.domain.Order;
import com.example.order.domain.OrderRepository;
import com.example.order.service.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Reacts to the inventory decision and finalises the order.
 *
 * <p>One record at a time. The container commits the offset once per poll
 * (AckMode.BATCH) after this method returns — no manual ack. On any exception we
 * let it propagate so DefaultErrorHandler retries the record and finally routes
 * it to the DLT; a crash before commit simply redelivers (handled idempotently).
 */
@Component
public class InventoryReservedListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservedListener.class);

    private final OrderRepository orderRepository;
    private final IdempotencyService idempotency;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate; // explicit, short transaction boundary

    public InventoryReservedListener(OrderRepository orderRepository, IdempotencyService idempotency,
                                     ObjectMapper objectMapper, TransactionTemplate transactionTemplate) {
        this.orderRepository = orderRepository;
        this.idempotency = idempotency;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
    public void onInventoryReserved(@Payload String payload) throws Exception {
        InventoryReservedEvent event = objectMapper.readValue(payload, InventoryReservedEvent.class);

        transactionTemplate.executeWithoutResult(status -> {
            // Inbox dedupe (writes the eventId row inside this transaction).
            if (!idempotency.shouldProcess(event.eventId())) {
                log.debug("Duplicate event {} ignored", event.eventId());
                return;
            }

            // Lock the row to avoid a race on the status update.
            Order order = orderRepository.findByIdForUpdate(event.orderId()).orElse(null);
            if (order == null) {
                log.warn("Order {} not found for inventory event", event.orderId());
                return;
            }

            if (event.success()) {
                order.markConfirmed();
            } else {
                order.markCancelled();
                log.info("Order {} cancelled: {}", order.getId(), event.reason());
            }
            orderRepository.save(order);
        });
    }
}
