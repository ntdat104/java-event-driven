package com.example.order.service;

import com.example.events.OrderCreatedEvent;
import com.example.events.Topics;
import com.example.order.domain.Order;
import com.example.order.domain.OrderRepository;
import com.example.order.outbox.OutboxEvent;
import com.example.order.outbox.OutboxRepository;
import com.example.order.web.CreateOrderRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.tracing.Tracer;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;

    public OrderService(OrderRepository orderRepository, OutboxRepository outboxRepository,
                        ObjectMapper objectMapper, Tracer tracer,
                        KafkaTemplate<String, String> kafkaTemplate,
                        TransactionTemplate transactionTemplate) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    /**
     * Persists the order and its outbox event atomically. We do NOT publish to Kafka
     * here — publishing inside a request would reintroduce the dual-write problem
     * (DB commit succeeds, broker call fails → lost event, or vice versa). The relay
     * publishes after commit instead, giving us at-least-once with zero loss.
     */
    public Order createOrder(CreateOrderRequest request) {
        String orderId = UUID.randomUUID().toString();

        // Step 1: Thực thi lưu Order và Outbox trong cùng 1 Transaction
        transactionTemplate.execute(status -> {
            Order order = new Order(orderId, request.customerId(), request.productId(),
                    request.quantity(), request.amount());
            orderRepository.save(order);

            String eventId = UUID.randomUUID().toString();
            OrderCreatedEvent event = new OrderCreatedEvent(
                    eventId, orderId, request.customerId(), request.productId(),
                    request.quantity(), request.amount(), Instant.now());

            return outboxRepository.save(new OutboxEvent(
                    eventId,
                    "Order",
                    orderId,              // partition key
                    Topics.ORDERS_CREATED,
                    OrderCreatedEvent.class.getSimpleName(),
                    serialize(event),
                    currentTraceParent()
            ));
        });

        // Trả về thực thể Order đã tạo thành công
        return new Order(orderId, request.customerId(), request.productId(), request.quantity(), request.amount());
    }

    private String serialize(Object event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize event", e);
        }
    }

    /** Capture the active trace as a W3C traceparent so the async publish stays linked. */
    private String currentTraceParent() {
        var span = tracer.currentSpan();
        if (span == null) {
            return null;
        }
        var ctx = span.context();
        return "00-" + ctx.traceId() + "-" + ctx.spanId() + "-01";
    }
}
