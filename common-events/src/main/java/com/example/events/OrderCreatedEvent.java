package com.example.events;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Published when a new order has been accepted and persisted.
 *
 * <p>{@code eventId} is the idempotency key: consumers dedupe on it so that
 * at-least-once redelivery does not cause double processing.
 */
public record OrderCreatedEvent(
        String eventId,
        String orderId,
        String customerId,
        String productId,
        int quantity,
        BigDecimal amount,
        Instant occurredAt
) {
}
