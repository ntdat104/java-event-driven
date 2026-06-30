package com.example.events;

import java.time.Instant;

/**
 * Published by inventory-service after attempting to reserve stock for an order.
 * {@code success=false} means the reservation could not be fulfilled (e.g. out of
 * stock); order-service reacts by cancelling the order.
 */
public record InventoryReservedEvent(
        String eventId,
        String orderId,
        String productId,
        int quantity,
        boolean success,
        String reason,
        Instant occurredAt
) {
}
