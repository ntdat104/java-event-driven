package com.example.events;

/**
 * Central registry of topic names so producer and consumer cannot drift.
 * DLQ topics follow the convention {@code <topic>.DLT} (default of
 * Spring Kafka's DeadLetterPublishingRecorder).
 */
public final class Topics {

    private Topics() {
    }

    /** Emitted by order-service after an order is persisted. */
    public static final String ORDERS_CREATED = "orders.created";

    /** Emitted by inventory-service after attempting to reserve stock. */
    public static final String INVENTORY_RESERVED = "inventory.reserved";
}
