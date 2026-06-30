package com.example.inventory.outbox;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    PUBLISHED
}
