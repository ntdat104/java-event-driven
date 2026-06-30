package com.example.order.outbox;

public enum OutboxStatus {
    PENDING,
    PROCESSING,
    PUBLISHED
}
