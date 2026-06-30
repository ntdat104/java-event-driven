package com.example.inventory.outbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Transactional outbox for the inventory.reserved outcome event. Written in the same
 * transaction as the reservation + inbox row, so the outcome can never be lost even if
 * the broker is briefly unreachable — the relay retries until the ack lands.
 */
@Entity
@Table(name = "outbox_event", indexes = {
        @Index(name = "idx_outbox_status_created", columnList = "status,createdAt")
})
public class OutboxEvent {

    @Id
    private String id; // == eventId, also the downstream idempotency key

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId; // partition key

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String eventType;

    @Lob
    @Column(nullable = false)
    private String payload;

    private String traceParent;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private OutboxStatus status;

    @Column(nullable = false)
    private Instant createdAt;

    private Instant publishedAt;

    private int attempts;

    protected OutboxEvent() {
    }

    public OutboxEvent(String id, String aggregateType, String aggregateId, String topic, String eventType,
                       String payload, String traceParent) {
        this.id = id;
        this.aggregateType = aggregateType;
        this.aggregateId = aggregateId;
        this.topic = topic;
        this.eventType = eventType;
        this.payload = payload;
        this.traceParent = traceParent;
        this.status = OutboxStatus.PENDING;
        this.createdAt = Instant.now();
    }

    public void markPending() {
        this.status = OutboxStatus.PENDING;
    }

    public void markProcessing() {
        this.status = OutboxStatus.PROCESSING;
    }

    public void markPublished() {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = Instant.now();
    }

    public void recordAttempt() {
        this.attempts++;
    }

    public String getId() {
        return id;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayload() {
        return payload;
    }

    public String getTraceParent() {
        return traceParent;
    }
}
