package com.example.order.outbox;

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
 * Transactional outbox row. Written in the SAME local transaction as the business
 * state change, then asynchronously relayed to Kafka by {@link OutboxRelay}. This
 * removes the dual-write problem: either both the order and its event are committed,
 * or neither is — so a crash can never lose an event.
 */
@Entity
@Table(name = "outbox_event", indexes = {
        @Index(name = "idx_outbox_status_created", columnList = "status,createdAt")
})
public class OutboxEvent {

    @Id
    private String id; // == eventId, also the idempotency key downstream

    @Column(nullable = false)
    private String aggregateType;

    @Column(nullable = false)
    private String aggregateId; // partition key → preserves per-aggregate ordering

    @Column(nullable = false)
    private String topic;

    @Column(nullable = false)
    private String eventType;

    @Lob
    @Column(nullable = false)
    private String payload; // JSON

    /** W3C traceparent captured at enqueue time so the trace spans the async hop. */
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

    public OutboxEvent(String id, String aggregateType, String aggregateId, String topic,
                       String eventType, String payload, String traceParent) {
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

    public int getAttempts() {
        return attempts;
    }
}
