package com.example.inventory.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Inbox row — authoritative dedup record, inserted in the same transaction as the
 * stock reservation and the outbox write. PK uniqueness ⇒ exactly-once effect.
 */
@Entity
@Table(name = "processed_event")
public class ProcessedEvent {

    @Id
    private String eventId;

    @Column(nullable = false)
    private Instant processedAt;

    protected ProcessedEvent() {
    }

    public ProcessedEvent(String eventId) {
        this.eventId = eventId;
        this.processedAt = Instant.now();
    }
}
