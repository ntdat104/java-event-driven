package com.example.order.inbox;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;

/**
 * Inbox row: the authoritative record that an event has been processed. Inserted in
 * the SAME transaction as the business change, so a crash can never leave us "marked
 * done but not actually done". The primary-key uniqueness gives crash-safe,
 * concurrency-safe exactly-once *effect* on local state.
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
