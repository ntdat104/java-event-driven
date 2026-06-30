package com.example.inventory.outbox;

import jakarta.persistence.LockModeType;
import java.util.List;

import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")}) // Giá trị "-2" trong Hibernate tương đương với cú pháp "SKIP LOCKED" của SQL dưới hạ tầng
    @Query("select o from OutboxEvent o where o.status = :status order by o.createdAt asc")
    List<OutboxEvent> findPendingBatch(Limit limit, OutboxStatus status);
}
