package com.example.order.outbox;

import jakarta.persistence.LockModeType;
import java.util.List;

import jakarta.persistence.QueryHint;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.QueryHints;

public interface OutboxRepository extends JpaRepository<OutboxEvent, String> {

    /**
     * Bạn vẫn giữ nguyên @Lock(LockModeType.PESSIMISTIC_WRITE), nhưng phải cấu hình thêm thuộc tính SKIP LOCKED (Bỏ qua những dòng đã bị máy khác khóa).
     */

    /**
     * Claims a batch of pending rows. PESSIMISTIC_WRITE + skip-locked semantics let
     * multiple relay instances poll concurrently without handing the same row twice.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @QueryHints({@QueryHint(name = "jakarta.persistence.lock.timeout", value = "-2")}) // Giá trị "-2" trong Hibernate tương đương với cú pháp "SKIP LOCKED" của SQL dưới hạ tầng
    @Query("select o from OutboxEvent o where o.status = :status order by o.createdAt asc")
    List<OutboxEvent> findPendingBatch(Limit limit, OutboxStatus status);
}
