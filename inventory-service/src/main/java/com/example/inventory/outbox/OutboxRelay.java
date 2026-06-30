package com.example.inventory.outbox;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    // How many pending rows to drain per tick. At a 50ms poll interval, 500 rows
    // ⇒ ~10k events/s of headroom per instance. Tune with app.outbox.batch-size.
    @Value("${app.outbox.batch-size:500}")
    private int batchSize;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    // Sử dụng TransactionTemplate thay vì @Transactional ở tầng hàm để kiểm soát biên (boundary) transaction cụ thể hơn
    private final TransactionTemplate transactionTemplate;

    public OutboxRelay(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate, TransactionTemplate transactionTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = transactionTemplate;
    }

    //    @Transactional
    @Scheduled(fixedDelayString = "${app.outbox.poll-interval-ms:100}")
    public void relay() {
        // STEP 1: Đọc và Lock dữ liệu trong DB thật nhanh, sau đó COMMIT luôn
        // Cần đảm bảo hàm repository.fetchAndLockBatch() sẽ cập nhật trạng thái từ PENDING -> PROCESSING
        List<OutboxEvent> batch = transactionTemplate.execute(status -> {
            List<OutboxEvent> pending = outboxRepository.findPendingBatch(Limit.of(batchSize), OutboxStatus.PENDING);
            if (pending.isEmpty()) {
                return Collections.emptyList();
            }
            pending.forEach(OutboxEvent::markProcessing); // Đổi trạng thái tạm thời để luồng khác không đọc trùng
            outboxRepository.saveAll(pending);
            return pending;
        }); // Kết thúc Transaction 1 tại đây. DB Connection được giải phóng!

        if (batch == null || batch.isEmpty()) {
            return;
        }

        // STEP 2: Thực hiện gọi mạng (I/O) sang Kafka - Hoàn toàn nằm ngoài Transaction
        var futures = batch.stream().map(event -> {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    event.getTopic(), null, event.getAggregateId(), event.getPayload());
            record.headers().add(new RecordHeader("eventId", event.getId().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("eventType", event.getEventType().getBytes(StandardCharsets.UTF_8)));
            if (event.getTraceParent() != null) {
                record.headers().add(new RecordHeader("traceparent", event.getTraceParent().getBytes(StandardCharsets.UTF_8)));
            }
            event.recordAttempt();

            return kafkaTemplate.send(record)
                    .thenApply(result -> {
                        event.markPublished(); // Đánh dấu thành công trong bộ nhớ
                        return event;
                    })
                    .exceptionally(ex -> {
                        log.warn("Kafka publish failed for event {}", event.getId(), ex);
                        event.markPending(); // Thất bại thì trả về PENDING trong bộ nhớ để gom lại ở tick sau
                        return event;
                    });
        }).toList();

        // Chờ Kafka phản hồi xong xuôi cho cả lô (vẫn nằm ngoài DB Transaction)
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // STEP 3: Mở một Transaction mới chỉ để cập nhật trạng thái cuối cùng vào DB
        transactionTemplate.executeWithoutResult(status -> {
            outboxRepository.saveAll(batch);
        }); // Kết thúc Transaction 2 cực nhanh.

        log.debug("Relayed {} outbox events", batch.size());
    }
}
