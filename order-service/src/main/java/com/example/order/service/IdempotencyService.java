package com.example.order.service;

import com.example.order.inbox.ProcessedEvent;
import com.example.order.inbox.ProcessedEventRepository;
import java.time.Duration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

@Service
public class IdempotencyService {

    private static final Duration DB_CACHE_TTL = Duration.ofDays(2);
    private static final Duration LOCK_TTL = Duration.ofMinutes(2);

    private static final String PREFIX = "idem:order:";
    private static final String STATUS_PROCESSING = "PROCESSING";
    private static final String STATUS_COMMITTED = "COMMITTED";

    private final StringRedisTemplate redis;
    private final ProcessedEventRepository inbox;

    public IdempotencyService(StringRedisTemplate redis, ProcessedEventRepository inbox) {
        this.redis = redis;
        this.inbox = inbox;
    }

    public boolean shouldProcess(String eventId) {
        String key = PREFIX + eventId;

        // BƯỚC 1: Đặt khóa phân tán nguyên tử
        Boolean acquiredLock = redis.opsForValue().setIfAbsent(key, STATUS_PROCESSING, LOCK_TTL);

        if (Boolean.FALSE.equals(acquiredLock)) {
            String currentStatus = redis.opsForValue().get(key);

            if (STATUS_COMMITTED.equals(currentStatus)) {
                return false;
            }

            if (inbox.existsById(eventId)) {
                cacheCommitted(key);
                return false;
            }

            return false; // Nhánh 1C: Đang có instance khác bận xử lý, bỏ qua chờ retry
        }

        // BƯỚC 2: Check DB cứu cánh (Nếu trùng, phải ghi đè 'COMMITTED' ngay để nhả lock 'PROCESSING')
        if (inbox.existsById(eventId)) {
            cacheCommitted(key); // Chuyển ngay sang trạng thái COMMITTED dài hạn để giải phóng trạng thái PROCESSING tạm thời
            return false;
        }

        // BƯỚC 3: Tin nhắn thực sự mới! Lưu vào DB Inbox
        inbox.save(new ProcessedEvent(eventId));

        // BƯỚC 4: Đăng ký đổi trạng thái sang COMMITTED dài hạn sau khi DB commit thực sự thành công
        updateCacheAfterCommit(key);

        return true;
    }

    private void cacheCommitted(String key) {
        redis.opsForValue().set(key, STATUS_COMMITTED, DB_CACHE_TTL);
    }

    private void updateCacheAfterCommit(String key) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    cacheCommitted(key);
                }

                @Override
                public void afterCompletion(int status) {
                    if (status == STATUS_ROLLED_BACK) {
                        redis.delete(key); // Rollback thì xóa sạch để Kafka redeliver làm lại luôn
                    }
                }
            });
        } else {
            cacheCommitted(key);
        }
    }
}