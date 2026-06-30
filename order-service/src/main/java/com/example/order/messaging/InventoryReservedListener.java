package com.example.order.messaging;

import com.example.events.InventoryReservedEvent;
import com.example.events.Topics;
import com.example.order.domain.Order;
import com.example.order.domain.OrderRepository;
import com.example.order.service.IdempotencyService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;

/**
 * Reacts to the inventory decision and finalises the order. Manual ack: we only
 * commit the offset after the DB update succeeds, so a crash before commit just
 * redelivers the event (handled idempotently).
 */
@Component
public class InventoryReservedListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservedListener.class);

    private final OrderRepository orderRepository;
    private final IdempotencyService idempotency;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate; // Dùng cụ thể để kiểm soát biên Transaction

    public InventoryReservedListener(OrderRepository orderRepository, IdempotencyService idempotency,
                                     ObjectMapper objectMapper, TransactionTemplate transactionTemplate) {
        this.orderRepository = orderRepository;
        this.idempotency = idempotency;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
    }

//    @KafkaListener(
//            topics = Topics.INVENTORY_RESERVED,
//            groupId = "order-service",
//            containerFactory = "kafkaBatchListenerContainerFactory"
//    )
//    public void onInventoryReservedBatch(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
//        log.info("Order-Service nhận một batch gồm {} tin nhắn từ topic {}", records.size(), Topics.INVENTORY_RESERVED);
//
//        try {
//            // STEP 1: Mở ĐÚNG một Transaction local duy nhất để xử lý toàn bộ lô dữ liệu
//            transactionTemplate.executeWithoutResult(status -> {
//
//                for (ConsumerRecord<String, String> record : records) {
//                    try {
//                        InventoryReservedEvent event = objectMapper.readValue(record.value(), InventoryReservedEvent.class);
//
//                        // 1. Kiểm tra trùng lặp (Idempotency / Inbox Pattern)
//                        if (!idempotency.shouldProcess(event.eventId())) {
//                            log.debug("Phát hiện event trùng lặp {}, bỏ qua", event.eventId());
//                            continue;
//                        }
//
//                        // 2. Tìm Order và Lock dòng (Pessimistic Lock) để tránh race condition khi cập nhật trạng thái
//                        Order order = orderRepository.findByIdForUpdate(event.orderId()).orElse(null);
//                        if (order == null) {
//                            log.warn("Không tìm thấy Order {} cho inventory event", event.orderId());
//                            continue;
//                        }
//
//                        // 3. Cập nhật trạng thái nghiệp vụ theo kết quả từ kho
//                        if (event.success()) {
//                            order.markConfirmed();
//                            log.debug("Order {} đã được CONFIRMED thành công", order.getId());
//                        } else {
//                            order.markCancelled();
//                            log.info("Order {} bị CANCELLED. Lý do: {}", order.getId(), event.reason());
//                        }
//
//                        // Lưu lại trạng thái mới (Lệnh này sẽ được Hibernate gom batch update tối ưu nhờ cấu hình ngầm)
//                        orderRepository.save(order);
//
//                    } catch (Exception e) {
//                        // Bắt lỗi riêng từng dòng lỗi định dạng (nếu có) để tránh làm sập (rollback) cả lô tin nhắn tốt
//                        log.error("Lỗi xử lý record tại offset: {}", record.offset(), e);
//                    }
//                }
//
//            }); // TRANSACTION kết thúc và COMMIT đồng loạt tại đây! Toàn bộ Locks trên DB được giải phóng cùng một lúc.
//
//            // STEP 2: Gửi tín hiệu Ack về Kafka sau khi DB đã COMMIT toàn bộ lô an toàn
//            ack.acknowledge();
//            log.debug("Đã commit DB và Ack thành công lô gồm {} bản ghi", records.size());
//
//        } catch (Exception e) {
//            log.error("Lỗi nghiêm trọng khi xử lý Batch Transaction. Toàn bộ lô sẽ bị rollback và thử lại.", e);
//            // KHÔNG gọi ack để Kafka phân phối lại cả lô tin nhắn này (hoặc đẩy sang DLT tùy cấu hình ErrorHandler)
//            throw e;
//        }
//    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
    public void onInventoryReserved(@Payload String payload, Acknowledgment ack) {
        try {
            InventoryReservedEvent event = objectMapper.readValue(payload, InventoryReservedEvent.class);

            // Thực thi toàn bộ logic DB bên trong một Transaction ngắn, tường minh
            Boolean processSuccess = transactionTemplate.execute(status -> {

                // 1. Kiểm tra trùng lặp (Đã bao gồm ghi nhận vào bảng Inbox của DB bên trong hàm)
                if (!idempotency.shouldProcess(event.eventId())) {
                    log.debug("Duplicate event {} ignored", event.eventId());
                    return true;
                }

                // 2. Tìm Order kết hợp KHÓA dòng (Pessimistic Lock) để tránh Race Condition
                Order order = orderRepository.findByIdForUpdate(event.orderId()).orElse(null);
                if (order == null) {
                    log.warn("Order {} not found for inventory event", event.orderId());
                    return true;
                }

                // 3. Cập nhật trạng thái nghiệp vụ
                if (event.success()) {
                    order.markConfirmed();
                } else {
                    order.markCancelled();
                    log.info("Order {} cancelled: {}", order.getId(), event.reason());
                }
                orderRepository.save(order);

                // 4. Bỏ dòng markAsProcessed thừa ở đây vì idempotency.shouldProcess() đã tự động thực hiện lưu DB.

                return true;
            }); // TRANSACTION kết thúc và COMMIT tại đây!

            // Gửi tín hiệu Ack về Kafka sau khi DB đã COMMIT an toàn
            if (Boolean.TRUE.equals(processSuccess)) {
                ack.acknowledge();
            }

        } catch (Exception e) {
            log.error("Failed to process inventory reserved event, payload: {}", payload, e);
            // KHÔNG gọi ack để Kafka redeliver lại tin nhắn sau
        }
    }
}
