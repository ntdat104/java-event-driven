package com.example.order.config;

import com.example.events.Topics;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.ContainerProperties.AckMode;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
public class KafkaConfig {

    private final KafkaProperties properties;

    @Value("${app.topics.partitions:12}")
    private int partitions;

    @Value("${app.topics.replicas:1}")
    private int replicas;

    public KafkaConfig(KafkaProperties properties) {
        this.properties = properties;
    }

    // --- Topology -------------------------------------------------------------
    // 12 partitions → up to 12 consumers in parallel. Size partitions for peak
    // throughput; they are cheap to add but cannot be reduced later.

    @Bean("kafkaBatchListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaBatchListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {

        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        // BẮT BUỘC: Kích hoạt chế độ Batch xử lý lô
        factory.setBatchListener(true);

        // Cấu hình Ack thủ công ngay lập tức theo thiết kế Outbox của bạn
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true);

        return factory;
    }

    @Bean
    NewTopic ordersCreatedTopic() {
        return TopicBuilder.name(Topics.ORDERS_CREATED).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(Topics.INVENTORY_RESERVED).partitions(partitions).replicas(replicas).build();
    }

    // --- Producer (used by the OutboxRelay) -----------------------------------
    // Tuned for high throughput WITHOUT sacrificing durability:
    //  - acks=all + idempotence  → no data loss, no duplicates on the broker
    //  - lz4 compression         → less network, higher effective throughput
    //  - linger.ms + batch.size  → amortise per-record overhead at 10k req/s

    @Bean
    ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>(properties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Relay template: observation is intentionally DISABLED so it does not overwrite
     * the {@code traceparent} header we replay from the outbox — that header carries
     * the original request's trace so the chain stays unbroken across the async hop.
     */
    @Bean
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
        template.setObservationEnabled(false);
        return template;
    }

    // --- Consumer (listens to inventory.reserved) -----------------------------
    // enable.auto.commit=false → we ack manually only after successful processing.

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>(properties.buildConsumerProperties(null));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // ErrorHandlingDeserializer prevents a single poison message from blocking the partition.
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> cf, DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(partitions);                       // one thread per partition
        factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
        factory.getContainerProperties().setObservationEnabled(true); // extract traceparent header
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    /**
     * Retry with exponential backoff, then route to {@code <topic>.DLT} for offline
     * inspection / replay. Deserialization failures are NOT retried (they will never
     * succeed) — DefaultErrorHandler treats ErrorHandlingDeserializer failures as
     * non-retryable and sends them straight to the DLT.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxInterval(10_000);
        backOff.setMaxElapsedTime(60_000);
        DeadLetterPublishingRecoverer recorder = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recorder, backOff);
    }
}
