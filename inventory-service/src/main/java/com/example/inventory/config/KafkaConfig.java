package com.example.inventory.config;

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

    @Bean
    NewTopic ordersCreatedTopic() {
        return TopicBuilder.name(Topics.ORDERS_CREATED).partitions(partitions).replicas(replicas).build();
    }

    @Bean
    NewTopic inventoryReservedTopic() {
        return TopicBuilder.name(Topics.INVENTORY_RESERVED).partitions(partitions).replicas(replicas).build();
    }

    // --- Producer (used by the OutboxRelay) -----------------------------------
    // Batching knobs (acks=all, lz4, linger.ms, batch.size) live in application.yml.
    // Here we set only what needs code or must be guaranteed:
    //  - idempotence + max.in.flight=5 → no duplicates, ordering kept, full throughput
    //  - buffer.memory 64MB            → absorb 10k req/s bursts

    @Bean
    ProducerFactory<String, String> producerFactory() {
        Map<String, Object> props = new HashMap<>(properties.buildProducerProperties(null));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 64L * 1024 * 1024);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /**
     * Relay template: observation DISABLED so it does not overwrite the traceparent
     * we replay from the outbox (captured from the incoming order event's trace).
     */
    @Bean
    KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> pf) {
        KafkaTemplate<String, String> template = new KafkaTemplate<>(pf);
        template.setObservationEnabled(false);
        return template;
    }

    // --- Consumer (listens to orders.created) ---------------------------------

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>(properties.buildConsumerProperties(null));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    // Single-record listener (one event at a time). Offsets commit once per poll
    // (AckMode.BATCH), not per record — per-record sync commits cap throughput at
    // 10k req/s. At-least-once redelivery is safe: the consumer dedupes on eventId
    // (DB inbox + Redis). DefaultErrorHandler retries a failing record, then DLT.
    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> cf, DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(cf);
        factory.setConcurrency(partitions);
        factory.getContainerProperties().setAckMode(AckMode.BATCH);
        factory.getContainerProperties().setObservationEnabled(true);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        ExponentialBackOff backOff = new ExponentialBackOff(500, 2.0);
        backOff.setMaxInterval(10_000);
        backOff.setMaxElapsedTime(60_000);
        DeadLetterPublishingRecoverer recorder = new DeadLetterPublishingRecoverer(template);
        return new DefaultErrorHandler(recorder, backOff);
    }
}
