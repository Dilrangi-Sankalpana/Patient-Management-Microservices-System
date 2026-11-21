package com.patient.management.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaErrorHandlingConfig {

    // Must exist explicitly: MSK has topic auto-creation disabled.
    @Bean
    public NewTopic patientDeadLetterTopic() {
        return TopicBuilder.name("PATIENT.DLT").build();
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, byte[]> kafkaTemplate) {
        // Failed record is copied to "<topic>.DLT". Partition -1 = let Kafka choose,
        // so the DLT does not need the same partition count as PATIENT.
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", -1));

        // 4 retries: waits of 1s, 2s, 4s, 8s, then the record goes to the DLT.
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(4);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);

        DefaultErrorHandler handler = new DefaultErrorHandler(recoverer, backOff);

        // Garbage bytes will never parse; retrying is pointless, go straight to the DLT.
        handler.addNotRetryableExceptions(InvalidProtocolBufferException.class);

        return handler;
    }
}