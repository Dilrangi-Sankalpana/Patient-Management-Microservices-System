package com.patient.management.patientservice.kafka;

import com.patient.management.patientservice.entity.OutboxEvent;
import com.patient.management.patientservice.repository.OutboxEventRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    public OutboxRelay(OutboxEventRepository outboxEventRepository,
                       KafkaTemplate<String, byte[]> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.delay-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> batch = outboxEventRepository.lockNextBatch();

        for (OutboxEvent event : batch) {
            try {
                // Block until the broker acknowledges, so "published" really means stored.
                kafkaTemplate.send(event.getTopic(), event.getEventKey(), event.getPayload())
                        .get(10, TimeUnit.SECONDS);
                event.setPublishedAt(Instant.now()); // saved on commit (dirty checking)
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                // Stop the batch so order is kept; the row stays unpublished and is retried next tick.
                log.error("Outbox event {} (key={}) not published, will retry", event.getId(), event.getEventKey(), e);
                return;
            }
        }
    }
}