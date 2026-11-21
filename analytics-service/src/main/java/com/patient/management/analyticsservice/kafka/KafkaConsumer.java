package com.patient.management.analyticsservice.kafka;

import com.google.protobuf.InvalidProtocolBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import patient.events.PatientEvent;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    private final ProcessedEventStore processedEvents;

    public KafkaConsumer(ProcessedEventStore processedEvents) {
        this.processedEvents = processedEvents;
    }

    // Exceptions are NOT caught here on purpose: the container's error handler
    // retries them and finally sends the record to PATIENT.DLT.
    @KafkaListener(topics = "PATIENT", groupId = "analytics-service")
    public void eventConsumer(byte[] event) throws InvalidProtocolBufferException {

        PatientEvent patientEvent = PatientEvent.parseFrom(event);

        String eventId = patientEvent.getEventId();

        // Events created before event_id existed have an empty id and can't be deduplicated.
        if (!eventId.isEmpty() && processedEvents.alreadyProcessed(eventId)) {
            log.info("Duplicate event {} ignored", eventId);
            return;
        }

        // ... Perform any business logic related to analytics-service

        // Mark AFTER the work succeeds; if it throws, the retry must not be skipped as a "duplicate".
        if (!eventId.isEmpty()) {
            processedEvents.markProcessed(eventId);
        }

        // Ids only: name and email are personal data and stay out of the logs.
        log.info("Processed {} event {} for patient {}",
                patientEvent.getEventType(), eventId, patientEvent.getPatientId());
    }
}