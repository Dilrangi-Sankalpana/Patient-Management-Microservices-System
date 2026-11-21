package com.patient.management.analyticsservice.kafka;

public interface ProcessedEventStore {

    boolean alreadyProcessed(String eventId);

    void markProcessed(String eventId);
}