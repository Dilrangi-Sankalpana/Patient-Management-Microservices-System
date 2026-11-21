package com.patient.management.analyticsservice.kafka;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

// Stand-in: bounded and in-memory, so it is lost on restart and not shared between replicas.
// The durable version is a table with a UNIQUE event_id, written in the same transaction as the business effect.
@Component
public class InMemoryProcessedEventStore implements ProcessedEventStore {

    private static final int MAX_ENTRIES = 10_000;

    private final Set<String> seen = Collections.newSetFromMap(
            Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
                @Override
                protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
                    return size() > MAX_ENTRIES;
                }
            }));

    @Override
    public boolean alreadyProcessed(String eventId) {
        return seen.contains(eventId);
    }

    @Override
    public void markProcessed(String eventId) {
        seen.add(eventId);
    }
}