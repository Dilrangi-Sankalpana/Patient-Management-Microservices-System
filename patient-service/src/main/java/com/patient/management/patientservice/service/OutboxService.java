package com.patient.management.patientservice.service;

import com.patient.management.patientservice.entity.OutboxEvent;
import com.patient.management.patientservice.entity.Patient;
import com.patient.management.patientservice.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import patient.events.PatientEvent;

import java.util.UUID;

@Service
public class OutboxService {

    public static final String PATIENT_TOPIC = "PATIENT";

    private final OutboxEventRepository outboxEventRepository;

    public OutboxService(OutboxEventRepository outboxEventRepository) {
        this.outboxEventRepository = outboxEventRepository;
    }

    // MANDATORY: throws if there is no surrounding transaction, so nobody can
    // record an event that isn't tied to a business write.
    @Transactional(propagation = Propagation.MANDATORY)
    public void recordPatientCreated(Patient patient) {
        String patientId = patient.getId().toString();

        PatientEvent event = PatientEvent.newBuilder()
                .setPatientId(patientId)
                .setName(patient.getName())
                .setEmail(patient.getEmail())
                .setEventType("PATIENT_CREATED")
                .setEventId(UUID.randomUUID().toString())
                .build();

        outboxEventRepository.save(new OutboxEvent(PATIENT_TOPIC, patientId, event.toByteArray()));
    }
}