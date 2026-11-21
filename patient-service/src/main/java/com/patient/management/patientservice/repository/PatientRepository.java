package com.patient.management.patientservice.repository;

import com.patient.management.patientservice.entity.Patient;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface PatientRepository extends JpaRepository<Patient, UUID> {
    boolean existsByEmail(String email);

    boolean existsByEmailAndIdNot(String email, UUID id);

    // SKIP LOCKED: several instances can run the worker without picking the same patient.
    @Query(value = """
            SELECT * FROM patient
            WHERE billing_status = 'PENDING' AND billing_next_attempt_at <= :now
            ORDER BY billing_next_attempt_at
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<Patient> lockDueForBilling(@Param("now") Instant now);
}