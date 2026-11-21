package com.patient.management.patientservice.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotNull;

import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicUpdate;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

// DynamicUpdate: UPDATE only the changed columns. Without it, a user editing a patient
// could overwrite billing_status with a stale value the billing worker just changed.
@Entity
@DynamicUpdate
@Getter
@Setter
public class Patient {

    @Id
    @GeneratedValue
    private UUID id;

    @NotNull
    private String name;

    @NotNull
    @Email
    @Column(unique = true)
    private String email;

    @NotNull
    private String address;

    @NotNull
    private LocalDate dateOfBirth;

    @NotNull
    private LocalDate registeredDate =  LocalDate.now();

    // The DB default ('ACTIVE') only applies to rows that already exist (including seed data).
    // New patients get PENDING from the Java default below.
    @Enumerated(EnumType.STRING)
    @Column(name = "billing_status", nullable = false, columnDefinition = "varchar(20) default 'ACTIVE'")
    private BillingStatus billingStatus = BillingStatus.PENDING;

    @Column(name = "billing_account_id")
    private String billingAccountId;

    @Column(name = "billing_attempts", nullable = false, columnDefinition = "integer default 0")
    private int billingAttempts = 0;

    @Column(name = "billing_next_attempt_at")
    private Instant billingNextAttemptAt = Instant.now();

    public void markBillingActive(String accountId) {
        this.billingStatus = BillingStatus.ACTIVE;
        this.billingAccountId = accountId;
        this.billingNextAttemptAt = null;
    }

    public void recordBillingFailure(boolean retryable, int maxAttempts, Instant now) {
        this.billingAttempts++;

        if (!retryable || this.billingAttempts >= maxAttempts) {
            this.billingStatus = BillingStatus.FAILED;
            this.billingNextAttemptAt = null;
            return;
        }

        // exponential backoff: 10s, 20s, 40s, 80s, 160s, then capped at 300s
        long delaySeconds = Math.min(5L * (1L << Math.min(this.billingAttempts, 10)), 300L);
        this.billingNextAttemptAt = now.plusSeconds(delaySeconds);
    }
}