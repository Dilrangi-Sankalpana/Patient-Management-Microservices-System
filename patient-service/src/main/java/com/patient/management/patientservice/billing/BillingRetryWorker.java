package com.patient.management.patientservice.billing;

import billing.BillingResponse;
import com.patient.management.patientservice.entity.BillingStatus;
import com.patient.management.patientservice.entity.Patient;
import com.patient.management.patientservice.grpc.BillingServiceGrpcClient;
import com.patient.management.patientservice.repository.PatientRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

@Slf4j
@Component
public class BillingRetryWorker {

    private final PatientRepository patientRepository;
    private final BillingServiceGrpcClient billingClient;
    private final int maxAttempts;

    public BillingRetryWorker(PatientRepository patientRepository,
                              BillingServiceGrpcClient billingClient,
                              @Value("${billing.retry.max-attempts:10}") int maxAttempts) {
        this.patientRepository = patientRepository;
        this.billingClient = billingClient;
        this.maxAttempts = maxAttempts;
    }

    @Scheduled(fixedDelayString = "${billing.retry.poll-ms:2000}")
    @Transactional
    public void processPending() {
        List<Patient> due = patientRepository.lockDueForBilling(Instant.now());

        for (Patient patient : due) {
            try {
                BillingResponse response = billingClient.createBillingAccount(patient);
                patient.markBillingActive(response.getAccountId());
                log.info("Billing account {} created for patient {}", response.getAccountId(), patient.getId());
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                boolean retryable = isRetryable(code);

                patient.recordBillingFailure(retryable, maxAttempts, Instant.now());

                if (patient.getBillingStatus() == BillingStatus.FAILED) {
                    log.error("Billing FAILED permanently for patient {} after {} attempts (last status {})",
                            patient.getId(), patient.getBillingAttempts(), code);
                } else {
                    log.warn("Billing attempt {} failed for patient {} (status {}), next try at {}",
                            patient.getBillingAttempts(), patient.getId(), code, patient.getBillingNextAttemptAt());
                }

                // Billing looks down: stop here instead of hammering it with the rest of the batch.
                if (retryable) {
                    return;
                }
            }
        }
    }

    // Errors that may succeed later vs. errors that never will (e.g. INVALID_ARGUMENT).
    private static boolean isRetryable(Status.Code code) {
        return switch (code) {
            case UNAVAILABLE, DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, INTERNAL, UNKNOWN -> true;
            default -> false;
        };
    }
}