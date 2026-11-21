package com.patient.management.patientservice.grpc;

// Generated classes from .proto file
import billing.BillingRequest;
import billing.BillingResponse;
import billing.BillingServiceGrpc;

import com.patient.management.patientservice.entity.Patient;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class BillingServiceGrpcClient {

    private static final Logger log = LoggerFactory.getLogger(BillingServiceGrpcClient.class);

    // Remote method caller
    private final BillingServiceGrpc.BillingServiceBlockingStub billingServiceBlockingStub;
    private final long timeoutMs;

    public BillingServiceGrpcClient(
            @Value("${billing.service.address:localhost}")  String serverAddress,
            @Value("${billing.service.grpc.port:9001}") int serverPort,
            @Value("${billing.service.timeout-ms:3000}") long timeoutMs
    ) {
        log.info("{}:{}", serverAddress, serverPort);
        this.timeoutMs = timeoutMs;

        ManagedChannel channel = ManagedChannelBuilder
                .forAddress(serverAddress, serverPort)
                .usePlaintext()
                .build();

        billingServiceBlockingStub = BillingServiceGrpc.newBlockingStub(channel);
    }

    // Throws io.grpc.StatusRuntimeException on failure; the caller decides about retries.
    public BillingResponse createBillingAccount(Patient patient) {

        BillingRequest request = BillingRequest.newBuilder()
                .setPatientId(patient.getId().toString())
                .setName(patient.getName())
                .setEmail(patient.getEmail())
                .build();

        // The deadline is measured from the moment it is set, so it must be applied per call.
        return billingServiceBlockingStub
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .createBillingAccount(request);
    }
}