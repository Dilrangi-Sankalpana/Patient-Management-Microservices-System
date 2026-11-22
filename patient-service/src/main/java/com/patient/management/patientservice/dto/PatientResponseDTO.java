package com.patient.management.patientservice.dto;

import lombok.Setter;

import java.time.LocalDate;

@Setter
public class PatientResponseDTO {
    private String id;
    private String name;
    private String email;
    private String address;
    private LocalDate dateOfBirth;
    private String billingStatus;
}
