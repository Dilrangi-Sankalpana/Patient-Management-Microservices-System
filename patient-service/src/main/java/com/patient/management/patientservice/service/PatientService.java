package com.patient.management.patientservice.service;

import com.patient.management.patientservice.dto.PatientRequestDTO;
import com.patient.management.patientservice.dto.PatientResponseDTO;
import com.patient.management.patientservice.exception.EmailAlreadyExistsException;
import com.patient.management.patientservice.exception.PatientNotFoundException;
import com.patient.management.patientservice.mapping.PatientMapper;
import com.patient.management.patientservice.entity.Patient;
import com.patient.management.patientservice.repository.PatientRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

@Service
public class PatientService {
    private final PatientRepository patientRepository;
    private final OutboxService outboxService;

    public PatientService(PatientRepository patientRepository, OutboxService outboxService) {

        this.patientRepository = patientRepository;
        this.outboxService = outboxService;
    }

    public List<PatientResponseDTO> getPatients() {
        List<Patient> patients = patientRepository.findAll();
        return patients.stream().map(PatientMapper::toPatientResponseDTO).toList();
    }

    @Transactional
    public PatientResponseDTO createPatient(PatientRequestDTO patientRequestDTO) {
        if (patientRepository.existsByEmail(patientRequestDTO.getEmail())) {
            throw new EmailAlreadyExistsException("A patient with the email " + patientRequestDTO.getEmail() + " already exists");
        }

        Patient newPatient = patientRepository.save(PatientMapper.toPatient(patientRequestDTO));

        outboxService.recordPatientCreated(newPatient);

        return PatientMapper.toPatientResponseDTO(newPatient);
    }

    @Transactional
    public PatientResponseDTO updatePatient(UUID id, PatientRequestDTO patientRequestDTO) {
        Patient patient = patientRepository.findById(id).orElseThrow(() -> new PatientNotFoundException("Patient couldn't find with the id " + id));

        if (patientRepository.existsByEmailAndIdNot(patientRequestDTO.getEmail(), id)) {
            throw new EmailAlreadyExistsException("A patient with the email " + patientRequestDTO.getEmail() + " already exists");
        }

        patient.setName(patientRequestDTO.getName());
        patient.setAddress(patientRequestDTO.getAddress());
        patient.setEmail(patientRequestDTO.getEmail());
        patient.setDateOfBirth(patientRequestDTO.getDateOfBirth());

        Patient updatedPatient = patientRepository.save(patient);
        return PatientMapper.toPatientResponseDTO(updatedPatient);
    }

    public void deletePatient(UUID id) {
        patientRepository.deleteById(id);
    }
}
