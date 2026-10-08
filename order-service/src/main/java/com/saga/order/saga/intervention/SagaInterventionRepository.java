package com.saga.order.saga.intervention;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaInterventionRepository extends JpaRepository<SagaIntervention, UUID> {

    List<SagaIntervention> findBySagaIdOrderByCreatedAt(UUID sagaId);
}
