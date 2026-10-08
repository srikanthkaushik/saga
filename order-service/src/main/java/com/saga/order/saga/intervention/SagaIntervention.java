package com.saga.order.saga.intervention;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import com.saga.order.saga.SagaState;

@Entity
@Table(name = "saga_intervention")
public class SagaIntervention {

    @Id
    private UUID id;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private InterventionAction action;

    @Column(nullable = false)
    private String operator;

    @Column
    private String note;

    @Enumerated(EnumType.STRING)
    @Column(name = "from_state", nullable = false)
    private SagaState fromState;

    @Enumerated(EnumType.STRING)
    @Column(name = "to_state", nullable = false)
    private SagaState toState;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected SagaIntervention() {
    }

    public SagaIntervention(UUID sagaId, InterventionAction action, String operator, String note,
                            SagaState fromState, SagaState toState) {
        this.id = UUID.randomUUID();
        this.sagaId = sagaId;
        this.action = action;
        this.operator = operator;
        this.note = note;
        this.fromState = fromState;
        this.toState = toState;
        this.createdAt = Instant.now();
    }

    public UUID getId() {
        return id;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public InterventionAction getAction() {
        return action;
    }

    public String getOperator() {
        return operator;
    }

    public String getNote() {
        return note;
    }

    public SagaState getFromState() {
        return fromState;
    }

    public SagaState getToState() {
        return toState;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
