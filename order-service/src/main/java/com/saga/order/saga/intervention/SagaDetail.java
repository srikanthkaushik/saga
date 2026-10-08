package com.saga.order.saga.intervention;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import com.saga.order.domain.OrderStatus;
import com.saga.order.saga.SagaState;

public record SagaDetail(
        UUID sagaId,
        UUID orderId,
        SagaState state,
        String failureReason,
        int compensationResends,
        Instant compensatingSince,
        Instant deadline,
        OrderStatus orderStatus,
        List<Entry> interventions) {

    public record Entry(InterventionAction action, String operator, String note,
                        SagaState fromState, SagaState toState, Instant at) {

        static Entry of(SagaIntervention i) {
            return new Entry(i.getAction(), i.getOperator(), i.getNote(), i.getFromState(), i.getToState(), i.getCreatedAt());
        }
    }
}
