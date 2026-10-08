package com.saga.order.saga.intervention;

import java.time.Instant;
import java.util.UUID;

import com.saga.order.domain.OrderStatus;
import com.saga.order.saga.SagaState;

public record InterventionResult(
        UUID sagaId,
        UUID orderId,
        InterventionAction action,
        String operator,
        SagaState fromState,
        SagaState toState,
        int compensationResends,
        Instant deadline,
        OrderStatus orderStatus) {
}
