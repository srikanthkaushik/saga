package com.saga.order.saga;

import java.time.Instant;
import java.util.UUID;

/** Projection of a saga whose compensation is not completing; what an on-call engineer needs to start digging. */
public record StuckSaga(
        UUID sagaId,
        UUID orderId,
        SagaState state,
        String failureReason,
        int compensationResends,
        Instant compensatingSince,
        Instant deadline) {
}
