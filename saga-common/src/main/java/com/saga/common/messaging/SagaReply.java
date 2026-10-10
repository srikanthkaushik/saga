package com.saga.common.messaging;

import java.util.UUID;

public sealed interface SagaReply
        permits SagaReply.PaymentProcessed, SagaReply.PaymentFailed, SagaReply.PaymentRefunded,
                SagaReply.InventoryReserved, SagaReply.InventoryFailed, SagaReply.InventoryReleased,
                SagaReply.ShipmentScheduled, SagaReply.ShipmentFailed, SagaReply.ShipmentCancelled {

    UUID sagaId();

    UUID orderId();

    record PaymentProcessed(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record PaymentFailed(UUID sagaId, UUID orderId, String reason) implements SagaReply {
    }

    record PaymentRefunded(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record InventoryReserved(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record InventoryFailed(UUID sagaId, UUID orderId, String reason) implements SagaReply {
    }

    record InventoryReleased(UUID sagaId, UUID orderId) implements SagaReply {
    }

    record ShipmentScheduled(UUID sagaId, UUID orderId, String trackingNumber) implements SagaReply {
    }

    record ShipmentFailed(UUID sagaId, UUID orderId, String reason) implements SagaReply {
    }

    record ShipmentCancelled(UUID sagaId, UUID orderId) implements SagaReply {
    }
}
