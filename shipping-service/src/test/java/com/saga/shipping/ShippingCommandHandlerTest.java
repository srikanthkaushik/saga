package com.saga.shipping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.SagaCommand.CancelShipment;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaCommand.ScheduleShipment;
import com.saga.common.messaging.SagaReply.ShipmentCancelled;
import com.saga.common.messaging.SagaReply.ShipmentFailed;
import com.saga.common.messaging.SagaReply.ShipmentScheduled;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxRepository;
import com.saga.common.test.PostgresSliceTest;
import com.saga.common.test.TestMessages;
import com.saga.shipping.domain.Shipment;
import com.saga.shipping.domain.ShipmentRepository;
import com.saga.shipping.domain.ShipmentStatus;

/**
 * The pivot participant. A re-sent ScheduleShipment must not book a second shipment and must report the same
 * tracking number; a re-sent CancelShipment cancels once; a cancel that overtakes the schedule fences it off.
 * Zones come from the Flyway seed (GB served, XX not).
 */
@PostgresSliceTest
@Import(ShippingCommandHandler.class)
class ShippingCommandHandlerTest {

    @Autowired
    private ShippingCommandHandler handler;
    @Autowired
    private MessageCodec codec;
    @Autowired
    private ShipmentRepository shipments;
    @Autowired
    private OutboxRepository outbox;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();

    @Test
    void duplicateDelivery_schedulesOnce_andRepliesOnce() {
        UUID messageId = UUID.randomUUID();

        deliver(messageId, schedule("GB"));
        deliver(messageId, schedule("GB"));

        Shipment shipment = shipments.findByOrderId(orderId).orElseThrow();
        assertThat(shipment.getStatus()).isEqualTo(ShipmentStatus.SCHEDULED);
        assertThat(replies()).containsExactly(new ShipmentScheduled(sagaId, orderId, shipment.getTrackingNumber()));
    }

    @Test
    void resentSchedule_booksOneShipment_andRepeatsItsTrackingNumber() {
        deliver(UUID.randomUUID(), schedule("GB"));
        deliver(UUID.randomUUID(), schedule("GB"));

        String trackingNumber = shipments.findByOrderId(orderId).orElseThrow().getTrackingNumber();
        assertThat(trackingNumber).isNotBlank();
        assertThat(replies()).containsExactly(
                new ShipmentScheduled(sagaId, orderId, trackingNumber),
                new ShipmentScheduled(sagaId, orderId, trackingNumber));
    }

    @Test
    void resentCancel_cancelsOnce_butRepliesEachTime() {
        deliver(UUID.randomUUID(), schedule("GB"));
        deliver(UUID.randomUUID(), new CancelShipment(sagaId, orderId));
        deliver(UUID.randomUUID(), new CancelShipment(sagaId, orderId));

        Shipment shipment = shipments.findByOrderId(orderId).orElseThrow();
        assertThat(shipment.getStatus()).isEqualTo(ShipmentStatus.CANCELLED);
        assertThat(replies()).containsExactly(
                new ShipmentScheduled(sagaId, orderId, shipment.getTrackingNumber()),
                new ShipmentCancelled(sagaId, orderId),
                new ShipmentCancelled(sagaId, orderId));
    }

    @Test
    void cancelBeforeSchedule_leavesTombstone_andRefusesTheLateShipment() {
        deliver(UUID.randomUUID(), new CancelShipment(sagaId, orderId));
        deliver(UUID.randomUUID(), schedule("GB"));

        assertThat(shipments.findByOrderId(orderId)).hasValueSatisfying(
                shipment -> assertThat(shipment.getStatus()).isEqualTo(ShipmentStatus.CANCELLED));
        assertThat(replies()).containsExactly(
                new ShipmentCancelled(sagaId, orderId),
                new ShipmentFailed(sagaId, orderId, "Order already cancelled"));
    }

    @Test
    void unservedDestination_failsWithoutBooking() {
        deliver(UUID.randomUUID(), schedule("XX"));

        assertThat(shipments.findByOrderId(orderId)).isEmpty();
        assertThat(replies()).containsExactly(new ShipmentFailed(sagaId, orderId, "No carrier serves XX"));
    }

    @Test
    void foreignCommand_isNonRetryable() {
        assertThatThrownBy(() -> deliver(UUID.randomUUID(), new RefundPayment(sagaId, orderId)))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("RefundPayment");
    }

    private ScheduleShipment schedule(String destination) {
        return new ScheduleShipment(sagaId, orderId, "product-1", 1, destination);
    }

    private void deliver(UUID messageId, Object command) {
        handler.onCommand(TestMessages.record(codec, Topics.SHIPPING_COMMANDS, messageId, command));
    }

    private List<Object> replies() {
        return TestMessages.sent(outbox, codec, Topics.SAGA_REPLIES, orderId);
    }
}
