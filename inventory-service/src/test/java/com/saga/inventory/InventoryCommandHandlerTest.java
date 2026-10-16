package com.saga.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;

import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaCommand.ReleaseInventory;
import com.saga.common.messaging.SagaCommand.ReserveInventory;
import com.saga.common.messaging.SagaReply.InventoryFailed;
import com.saga.common.messaging.SagaReply.InventoryReleased;
import com.saga.common.messaging.SagaReply.InventoryReserved;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxRepository;
import com.saga.common.test.PostgresSliceTest;
import com.saga.common.test.TestMessages;
import com.saga.inventory.domain.Product;
import com.saga.inventory.domain.ProductRepository;
import com.saga.inventory.domain.ReservationRepository;
import com.saga.inventory.domain.ReservationStatus;

/**
 * Duplicate delivery (same messageId) is absorbed by the idempotency guard; a re-sent command (new messageId)
 * by the reservation row: stock is reserved and released at most once, and every command is answered.
 */
@PostgresSliceTest
@Import(InventoryCommandHandler.class)
class InventoryCommandHandlerTest {

    private static final int INITIAL_STOCK = 10;

    @Autowired
    private InventoryCommandHandler handler;
    @Autowired
    private MessageCodec codec;
    @Autowired
    private ProductRepository products;
    @Autowired
    private ReservationRepository reservations;
    @Autowired
    private OutboxRepository outbox;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final String product = "slice-" + UUID.randomUUID();

    @BeforeEach
    void seedProduct() {
        products.saveAndFlush(new Product(product, INITIAL_STOCK));
    }

    @Test
    void duplicateDelivery_reservesOnce_andRepliesOnce() {
        UUID messageId = UUID.randomUUID();

        deliver(messageId, reserve(3));
        deliver(messageId, reserve(3));

        assertThat(stock()).isEqualTo(7);
        assertThat(replies()).containsExactly(new InventoryReserved(sagaId, orderId));
    }

    @Test
    void resentReserve_reservesOnce_butRepliesEachTime() {
        deliver(UUID.randomUUID(), reserve(3));
        deliver(UUID.randomUUID(), reserve(3));

        assertThat(stock()).isEqualTo(7);
        assertThat(replies()).containsExactly(
                new InventoryReserved(sagaId, orderId),
                new InventoryReserved(sagaId, orderId));
    }

    @Test
    void resentRelease_releasesOnce_butRepliesEachTime() {
        deliver(UUID.randomUUID(), reserve(3));
        deliver(UUID.randomUUID(), release(3));
        deliver(UUID.randomUUID(), release(3));

        assertThat(stock()).isEqualTo(INITIAL_STOCK);
        assertThat(reservations.findByOrderId(orderId)).hasValueSatisfying(
                reservation -> assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED));
        assertThat(replies()).containsExactly(
                new InventoryReserved(sagaId, orderId),
                new InventoryReleased(sagaId, orderId),
                new InventoryReleased(sagaId, orderId));
    }

    @Test
    void releaseBeforeReserve_leavesTombstone_andRefusesTheLateReservation() {
        deliver(UUID.randomUUID(), release(3));
        deliver(UUID.randomUUID(), reserve(3));

        assertThat(stock()).isEqualTo(INITIAL_STOCK);
        assertThat(replies()).containsExactly(
                new InventoryReleased(sagaId, orderId),
                new InventoryFailed(sagaId, orderId, "Order already released"));
    }

    @Test
    void releaseOfUnknownProduct_repliesReleased_withoutTombstone() {
        deliver(UUID.randomUUID(), new ReleaseInventory(sagaId, orderId, "no-such-product", 1));

        assertThat(reservations.findByOrderId(orderId)).isEmpty();
        assertThat(replies()).containsExactly(new InventoryReleased(sagaId, orderId));
    }

    @Test
    void insufficientStock_failsWithoutReserving() {
        deliver(UUID.randomUUID(), reserve(INITIAL_STOCK + 1));

        assertThat(stock()).isEqualTo(INITIAL_STOCK);
        assertThat(reservations.findByOrderId(orderId)).isEmpty();
        assertThat(replies()).containsExactly(new InventoryFailed(sagaId, orderId, "Insufficient stock"));
    }

    @Test
    void foreignCommand_isNonRetryable() {
        assertThatThrownBy(() -> deliver(UUID.randomUUID(), new RefundPayment(sagaId, orderId)))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("RefundPayment");
    }

    private ReserveInventory reserve(int quantity) {
        return new ReserveInventory(sagaId, orderId, product, quantity);
    }

    private ReleaseInventory release(int quantity) {
        return new ReleaseInventory(sagaId, orderId, product, quantity);
    }

    private void deliver(UUID messageId, Object command) {
        handler.onCommand(TestMessages.record(codec, Topics.INVENTORY_COMMANDS, messageId, command));
    }

    private int stock() {
        return products.findById(product).orElseThrow().getAvailableQuantity();
    }

    private List<Object> replies() {
        return TestMessages.sent(outbox, codec, Topics.SAGA_REPLIES, orderId);
    }
}
