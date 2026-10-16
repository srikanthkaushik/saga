package com.saga.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.idempotency.ProcessedMessageRepository;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.SagaCommand.ProcessPayment;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaCommand.ReserveInventory;
import com.saga.common.messaging.SagaReply.PaymentFailed;
import com.saga.common.messaging.SagaReply.PaymentProcessed;
import com.saga.common.messaging.SagaReply.PaymentRefunded;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxRepository;
import com.saga.common.test.PostgresSliceTest;
import com.saga.common.test.TestMessages;
import com.saga.payment.domain.CustomerCredit;
import com.saga.payment.domain.CustomerCreditRepository;
import com.saga.payment.domain.PaymentRepository;
import com.saga.payment.domain.PaymentStatus;

/**
 * Duplicate delivery (same messageId) is absorbed by the idempotency guard. A re-sent command (new messageId,
 * e.g. a compensation re-sent after a timeout) must be absorbed by the handler itself: charge and refund at most
 * once, but answer every time so the saga can move on.
 */
@PostgresSliceTest
@Import(PaymentCommandHandler.class)
class PaymentCommandHandlerTest {

    private static final BigDecimal INITIAL_CREDIT = new BigDecimal("100.00");

    @Autowired
    private PaymentCommandHandler handler;
    @Autowired
    private MessageCodec codec;
    @Autowired
    private CustomerCreditRepository credits;
    @Autowired
    private PaymentRepository payments;
    @Autowired
    private OutboxRepository outbox;
    @Autowired
    private ProcessedMessageRepository processed;

    private final UUID sagaId = UUID.randomUUID();
    private final UUID orderId = UUID.randomUUID();
    private final String customer = "slice-" + UUID.randomUUID();

    @BeforeEach
    void seedCustomer() {
        credits.saveAndFlush(new CustomerCredit(customer, INITIAL_CREDIT));
    }

    @Test
    void duplicateDelivery_chargesOnce_andRepliesOnce() {
        UUID messageId = UUID.randomUUID();
        ProcessPayment charge = charge("30.00");

        deliver(messageId, charge);
        deliver(messageId, charge);

        assertThat(credit()).isEqualByComparingTo("70.00");
        assertThat(replies()).containsExactly(new PaymentProcessed(sagaId, orderId));
    }

    @Test
    void resentCharge_chargesOnce_butRepliesEachTime() {
        deliver(UUID.randomUUID(), charge("30.00"));
        deliver(UUID.randomUUID(), charge("30.00"));

        assertThat(credit()).isEqualByComparingTo("70.00");
        assertThat(replies()).containsExactly(
                new PaymentProcessed(sagaId, orderId),
                new PaymentProcessed(sagaId, orderId));
    }

    @Test
    void resentRefund_refundsOnce_butRepliesEachTime() {
        deliver(UUID.randomUUID(), charge("30.00"));
        deliver(UUID.randomUUID(), new RefundPayment(sagaId, orderId));
        deliver(UUID.randomUUID(), new RefundPayment(sagaId, orderId));

        assertThat(credit()).isEqualByComparingTo(INITIAL_CREDIT);
        assertThat(payments.findByOrderId(orderId)).hasValueSatisfying(
                payment -> assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REFUNDED));
        assertThat(replies()).containsExactly(
                new PaymentProcessed(sagaId, orderId),
                new PaymentRefunded(sagaId, orderId),
                new PaymentRefunded(sagaId, orderId));
    }

    @Test
    void refundBeforeCharge_leavesTombstone_andRefusesTheLateCharge() {
        deliver(UUID.randomUUID(), new RefundPayment(sagaId, orderId));
        deliver(UUID.randomUUID(), charge("30.00"));

        assertThat(credit()).isEqualByComparingTo(INITIAL_CREDIT);
        assertThat(payments.findByOrderId(orderId)).hasValueSatisfying(
                payment -> assertThat(payment.getStatus()).isEqualTo(PaymentStatus.CANCELLED));
        assertThat(replies()).containsExactly(
                new PaymentRefunded(sagaId, orderId),
                new PaymentFailed(sagaId, orderId, "Order already CANCELLED"));
    }

    @Test
    void foreignCommand_isNonRetryable() {
        ReserveInventory foreign = new ReserveInventory(sagaId, orderId, "product-1", 1);

        assertThatThrownBy(() -> deliver(UUID.randomUUID(), foreign))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("ReserveInventory");
    }

    /**
     * The processed marker is written in the handler's transaction: when the handler fails, the marker rolls back
     * with everything else, so the redelivered record is handled again instead of being skipped as a duplicate.
     */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void failedAttempt_rollsBackTheProcessedMarker_soTheRedeliveryIsHandledAgain() {
        UUID messageId = UUID.randomUUID();
        ProcessPayment broken = new ProcessPayment(sagaId, orderId, customer, null);

        assertThatThrownBy(() -> deliver(messageId, broken)).isInstanceOf(NullPointerException.class);
        assertThat(processed.existsById(messageId)).isFalse();
        assertThat(payments.findByOrderId(orderId)).isEmpty();
        assertThat(replies()).isEmpty();

        // A duplicate would return silently; the redelivery reaches the handler and fails the same way
        assertThatThrownBy(() -> deliver(messageId, broken)).isInstanceOf(NullPointerException.class);
        assertThat(credit()).isEqualByComparingTo(INITIAL_CREDIT);
    }

    private ProcessPayment charge(String amount) {
        return new ProcessPayment(sagaId, orderId, customer, new BigDecimal(amount));
    }

    private void deliver(UUID messageId, Object command) {
        handler.onCommand(TestMessages.record(codec, Topics.PAYMENT_COMMANDS, messageId, command));
    }

    private BigDecimal credit() {
        return credits.findById(customer).orElseThrow().getAvailableCredit();
    }

    private List<Object> replies() {
        return TestMessages.sent(outbox, codec, Topics.SAGA_REPLIES, orderId);
    }
}
