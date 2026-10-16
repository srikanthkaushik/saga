package com.saga.order.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import com.saga.common.idempotency.IdempotencyGuard;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.OrderEvent.OrderApproved;
import com.saga.common.messaging.OrderEvent.OrderRejected;
import com.saga.common.messaging.SagaCommand.CancelShipment;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.SagaCommand.ReleaseInventory;
import com.saga.common.messaging.SagaCommand.ReserveInventory;
import com.saga.common.messaging.SagaCommand.ScheduleShipment;
import com.saga.common.messaging.SagaReply;
import com.saga.common.messaging.SagaReply.InventoryFailed;
import com.saga.common.messaging.SagaReply.InventoryReleased;
import com.saga.common.messaging.SagaReply.InventoryReserved;
import com.saga.common.messaging.SagaReply.PaymentFailed;
import com.saga.common.messaging.SagaReply.PaymentProcessed;
import com.saga.common.messaging.SagaReply.PaymentRefunded;
import com.saga.common.messaging.SagaReply.ShipmentCancelled;
import com.saga.common.messaging.SagaReply.ShipmentFailed;
import com.saga.common.messaging.SagaReply.ShipmentScheduled;
import com.saga.common.messaging.Topics;
import com.saga.common.outbox.OutboxWriter;
import com.saga.common.test.TestMessages;
import com.saga.order.domain.Order;
import com.saga.order.domain.OrderRepository;
import com.saga.order.domain.OrderStatus;

import tools.jackson.databind.json.JsonMapper;

/**
 * The orchestrator against mocked persistence: which reply is accepted in which state, what it sends next, and
 * that duplicate, late, early and foreign replies change nothing. Replies go through the real codec, built the way
 * the outbox relay publishes them. Transactions are not exercised here (the e2e suite covers commit, rollback and
 * optimistic locking); {@code MANDATORY} propagation is inert without Spring proxies.
 */
class OrderSagaOrchestratorTest {

    private static final Duration STEP_TIMEOUT = Duration.ofSeconds(30);

    private final MessageCodec codec = new MessageCodec(JsonMapper.builder().build());
    private IdempotencyGuard idempotencyGuard;
    private OrderSagaRepository sagaRepository;
    private OrderRepository orderRepository;
    private OutboxWriter outbox;
    private CompensationMonitor compensationMonitor;
    private OrderSagaOrchestrator orchestrator;

    private Order order;

    /** A reply for a given saga; the name is what the parameterized test reports. */
    private record Reply(String name, SagaState expectedState, Function<OrderSaga, SagaReply> of) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static final List<Reply> REPLIES = List.of(
            new Reply("PaymentProcessed", SagaState.PAYMENT_PENDING, s -> new PaymentProcessed(s.getId(), s.getOrderId())),
            new Reply("PaymentFailed", SagaState.PAYMENT_PENDING, s -> new PaymentFailed(s.getId(), s.getOrderId(), "Insufficient credit")),
            new Reply("InventoryReserved", SagaState.INVENTORY_PENDING, s -> new InventoryReserved(s.getId(), s.getOrderId())),
            new Reply("InventoryFailed", SagaState.INVENTORY_PENDING, s -> new InventoryFailed(s.getId(), s.getOrderId(), "Insufficient stock")),
            new Reply("ShipmentScheduled", SagaState.SHIPPING_PENDING, s -> new ShipmentScheduled(s.getId(), s.getOrderId(), "TRK-1")),
            new Reply("ShipmentFailed", SagaState.SHIPPING_PENDING, s -> new ShipmentFailed(s.getId(), s.getOrderId(), "No carrier serves XX")),
            new Reply("ShipmentCancelled", SagaState.CANCELLING_SHIPMENT, s -> new ShipmentCancelled(s.getId(), s.getOrderId())),
            new Reply("InventoryReleased", SagaState.RELEASING_INVENTORY, s -> new InventoryReleased(s.getId(), s.getOrderId())),
            new Reply("PaymentRefunded", SagaState.COMPENSATING, s -> new PaymentRefunded(s.getId(), s.getOrderId())));

    @BeforeEach
    void setUp() {
        idempotencyGuard = mock(IdempotencyGuard.class);
        sagaRepository = mock(OrderSagaRepository.class);
        orderRepository = mock(OrderRepository.class);
        outbox = mock(OutboxWriter.class);
        compensationMonitor = mock(CompensationMonitor.class);
        SagaTimeoutProperties timeouts = new SagaTimeoutProperties(
                STEP_TIMEOUT, STEP_TIMEOUT, STEP_TIMEOUT, STEP_TIMEOUT, Duration.ofSeconds(5), 100);
        orchestrator = new OrderSagaOrchestrator(codec, idempotencyGuard, sagaRepository, orderRepository, outbox,
                timeouts, compensationMonitor, new OrderEventPublisher(outbox));

        order = new Order("customer-1", "product-1", 2, new BigDecimal("20.00"), "GB");
        when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
        when(idempotencyGuard.firstDelivery(any())).thenReturn(true);
    }

    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    // ---- replies ------------------------------------------------------------------------------------

    static Stream<Arguments> acceptedReplies() {
        return Stream.of(
                Arguments.of(reply("PaymentProcessed"), SagaState.INVENTORY_PENDING, Topics.INVENTORY_COMMANDS, ReserveInventory.class),
                Arguments.of(reply("PaymentFailed"), SagaState.FAILED, Topics.ORDER_EVENTS, OrderRejected.class),
                Arguments.of(reply("InventoryReserved"), SagaState.SHIPPING_PENDING, Topics.SHIPPING_COMMANDS, ScheduleShipment.class),
                Arguments.of(reply("InventoryFailed"), SagaState.COMPENSATING, Topics.PAYMENT_COMMANDS, RefundPayment.class),
                Arguments.of(reply("ShipmentScheduled"), SagaState.COMPLETED, Topics.ORDER_EVENTS, OrderApproved.class),
                Arguments.of(reply("ShipmentFailed"), SagaState.RELEASING_INVENTORY, Topics.INVENTORY_COMMANDS, ReleaseInventory.class),
                Arguments.of(reply("ShipmentCancelled"), SagaState.RELEASING_INVENTORY, Topics.INVENTORY_COMMANDS, ReleaseInventory.class),
                Arguments.of(reply("InventoryReleased"), SagaState.COMPENSATING, Topics.PAYMENT_COMMANDS, RefundPayment.class),
                Arguments.of(reply("PaymentRefunded"), SagaState.FAILED, Topics.ORDER_EVENTS, OrderRejected.class));
    }

    @ParameterizedTest(name = "{0} -> {1}, sends {3}")
    @MethodSource("acceptedReplies")
    void reply_inItsState_advancesTheSaga_andSendsExactlyTheNextMessage(
            Reply reply, SagaState next, String topic, Class<?> messageType) {
        OrderSaga saga = sagaIn(reply.expectedState());

        deliver(UUID.randomUUID(), reply.of().apply(saga));

        assertThat(saga.getState()).isEqualTo(next);
        Object sent = onlyMessageSentTo(topic);
        assertThat(sent).isInstanceOf(messageType);
    }

    static Stream<Arguments> repliesInTheWrongState() {
        return REPLIES.stream().flatMap(reply -> Arrays.stream(SagaState.values())
                .filter(state -> state != reply.expectedState())
                .map(state -> Arguments.of(reply, state)));
    }

    /** Late (after a timeout moved on), early (overtaking an earlier step), stale (after the end), or misrouted. */
    @ParameterizedTest(name = "{0} in {1}")
    @MethodSource("repliesInTheWrongState")
    void reply_inAnyOtherState_isIgnored(Reply reply, SagaState state) {
        OrderSaga saga = sagaIn(state);
        Instant updatedAt = saga.getUpdatedAt();
        Instant deadline = saga.getDeadline();

        deliver(UUID.randomUUID(), reply.of().apply(saga));

        assertThat(saga.getState()).isEqualTo(state);
        assertThat(saga.getUpdatedAt()).isEqualTo(updatedAt);
        assertThat(saga.getDeadline()).isEqualTo(deadline);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);
        verifyNoInteractions(outbox);
    }

    @Test
    void lateShipmentScheduled_afterShippingTimedOut_doesNotApprove_andCompensationCarriesOn() {
        OrderSaga saga = sagaIn(SagaState.CANCELLING_SHIPMENT);

        deliver(UUID.randomUUID(), new ShipmentScheduled(saga.getId(), saga.getOrderId(), "TRK-LATE"));
        assertThat(saga.getState()).isEqualTo(SagaState.CANCELLING_SHIPMENT);
        assertThat(order.getStatus()).isEqualTo(OrderStatus.PENDING);

        deliver(UUID.randomUUID(), new ShipmentCancelled(saga.getId(), saga.getOrderId()));
        assertThat(saga.getState()).isEqualTo(SagaState.RELEASING_INVENTORY);
        assertThat(onlyMessageSentTo(Topics.INVENTORY_COMMANDS)).isInstanceOf(ReleaseInventory.class);
    }

    @Test
    void duplicateReply_isDroppedBeforeTheSagaIsEvenLoaded() {
        when(idempotencyGuard.firstDelivery(any())).thenReturn(false);
        OrderSaga saga = sagaIn(SagaState.PAYMENT_PENDING);

        deliver(UUID.randomUUID(), new PaymentProcessed(saga.getId(), saga.getOrderId()));

        assertThat(saga.getState()).isEqualTo(SagaState.PAYMENT_PENDING);
        verifyNoInteractions(sagaRepository, outbox);
    }

    @Test
    void sameReplyDeliveredTwice_advancesOnce_andSendsOnce() {
        when(idempotencyGuard.firstDelivery(any())).thenReturn(true, false);
        OrderSaga saga = sagaIn(SagaState.PAYMENT_PENDING);
        UUID messageId = UUID.randomUUID();
        PaymentProcessed reply = new PaymentProcessed(saga.getId(), saga.getOrderId());

        deliver(messageId, reply);
        deliver(messageId, reply);

        assertThat(saga.getState()).isEqualTo(SagaState.INVENTORY_PENDING);
        verify(outbox, times(1)).write(anyString(), anyString(), any());
    }

    @Test
    void resentStepReply_withANewMessageId_isDroppedByTheStateGuard() {
        OrderSaga saga = sagaIn(SagaState.PAYMENT_PENDING);
        PaymentProcessed reply = new PaymentProcessed(saga.getId(), saga.getOrderId());

        deliver(UUID.randomUUID(), reply);
        deliver(UUID.randomUUID(), reply);

        assertThat(saga.getState()).isEqualTo(SagaState.INVENTORY_PENDING);
        verify(outbox, times(1)).write(anyString(), anyString(), any());
    }

    @Test
    void rejection_publishesTheFirstFailureReason() {
        OrderSaga saga = sagaIn(SagaState.SHIPPING_PENDING);

        deliver(UUID.randomUUID(), new ShipmentFailed(saga.getId(), saga.getOrderId(), "No carrier serves XX"));
        deliver(UUID.randomUUID(), new InventoryReleased(saga.getId(), saga.getOrderId()));
        deliver(UUID.randomUUID(), new PaymentRefunded(saga.getId(), saga.getOrderId()));

        assertThat(order.getStatus()).isEqualTo(OrderStatus.REJECTED);
        ArgumentCaptor<Object> messages = ArgumentCaptor.forClass(Object.class);
        verify(outbox, times(3)).write(anyString(), anyString(), messages.capture());
        assertThat(messages.getAllValues()).map(Object::getClass).map(Class::getSimpleName)
                .containsExactly("ReleaseInventory", "RefundPayment", "OrderRejected");
        assertThat(messages.getAllValues().getLast())
                .isInstanceOfSatisfying(OrderRejected.class, e -> assertThat(e.reason()).isEqualTo("No carrier serves XX"));
    }

    @Test
    void replyForUnknownSaga_isNonRetryable() {
        UUID sagaId = UUID.randomUUID();
        when(sagaRepository.findById(sagaId)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> deliver(UUID.randomUUID(), new PaymentProcessed(sagaId, order.getId())))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("Unknown saga " + sagaId);
    }

    @Test
    void commandOnTheReplyTopic_isNonRetryable() {
        OrderSaga saga = sagaIn(SagaState.PAYMENT_PENDING);

        assertThatThrownBy(() -> deliver(UUID.randomUUID(), new RefundPayment(saga.getId(), saga.getOrderId())))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("Non-reply message RefundPayment");
        verifyNoInteractions(outbox);
    }

    // ---- timeouts -----------------------------------------------------------------------------------

    static Stream<Arguments> stepTimeouts() {
        return Stream.of(
                Arguments.of(SagaState.PAYMENT_PENDING, SagaState.COMPENSATING, Topics.PAYMENT_COMMANDS, RefundPayment.class, "Payment timed out"),
                Arguments.of(SagaState.INVENTORY_PENDING, SagaState.RELEASING_INVENTORY, Topics.INVENTORY_COMMANDS, ReleaseInventory.class, "Inventory timed out"),
                Arguments.of(SagaState.SHIPPING_PENDING, SagaState.CANCELLING_SHIPMENT, Topics.SHIPPING_COMMANDS, CancelShipment.class, "Shipping timed out"));
    }

    @ParameterizedTest(name = "{0} -> {1}")
    @MethodSource("stepTimeouts")
    void stepTimeout_startsCompensation_withAFreshDeadline(
            SagaState state, SagaState next, String topic, Class<?> messageType, String reason) {
        Instant now = Instant.now();
        OrderSaga saga = overdueSagaIn(state, now);

        orchestrator.onTimeout(saga.getId(), now);

        assertThat(saga.getState()).isEqualTo(next);
        assertThat(saga.getFailureReason()).isEqualTo(reason);
        assertThat(saga.getDeadline()).isEqualTo(now.plus(STEP_TIMEOUT));
        assertThat(onlyMessageSentTo(topic)).isInstanceOf(messageType);
    }

    static Stream<Arguments> compensationTimeouts() {
        return Stream.of(
                Arguments.of(SagaState.CANCELLING_SHIPMENT, Topics.SHIPPING_COMMANDS, CancelShipment.class),
                Arguments.of(SagaState.RELEASING_INVENTORY, Topics.INVENTORY_COMMANDS, ReleaseInventory.class),
                Arguments.of(SagaState.COMPENSATING, Topics.PAYMENT_COMMANDS, RefundPayment.class));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("compensationTimeouts")
    void compensationTimeout_resendsTheSameCommand_andReportsOnlyAfterCommit(
            SagaState state, String topic, Class<?> messageType) {
        TransactionSynchronizationManager.initSynchronization();
        Instant now = Instant.now();
        OrderSaga saga = overdueSagaIn(state, now);

        orchestrator.onTimeout(saga.getId(), now);

        assertThat(saga.getState()).isEqualTo(state);
        assertThat(saga.getCompensationResends()).isEqualTo(1);
        assertThat(saga.getDeadline()).isEqualTo(now.plus(STEP_TIMEOUT));
        assertThat(onlyMessageSentTo(topic)).isInstanceOf(messageType);

        verify(compensationMonitor, never()).compensationResent(any(), anyInt());
        TransactionSynchronizationManager.getSynchronizations().forEach(sync -> sync.afterCommit());
        verify(compensationMonitor).compensationResent(saga, 1);
    }

    @ParameterizedTest
    @EnumSource(value = SagaState.class, names = {"COMPLETED", "FAILED"})
    void timeout_ofAFinishedSaga_doesNothing(SagaState state) {
        OrderSaga saga = sagaIn(state);

        orchestrator.onTimeout(saga.getId(), Instant.now().plus(Duration.ofDays(1)));

        assertThat(saga.getState()).isEqualTo(state);
        verifyNoInteractions(outbox);
    }

    /** The scanner's query and the timeout race with replies: a saga that moved on is re-checked and left alone. */
    @Test
    void timeout_ofASagaThatIsNoLongerOverdue_doesNothing() {
        Instant now = Instant.now();
        OrderSaga saga = OrderSagas.in(SagaState.PAYMENT_PENDING, order.getId(), now.plusSeconds(1));
        when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

        orchestrator.onTimeout(saga.getId(), now);

        assertThat(saga.getState()).isEqualTo(SagaState.PAYMENT_PENDING);
        verifyNoInteractions(outbox);
    }

    @Test
    void timeout_ofAMissingSaga_doesNothing() {
        UUID sagaId = UUID.randomUUID();
        when(sagaRepository.findById(sagaId)).thenReturn(Optional.empty());

        orchestrator.onTimeout(sagaId, Instant.now());

        verifyNoInteractions(outbox);
    }

    // ---- helpers ------------------------------------------------------------------------------------

    private static Reply reply(String name) {
        return REPLIES.stream().filter(r -> r.name().equals(name)).findFirst().orElseThrow();
    }

    private OrderSaga sagaIn(SagaState state) {
        OrderSaga saga = OrderSagas.in(state, order.getId(), Instant.now().plus(STEP_TIMEOUT));
        when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));
        return saga;
    }

    private OrderSaga overdueSagaIn(SagaState state, Instant now) {
        OrderSaga saga = OrderSagas.in(state, order.getId(), now.minusSeconds(1));
        when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));
        return saga;
    }

    private void deliver(UUID messageId, Object reply) {
        ConsumerRecord<String, String> record = TestMessages.record(codec, Topics.SAGA_REPLIES, messageId, reply);
        orchestrator.onReply(record);
    }

    /** Asserts the outbox received exactly one message in total, on {@code topic} keyed by the order, and returns it. */
    private Object onlyMessageSentTo(String topic) {
        ArgumentCaptor<String> topics = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> messages = ArgumentCaptor.forClass(Object.class);
        verify(outbox, times(1)).write(topics.capture(), keys.capture(), messages.capture());
        assertThat(topics.getValue()).isEqualTo(topic);
        assertThat(keys.getValue()).isEqualTo(order.getId().toString());
        return messages.getValue();
    }
}
