package com.saga.order.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** The saga state machine on its own: every transition is legal from exactly one state, and nowhere else. */
class OrderSagaTest {

    private static final Instant DEADLINE = Instant.parse("2030-01-01T00:00:00Z");

    private record Transition(String name, SagaState from, SagaState to, Consumer<OrderSaga> apply) {
        @Override
        public String toString() {
            return name;
        }
    }

    private static final List<Transition> TRANSITIONS = List.of(
            new Transition("paymentCompleted", SagaState.PAYMENT_PENDING, SagaState.INVENTORY_PENDING, s -> s.paymentCompleted(DEADLINE)),
            new Transition("paymentFailed", SagaState.PAYMENT_PENDING, SagaState.FAILED, s -> s.paymentFailed("x")),
            new Transition("paymentTimedOut", SagaState.PAYMENT_PENDING, SagaState.COMPENSATING, s -> s.paymentTimedOut(DEADLINE)),
            new Transition("inventoryReserved", SagaState.INVENTORY_PENDING, SagaState.SHIPPING_PENDING, s -> s.inventoryReserved(DEADLINE)),
            new Transition("inventoryFailed", SagaState.INVENTORY_PENDING, SagaState.COMPENSATING, s -> s.inventoryFailed("x", DEADLINE)),
            new Transition("inventoryTimedOut", SagaState.INVENTORY_PENDING, SagaState.RELEASING_INVENTORY, s -> s.inventoryTimedOut(DEADLINE)),
            new Transition("shipmentScheduled", SagaState.SHIPPING_PENDING, SagaState.COMPLETED, OrderSaga::shipmentScheduled),
            new Transition("shipmentFailed", SagaState.SHIPPING_PENDING, SagaState.RELEASING_INVENTORY, s -> s.shipmentFailed("x", DEADLINE)),
            new Transition("shippingTimedOut", SagaState.SHIPPING_PENDING, SagaState.CANCELLING_SHIPMENT, s -> s.shippingTimedOut(DEADLINE)),
            new Transition("shipmentCancelled", SagaState.CANCELLING_SHIPMENT, SagaState.RELEASING_INVENTORY, s -> s.shipmentCancelled(DEADLINE)),
            new Transition("inventoryReleased", SagaState.RELEASING_INVENTORY, SagaState.COMPENSATING, s -> s.inventoryReleased(DEADLINE)),
            new Transition("compensated", SagaState.COMPENSATING, SagaState.FAILED, OrderSaga::compensated));

    static Stream<Transition> legalTransitions() {
        return TRANSITIONS.stream();
    }

    static Stream<Arguments> illegalTransitions() {
        return TRANSITIONS.stream().flatMap(transition -> Arrays.stream(SagaState.values())
                .filter(state -> state != transition.from())
                .map(state -> Arguments.of(transition, state)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("legalTransitions")
    void transition_fromItsState_movesOn_andSetsOrClearsTheDeadline(Transition transition) {
        OrderSaga saga = OrderSagas.in(transition.from());

        transition.apply().accept(saga);

        assertThat(saga.getState()).isEqualTo(transition.to());
        if (transition.to().isTerminal()) {
            assertThat(saga.getDeadline()).isNull();
        } else {
            assertThat(saga.getDeadline()).isEqualTo(DEADLINE);
        }
    }

    @ParameterizedTest(name = "{0} from {1}")
    @MethodSource("illegalTransitions")
    void transition_fromAnyOtherState_isRefused_andChangesNothing(Transition transition, SagaState state) {
        OrderSaga saga = OrderSagas.in(state);
        Instant deadline = saga.getDeadline();
        Instant updatedAt = saga.getUpdatedAt();
        String reason = saga.getFailureReason();

        assertThatThrownBy(() -> transition.apply().accept(saga))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("cannot move " + state);

        assertThat(saga.getState()).isEqualTo(state);
        assertThat(saga.getDeadline()).isEqualTo(deadline);
        assertThat(saga.getUpdatedAt()).isEqualTo(updatedAt);
        assertThat(saga.getFailureReason()).isEqualTo(reason);
    }

    @Test
    void happyPath_neverCompensates() {
        OrderSaga saga = new OrderSaga(UUID.randomUUID(), DEADLINE);
        saga.paymentCompleted(DEADLINE);
        saga.inventoryReserved(DEADLINE);
        saga.shipmentScheduled();

        assertThat(saga.getState()).isEqualTo(SagaState.COMPLETED);
        assertThat(saga.getCompensatingSince()).isNull();
        assertThat(saga.getFailureReason()).isNull();
    }

    @Test
    void reverseOrderCompensation_keepsTheFirstCompensationTime_andResetsResendsPerStep() {
        OrderSaga saga = OrderSagas.in(SagaState.SHIPPING_PENDING);
        saga.shippingTimedOut(DEADLINE);
        Instant since = saga.getCompensatingSince();
        saga.resendCompensation(DEADLINE);
        saga.resendCompensation(DEADLINE);
        assertThat(saga.getCompensationResends()).isEqualTo(2);

        saga.shipmentCancelled(DEADLINE);
        assertThat(saga.getCompensationResends()).isZero();
        saga.resendCompensation(DEADLINE);
        saga.inventoryReleased(DEADLINE);
        assertThat(saga.getCompensationResends()).isZero();
        saga.compensated();

        assertThat(saga.getState()).isEqualTo(SagaState.FAILED);
        assertThat(saga.getCompensatingSince()).isEqualTo(since).isNotNull();
        assertThat(saga.getFailureReason()).isEqualTo("Shipping timed out");
        assertThat(saga.getDeadline()).isNull();
    }

    @Test
    void failureReason_isTheFirstFailure_notOverwrittenByLaterSteps() {
        OrderSaga saga = OrderSagas.in(SagaState.SHIPPING_PENDING);
        saga.shipmentFailed("No carrier serves XX", DEADLINE);
        saga.inventoryReleased(DEADLINE);
        saga.compensated();

        assertThat(saga.getFailureReason()).isEqualTo("No carrier serves XX");
    }

    @ParameterizedTest
    @EnumSource(value = SagaState.class, names = {"CANCELLING_SHIPMENT", "RELEASING_INVENTORY", "COMPENSATING"})
    void compensatingStates_acceptResendRetryAndResolve(SagaState state) {
        OrderSaga saga = OrderSagas.in(state);
        Instant later = DEADLINE.plusSeconds(60);

        assertThat(saga.resendCompensation(later)).isEqualTo(1);
        assertThat(saga.resendCompensation(later)).isEqualTo(2);
        assertThat(saga.getState()).isEqualTo(state);
        assertThat(saga.getDeadline()).isEqualTo(later);

        saga.retryCompensationManually(DEADLINE);
        assertThat(saga.getCompensationResends()).isZero();
        assertThat(saga.getState()).isEqualTo(state);

        saga.resolveManually();
        assertThat(saga.getState()).isEqualTo(SagaState.FAILED);
        assertThat(saga.getDeadline()).isNull();
    }

    @ParameterizedTest
    @EnumSource(value = SagaState.class, names = {"CANCELLING_SHIPMENT", "RELEASING_INVENTORY", "COMPENSATING"},
            mode = EnumSource.Mode.EXCLUDE)
    void otherStates_refuseResendRetryAndResolve(SagaState state) {
        OrderSaga saga = OrderSagas.in(state);

        assertThatThrownBy(() -> saga.resendCompensation(DEADLINE)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> saga.retryCompensationManually(DEADLINE)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(saga::resolveManually).isInstanceOf(IllegalStateException.class);
        assertThat(saga.getState()).isEqualTo(state);
        assertThat(saga.getCompensationResends()).isZero();
    }

    @Test
    void isOverdue_atAndAfterTheDeadline_neverWithoutOne() {
        OrderSaga pending = new OrderSaga(UUID.randomUUID(), DEADLINE);
        assertThat(pending.isOverdue(DEADLINE.minusMillis(1))).isFalse();
        assertThat(pending.isOverdue(DEADLINE)).isTrue();

        OrderSaga completed = OrderSagas.in(SagaState.COMPLETED);
        assertThat(completed.isOverdue(Instant.MAX)).isFalse();
    }
}
