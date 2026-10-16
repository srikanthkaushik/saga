package com.saga.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.idempotency.ProcessedMessageRepository;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.OrderEvent.OrderApproved;
import com.saga.common.messaging.OrderEvent.OrderRejected;
import com.saga.common.messaging.SagaCommand.RefundPayment;
import com.saga.common.messaging.Topics;
import com.saga.common.test.PostgresSliceTest;
import com.saga.common.test.TestMessages;
import com.saga.notification.NotificationProvider.ProviderUnavailableException;
import com.saga.notification.domain.Notification;
import com.saga.notification.domain.NotificationRepository;
import com.saga.notification.domain.NotificationType;

/**
 * Each order outcome is notified at most once, however often its event is delivered, and a provider outage is a
 * retriable failure that leaves nothing behind.
 */
@PostgresSliceTest
@Import({NotificationHandler.class, NotificationProvider.class})
class NotificationHandlerTest {

    @Autowired
    private NotificationHandler handler;
    @Autowired
    private NotificationProvider provider;
    @Autowired
    private MessageCodec codec;
    @Autowired
    private NotificationRepository notifications;
    @Autowired
    private ProcessedMessageRepository processed;

    private final UUID orderId = UUID.randomUUID();

    @AfterEach
    void providerBackOn() {
        provider.setAvailable(true);
    }

    @Test
    void duplicateDelivery_notifiesOnce() {
        UUID messageId = UUID.randomUUID();
        long sentBefore = provider.sent();

        deliver(messageId, approved());
        deliver(messageId, approved());

        assertThat(notifications.findByOrderIdOrderBySentAt(orderId))
                .extracting(Notification::getType)
                .containsExactly(NotificationType.ORDER_APPROVED);
        assertThat(provider.sent() - sentBefore).isEqualTo(1);
    }

    @Test
    void sameOutcomeUnderANewMessageId_isNotNotifiedTwice() {
        long sentBefore = provider.sent();

        deliver(UUID.randomUUID(), rejected());
        deliver(UUID.randomUUID(), rejected());

        assertThat(notifications.findByOrderIdOrderBySentAt(orderId))
                .extracting(Notification::getType)
                .containsExactly(NotificationType.ORDER_REJECTED);
        assertThat(provider.sent() - sentBefore).isEqualTo(1);
    }

    @Test
    void sagaCommandOnTheEventTopic_isNonRetryable() {
        assertThatThrownBy(() -> deliver(UUID.randomUUID(), new RefundPayment(UUID.randomUUID(), orderId)))
                .isInstanceOf(NonRetryableMessageException.class)
                .hasMessageContaining("RefundPayment");
    }

    /** The outage rolls back the processed marker, so the redelivery (retry or DLT replay) sends the e-mail. */
    @Test
    @Transactional(propagation = Propagation.NOT_SUPPORTED)
    void providerOutage_isRetriable_andTheRedeliveryNotifiesOnce() {
        UUID messageId = UUID.randomUUID();
        provider.setAvailable(false);

        assertThatThrownBy(() -> deliver(messageId, approved())).isInstanceOf(ProviderUnavailableException.class);
        assertThat(processed.existsById(messageId)).isFalse();
        assertThat(notifications.findByOrderIdOrderBySentAt(orderId)).isEmpty();

        provider.setAvailable(true);
        deliver(messageId, approved());
        deliver(messageId, approved());

        assertThat(notifications.findByOrderIdOrderBySentAt(orderId)).hasSize(1);
    }

    private OrderApproved approved() {
        return new OrderApproved(orderId, "customer-1", "product-1", 2, new BigDecimal("20.00"), "GB", "TRK-1");
    }

    private OrderRejected rejected() {
        return new OrderRejected(orderId, "customer-1", "product-1", 2, new BigDecimal("20.00"), "GB", "Insufficient credit");
    }

    private void deliver(UUID messageId, Object event) {
        handler.onEvent(TestMessages.record(codec, Topics.ORDER_EVENTS, messageId, event));
    }
}
