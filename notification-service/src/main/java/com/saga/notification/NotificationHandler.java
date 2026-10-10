package com.saga.notification;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.saga.common.idempotency.IdempotencyGuard;
import com.saga.common.messaging.InboundMessage;
import com.saga.common.messaging.MessageCodec;
import com.saga.common.messaging.NonRetryableMessageException;
import com.saga.common.messaging.OrderEvent;
import com.saga.common.messaging.OrderEvent.OrderApproved;
import com.saga.common.messaging.OrderEvent.OrderRejected;
import com.saga.common.messaging.Topics;
import com.saga.notification.domain.Notification;
import com.saga.notification.domain.NotificationRepository;
import com.saga.notification.domain.NotificationType;

/**
 * Tells the customer how their order ended. Not a saga participant: it subscribes to order-service's domain events
 * (choreography) and nothing waits for it. It runs after the pivot, so it must never fail the order - it can only
 * retry: a provider failure is retried with backoff, then dead-lettered for replay.
 * <p>
 * The provider is called before the row is committed, so a crash between the two re-sends on redelivery
 * (at-least-once, like any real e-mail gateway). The (order_id, type) check and processed_message make every
 * redelivery or replay after a successful send a no-op.
 */
@Component
public class NotificationHandler {

    private static final Logger log = LoggerFactory.getLogger(NotificationHandler.class);
    private static final String CHANNEL = "EMAIL";

    private final MessageCodec codec;
    private final IdempotencyGuard idempotencyGuard;
    private final NotificationRepository notificationRepository;
    private final NotificationProvider provider;

    public NotificationHandler(MessageCodec codec,
                               IdempotencyGuard idempotencyGuard,
                               NotificationRepository notificationRepository,
                               NotificationProvider provider) {
        this.codec = codec;
        this.idempotencyGuard = idempotencyGuard;
        this.notificationRepository = notificationRepository;
        this.provider = provider;
    }

    @KafkaListener(topics = Topics.ORDER_EVENTS)
    @Transactional
    public void onEvent(ConsumerRecord<String, String> record) {
        InboundMessage inbound = codec.decode(record);
        if (!idempotencyGuard.firstDelivery(inbound.messageId())) {
            log.debug("Duplicate event {} ignored", inbound.messageId());
            return;
        }
        if (!(inbound.payload() instanceof OrderEvent event)) {
            throw new NonRetryableMessageException(
                    "Non-event message " + inbound.payload().getClass().getSimpleName() + " on " + record.topic());
        }

        NotificationType type = switch (event) {
            case OrderApproved a -> NotificationType.ORDER_APPROVED;
            case OrderRejected r -> NotificationType.ORDER_REJECTED;
        };
        if (notificationRepository.existsByOrderIdAndType(event.orderId(), type)) {
            log.info("Order {} already notified of {}", event.orderId(), type);
            return;
        }

        String recipient = event.customerId() + "@customers.example";
        String subject = switch (event) {
            case OrderApproved a -> "Your order " + shortId(a.orderId()) + " is on its way";
            case OrderRejected r -> "We could not complete your order " + shortId(r.orderId());
        };
        String body = switch (event) {
            case OrderApproved a -> "We charged %s for %d x %s. It ships to %s with tracking number %s."
                    .formatted(money(a.amount()), a.quantity(), a.productId(), a.destination(), a.trackingNumber());
            case OrderRejected r -> "Your order for %d x %s could not be completed: %s. Nothing was charged, or any charge has been refunded."
                    .formatted(r.quantity(), r.productId(), r.reason());
        };

        provider.send(CHANNEL, recipient, subject, body);
        notificationRepository.save(new Notification(event.orderId(), type, event.customerId(), CHANNEL, recipient, subject, body));
        log.info("Order {} -> {} sent to {}", event.orderId(), type, recipient);
    }

    private static String shortId(UUID id) {
        return id.toString().substring(0, 8);
    }

    private static String money(BigDecimal amount) {
        return amount == null ? "-" : amount.setScale(2, RoundingMode.HALF_UP).toPlainString();
    }
}
