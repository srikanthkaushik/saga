package com.saga.notification;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Stand-in for an e-mail gateway: "sending" logs the message. Its availability can be switched off at runtime
 * (actuator {@code notificationprovider}) to demonstrate a retriable step: sends fail, the event is retried with
 * backoff and then dead-lettered, and replaying it once the provider is back delivers it - without ever affecting
 * the order, which was decided before the event was published.
 */
@Component
public class NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(NotificationProvider.class);

    private final AtomicBoolean available = new AtomicBoolean(true);
    private final AtomicLong sent = new AtomicLong();
    private final AtomicLong refused = new AtomicLong();

    /**
     * @throws ProviderUnavailableException while the provider is switched off (retryable)
     */
    public void send(String channel, String recipient, String subject, String body) {
        if (!available.get()) {
            refused.incrementAndGet();
            throw new ProviderUnavailableException("Notification provider unavailable; " + channel + " to " + recipient + " not sent");
        }
        sent.incrementAndGet();
        log.info("{} to {}: {} | {}", channel, recipient, subject, body);
    }

    public boolean isAvailable() {
        return available.get();
    }

    public void setAvailable(boolean value) {
        boolean previous = available.getAndSet(value);
        if (previous != value) {
            log.warn("Notification provider is now {}", value ? "AVAILABLE" : "UNAVAILABLE");
        }
    }

    public long sent() {
        return sent.get();
    }

    public long refused() {
        return refused.get();
    }

    public static class ProviderUnavailableException extends RuntimeException {
        ProviderUnavailableException(String message) {
            super(message);
        }
    }
}
