package com.saga.notification;

import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

/**
 * {@code GET /actuator/notificationprovider} - is the simulated gateway up, and how many sends went through or
 * were refused since start. {@code POST /actuator/notificationprovider {"available": false}} switches it off
 * (operator role) to simulate an outage.
 */
@Component
@Endpoint(id = "notificationprovider")
public class NotificationProviderEndpoint {

    private final NotificationProvider provider;

    public NotificationProviderEndpoint(NotificationProvider provider) {
        this.provider = provider;
    }

    @ReadOperation
    public Status status() {
        return new Status(provider.isAvailable(), provider.sent(), provider.refused());
    }

    @WriteOperation
    public Status setAvailable(boolean available) {
        provider.setAvailable(available);
        return status();
    }

    public record Status(boolean available, long sent, long refused) {
    }
}
