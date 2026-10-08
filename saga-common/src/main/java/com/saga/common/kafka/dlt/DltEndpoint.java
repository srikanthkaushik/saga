package com.saga.common.kafka.dlt;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.InvalidEndpointRequestException;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

/**
 * {@code GET  /actuator/dlt}           - DLTs owned by this service with pending counts
 * {@code GET  /actuator/dlt/{topic}}   - one DLT
 * {@code POST /actuator/dlt/{topic}}   - replay pending records; optional body {@code {"limit": n}}
 * <p>
 * {@code topic} is the <em>source</em> topic (e.g. {@code payment.commands}). A service only owns the DLTs of
 * topics its own listeners consume, so each service can only replay into itself.
 * <p>
 * Must be exposed explicitly via {@code management.endpoints.web.exposure.include}. It is unauthenticated:
 * put actuator on a separate management port and/or behind security before production.
 */
@Component
@Endpoint(id = "dlt")
public class DltEndpoint {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 10_000;

    private final DltReplayer replayer;
    private final KafkaListenerEndpointRegistry listenerRegistry;

    public DltEndpoint(DltReplayer replayer, KafkaListenerEndpointRegistry listenerRegistry) {
        this.replayer = replayer;
        this.listenerRegistry = listenerRegistry;
    }

    @ReadOperation
    public List<DltStatus> dlts() {
        return ownedTopics().stream().map(replayer::status).toList();
    }

    @ReadOperation
    public DltStatus dlt(@Selector String topic) {
        return replayer.status(requireOwned(topic));
    }

    @WriteOperation
    public ReplayResult replay(@Selector String topic, @Nullable Integer limit) {
        int effectiveLimit = limit == null ? DEFAULT_LIMIT : limit;
        if (effectiveLimit < 1 || effectiveLimit > MAX_LIMIT) {
            throw new InvalidEndpointRequestException(
                    "limit must be between 1 and " + MAX_LIMIT, "Invalid limit " + effectiveLimit);
        }
        return replayer.replay(requireOwned(topic), effectiveLimit);
    }

    private String requireOwned(String topic) {
        Set<String> owned = ownedTopics();
        if (!owned.contains(topic)) {
            throw new InvalidEndpointRequestException(
                    "Topic '" + topic + "' is not consumed by this service; owned: " + owned, "Unknown topic");
        }
        return topic;
    }

    private Set<String> ownedTopics() {
        Set<String> topics = new TreeSet<>();
        listenerRegistry.getListenerContainers().forEach(container -> {
            String[] containerTopics = container.getContainerProperties().getTopics();
            if (containerTopics != null) {
                topics.addAll(Arrays.asList(containerTopics));
            }
        });
        return topics;
    }
}
