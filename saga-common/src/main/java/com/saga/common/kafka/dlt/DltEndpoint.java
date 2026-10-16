package com.saga.common.kafka.dlt;

import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;
import org.springframework.boot.actuate.endpoint.InvalidEndpointRequestException;
import org.springframework.boot.actuate.endpoint.annotation.Endpoint;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.annotation.Selector;
import org.springframework.boot.actuate.endpoint.annotation.WriteOperation;
import org.springframework.stereotype.Component;

/**
 * {@code GET  /actuator/dlt}           - DLTs owned by this service with pending counts
 * {@code GET  /actuator/dlt/{topic}}   - one DLT
 * {@code POST /actuator/dlt/{topic}}   - replay pending records; optional body {@code {"limit": n}}
 * <p>
 * {@code topic} is the <em>source</em> topic (e.g. {@code payment.commands}). A service only owns the DLTs of
 * topics its own listeners consume, so each service can only replay into itself.
 * <p>
 * Every read and replay also updates the {@code saga.dlt.pending} gauge, so an alert clears as soon as an
 * operator has replayed rather than at the next scheduled refresh.
 */
@Component
@Endpoint(id = "dlt")
public class DltEndpoint {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 10_000;

    private final DltReplayer replayer;
    private final OwnedTopics ownedTopics;
    private final DltMetrics metrics;

    public DltEndpoint(DltReplayer replayer, OwnedTopics ownedTopics, DltMetrics metrics) {
        this.replayer = replayer;
        this.ownedTopics = ownedTopics;
        this.metrics = metrics;
    }

    @ReadOperation
    public List<DltStatus> dlts() {
        return ownedTopics.get().stream().map(this::status).toList();
    }

    @ReadOperation
    public DltStatus dlt(@Selector String topic) {
        return status(requireOwned(topic));
    }

    @WriteOperation
    public ReplayResult replay(@Selector String topic, @Nullable Integer limit) {
        int effectiveLimit = limit == null ? DEFAULT_LIMIT : limit;
        if (effectiveLimit < 1 || effectiveLimit > MAX_LIMIT) {
            throw new InvalidEndpointRequestException(
                    "limit must be between 1 and " + MAX_LIMIT, "Invalid limit " + effectiveLimit);
        }
        ReplayResult result = replayer.replay(requireOwned(topic), effectiveLimit);
        metrics.record(new DltStatus(result.topic(), result.dltTopic(), result.pending()));
        return result;
    }

    private DltStatus status(String topic) {
        DltStatus status = replayer.status(topic);
        metrics.record(status);
        return status;
    }

    private String requireOwned(String topic) {
        Set<String> owned = ownedTopics.get();
        if (!owned.contains(topic)) {
            throw new InvalidEndpointRequestException(
                    "Topic '" + topic + "' is not consumed by this service; owned: " + owned, "Unknown topic");
        }
        return topic;
    }
}
