package com.saga.common.kafka.dlt;

/**
 * @param replayed records re-published to {@code topic} by this call
 * @param pending  records still waiting, as of the moment the replay started
 */
public record ReplayResult(String topic, String dltTopic, int replayed, long pending) {
}
