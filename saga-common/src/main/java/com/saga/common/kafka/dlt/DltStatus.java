package com.saga.common.kafka.dlt;

/**
 * @param pending dead-lettered records not yet replayed
 */
public record DltStatus(String topic, String dltTopic, long pending) {
}
