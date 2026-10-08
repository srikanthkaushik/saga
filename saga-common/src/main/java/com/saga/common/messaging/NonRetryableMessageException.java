package com.saga.common.messaging;

/**
 * A message that can never succeed no matter how often it is redelivered (malformed, unknown type,
 * references a saga that does not exist). The Kafka error handler skips retries and sends it straight
 * to the dead-letter topic.
 */
public class NonRetryableMessageException extends RuntimeException {

    public NonRetryableMessageException(String message) {
        super(message);
    }

    public NonRetryableMessageException(String message, Throwable cause) {
        super(message, cause);
    }
}
