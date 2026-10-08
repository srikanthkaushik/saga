package com.saga.common.messaging;

import java.util.List;

public final class Topics {

    public static final String PAYMENT_COMMANDS = "payment.commands";
    public static final String INVENTORY_COMMANDS = "inventory.commands";
    public static final String SAGA_REPLIES = "order.saga.replies";

    public static final List<String> ALL = List.of(PAYMENT_COMMANDS, INVENTORY_COMMANDS, SAGA_REPLIES);

    /** Spring Kafka's DeadLetterPublishingRecoverer default suffix. */
    public static final String DLT_SUFFIX = "-dlt";

    public static final String HEADER_MESSAGE_ID = "messageId";
    public static final String HEADER_MESSAGE_TYPE = "messageType";

    private Topics() {
    }

    public static String dlt(String topic) {
        return topic + DLT_SUFFIX;
    }
}
