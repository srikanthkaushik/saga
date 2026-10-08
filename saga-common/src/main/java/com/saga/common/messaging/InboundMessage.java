package com.saga.common.messaging;

import java.util.UUID;

public record InboundMessage(UUID messageId, Object payload) {
}
