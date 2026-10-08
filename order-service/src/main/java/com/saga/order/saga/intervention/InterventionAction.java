package com.saga.order.saga.intervention;

import java.util.Arrays;
import java.util.Optional;

public enum InterventionAction {
    /** Re-send the current compensation command now and restart the stuck count. */
    RETRY,
    /**
     * The compensation was completed by hand in the participant's own records (payment REFUNDED, reservation
     * RELEASED); close the saga as FAILED. Because the participant's record shows it done, any queued or
     * DLT-replayed compensation for this order is a no-op there - no double refund/release.
     */
    RESOLVE;

    public static Optional<InterventionAction> parse(String value) {
        return Arrays.stream(values()).filter(a -> a.name().equalsIgnoreCase(value)).findFirst();
    }
}
