package com.certifyos.directory_accuracy.export.batch;

/**
 * The schedule changed between the read and the batch transaction (an operator disabled or
 * replaced it), so the compare-and-set on its version failed and the transaction was abandoned.
 */
public class StaleScheduleException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public StaleScheduleException(String scheduleId, long expectedVersion) {
        super("schedule " + scheduleId + " is no longer at version " + expectedVersion);
    }
}
