package com.certifyos.directory_accuracy.ingestion.inbound;

/**
 * The batch left the state its claimer read before the acknowledgement could be written. The
 * claim's transaction must roll back; a caller may answer a conflict. Any other {@link
 * IllegalStateException} from a claim is a defect and reaches the 500 mapper.
 */
public class BatchMovedException extends IllegalStateException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message the batch, the state read and what was not written
     */
    public BatchMovedException(String message) {
        super(message);
    }
}
