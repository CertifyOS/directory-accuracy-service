package com.certifyos.vendor_exchange.ingestion.release;

/**
 * Another release of the same practitioner is {@code RELEASING} (plan P12): the claiming job has put
 * its own fields back to {@code RELEASE_REQUESTED} and throws this so JobRunr retries it later, after
 * the other release has finished. Two merges never write one practitioner's slice at once.
 */
public class ReleaseInProgressException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ReleaseInProgressException(String message) {
        super(message);
    }
}
