package com.certifyos.vendor_exchange.ingestion.release;

/**
 * Another release of the same practitioner, or an unwritten one at one of its locations, is {@code
 * RELEASING} (plan P12): the claiming job has put its own fields back to {@code RELEASE_REQUESTED} and
 * throws this; the release job schedules itself again rather than failing, so the wait never spends
 * JobRunr's retries. Two merges never write one slice at once.
 */
public class ReleaseInProgressException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public ReleaseInProgressException(String message) {
        super(message);
    }
}
