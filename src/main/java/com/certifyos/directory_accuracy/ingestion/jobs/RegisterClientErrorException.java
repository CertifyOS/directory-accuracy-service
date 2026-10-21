package com.certifyos.directory_accuracy.ingestion.jobs;

import jakarta.ws.rs.WebApplicationException;

/**
 * file-ingestion answered the register call with a 4xx other than the two that fail the entry at
 * once (422 {@code MAPPING_UNKNOWN}, 403 {@code FILE_INGESTION_REFUSED}): most likely it could not
 * read the archive object (the design's proposed {@code SOURCE_UNREADABLE}). Thrown for JobRunr's
 * retry, as the refused call itself would be; its own type is what lets the final-failure filter
 * record {@code SOURCE_UNREADABLE} instead of {@code JOB_RETRIES_EXHAUSTED} when the retries run
 * out. The message names the status only, never the problem body, because JobRunr stores it and the
 * filter copies it into {@code lastError}.
 */
public class RegisterClientErrorException extends WebApplicationException {

    private static final long serialVersionUID = 1L;

    /**
     * Wraps the refused register call.
     *
     * @param status the 4xx file-ingestion answered
     * @param refused the REST client's exception, kept as the cause with its response
     */
    public RegisterClientErrorException(int status, WebApplicationException refused) {
        super("file-ingestion answered register with HTTP " + status, refused, refused.getResponse());
    }
}
