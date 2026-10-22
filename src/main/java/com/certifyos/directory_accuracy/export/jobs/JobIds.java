package com.certifyos.directory_accuracy.export.jobs;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Deterministic JobRunr job ids: {@code nameUUIDFromBytes("<job>:<batchId>:<attempt>")}. A repeated
 * enqueue with the same id is a no-op in JobRunr, which is what makes the tick, the event handler
 * and the reconciler safe to run twice.
 */
public final class JobIds {

    private JobIds() {}

    /**
     * The id of a batch job for one attempt.
     *
     * @param job the job name, for example {@code finish}
     * @param exportBatchId the batch
     * @param attempt the attempt the job belongs to
     * @return a stable UUID
     */
    public static UUID of(String job, String exportBatchId, int attempt) {
        return UUID.nameUUIDFromBytes((job + ":" + exportBatchId + ":" + attempt).getBytes(StandardCharsets.UTF_8));
    }

    /**
     * The id of a deadline check, one per check number.
     *
     * @param exportBatchId the batch
     * @param attempt the attempt
     * @param check 1 or 2
     * @return a stable UUID
     */
    public static UUID deadline(String exportBatchId, int attempt, int check) {
        return of("deadline-" + check, exportBatchId, attempt);
    }

    /**
     * The id of an egress request scheduled after a selection-quota refusal, one per wait.
     *
     * @param exportBatchId the batch
     * @param attempt the attempt
     * @param wait the wait number, from 1
     * @return a stable UUID
     */
    public static UUID quotaWait(String exportBatchId, int attempt, int wait) {
        return of(RequestEgressJob.NAME + "-quota-" + wait, exportBatchId, attempt);
    }
}
