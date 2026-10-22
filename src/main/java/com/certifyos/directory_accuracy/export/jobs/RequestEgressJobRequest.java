package com.certifyos.directory_accuracy.export.jobs;

/**
 * The request-egress job's payload.
 *
 * @param exportBatchId the batch
 * @param attempt the attempt
 * @param quotaWait 0 for the first request; the wait number when scheduled after egress's
 *     selection quota refused an earlier one
 */
public record RequestEgressJobRequest(String exportBatchId, int attempt, int quotaWait) implements BatchJobRequest {

    @Override
    public Class<RequestEgressJob> getJobRequestHandler() {
        return RequestEgressJob.class;
    }

    @Override
    public String toString() {
        return quotaWait == 0
                ? exportBatchId + " attempt " + attempt
                : exportBatchId + " attempt " + attempt + " quota wait " + quotaWait;
    }
}
