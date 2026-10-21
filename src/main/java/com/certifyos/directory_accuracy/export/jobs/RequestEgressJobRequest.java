package com.certifyos.directory_accuracy.export.jobs;

/**
 * The request-egress job's payload.
 *
 * @param exportBatchId the batch
 * @param attempt the attempt
 */
public record RequestEgressJobRequest(String exportBatchId, int attempt) implements BatchJobRequest {

    @Override
    public Class<RequestEgressJob> getJobRequestHandler() {
        return RequestEgressJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " attempt " + attempt;
    }
}
