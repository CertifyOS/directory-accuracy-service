package com.certifyos.directory_accuracy.export.jobs;

/**
 * The finish job's payload.
 *
 * @param exportBatchId the batch
 * @param attempt the attempt
 */
public record FinishJobRequest(String exportBatchId, int attempt) implements BatchJobRequest {

    @Override
    public Class<FinishJob> getJobRequestHandler() {
        return FinishJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " attempt " + attempt;
    }
}
