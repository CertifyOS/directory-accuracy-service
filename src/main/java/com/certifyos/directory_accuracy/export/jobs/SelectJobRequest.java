package com.certifyos.directory_accuracy.export.jobs;

/**
 * The select job's payload.
 *
 * @param exportBatchId the batch
 * @param attempt the attempt
 */
public record SelectJobRequest(String exportBatchId, int attempt) implements BatchJobRequest {

    @Override
    public Class<SelectJob> getJobRequestHandler() {
        return SelectJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " attempt " + attempt;
    }
}
