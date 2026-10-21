package com.certifyos.directory_accuracy.ingestion.jobs;

/**
 * The stage-rows job's payload.
 *
 * @param exportBatchId the batch
 * @param uploadId the upload
 * @param attempt the entry's attempt
 */
public record StageRowsJobRequest(String exportBatchId, String uploadId, int attempt) implements InboundJobRequest {

    @Override
    public Class<StageRowsJob> getJobRequestHandler() {
        return StageRowsJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " upload " + uploadId + " attempt " + attempt;
    }
}
