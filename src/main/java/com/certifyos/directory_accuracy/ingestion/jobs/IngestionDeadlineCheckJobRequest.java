package com.certifyos.directory_accuracy.ingestion.jobs;

/**
 * The ingestion deadline check's payload.
 *
 * @param exportBatchId the batch
 * @param uploadId the upload
 * @param attempt the entry's attempt
 */
public record IngestionDeadlineCheckJobRequest(String exportBatchId, String uploadId, int attempt)
        implements InboundJobRequest {

    @Override
    public Class<IngestionDeadlineCheckJob> getJobRequestHandler() {
        return IngestionDeadlineCheckJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " upload " + uploadId + " attempt " + attempt;
    }
}
