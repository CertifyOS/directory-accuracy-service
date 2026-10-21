package com.certifyos.directory_accuracy.ingestion.jobs;

/**
 * The request-ingestion job's payload.
 *
 * @param exportBatchId the batch
 * @param uploadId the upload
 * @param attempt the entry's attempt
 */
public record RequestIngestionJobRequest(String exportBatchId, String uploadId, int attempt)
        implements InboundJobRequest {

    @Override
    public Class<RequestIngestionJob> getJobRequestHandler() {
        return RequestIngestionJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " upload " + uploadId + " attempt " + attempt;
    }
}
