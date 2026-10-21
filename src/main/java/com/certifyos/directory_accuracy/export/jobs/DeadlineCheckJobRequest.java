package com.certifyos.directory_accuracy.export.jobs;

/**
 * The deadline check's payload.
 *
 * @param exportBatchId the batch
 * @param attempt the attempt
 * @param check 1 for the deadline check, 2 for the abandon check
 */
public record DeadlineCheckJobRequest(String exportBatchId, int attempt, int check) implements BatchJobRequest {

    @Override
    public Class<DeadlineCheckJob> getJobRequestHandler() {
        return DeadlineCheckJob.class;
    }

    @Override
    public String toString() {
        return exportBatchId + " attempt " + attempt + " check " + check;
    }
}
