package com.certifyos.vendor_exchange.ingestion.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/**
 * What every inbound file job carries: the batch, the upload and the attempt, nothing else. A plain
 * record JobRunr serialises as JSON, as the export lane's {@code BatchJobRequest} does. Not a
 * {@code BatchJobRequest}: the export lane's final-failure filter must never read one of these.
 */
public interface InboundJobRequest extends JobRequest {

    /** The batch the upload belongs to. */
    String exportBatchId();

    /** The upload, {@code <bucket>|<objectName>|<generation>}. */
    String uploadId();

    /** The entry's ingestion attempt the job belongs to. */
    int attempt();
}
