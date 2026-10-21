package com.certifyos.directory_accuracy.export.jobs;

import org.jobrunr.jobs.lambdas.JobRequest;

/**
 * What every batch job carries: the batch id and the attempt, nothing else. A plain record JobRunr
 * serialises as JSON, so no bytecode analysis of a lambda is involved and every worker version can
 * read it. The final-failure filter reads the batch id from here.
 */
public interface BatchJobRequest extends JobRequest {

    /** The batch the job works on. */
    String exportBatchId();

    /** The attempt the job belongs to. */
    int attempt();
}
