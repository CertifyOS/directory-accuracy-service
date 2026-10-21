package com.certifyos.directory_accuracy.export.jobs;

import org.jboss.logging.MDC;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.server.runner.ThreadLocalJobContext;

/**
 * The three keys every job log line carries, put on the MDC for the job's duration: {@code
 * exportBatchId} and {@code jobId} when the job starts, {@code tenantId} once the batch row is
 * loaded. The JSON console formatter writes the MDC on every line, so a batch's whole history is
 * one filter in Cloud Logging. Closing removes all three, since JobRunr reuses worker threads.
 */
public final class JobLogContext implements AutoCloseable {

    /** MDC key: the export batch. */
    public static final String EXPORT_BATCH_ID = "exportBatchId";

    /** MDC key: the tenant. */
    public static final String TENANT_ID = "tenantId";

    /** MDC key: the JobRunr job id. */
    public static final String JOB_ID = "jobId";

    private JobLogContext() {}

    /**
     * Opens the context for one job run.
     *
     * @param exportBatchId the batch
     * @param job JobRunr's context, null or {@code JobContext.Null} outside a job run
     * @return the context to close when the run ends
     */
    /**
     * The job JobRunr is running on this thread, or null when the handler was called directly (a
     * test, or an operator tool). {@code JobRequestHandler.jobContext()} throws in that case.
     *
     * @return the current job context, or null
     */
    public static JobContext currentJob() {
        return ThreadLocalJobContext.hasJobContext() ? ThreadLocalJobContext.getJobContext() : null;
    }

    public static JobLogContext open(String exportBatchId, JobContext job) {
        MDC.put(EXPORT_BATCH_ID, exportBatchId);
        if (job != null && job != JobContext.Null && job.getJobId() != null) {
            MDC.put(JOB_ID, job.getJobId().toString());
        }
        return new JobLogContext();
    }

    /**
     * Adds the tenant once it is known.
     *
     * @param tenantId the tenant
     */
    public static void tenant(String tenantId) {
        if (tenantId != null) {
            MDC.put(TENANT_ID, tenantId);
        }
    }

    @Override
    public void close() {
        MDC.remove(EXPORT_BATCH_ID);
        MDC.remove(TENANT_ID);
        MDC.remove(JOB_ID);
    }
}
