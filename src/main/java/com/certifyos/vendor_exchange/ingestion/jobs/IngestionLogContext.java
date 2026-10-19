package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.export.jobs.JobLogContext;
import org.jboss.logging.MDC;
import org.jobrunr.jobs.context.JobContext;

/**
 * The keys every ingestion job log line carries, put on the MDC for the job's duration. An inbound
 * file job carries {@code exportBatchId}, {@code uploadId} and {@code jobId} from the start and
 * {@code tenantId} once the entry is loaded; a release job carries {@code tenantId}, {@code
 * releaseId} and {@code jobId}. The batch, tenant and job keys are the export lane's names, so one
 * Cloud Logging filter follows a batch across both lanes. Closing removes every key, since JobRunr
 * reuses worker threads.
 */
public final class IngestionLogContext implements AutoCloseable {

    /** MDC key: the export batch. */
    public static final String EXPORT_BATCH_ID = JobLogContext.EXPORT_BATCH_ID;

    /** MDC key: the inbound upload. */
    public static final String UPLOAD_ID = "uploadId";

    /** MDC key: the tenant. */
    public static final String TENANT_ID = JobLogContext.TENANT_ID;

    /** MDC key: the JobRunr job id. */
    public static final String JOB_ID = JobLogContext.JOB_ID;

    /** MDC key: the field release. */
    public static final String RELEASE_ID = "releaseId";

    private IngestionLogContext() {}

    /**
     * Opens the context for one inbound file job run.
     *
     * @param exportBatchId the batch
     * @param uploadId the upload
     * @param job JobRunr's context, null or {@code JobContext.Null} outside a job run
     * @return the context to close when the run ends
     */
    public static IngestionLogContext open(String exportBatchId, String uploadId, JobContext job) {
        put(EXPORT_BATCH_ID, exportBatchId);
        put(UPLOAD_ID, uploadId);
        putJob(job);
        return new IngestionLogContext();
    }

    /**
     * Opens the context for one release job run.
     *
     * @param tenantId the tenant
     * @param releaseId the release
     * @param job JobRunr's context, null or {@code JobContext.Null} outside a job run
     * @return the context to close when the run ends
     */
    public static IngestionLogContext openRelease(String tenantId, String releaseId, JobContext job) {
        put(TENANT_ID, tenantId);
        put(RELEASE_ID, releaseId);
        putJob(job);
        return new IngestionLogContext();
    }

    /**
     * Adds the tenant once it is known.
     *
     * @param tenantId the tenant
     */
    public static void tenant(String tenantId) {
        put(TENANT_ID, tenantId);
    }

    private static void putJob(JobContext job) {
        if (job != null && job != JobContext.Null && job.getJobId() != null) {
            MDC.put(JOB_ID, job.getJobId().toString());
        }
    }

    private static void put(String key, String value) {
        if (value != null) {
            MDC.put(key, value);
        }
    }

    @Override
    public void close() {
        MDC.remove(EXPORT_BATCH_ID);
        MDC.remove(UPLOAD_ID);
        MDC.remove(TENANT_ID);
        MDC.remove(JOB_ID);
        MDC.remove(RELEASE_ID);
    }
}
