package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.export.jobs.JobIds;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.scheduling.exceptions.JobNotFoundException;

/**
 * The one place inbound jobs are enqueued, with deterministic ids ({@code <job>:<uploadId>:<attempt>})
 * and JobRunr's {@code JobRequest} form, as the export lane's {@code JobEnqueuer} does. A repeated
 * enqueue with the same id is a no-op in JobRunr. Never call these inside {@code Transactions.run}:
 * the body may be retried and an enqueue is not a Mongo write.
 */
@ApplicationScoped
public class IngestionJobEnqueuer {

    private final JobRequestScheduler scheduler;

    public IngestionJobEnqueuer(JobRequestScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * The id of the deadline check for an upload attempt. The request job writes it on the entry
     * before {@link #deadlineCheck} schedules a job under the same id.
     *
     * @param uploadId the upload
     * @param attempt the entry's attempt
     * @return a stable UUID
     */
    public static UUID deadlineCheckId(String uploadId, int attempt) {
        return JobIds.of(IngestionDeadlineCheckJob.NAME, uploadId, attempt);
    }

    /** Enqueues the request-ingestion job for an upload attempt and returns its id. */
    public UUID requestIngestion(String exportBatchId, String uploadId, int attempt) {
        UUID id = JobIds.of(RequestIngestionJob.NAME, uploadId, attempt);
        scheduler.enqueue(id, new RequestIngestionJobRequest(exportBatchId, uploadId, attempt));
        return id;
    }

    /** Schedules the ingestion deadline check at {@code at} and returns its id. */
    public UUID deadlineCheck(String exportBatchId, String uploadId, int attempt, Instant at) {
        UUID id = deadlineCheckId(uploadId, attempt);
        scheduler.schedule(id, at, new IngestionDeadlineCheckJobRequest(exportBatchId, uploadId, attempt));
        return id;
    }

    /**
     * Deletes a pending job (a deadline check the completion event made unnecessary). An id JobRunr
     * has never seen is ignored; both not-found exceptions are caught, as in the export lane.
     */
    public void delete(UUID jobId) {
        try {
            scheduler.delete(jobId);
        } catch (JobNotFoundException | org.jobrunr.storage.JobNotFoundException ignored) {
            // already gone, which is the outcome we wanted
        }
    }
}
