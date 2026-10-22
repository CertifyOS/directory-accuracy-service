package com.certifyos.directory_accuracy.export.jobs;

import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.UUID;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.scheduling.exceptions.JobNotFoundException;

/**
 * The one place jobs are enqueued, with deterministic ids and JobRunr's {@code JobRequest} form: a
 * plain record is serialised as JSON and its handler bean is resolved from CDI when the job runs.
 * No lambda, so no bytecode analysis (which fails inside Quarkus-transformed bean classes). Never
 * call these inside {@code Transactions.run}: the body may be retried and an enqueue is not a Mongo
 * write.
 */
@ApplicationScoped
public class JobEnqueuer {

    private final JobRequestScheduler scheduler;

    public JobEnqueuer(JobRequestScheduler scheduler) {
        this.scheduler = scheduler;
    }

    /** Enqueues the egress request job for a batch attempt and returns its id. */
    public UUID requestEgress(String exportBatchId, int attempt) {
        UUID id = JobIds.of(RequestEgressJob.NAME, exportBatchId, attempt);
        scheduler.enqueue(id, new RequestEgressJobRequest(exportBatchId, attempt, 0));
        return id;
    }

    /**
     * Schedules the egress request job again after egress refused it for the tenant's selection
     * quota; each wait is a new job, so JobRunr's retries stay untouched.
     */
    public UUID requestEgressAfterQuota(String exportBatchId, int attempt, int wait, Instant at) {
        UUID id = JobIds.quotaWait(exportBatchId, attempt, wait);
        scheduler.schedule(id, at, new RequestEgressJobRequest(exportBatchId, attempt, wait));
        return id;
    }

    /** Schedules a deadline check at a future instant and returns its id. */
    public UUID deadlineCheck(String exportBatchId, int attempt, int check, Instant at) {
        UUID id = JobIds.deadline(exportBatchId, attempt, check);
        scheduler.schedule(id, at, new DeadlineCheckJobRequest(exportBatchId, attempt, check));
        return id;
    }

    /** Enqueues the finish job for a batch attempt and returns its id. */
    public UUID finish(String exportBatchId, int attempt) {
        UUID id = JobIds.of(FinishJob.NAME, exportBatchId, attempt);
        scheduler.enqueue(id, new FinishJobRequest(exportBatchId, attempt));
        return id;
    }

    /**
     * Deletes a pending job (a deadline check that is no longer needed). JobRunr keeps the row in
     * state {@code DELETED} rather than removing it; an id it has never seen is ignored. Both
     * not-found exceptions are caught: the scheduler declares one, the storage provider throws
     * another.
     */
    public void delete(UUID jobId) {
        try {
            scheduler.delete(jobId);
        } catch (JobNotFoundException | org.jobrunr.storage.JobNotFoundException ignored) {
            // already gone, which is the outcome we wanted
        }
    }
}
