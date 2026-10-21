package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.export.jobs.JobIds;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;
import java.util.UUID;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.scheduling.JobRequestScheduler;
import org.jobrunr.scheduling.exceptions.JobNotFoundException;
import org.jobrunr.storage.StorageProvider;

/**
 * The one place inbound jobs are enqueued, with deterministic ids ({@code <job>:<uploadId>:<attempt>})
 * and JobRunr's {@code JobRequest} form, as the export lane's {@code JobEnqueuer} does. A repeated
 * enqueue with the same id is a no-op in JobRunr. Never call these inside {@code Transactions.run}:
 * the body may be retried and an enqueue is not a Mongo write. Also the one place JobRunr's storage
 * is read for a job's state, for the reconciler's question "is the entry's job still going to run?".
 */
@ApplicationScoped
public class IngestionJobEnqueuer {

    /** Job states in which JobRunr will still run the job: it waits, is due, or runs now. */
    private static final Set<StateName> PENDING =
            EnumSet.of(StateName.AWAITING, StateName.SCHEDULED, StateName.ENQUEUED, StateName.PROCESSING);

    private final JobRequestScheduler scheduler;
    private final StorageProvider storage;

    public IngestionJobEnqueuer(JobRequestScheduler scheduler, StorageProvider storage) {
        this.scheduler = scheduler;
        this.storage = storage;
    }

    /**
     * The id of the deadline check for an upload attempt: what {@link #deadlineCheck} schedules and
     * the request job then writes on the entry as its {@code jobId}.
     *
     * @param uploadId the upload
     * @param attempt the entry's attempt
     * @return a stable UUID
     */
    public static UUID deadlineCheckId(String uploadId, int attempt) {
        return JobIds.of(IngestionDeadlineCheckJob.NAME, uploadId, attempt);
    }

    /**
     * The id of the reconciler's {@code reconciliation}-th re-enqueue of an entry's job: {@code
     * <job>:<uploadId>:reconciled:<n>:<attempt>}. Distinct from the first id and from every other
     * re-enqueue, so a job JobRunr already holds under an earlier id, in whatever state, never turns
     * the re-enqueue into a no-op.
     *
     * @param job the job name, for example {@link StageRowsJob#NAME}
     * @param uploadId the upload
     * @param reconciliation the entry's {@code reconciliations} after this re-enqueue, from 1
     * @param attempt the entry's attempt
     * @return a stable UUID
     */
    public static UUID reconciledId(String job, String uploadId, int reconciliation, int attempt) {
        return JobIds.of(job, uploadId + ":reconciled:" + reconciliation, attempt);
    }

    /** Enqueues the request-ingestion job for an upload attempt and returns its id. */
    public UUID requestIngestion(String exportBatchId, String uploadId, int attempt) {
        UUID id = JobIds.of(RequestIngestionJob.NAME, uploadId, attempt);
        scheduler.enqueue(id, new RequestIngestionJobRequest(exportBatchId, uploadId, attempt));
        return id;
    }

    /**
     * The stage-rows job id of an upload attempt, the one place it is computed.
     *
     * @param uploadId the upload
     * @param attempt the entry attempt
     * @return the deterministic job id
     */
    public static UUID stageRowsId(String uploadId, int attempt) {
        return JobIds.of(StageRowsJob.NAME, uploadId, attempt);
    }

    /** Enqueues the stage-rows job for a parsed upload attempt and returns its id. */
    public UUID stageRows(String exportBatchId, String uploadId, int attempt) {
        UUID id = stageRowsId(uploadId, attempt);
        scheduler.enqueue(id, new StageRowsJobRequest(exportBatchId, uploadId, attempt));
        return id;
    }

    /** Schedules the ingestion deadline check at {@code at} and returns its id. */
    public UUID deadlineCheck(String exportBatchId, String uploadId, int attempt, Instant at) {
        UUID id = deadlineCheckId(uploadId, attempt);
        scheduler.schedule(id, at, new IngestionDeadlineCheckJobRequest(exportBatchId, uploadId, attempt));
        return id;
    }

    /**
     * The id of a release's job: unique per release, so a second release of the practitioner gets
     * its own job (plan P12) and a repeated enqueue of one release is a no-op.
     *
     * @param releaseId the release
     * @return a stable UUID
     */
    public static UUID releaseJobId(String releaseId) {
        return JobIds.of(ReleaseJob.NAME, releaseId, 1);
    }

    /** Enqueues the release job for one release of one practitioner and returns its id. */
    public UUID release(String tenantId, String certifyPractitionerId, String releaseId) {
        UUID id = releaseJobId(releaseId);
        scheduler.enqueue(id, new ReleaseJobRequest(tenantId, certifyPractitionerId, releaseId));
        return id;
    }

    /**
     * The id of a release job scheduled again because the release had to wait: one per second it is
     * due at, so each wait is a new job with JobRunr's retries untouched.
     *
     * @param releaseId the release
     * @param at when the job is due
     * @return a stable UUID for that release and second
     */
    public static UUID releaseWaitId(String releaseId, Instant at) {
        return JobIds.of(ReleaseJob.NAME, releaseId + ":wait:" + at.getEpochSecond(), 1);
    }

    /**
     * The id of a release job the release reconciler enqueues: one per second of the pass, so an earlier
     * release job JobRunr holds in any state never turns the enqueue into a no-op.
     *
     * @param releaseId the release
     * @param now the reconciler's pass time
     * @return a stable UUID for that release and second
     */
    public static UUID releaseReconciledId(String releaseId, Instant now) {
        return JobIds.of(ReleaseJob.NAME, releaseId + ":reconciled:" + now.getEpochSecond(), 1);
    }

    /** Enqueues a release's job for the release reconciler and returns its id. */
    public UUID releaseReconciled(ReleaseJobRequest request, Instant now) {
        UUID id = releaseReconciledId(request.releaseId(), now);
        scheduler.enqueue(id, request);
        return id;
    }

    /** Schedules a release's job again at {@code at}, after a wait, and returns its id. */
    public UUID releaseAgain(ReleaseJobRequest request, Instant at) {
        UUID id = releaseWaitId(request.releaseId(), at);
        scheduler.schedule(id, at, request);
        return id;
    }

    /**
     * The id of a release's read-back deadline: the one the release job records on the fields as
     * {@code deadlineJobId} and the event handler deletes once the release is read back (plan P13).
     *
     * @param releaseId the release
     * @return a stable UUID
     */
    public static UUID readBackDeadlineId(String releaseId) {
        return JobIds.of(ReleaseReadBackDeadlineJob.NAME, releaseId, 1);
    }

    /** Schedules a release's read-back deadline at {@code at} and returns its id. */
    public UUID readBackDeadline(String tenantId, String certifyPractitionerId, String releaseId, Instant at) {
        UUID id = readBackDeadlineId(releaseId);
        scheduler.schedule(id, at, new ReleaseReadBackDeadlineJobRequest(tenantId, certifyPractitionerId, releaseId));
        return id;
    }

    /**
     * Enqueues a job the reconciler re-enqueues, now, under the id {@link #reconciledId} gave it. The
     * request carries the real upload id; only the job id carries the reconciliation number.
     *
     * @param job {@link RequestIngestionJob#NAME}, {@link IngestionDeadlineCheckJob#NAME} or {@link
     *     StageRowsJob#NAME}
     * @param id the reconciled id
     * @param exportBatchId the batch
     * @param uploadId the upload
     * @param attempt the entry's attempt
     * @throws IllegalArgumentException for another job name
     */
    public void reenqueue(String job, UUID id, String exportBatchId, String uploadId, int attempt) {
        switch (job) {
            case RequestIngestionJob.NAME -> scheduler.enqueue(
                    id, new RequestIngestionJobRequest(exportBatchId, uploadId, attempt));
            case IngestionDeadlineCheckJob.NAME -> scheduler.enqueue(
                    id, new IngestionDeadlineCheckJobRequest(exportBatchId, uploadId, attempt));
            case StageRowsJob.NAME -> scheduler.enqueue(id, new StageRowsJobRequest(exportBatchId, uploadId, attempt));
            default -> throw new IllegalArgumentException("no inbound job named " + job);
        }
    }

    /**
     * Whether JobRunr still holds the job as one it will run, as a job of {@code handler}. An id it
     * never saw, a job that ended ({@code SUCCEEDED}, {@code FAILED}, {@code DELETED}) and a pending
     * job of another kind are all not pending: the entry has no job of the kind its state needs.
     *
     * @param jobId the entry's {@code jobId}, may be null or not a UUID
     * @param handler the job class the entry's state needs
     * @return true when that job is still waiting, due or running
     */
    public boolean isPending(String jobId, Class<?> handler) {
        if (jobId == null) {
            return false;
        }
        UUID id;
        try {
            id = UUID.fromString(jobId);
        } catch (IllegalArgumentException notAUuid) {
            return false;
        }
        Job job;
        try {
            job = storage.getJobById(id);
        } catch (org.jobrunr.storage.JobNotFoundException missing) {
            return false;
        }
        return PENDING.contains(job.getState())
                && handler.getName().equals(job.getJobDetails().getClassName());
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
