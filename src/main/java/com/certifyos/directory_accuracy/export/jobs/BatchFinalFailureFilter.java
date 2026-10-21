package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.filters.ApplyStateFilter;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.StateName;

/**
 * The first safety net JobRunr cannot provide (design decision D13). When a batch job has used up
 * its retries, JobRunr marks the job failed in its own collection and stops; the batch row would
 * still say {@code SCHEDULED} or {@code NPIS_SELECTED} and nobody would know. This filter runs at
 * that moment, moves the batch to {@code FAILED} with the step name and the exception, and writes
 * {@code EXPORT_BATCH_FAILED} in the same transaction. The metric and alert hooks arrive with the
 * observability subtask.
 *
 * <p>A {@code JobRequest} job's {@code JobDetails.className} is the handler class and its single
 * parameter is the request record, which is where the batch id comes from.
 *
 * <p>How "retries exhausted" is detected: JobRunr's own {@code RetryFilter} is an elect-state filter
 * that turns a failed state into a scheduled one while retries remain. By the time this
 * apply-state filter runs, a job that still reads {@code FAILED} has no retry left.
 */
@ApplicationScoped
public class BatchFinalFailureFilter implements ApplyStateFilter {

    private static final Logger LOG = Logger.getLogger(BatchFinalFailureFilter.class);

    private static final Map<String, ExportBatch.FailedStep> STEP_BY_JOB_CLASS = Map.of(
            SelectJob.class.getName(), ExportBatch.FailedStep.SELECT,
            RequestEgressJob.class.getName(), ExportBatch.FailedStep.EGRESS,
            DeadlineCheckJob.class.getName(), ExportBatch.FailedStep.EGRESS,
            FinishJob.class.getName(), ExportBatch.FailedStep.EGRESS);

    private final ExportBatchRepository batches;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final DirectoryAccuracyMetrics metrics;
    private final Clock clock;

    public BatchFinalFailureFilter(
            ExportBatchRepository batches,
            AuditRepository audit,
            Transactions transactions,
            DirectoryAccuracyMetrics metrics,
            Clock clock) {
        this.batches = batches;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void onStateApplied(Job job, JobState oldState, JobState newState) {
        if (!(newState instanceof FailedState failed)) {
            return;
        }
        String jobName = jobNameOf(job);
        if (job.getState() != StateName.FAILED) {
            // A failed attempt with retries left: JobRunr has already elected the next SCHEDULED state.
            metrics.jobRetry(jobName);
            return;
        }
        metrics.jobFinalFailure(jobName);
        ExportBatch.FailedStep step = STEP_BY_JOB_CLASS.get(job.getJobDetails().getClassName());
        if (step == null) {
            return;
        }
        String exportBatchId = batchIdOf(job);
        try {
            markFailed(job, failed, step, exportBatchId);
        } catch (RuntimeException failure) {
            // The job state is already saved; a missing batch update must not take the worker down.
            // The hourly reconciler and alert E2 are the backstop for a row this leaves behind.
            LOG.errorf(
                    failure,
                    "could not mark batch %s FAILED after job %s exhausted its retries",
                    exportBatchId,
                    job.getId());
        }
    }

    private void markFailed(Job job, FailedState failed, ExportBatch.FailedStep step, String exportBatchId) {
        Instant now = clock.instant();
        String lastError = failed.getExceptionType() + ": " + failed.getExceptionMessage();
        int jobAttempts = countFailures(job);
        transactions.run(session -> {
            Optional<ExportBatch> found = batches.find(session, exportBatchId);
            if (found.isEmpty() || found.get().state().isTerminal()) {
                LOG.warnf(
                        "job %s exhausted retries but batch %s is %s; nothing to mark",
                        job.getId(),
                        exportBatchId,
                        found.map(batch -> batch.state().name()).orElse("missing"));
                return null;
            }
            ExportBatch batch = found.get();
            boolean moved = batches.transition(
                    session,
                    exportBatchId,
                    batch.state(),
                    BatchState.FAILED,
                    now,
                    Updates.combine(Updates.set("failedStep", step.name()), Updates.set("lastError", lastError)));
            if (moved) {
                audit.write(
                        session,
                        AuditEvent.forBatch(
                                        AuditEventType.EXPORT_BATCH_FAILED,
                                        batch.tenantId(),
                                        batch.vendor(),
                                        exportBatchId,
                                        batch.attempt())
                                .jobId(job.getId().toString())
                                .occurredAt(now)
                                .detail("failedStep", step.name())
                                .detail("cause", "JOB_RETRIES_EXHAUSTED")
                                .detail("lastError", lastError)
                                .detail("jobAttempts", jobAttempts)
                                .build());
            }
            return null;
        });
        LOG.warnf(
                "batch %s marked FAILED at step %s after %d attempts: %s", exportBatchId, step, jobAttempts, lastError);
    }

    /** The handler's simple class name, the {@code job} tag of the retry and final-failure counters. */
    static String jobNameOf(Job job) {
        String className = job.getJobDetails().getClassName();
        return className.substring(className.lastIndexOf('.') + 1);
    }

    static String batchIdOf(Job job) {
        Object request = job.getJobDetails().getJobParameters().get(0).getObject();
        return ((BatchJobRequest) request).exportBatchId();
    }

    static int countFailures(Job job) {
        return (int) job.getJobStates().stream()
                .filter(FailedState.class::isInstance)
                .count();
    }
}
