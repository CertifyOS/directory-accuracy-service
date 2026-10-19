package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobParameter;
import org.jobrunr.jobs.filters.ApplyStateFilter;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.StateName;

/**
 * The ingestion lane's net for a job whose retries ran out (plan P6), beside the export lane's
 * {@code BatchFinalFailureFilter}, which never reads an inbound request. JobRunr's own retry filter
 * turns a failed state into a scheduled one while retries remain, so a job that still reads {@code
 * FAILED} when this apply-state filter runs has none left. The entry then moves to {@code FAILED}
 * through {@link InboundLifecycle#failed}, with {@code INBOUND_FILE_FAILED} in the same transaction:
 * the step the job was doing, {@code SOURCE_UNREADABLE} when the last failure was a register 4xx
 * ({@link RegisterClientErrorException}) and {@code JOB_RETRIES_EXHAUSTED} otherwise, the last
 * exception as {@code <type>: <message>}, and how many times the job ran.
 *
 * <p>Only an entry the job was working on is failed: the request's attempt, and a state the job
 * works from. An exhausted deadline check never fails an entry still {@code ARCHIVING}: the request
 * job may still commit {@code INGESTION_REQUESTED}, and the reconciler re-enqueues the check once
 * its deadline is past. Metrics count every inbound job's retries and final failures.
 */
@ApplicationScoped
public class IngestionFinalFailureFilter implements ApplyStateFilter {

    /** The reason for an exhausted job whose last failure was anything but a register 4xx. */
    static final String JOB_RETRIES_EXHAUSTED = "JOB_RETRIES_EXHAUSTED";

    /** The reason for an exhausted request job whose last failure was a register 4xx (design step 3). */
    static final String SOURCE_UNREADABLE = "SOURCE_UNREADABLE";

    /** The states each inbound job works from, by handler class: the entries its failure may fail. */
    private static final Map<String, Set<InboundFileState>> OWNED_BY_JOB_CLASS = Map.of(
            RequestIngestionJob.class.getName(), EnumSet.of(InboundFileState.RECEIVED, InboundFileState.ARCHIVING),
            IngestionDeadlineCheckJob.class.getName(), EnumSet.of(InboundFileState.INGESTION_REQUESTED),
            StageRowsJob.class.getName(), EnumSet.of(InboundFileState.PARSED, InboundFileState.STAGING));

    private static final Logger LOG = Logger.getLogger(IngestionFinalFailureFilter.class);

    private final InboundFileRepository inbound;
    private final InboundLifecycle lifecycle;
    private final VendorExchangeMetrics metrics;
    private final Clock clock;

    public IngestionFinalFailureFilter(
            InboundFileRepository inbound, InboundLifecycle lifecycle, VendorExchangeMetrics metrics, Clock clock) {
        this.inbound = inbound;
        this.lifecycle = lifecycle;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void onStateApplied(Job job, JobState oldState, JobState newState) {
        if (!(newState instanceof FailedState failed)) {
            return;
        }
        Object request = requestOf(job);
        if (!isIngestionRequest(request)) {
            // Not an ingestion job (an export job, a recurring job with no request): not ours.
            return;
        }
        String jobName = jobNameOf(job);
        if (job.getState() != StateName.FAILED) {
            // A failed attempt with retries left: JobRunr has already elected the next SCHEDULED state.
            metrics.ingestionJobRetry(jobName);
            return;
        }
        metrics.ingestionJobFinalFailure(jobName);
        try {
            // One branch per ingestion request type; a release job's branch joins here.
            switch (request) {
                case InboundJobRequest inboundRequest -> failEntry(job, failed, inboundRequest);
                default -> LOG.warnf("job %s exhausted its retries; no final-failure rule for it", job.getId());
            }
        } catch (RuntimeException failure) {
            // The job state is already saved; a missing update must not take the worker down. The
            // reconciler and the stuck-work alerts are the backstop for what this leaves.
            LOG.errorf(failure, "could not record the final failure of job %s", job.getId());
        }
    }

    /**
     * Whether a job's request belongs to the ingestion lane, whose retries and final failures this
     * filter counts. One case per request type, beside the dispatch in {@link #onStateApplied}.
     */
    static boolean isIngestionRequest(Object request) {
        return request instanceof InboundJobRequest;
    }

    private void failEntry(Job job, FailedState failed, InboundJobRequest request) {
        String className = job.getJobDetails().getClassName();
        Optional<Located> found = inbound.findByUploadId(request.uploadId());
        if (found.isEmpty()) {
            LOG.warnf("job %s exhausted its retries but upload %s is gone", job.getId(), request.uploadId());
            return;
        }
        Located located = found.get();
        InboundFile entry = located.entry();
        if (entry.attempt() != request.attempt()
                || !owns(className, entry.state())
                || !entry.state().canTransitionTo(InboundFileState.FAILED)) {
            LOG.warnf(
                    "job %s exhausted its retries but %s upload %s is %s at attempt %d (job for attempt %d); nothing to fail",
                    job.getId(),
                    located.batch().id(),
                    entry.uploadId(),
                    entry.state(),
                    entry.attempt(),
                    request.attempt());
            return;
        }
        String reason = reasonOf(failed);
        String step = stepOf(className, entry.archivePath() != null, reason);
        String lastError = failed.getExceptionType() + ": " + failed.getExceptionMessage();
        try (IngestionLogContext ignored =
                IngestionLogContext.openFor(located.batch().id(), entry.uploadId(), job.getId())) {
            IngestionLogContext.tenant(located.batch().tenantId());
            lifecycle.failed(located, entry.state(), step, reason, lastError, countFailures(job), clock.instant());
        }
    }

    /**
     * The failure reason: {@value #SOURCE_UNREADABLE} when the last exception was a register 4xx,
     * {@value #JOB_RETRIES_EXHAUSTED} otherwise.
     */
    static String reasonOf(FailedState failed) {
        return RegisterClientErrorException.class.getName().equals(failed.getExceptionType())
                ? SOURCE_UNREADABLE
                : JOB_RETRIES_EXHAUSTED;
    }

    /**
     * The step the exhausted job was doing: the request job archives until the entry has an archive
     * path and registers after (a register 4xx is a register failure whatever the entry says), the
     * deadline check asks about the parse and the stage job stages.
     */
    static String stepOf(String className, boolean archived, String reason) {
        if (RequestIngestionJob.class.getName().equals(className)) {
            return archived || SOURCE_UNREADABLE.equals(reason)
                    ? RequestIngestionJob.STEP_REGISTER
                    : RequestIngestionJob.STEP_ARCHIVE;
        }
        if (IngestionDeadlineCheckJob.class.getName().equals(className)) {
            return IngestionDeadlineCheckJob.STEP_PARSE;
        }
        return StageRowsJob.STEP_STAGE;
    }

    /** Whether a job of {@code className} works from {@code state}, so its exhaustion may fail the entry. */
    static boolean owns(String className, InboundFileState state) {
        Set<InboundFileState> owned = OWNED_BY_JOB_CLASS.get(className);
        return owned != null && owned.contains(state);
    }

    /** The job's request (its first parameter), or null for a job without one, such as a recurring job. */
    static Object requestOf(Job job) {
        List<JobParameter> parameters = job.getJobDetails().getJobParameters();
        if (parameters == null || parameters.isEmpty()) {
            return null;
        }
        return parameters.get(0).getObject();
    }

    /** The handler's simple class name, the {@code job} tag of the retry and final-failure counters. */
    static String jobNameOf(Job job) {
        String className = job.getJobDetails().getClassName();
        return className.substring(className.lastIndexOf('.') + 1);
    }

    static int countFailures(Job job) {
        return (int) job.getJobStates().stream()
                .filter(FailedState.class::isInstance)
                .count();
    }
}
