package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
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
 * {@code BatchFinalFailureFilter}. JobRunr's retry filter turns a failed state into a scheduled one
 * while retries remain, so a job that still reads {@code FAILED} here has none left.
 *
 * <p>Inbound jobs move only the entry and attempt they own to {@code FAILED}. An exhausted release
 * job moves its fields to {@code RELEASE_FAILED}; the read-back deadline job records only its metric
 * because it marks {@code UNKNOWN} itself when its read fails.
 */
@ApplicationScoped
public class IngestionFinalFailureFilter implements ApplyStateFilter {

    /** The reason for an exhausted inbound job whose last failure was not a register 4xx. */
    static final String JOB_RETRIES_EXHAUSTED = "JOB_RETRIES_EXHAUSTED";

    /** The reason for an exhausted request job whose last failure was a register 4xx. */
    static final String SOURCE_UNREADABLE = "SOURCE_UNREADABLE";

    /** The reason for an exhausted release job. */
    static final String EXHAUSTED = "EXHAUSTED";

    /** The states each inbound job works from, by handler class. */
    private static final Map<String, Set<InboundFileState>> OWNED_BY_JOB_CLASS = Map.of(
            RequestIngestionJob.class.getName(), EnumSet.of(InboundFileState.RECEIVED, InboundFileState.ARCHIVING),
            IngestionDeadlineCheckJob.class.getName(), EnumSet.of(InboundFileState.INGESTION_REQUESTED),
            StageRowsJob.class.getName(), EnumSet.of(InboundFileState.PARSED, InboundFileState.STAGING));

    private static final Logger LOG = Logger.getLogger(IngestionFinalFailureFilter.class);

    private final InboundFileRepository inbound;
    private final InboundLifecycle inboundLifecycle;
    private final ReleaseLifecycle releaseLifecycle;
    private final VendorExchangeMetrics metrics;
    private final Clock clock;

    public IngestionFinalFailureFilter(
            InboundFileRepository inbound,
            InboundLifecycle inboundLifecycle,
            ReleaseLifecycle releaseLifecycle,
            VendorExchangeMetrics metrics,
            Clock clock) {
        this.inbound = inbound;
        this.inboundLifecycle = inboundLifecycle;
        this.releaseLifecycle = releaseLifecycle;
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
            return;
        }
        String jobName = jobNameOf(job);
        if (job.getState() != StateName.FAILED) {
            metrics.ingestionJobRetry(jobName);
            return;
        }
        metrics.ingestionJobFinalFailure(jobName);
        try {
            switch (request) {
                case InboundJobRequest inboundRequest -> failEntry(job, failed, inboundRequest);
                case ReleaseJobRequest releaseRequest -> failRelease(job, failed, releaseRequest);
                case ReleaseReadBackDeadlineJobRequest ignored -> {
                    // The deadline job marks UNKNOWN itself when the read cannot complete.
                }
                default -> LOG.warnf("job %s exhausted its retries; no final-failure rule for it", job.getId());
            }
        } catch (RuntimeException failure) {
            // The job state is already saved; reconcilers and stuck-work alerts are the backstop.
            LOG.errorf(failure, "could not record the final failure of job %s", job.getId());
        }
    }

    /** Whether a job request belongs to the ingestion lane. */
    static boolean isIngestionRequest(Object request) {
        return request instanceof InboundJobRequest
                || request instanceof ReleaseJobRequest
                || request instanceof ReleaseReadBackDeadlineJobRequest;
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
            inboundLifecycle.failed(
                    located, entry.state(), step, reason, lastError, countFailures(job), clock.instant());
        }
    }

    private void failRelease(Job job, FailedState failed, ReleaseJobRequest request) {
        releaseLifecycle.failed(
                request.tenantId(),
                request.releaseId(),
                EXHAUSTED,
                failed.getExceptionType() + ": " + failed.getExceptionMessage(),
                false,
                countFailures(job),
                clock.instant(),
                job.getId().toString());
    }

    /** Maps a register 4xx to {@value #SOURCE_UNREADABLE}; every other failure exhausted retries. */
    static String reasonOf(FailedState failed) {
        return RegisterClientErrorException.class.getName().equals(failed.getExceptionType())
                ? SOURCE_UNREADABLE
                : JOB_RETRIES_EXHAUSTED;
    }

    /** Finds the lifecycle step owned by the failed inbound job. */
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

    /** Whether a job of {@code className} works from {@code state}. */
    static boolean owns(String className, InboundFileState state) {
        Set<InboundFileState> owned = OWNED_BY_JOB_CLASS.get(className);
        return owned != null && owned.contains(state);
    }

    /** Returns the job's first request parameter, or null for a job without one. */
    static Object requestOf(Job job) {
        List<JobParameter> parameters = job.getJobDetails().getJobParameters();
        if (parameters == null || parameters.isEmpty()) {
            return null;
        }
        return parameters.get(0).getObject();
    }

    /** Returns the handler's simple class name for the {@code job} metric tag. */
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
