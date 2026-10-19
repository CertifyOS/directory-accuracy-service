package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.util.Set;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.filters.ApplyStateFilter;
import org.jobrunr.jobs.states.FailedState;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.StateName;

/**
 * The ingestion lane's final-failure filter (plan P6), the counterpart of the export lane's {@code
 * BatchFinalFailureFilter}: when an ingestion job has used up its retries, JobRunr marks it failed and
 * stops; this filter moves what the job was working on to its failed state. This step adds the release
 * branch: an exhausted {@code ReleaseJob} moves its release's fields to {@code RELEASE_FAILED} with
 * reason {@code EXHAUSTED} (alert A12), so an operator can re-release them. The read-back deadline job
 * only counts (it marks {@code UNKNOWN} itself on a read failure). The inbound-file branches arrive with
 * their step.
 *
 * <p>As in the export filter, a job that still reads {@code FAILED} when this apply-state filter runs
 * has no retry left: JobRunr's own {@code RetryFilter} has already turned a retryable failure into a
 * scheduled state.
 */
@ApplicationScoped
public class IngestionFinalFailureFilter implements ApplyStateFilter {

    static final String EXHAUSTED = "EXHAUSTED";

    private static final Set<String> RELEASE_JOBS =
            Set.of(ReleaseJob.class.getName(), ReleaseReadBackDeadlineJob.class.getName());
    private static final Logger LOG = Logger.getLogger(IngestionFinalFailureFilter.class);

    private final ReleaseLifecycle lifecycle;
    private final VendorExchangeMetrics metrics;
    private final Clock clock;

    public IngestionFinalFailureFilter(ReleaseLifecycle lifecycle, VendorExchangeMetrics metrics, Clock clock) {
        this.lifecycle = lifecycle;
        this.metrics = metrics;
        this.clock = clock;
    }

    @Override
    public void onStateApplied(Job job, JobState oldState, JobState newState) {
        if (!(newState instanceof FailedState failed)) {
            return;
        }
        String className = job.getJobDetails().getClassName();
        if (!RELEASE_JOBS.contains(className)) {
            return;
        }
        String jobName = className.substring(className.lastIndexOf('.') + 1);
        if (job.getState() != StateName.FAILED) {
            metrics.ingestionJobRetry(jobName);
            return;
        }
        metrics.ingestionJobFinalFailure(jobName);
        if (!ReleaseJob.class.getName().equals(className)) {
            return;
        }
        ReleaseJobRequest request = (ReleaseJobRequest)
                job.getJobDetails().getJobParameters().get(0).getObject();
        try {
            lifecycle.failed(
                    request.tenantId(),
                    request.releaseId(),
                    EXHAUSTED,
                    failed.getExceptionType() + ": " + failed.getExceptionMessage(),
                    false,
                    countFailures(job),
                    clock.instant(),
                    job.getId().toString());
        } catch (RuntimeException failure) {
            // The job state is already saved; a missing field update must not take the worker down.
            LOG.errorf(
                    failure,
                    "could not fail release %s after job %s exhausted its retries",
                    request.releaseId(),
                    job.getId());
        }
    }

    static int countFailures(Job job) {
        return (int) job.getJobStates().stream()
                .filter(FailedState.class::isInstance)
                .count();
    }
}
