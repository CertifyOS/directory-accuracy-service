package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Recurring;

/**
 * Lifecycle step 7, the hourly reconciler: a batch row written but never enqueued (a crash between
 * the two writes) sits in {@code SCHEDULED}, {@code NPIS_SELECTED} or {@code EGRESS_COMPLETED} with
 * no job. This job finds rows untouched for longer than {@code reconcilerStaleMinutes} and
 * re-enqueues the job for their state: {@code SCHEDULED} the select job, {@code NPIS_SELECTED} the
 * egress request, {@code EGRESS_COMPLETED} the finish job, each under the batch's current attempt.
 * Job ids are deterministic, so a job that already exists is not enqueued twice, and every job
 * checks the row state first, so re-enqueueing one that is already running does nothing.
 * {@code EGRESS_REQUESTED} is owned by the deadline check and never touched here. Each re-enqueue
 * is one {@code EXPORT_RECONCILER_REENQUEUED} warning line, which is what the alert reads.
 */
@ApplicationScoped
public class ReconcilerJob {

    /** Recurring job id; registration is idempotent on it. */
    public static final String RECURRING_ID = "vendor-export-reconciler";

    /** Every hour on the hour. */
    public static final String CRON = "0 * * * *";

    /** The warning line's prefix; the deployment's alert E2 counts these lines. */
    public static final String REENQUEUED_PREFIX = "EXPORT_RECONCILER_REENQUEUED";

    /** The most stale rows one run looks at. */
    static final int STALE_LIMIT = 500;

    private static final Logger LOG = Logger.getLogger(ReconcilerJob.class);

    private final VendorExchangeConfig cfg;
    private final ExportBatchRepository batches;
    private final JobEnqueuer enqueuer;
    private final Clock clock;

    public ReconcilerJob(VendorExchangeConfig cfg, ExportBatchRepository batches, JobEnqueuer enqueuer, Clock clock) {
        this.cfg = cfg;
        this.batches = batches;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /**
     * One re-enqueue.
     *
     * @param exportBatchId the stuck batch
     * @param state the state it sat in
     * @param job the job name enqueued
     * @param jobId the job id
     */
    public record Reenqueued(String exportBatchId, BatchState state, String job, UUID jobId) {}

    /** The recurring entry point: every hour on the hour, registered by id at startup. */
    @Recurring(id = RECURRING_ID, cron = CRON, zoneId = "UTC")
    public void reconcile() {
        run();
    }

    /**
     * One pass.
     *
     * @return the jobs re-enqueued, one per stale batch
     */
    public List<Reenqueued> run() {
        if (!cfg.enabled()) {
            LOG.info("reconciler: service disabled, exiting as a no-op");
            return List.of();
        }
        Instant threshold = clock.instant().minus(Duration.ofMinutes(cfg.reconcilerStaleMinutes()));
        List<ExportBatch> stale = batches.findStale(BatchState.reconcilable(), threshold, STALE_LIMIT);
        List<Reenqueued> reenqueued = new ArrayList<>();
        for (ExportBatch batch : stale) {
            Reenqueued entry = reenqueue(batch);
            LOG.warnf(
                    "%s batch=%s state=%s since=%s job=%s jobId=%s",
                    REENQUEUED_PREFIX, batch.id(), batch.state(), batch.updatedAt(), entry.job(), entry.jobId());
            reenqueued.add(entry);
        }
        return reenqueued;
    }

    private Reenqueued reenqueue(ExportBatch batch) {
        return switch (batch.state()) {
            case SCHEDULED -> new Reenqueued(
                    batch.id(), batch.state(), SelectJob.NAME, enqueuer.select(batch.id(), batch.attempt()));
            case NPIS_SELECTED -> new Reenqueued(
                    batch.id(),
                    batch.state(),
                    RequestEgressJob.NAME,
                    enqueuer.requestEgress(batch.id(), batch.attempt()));
            case EGRESS_COMPLETED -> new Reenqueued(
                    batch.id(), batch.state(), FinishJob.NAME, enqueuer.finish(batch.id(), batch.attempt()));
            default -> throw new IllegalStateException(batch.id() + " in " + batch.state() + " is not reconcilable");
        };
    }
}
