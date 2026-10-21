package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.ingestion.release.ReleaseFieldRepository;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Recurring;

/**
 * The release safety net: every ten minutes, find releases whose fields sit in {@code
 * RELEASE_REQUESTED} untouched for more than {@code inboundStaleMinutes} and enqueue their release
 * job again. A request commits its fields before it enqueues the job, so an enqueue that failed (or a
 * process that died in between) would otherwise leave them with no job: a repeated request answers
 * {@code RELEASE_NOT_ALLOWED} and a rollback is refused. A release waiting for another one is put back
 * every minute and so never stale; a release whose own job JobRunr still holds as waiting, due or
 * running is left alone, and so is one whose job an earlier pass enqueued: each pass touches the
 * fields' {@code updatedAt} with its own time and enqueues under {@code
 * release:<releaseId>:reconciled:<that second>}, so the next pass reads that id back from {@code
 * updatedAt} and finds it pending. Two jobs never claim one release at once. Each re-enqueue is one
 * {@value #REENQUEUED_PREFIX} warning line.
 */
@ApplicationScoped
public class ReleaseReconcilerJob {

    /** Recurring job id; registration is idempotent on it. */
    public static final String RECURRING_ID = "vendor-release-reconciler";

    /** Every ten minutes, as the inbound reconciler. */
    public static final String CRON = "*/10 * * * *";

    /** The warning line's prefix, for an alert to count. */
    public static final String REENQUEUED_PREFIX = "RELEASE_RECONCILER_REENQUEUED";

    /** The most stale fields one pass reads. */
    static final int STALE_LIMIT = 500;

    private static final Logger LOG = Logger.getLogger(ReleaseReconcilerJob.class);

    private final DirectoryAccuracyConfig cfg;
    private final ReleaseFieldRepository releaseFields;
    private final IngestionJobEnqueuer enqueuer;
    private final Clock clock;

    public ReleaseReconcilerJob(
            DirectoryAccuracyConfig cfg,
            ReleaseFieldRepository releaseFields,
            IngestionJobEnqueuer enqueuer,
            Clock clock) {
        this.cfg = cfg;
        this.releaseFields = releaseFields;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /** The recurring entry point: every ten minutes, registered by id at startup. */
    @Recurring(id = RECURRING_ID, cron = CRON, zoneId = "UTC")
    public void reconcile() {
        run();
    }

    /**
     * One pass.
     *
     * @return the release jobs enqueued, one per stuck release
     */
    public List<UUID> run() {
        if (!cfg.enabled()) {
            LOG.info("release reconciler: service disabled, exiting as a no-op");
            return List.of();
        }
        Instant now = clock.instant();
        Instant before = now.minus(Duration.ofMinutes(cfg.ingestion().inboundStaleMinutes()));
        Map<String, ReleaseJobRequest> stuck = new LinkedHashMap<>();
        Map<String, Instant> lastTouched = new LinkedHashMap<>();
        for (FieldRecommendation field : releaseFields.staleRequested(before, STALE_LIMIT)) {
            String releaseId = field.goldenRecordUpdate() == null
                    ? null
                    : field.goldenRecordUpdate().releaseId();
            if (releaseId != null) {
                stuck.putIfAbsent(
                        releaseId,
                        new ReleaseJobRequest(
                                field.tenantId(), field.practitioner().certifyPractitionerId(), releaseId));
                lastTouched.putIfAbsent(releaseId, field.updatedAt());
            }
        }
        List<UUID> enqueued = new ArrayList<>();
        for (ReleaseJobRequest request : stuck.values()) {
            if (pending(request.releaseId(), lastTouched.get(request.releaseId()))) {
                LOG.infof("release reconciler: release %s still has a job pending", request.releaseId());
                continue;
            }
            // Touch first: the fields' updatedAt names this pass, so a later pass finds the job it
            // enqueues by the same id and leaves it alone while JobRunr holds it.
            releaseFields.touchRequested(request.tenantId(), request.releaseId(), now);
            UUID id = enqueuer.releaseReconciled(request, now);
            enqueued.add(id);
            LOG.warnf(
                    "%s tenant=%s release=%s practitioner=%s jobId=%s",
                    REENQUEUED_PREFIX, request.tenantId(), request.releaseId(), request.certifyPractitionerId(), id);
        }
        return enqueued;
    }

    /**
     * Whether a job of the release is still waiting, due or running: the request's own job, or the one
     * an earlier pass enqueued, whose id is keyed on the second that pass touched the fields.
     */
    private boolean pending(String releaseId, Instant touched) {
        if (enqueuer.isPending(IngestionJobEnqueuer.releaseJobId(releaseId).toString(), ReleaseJob.class)) {
            return true;
        }
        return touched != null
                && enqueuer.isPending(
                        IngestionJobEnqueuer.releaseReconciledId(releaseId, touched)
                                .toString(),
                        ReleaseJob.class);
    }
}
