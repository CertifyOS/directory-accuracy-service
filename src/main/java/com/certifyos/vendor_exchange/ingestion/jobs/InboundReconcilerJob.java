package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.jobs.JobIds;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.persistence.Transactions;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Recurring;

/**
 * The inbound safety net (design: stuck-work detection, alert A3): every ten minutes, find entries
 * whose work stopped and hand them back to the job their state needs. Watched: {@code RECEIVED} and
 * {@code ARCHIVING} (the request-ingestion job; a {@code RECEIVED} entry with no job is the claim or
 * attach whose post-commit enqueue failed), {@code PARSED} and {@code STAGING} (the stage-rows job),
 * each untouched for more than {@code inboundStaleMinutes}; and {@code INGESTION_REQUESTED} once its
 * deadline ({@code requestedAt + ingestionDeadlineHours}) is more than {@code inboundStaleMinutes}
 * past (the deadline check, which was lost). Before that the deadline check owns the entry.
 *
 * <p>An entry is left alone while JobRunr still holds, as waiting, due or running and of the kind
 * the state needs, either its recorded job or the state's first job ({@code <job>:<uploadId>:
 * <attempt>}, which a hand-over enqueues before it records it): a job in a long retry backoff is not
 * stuck, and a second stage job streaming beside the first would add every chunk's counts twice. Otherwise the job is
 * re-enqueued under {@code <job>:<uploadId>:reconciled:<n>:<attempt>}, where {@code n} is the entry's
 * {@code reconciliations} counter after this pass, so an earlier id JobRunr holds in any state never
 * turns the enqueue into a no-op. The counter, the new job id and {@code updatedAt} are written in
 * one guarded write with {@code INBOUND_FILE_RECONCILED} (action {@code RE_ENQUEUED}); the enqueue
 * follows the commit, then one {@value #REENQUEUED_PREFIX} warning line. The reconciler never
 * changes a state; every job checks the entry first, so a re-enqueue that loses a race is a no-op.
 */
@ApplicationScoped
public class InboundReconcilerJob {

    /** Recurring job id; registration is idempotent on it. */
    public static final String RECURRING_ID = "vendor-ingestion-reconciler";

    /** Every ten minutes (design: one reconciler every 10 minutes). */
    public static final String CRON = "*/10 * * * *";

    /** The warning line's prefix; the deployment's alert A3 counts these lines. */
    public static final String REENQUEUED_PREFIX = "INGESTION_RECONCILER_REENQUEUED";

    /** {@code action} on {@code INBOUND_FILE_RECONCILED}. */
    static final String RE_ENQUEUED = "RE_ENQUEUED";

    /** The most stale entries one query returns; there are two queries per pass. */
    static final int STALE_LIMIT = 500;

    private static final Logger LOG = Logger.getLogger(InboundReconcilerJob.class);

    private final VendorExchangeConfig cfg;
    private final InboundFileRepository inbound;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final IngestionJobEnqueuer enqueuer;
    private final Clock clock;

    public InboundReconcilerJob(
            VendorExchangeConfig cfg,
            InboundFileRepository inbound,
            AuditRepository audit,
            Transactions transactions,
            IngestionJobEnqueuer enqueuer,
            Clock clock) {
        this.cfg = cfg;
        this.inbound = inbound;
        this.audit = audit;
        this.transactions = transactions;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /**
     * One re-enqueue.
     *
     * @param uploadId the stuck entry
     * @param state the state it sat in
     * @param job the job name enqueued
     * @param jobId the job id
     */
    public record Reenqueued(String uploadId, InboundFileState state, String job, UUID jobId) {}

    /** The recurring entry point: every ten minutes, registered by id at startup. */
    @Recurring(id = RECURRING_ID, cron = CRON, zoneId = "UTC")
    public void reconcile() {
        run();
    }

    /**
     * One pass.
     *
     * @return the jobs re-enqueued, one per stuck entry
     */
    public List<Reenqueued> run() {
        if (!cfg.enabled()) {
            LOG.info("inbound reconciler: service disabled, exiting as a no-op");
            return List.of();
        }
        Instant now = clock.instant();
        Duration stale = Duration.ofMinutes(cfg.ingestion().inboundStaleMinutes());
        Duration deadline = Duration.ofHours(cfg.ingestion().ingestionDeadlineHours());
        Instant threshold = now.minus(stale);
        List<Located> stuck =
                new ArrayList<>(inbound.findStale(InboundFileState.reconcilable(), threshold, STALE_LIMIT));
        for (Located requested :
                inbound.findStale(EnumSet.of(InboundFileState.INGESTION_REQUESTED), threshold, STALE_LIMIT)) {
            InboundFile entry = requested.entry();
            Instant from = entry.requestedAt() == null ? entry.updatedAt() : entry.requestedAt();
            if (deadlineLost(from, deadline, stale, now)) {
                stuck.add(requested);
            }
        }
        List<Reenqueued> reenqueued = new ArrayList<>();
        for (Located located : stuck) {
            try {
                reenqueue(located, now).ifPresent(reenqueued::add);
            } catch (RuntimeException failure) {
                // One entry must not stop the pass; the next pass finds it again.
                LOG.errorf(
                        failure,
                        "inbound reconciler: %s upload %s could not be re-enqueued",
                        located.batch().id(),
                        located.entry().uploadId());
            }
        }
        return reenqueued;
    }

    private Optional<Reenqueued> reenqueue(Located located, Instant now) {
        InboundFile entry = located.entry();
        Class<?> handler = handlerFor(entry.state());
        String job = nameOf(handler);
        // The state's first job too: a hand-over that enqueued it but failed to record it (or lost
        // the race with the job itself) leaves the previous job's id on the entry.
        UUID firstJob = JobIds.of(job, entry.uploadId(), entry.attempt());
        if (enqueuer.isPending(entry.jobId(), handler) || enqueuer.isPending(firstJob.toString(), handler)) {
            LOG.infof(
                    "inbound reconciler: %s upload %s in %s since %s; its job is still pending",
                    located.batch().id(), entry.uploadId(), entry.state(), entry.updatedAt());
            return Optional.empty();
        }
        int reconciliation = entry.reconciliations() + 1;
        UUID jobId = IngestionJobEnqueuer.reconciledId(job, entry.uploadId(), reconciliation, entry.attempt());
        long ageMinutes = entry.updatedAt() == null
                ? 0
                : Duration.between(entry.updatedAt(), now).toMinutes();
        AuditEvent event = InboundLifecycle.envelope(
                        AuditEventType.INBOUND_FILE_RECONCILED, located.batch(), entry, Actors.INGESTION)
                .occurredAt(now)
                .detail("state", entry.state().name())
                .detail("ageMinutes", ageMinutes)
                .detail("action", RE_ENQUEUED)
                .detail("job", job)
                .detail("reconciledJobId", jobId.toString())
                .detail("reconciliation", reconciliation)
                .build();
        boolean written = transactions.run(session -> {
            boolean untouched = inbound.reconciled(session, located, jobId, now);
            if (untouched) {
                audit.write(session, event);
            }
            return untouched;
        });
        if (!written) {
            LOG.infof(
                    "inbound reconciler: %s upload %s was written since the read; not re-enqueued",
                    located.batch().id(), entry.uploadId());
            return Optional.empty();
        }
        enqueuer.reenqueue(job, jobId, located.batch().id(), entry.uploadId(), entry.attempt());
        LOG.warnf(
                "%s batch=%s upload=%s state=%s since=%s job=%s jobId=%s",
                REENQUEUED_PREFIX,
                located.batch().id(),
                entry.uploadId(),
                entry.state(),
                entry.updatedAt(),
                job,
                jobId);
        return Optional.of(new Reenqueued(entry.uploadId(), entry.state(), job, jobId));
    }

    /**
     * Whether a deadline check was lost: the deadline ({@code requestedAt + deadline}) is more than
     * the stale margin past.
     */
    static boolean deadlineLost(Instant requestedAt, Duration deadline, Duration stale, Instant now) {
        return requestedAt != null && requestedAt.plus(deadline).plus(stale).isBefore(now);
    }

    /** The job an entry in {@code state} waits on. */
    static Class<?> handlerFor(InboundFileState state) {
        return switch (state) {
            case RECEIVED, ARCHIVING -> RequestIngestionJob.class;
            case INGESTION_REQUESTED -> IngestionDeadlineCheckJob.class;
            case PARSED, STAGING -> StageRowsJob.class;
            default -> throw new IllegalStateException(state + " is not reconcilable");
        };
    }

    private static String nameOf(Class<?> handler) {
        if (handler == RequestIngestionJob.class) {
            return RequestIngestionJob.NAME;
        }
        if (handler == IngestionDeadlineCheckJob.class) {
            return IngestionDeadlineCheckJob.NAME;
        }
        return StageRowsJob.NAME;
    }
}
