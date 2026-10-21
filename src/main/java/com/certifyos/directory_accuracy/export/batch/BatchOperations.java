package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.export.jobs.JobEnqueuer;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.export.jobs.RequestEgressJob;
import com.certifyos.directory_accuracy.export.jobs.SelectJob;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.UUID;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The operator's batch reads and writes (design step 8): tenant-scoped reads, retry from
 * {@code FAILED} and supersede from {@code DELIVERED}. Each write is one transaction with its
 * events, the operator as actor, and the job enqueued after the commit.
 */
@ApplicationScoped
public class BatchOperations {

    static final int LIST_LIMIT = 100;

    /** The most audit events one read returns; a batch writes well under this over its whole life. */
    public static final int TRAIL_LIMIT = 500;

    private static final Logger LOG = Logger.getLogger(BatchOperations.class);

    private final ExportBatchRepository batches;
    private final ScheduleRepository schedules;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final JobEnqueuer enqueuer;
    private final DirectoryAccuracyMetrics metrics;
    private final Clock clock;

    public BatchOperations(
            ExportBatchRepository batches,
            ScheduleRepository schedules,
            AuditRepository audit,
            Transactions transactions,
            JobEnqueuer enqueuer,
            DirectoryAccuracyMetrics metrics,
            Clock clock) {
        this.batches = batches;
        this.schedules = schedules;
        this.audit = audit;
        this.transactions = transactions;
        this.enqueuer = enqueuer;
        this.metrics = metrics;
        this.clock = clock;
    }

    /** The retry outcome. */
    public record Retried(ExportBatch batch, UUID jobId) {}

    /** The supersede outcome. */
    public record Superseded(ExportBatch newBatch, UUID jobId) {}

    /**
     * One batch of the caller's tenant.
     *
     * @param tenantId the caller's tenant
     * @param exportBatchId the batch
     * @return the batch
     * @throws ProblemException 404 when it does not exist or belongs to another tenant
     */
    public ExportBatch require(String tenantId, String exportBatchId) {
        return batches.find(exportBatchId)
                .filter(batch -> batch.tenantId().equals(tenantId))
                .orElseThrow(() -> ProblemException.notFound("BATCH_NOT_FOUND", "no export batch " + exportBatchId));
    }

    /**
     * A tenant's batches, newest first.
     *
     * @param tenantId the tenant
     * @param period {@code yyyy-MM}, or null for all
     * @return the batches
     */
    public List<ExportBatch> list(String tenantId, String period) {
        if (period != null && !period.isBlank()) {
            try {
                YearMonth.parse(period);
            } catch (java.time.format.DateTimeParseException invalid) {
                throw ProblemException.badRequest("PERIOD_INVALID", "period must be yyyy-MM, got " + period);
            }
        }
        return batches.findForTenant(tenantId, period == null || period.isBlank() ? null : period, LIST_LIMIT);
    }

    /**
     * A batch's audit trail in the order it was written.
     *
     * @param batch the batch, from {@link #require}
     * @return the events, oldest first, at most {@link #TRAIL_LIMIT}
     */
    public List<AuditEvent> trail(ExportBatch batch) {
        return audit.trailForBatch(batch.id(), TRAIL_LIMIT);
    }

    /**
     * Retries a failed batch: attempt + 1, back to the state its failed step names, that step's job.
     *
     * @param batch the batch, from {@link #require}
     * @param reason mandatory
     * @param actor the operator
     * @return the batch after the write and the job
     */
    public Retried retry(ExportBatch batch, String reason, String actor) {
        String why = requireReason(reason);
        if (batch.state() != BatchState.FAILED || batch.failedStep() == null) {
            throw ProblemException.conflict(
                    "RETRY_NOT_ALLOWED",
                    "batch " + batch.id() + " is " + batch.state() + "; only FAILED can be retried");
        }
        int attempt = batch.attempt() + 1;
        BatchState target =
                batch.failedStep() == ExportBatch.FailedStep.SELECT ? BatchState.SCHEDULED : BatchState.NPIS_SELECTED;
        String job = target == BatchState.SCHEDULED ? SelectJob.NAME : RequestEgressJob.NAME;
        UUID jobId = JobIds.of(job, batch.id(), attempt);
        Instant now = clock.instant();
        Bson updates = Updates.combine(
                Updates.set("attempt", attempt), Updates.unset("failedStep"), Updates.unset("lastError"));
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_RETRY_REQUESTED, batch.tenantId(), batch.vendor(), batch.id(), attempt)
                .actor(actor)
                .jobId(jobId.toString())
                .occurredAt(now)
                .detail("fromState", batch.state().name())
                .detail("failedStep", batch.failedStep().name())
                .detail("newAttempt", attempt)
                .detail("reason", why)
                .build();
        boolean moved = transactions.run(session -> {
            boolean ok = batches.transition(session, batch.id(), BatchState.FAILED, target, now, updates);
            if (ok) {
                audit.write(session, event);
            }
            return ok;
        });
        if (!moved) {
            throw ProblemException.conflict("RETRY_NOT_ALLOWED", "batch " + batch.id() + " changed under this request");
        }
        if (target == BatchState.SCHEDULED) {
            enqueuer.select(batch.id(), attempt);
        } else {
            enqueuer.requestEgress(batch.id(), attempt);
        }
        LOG.infof(
                "%s retried by %s as attempt %d from %s; %s job %s",
                batch.id(), actor, attempt, batch.failedStep(), job, jobId);
        return new Retried(batches.find(batch.id()).orElseThrow(), jobId);
    }

    /**
     * Supersedes a delivered batch: a new batch at {@code seq + 1} in {@code SCHEDULED}, the old
     * one {@code SUPERSEDED}, the schedule's {@code lastBatchId} on the new one, all in one
     * transaction; then the new batch's select job. The old file stays where the vendor can read it.
     *
     * @param batch the batch, from {@link #require}
     * @param reason mandatory
     * @param actor the operator
     * @return the new batch and its job
     */
    public Superseded supersede(ExportBatch batch, String reason, String actor) {
        String why = requireReason(reason);
        if (batch.state() != BatchState.DELIVERED) {
            throw ProblemException.conflict(
                    "SUPERSEDE_NOT_ALLOWED",
                    "batch " + batch.id() + " is " + batch.state() + "; only DELIVERED can be superseded");
        }
        Schedule schedule = schedules
                .find(batch.tenantId(), batch.vendor())
                .orElseThrow(() -> ProblemException.conflict("SCHEDULE_NOT_FOUND", "no schedule for " + batch.id()));
        Instant now = clock.instant();
        ExportBatch replacement = ExportBatch.scheduled(
                batch.tenantId(),
                batch.vendor(),
                YearMonth.parse(batch.period()),
                batch.seq() + 1,
                schedule.selection(),
                now);
        UUID jobId = JobIds.of(SelectJob.NAME, replacement.id(), replacement.attempt());
        AuditEvent superseded = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_SUPERSEDED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .actor(actor)
                .occurredAt(now)
                .detail("supersededBy", replacement.id())
                .detail("reason", why)
                .build();
        AuditEvent scheduled = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        batch.tenantId(),
                        batch.vendor(),
                        replacement.id(),
                        replacement.attempt())
                .actor(actor)
                .jobId(jobId.toString())
                .occurredAt(now)
                .detail("period", replacement.period())
                .detail("seq", replacement.seq())
                .detail("cadence", schedule.cadence().toDocument())
                .detail("trigger", BatchLifecycle.Trigger.SUPERSEDE.name())
                .detail("nextDueAt", schedule.nextDueAt())
                .detail("scheduleVersion", schedule.version())
                .detail("supersedes", batch.id())
                .detail("reason", why)
                .build();
        Bson scheduleUpdate = Updates.combine(
                Updates.set("lastBatchId", replacement.id()),
                Updates.set("updatedBy", actor),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        try {
            boolean done = transactions.run(session -> {
                batches.insert(session, replacement);
                boolean old = batches.transition(
                        session,
                        batch.id(),
                        BatchState.DELIVERED,
                        BatchState.SUPERSEDED,
                        now,
                        Updates.set("supersededBy", replacement.id()));
                boolean sched = old && schedules.update(session, schedule.id(), schedule.version(), scheduleUpdate);
                if (!sched) {
                    throw new StaleScheduleException(schedule.id(), schedule.version());
                }
                audit.write(session, superseded);
                audit.write(session, scheduled);
                return true;
            });
            if (!done) {
                throw ProblemException.conflict(
                        "SUPERSEDE_NOT_ALLOWED", "batch " + batch.id() + " changed under this request");
            }
        } catch (AlreadyExistsException exists) {
            throw ProblemException.conflict("BATCH_EXISTS", "batch " + replacement.id() + " already exists");
        } catch (StaleScheduleException changed) {
            throw ProblemException.conflict(
                    "VERSION_STALE", "the batch or its schedule changed under this request; read and retry");
        }
        enqueuer.select(replacement.id(), replacement.attempt());
        metrics.batchCreated(batch.tenantId(), batch.vendor());
        LOG.infof("%s superseded by %s (%s); select job %s", batch.id(), replacement.id(), actor, jobId);
        return new Superseded(batches.find(replacement.id()).orElseThrow(), jobId);
    }

    private static String requireReason(String reason) {
        if (reason == null || reason.isBlank()) {
            throw ProblemException.badRequest("REASON_REQUIRED", "reason is required");
        }
        return reason.trim();
    }
}
