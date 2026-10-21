package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.clients.EgressExportRequest;
import com.certifyos.directory_accuracy.clients.EgressTemplate;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.export.jobs.JobEnqueuer;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.export.jobs.RequestEgressJob;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The batch transitions more than one caller performs, each one transaction with its audit event,
 * so the tick, run-now and supersede write the same facts the same way. Jobs are enqueued after
 * the transaction commits, never inside it.
 */
@ApplicationScoped
public class BatchLifecycle {

    /** Who asked for a batch, as {@code EXPORT_BATCH_SCHEDULED} records it. */
    public enum Trigger {
        TICK,
        MANUAL,
        SUPERSEDE
    }

    private static final Logger LOG = Logger.getLogger(BatchLifecycle.class);

    private final ExportBatchRepository batches;
    private final ScheduleRepository schedules;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final JobEnqueuer enqueuer;
    private final DirectoryAccuracyMetrics metrics;

    public BatchLifecycle(
            ExportBatchRepository batches,
            ScheduleRepository schedules,
            AuditRepository audit,
            Transactions transactions,
            JobEnqueuer enqueuer,
            DirectoryAccuracyMetrics metrics) {
        this.batches = batches;
        this.schedules = schedules;
        this.audit = audit;
        this.transactions = transactions;
        this.enqueuer = enqueuer;
        this.metrics = metrics;
    }

    /**
     * A batch that was just scheduled.
     *
     * @param batch the inserted row
     * @param jobId its egress request job
     * @param nextDueAt the schedule's new due instant
     */
    public record Scheduled(ExportBatch batch, UUID jobId, Instant nextDueAt) {}

    /**
     * Creates a batch for a schedule's period: inserts the row in {@code SCHEDULED}, advances the
     * schedule ({@code nextDueAt}, {@code lastBatchId}, {@code lastRunAt}) and writes
     * {@code EXPORT_BATCH_SCHEDULED} in one transaction, then enqueues the egress request job.
     *
     * @param schedule the schedule as the caller read it; its version is the compare-and-set token
     * @param period the month the batch is for
     * @param seq 1 from the tick and run-now; supersede passes the next
     * @param trigger who asked
     * @param actor the operator or the system
     * @param now the time of the write
     * @param companion another event to commit with the batch (run-now's {@code SCHEDULE_RUN_NOW}),
     *     or null
     * @return the batch, or empty when the period already has this sequence (the unique index
     *     refused, nothing was written)
     * @throws StaleScheduleException when the schedule is no longer at the version read
     */
    public Optional<Scheduled> schedule(
            Schedule schedule,
            YearMonth period,
            int seq,
            Trigger trigger,
            String actor,
            Instant now,
            AuditEvent companion) {
        ExportBatch batch =
                ExportBatch.scheduled(schedule.tenantId(), schedule.vendor(), period, seq, schedule.filterId(), now);
        Instant nextDueAt = schedule.cadence().nextAfterPeriod(now, schedule.timezone(), period);
        UUID jobId = JobIds.of(RequestEgressJob.NAME, batch.id(), batch.attempt());
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        schedule.tenantId(),
                        schedule.vendor(),
                        batch.id(),
                        batch.attempt())
                .actor(actor)
                .jobId(jobId.toString())
                .occurredAt(now)
                .detail("period", period.toString())
                .detail("seq", seq)
                .detail("cadence", schedule.cadence().toDocument())
                .detail("trigger", trigger.name())
                .detail("nextDueAt", nextDueAt)
                .detail("scheduleVersion", schedule.version())
                .build();
        try {
            transactions.run(session -> {
                batches.insert(session, batch);
                if (!schedules.advance(session, schedule.id(), schedule.version(), nextDueAt, batch.id(), now)) {
                    throw new StaleScheduleException(schedule.id(), schedule.version());
                }
                audit.write(session, event);
                if (companion != null) {
                    audit.write(session, companion);
                }
                return null;
            });
        } catch (AlreadyExistsException exists) {
            LOG.warnf("%s already exists; %s trigger wrote nothing", batch.id(), trigger);
            return Optional.empty();
        }
        enqueuer.requestEgress(batch.id(), batch.attempt());
        metrics.batchCreated(schedule.tenantId(), schedule.vendor());
        LOG.infof(
                "%s scheduled by %s (%s), egress request job %s, next due %s",
                batch.id(), actor, trigger, jobId, nextDueAt);
        return Optional.of(new Scheduled(batch, jobId, nextDueAt));
    }

    /**
     * The tick found the period's batch already created (a race with run-now, or a batch created
     * before this rule existed): move the schedule's next due instant past that period so it is not
     * due again for the same month, without touching {@code lastBatchId}. A schedule changed under
     * the tick is left as is; the next tick reads it again.
     *
     * @param schedule the schedule as the tick read it
     * @param period the month that already has its batch
     * @param now the time of the write
     * @return the new next due instant, or empty when the schedule had changed
     */
    public Optional<Instant> skipPeriod(Schedule schedule, YearMonth period, Instant now) {
        Instant nextDueAt = schedule.cadence().nextAfterPeriod(now, schedule.timezone(), period);
        Bson update = Updates.combine(
                Updates.set("nextDueAt", nextDueAt),
                Updates.set("updatedBy", com.certifyos.directory_accuracy.audit.Actors.SYSTEM),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        boolean moved =
                transactions.run(session -> schedules.update(session, schedule.id(), schedule.version(), update));
        if (moved) {
            LOG.infof("%s: %s already has its batch; next due %s", schedule.id(), period, nextDueAt);
            return Optional.of(nextDueAt);
        }
        return Optional.empty();
    }

    /**
     * The saved filter matched nobody: {@code EGRESS_REQUESTED} to {@code EMPTY} with a zero
     * selection count and {@code EXPORT_BATCH_EMPTY}. No job follows; the period is done. The caller
     * deletes the pending deadline check.
     *
     * @param batch the batch
     * @param failureReason egress's reason text
     * @param completionSource how the outcome arrived ({@code EVENT} or {@code DEADLINE_CHECK})
     * @param companion another event to commit with the transition (the event handler's
     *     {@code EXPORT_EVENT_RECEIVED}), or null
     * @param now the time of the write
     */
    public void markEmpty(
            ExportBatch batch, String failureReason, String completionSource, AuditEvent companion, Instant now) {
        Bson updates =
                Updates.combine(Updates.set("selection.distinct", 0L), Updates.set("selection.completedAt", now));
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_EMPTY,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("filterId", batch.selection().filterId())
                .detail("failureReason", failureReason)
                .detail("completionSource", completionSource)
                .build();
        boolean moved = transactions.run(session -> {
            boolean ok = batches.transition(session, batch.id(), batch.state(), BatchState.EMPTY, now, updates);
            if (ok) {
                if (companion != null) {
                    audit.write(session, companion);
                }
                audit.write(session, event);
            }
            return ok;
        });
        if (!moved) {
            throw new IllegalStateException(batch.id() + " is no longer " + batch.state() + "; EMPTY not written");
        }
        metrics.selectionPractitioners(batch.tenantId(), batch.vendor(), 0);
        LOG.infof(
                "%s: saved filter %s matched nobody; EMPTY",
                batch.id(), batch.selection().filterId());
    }

    private void transition(ExportBatch batch, BatchState to, Bson updates, AuditEvent event, Instant now) {
        boolean moved = transactions.run(session -> {
            boolean ok = batches.transition(session, batch.id(), batch.state(), to, now, updates);
            if (ok) {
                audit.write(session, event);
            }
            return ok;
        });
        if (!moved) {
            throw new IllegalStateException(batch.id() + " is no longer " + batch.state() + "; " + to + " not written");
        }
    }

    /**
     * The schedule a batch belongs to.
     *
     * @param batch the batch
     * @return the schedule
     * @throws IllegalStateException when it no longer exists, which no job can recover from
     */
    public Schedule scheduleOf(ExportBatch batch) {
        return schedules
                .find(batch.tenantId(), batch.vendor())
                .orElseThrow(() -> new IllegalStateException("no schedule for " + batch.id()));
    }

    /**
     * Pins the template and the destination on the batch before the first egress call, so every
     * attempt sends the same values and a retry never re-reads the template. Written with
     * {@code updateInState} on {@code SCHEDULED}; no audit event, the request event carries the
     * template id.
     *
     * @param batch the batch
     * @param schedule its schedule, for the timezone of the file date
     * @param template the template as api-layer returned it
     * @param vendorBucket the destination bucket
     * @param now the time of the write; its date in the schedule's timezone names the file
     * @return the batch as stored after the pin
     */
    public ExportBatch pinTemplate(
            ExportBatch batch, Schedule schedule, EgressTemplate template, String vendorBucket, Instant now) {
        ZoneId zone = schedule.timezone() == null ? ZoneId.of("UTC") : schedule.timezone();
        LocalDate day = now.atZone(zone).toLocalDate();
        String fileName = DestinationNames.fileName(batch.tenantId(), batch.id(), day);
        String objectName = DestinationNames.objectName(batch.tenantId(), fileName);
        Document destination = new EgressDetails.Destination(vendorBucket, objectName).toDocument();
        Bson updates = Updates.combine(
                Updates.set("egress.templateId", template.id()),
                Updates.set("egress.templateVersion", template.version()),
                Updates.set("egress.mappingsCsvUrl", template.mappingsCsvUrl()),
                Updates.set("egress.separator", template.separator()),
                Updates.set("egress.outputFormat", template.outputFormat()),
                Updates.set("egress.rowExpansionKeys", template.rowExpansionKeys()),
                Updates.set("egress.destination", destination),
                Updates.set("file.name", fileName),
                Updates.set("file.path", DestinationNames.gsPath(vendorBucket, objectName)));
        boolean still = transactions.run(
                session -> batches.updateInState(session, batch.id(), BatchState.SCHEDULED, now, updates));
        if (!still) {
            throw new IllegalStateException(batch.id() + " left SCHEDULED before the template was pinned");
        }
        LOG.infof(
                "%s pinned template %s v%s, destination %s", batch.id(), template.id(), template.version(), objectName);
        return batches.find(batch.id()).orElseThrow();
    }

    /**
     * Egress accepted (or already held) the request: {@code SCHEDULED} to {@code EGRESS_REQUESTED}
     * with the correlation id, job reference, request time and the deadline job id, plus
     * {@code EXPORT_EGRESS_REQUESTED}; then the first deadline check is scheduled. A retry that sent
     * the filter id rather than {@code reuseSelectionFrom} could not reuse the earlier list, and
     * the batch records {@code selection.reEvaluated}.
     *
     * @param batch the batch, pinned
     * @param correlationId this attempt's egress correlation id
     * @param jobReference what egress called the job
     * @param selection the selection sent
     * @param egressResponse a short description of egress's answer, for the event
     * @param deadlineAt when the first deadline check runs
     * @param now the time of the write
     */
    public void egressRequested(
            ExportBatch batch,
            String correlationId,
            String jobReference,
            EgressExportRequest.Selection selection,
            String egressResponse,
            Instant deadlineAt,
            Instant now) {
        UUID deadlineJobId = JobIds.deadline(batch.id(), batch.attempt(), 1);
        boolean reEvaluated = batch.attempt() > 1 && selection.reuseSelectionFrom() == null;
        Bson updates = Updates.combine(
                Updates.set("egress.correlationId", correlationId),
                Updates.set("egress.jobReference", jobReference),
                Updates.set("egress.requestedAt", now),
                Updates.set("egress.deadlineJobId", deadlineJobId.toString()),
                Updates.unset("egress.completedAt"),
                Updates.unset("egress.completionSource"),
                reEvaluated ? Updates.set("selection.reEvaluated", true) : Updates.unset("selection.reEvaluated"));
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_EGRESS_REQUESTED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("egressCorrelationId", correlationId)
                .detail("templateId", batch.egress().templateId())
                .detail("templateVersion", batch.egress().templateVersion())
                .detail("filterId", batch.selection().filterId())
                .detail("reuseSelectionFrom", selection.reuseSelectionFrom())
                .detail("egressResponse", egressResponse)
                .detail("destination", batch.egress().destination().toDocument())
                .detail("deadlineAt", deadlineAt)
                .detail("deadlineJobId", deadlineJobId.toString())
                .build();
        transition(batch, BatchState.EGRESS_REQUESTED, updates, event, now);
        enqueuer.deadlineCheck(batch.id(), batch.attempt(), 1, deadlineAt);
        LOG.infof(
                "%s egress requested as %s, deadline check %s at %s",
                batch.id(), correlationId, deadlineJobId, deadlineAt);
    }

    /**
     * Records the cancel of the prior attempt before a retry asks egress again.
     *
     * @param batch the batch
     * @param priorCorrelationId the attempt cancelled
     * @param accepted whether egress accepted the cancel
     * @param answer egress's status or answer text
     * @param now the time
     */
    public void priorAttemptCancel(
            ExportBatch batch, String priorCorrelationId, boolean accepted, String answer, Instant now) {
        AuditEventType type = accepted
                ? AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED
                : AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED;
        audit.write(AuditEvent.forBatch(type, batch.tenantId(), batch.vendor(), batch.id(), batch.attempt())
                .occurredAt(now)
                .detail("cancelledCorrelationId", priorCorrelationId)
                .detail("outcome", answer)
                .build());
    }

    /**
     * A retry found the prior attempt's complete file already at the destination, so there is
     * nothing to ask egress for: {@code SCHEDULED} to {@code EGRESS_REQUESTED} to
     * {@code EGRESS_COMPLETED} in one transaction (the state machine has no shortcut), with
     * {@code completionSource = PRIOR_ATTEMPT} and {@code EXPORT_EGRESS_COMPLETED}; then the finish
     * job.
     *
     * @param batch the batch
     * @param object the complete object
     * @param now the time of the write
     * @return the finish job id
     */
    public UUID completedByPriorAttempt(ExportBatch batch, VendorBucket.ObjectInfo object, Instant now) {
        Bson updates = Updates.combine(
                Updates.set("egress.completedAt", now),
                Updates.set("egress.completionSource", EgressDetails.CompletionSource.PRIOR_ATTEMPT.name()),
                Updates.set("egress.fileProducedBy", object.producedBy()));
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_EGRESS_COMPLETED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("outputUri", DestinationNames.gsPath(object.bucket(), object.objectName()))
                .detail("totalRecords", object.metadataNumber("totalRecords"))
                .detail(
                        "waitSeconds",
                        batch.egress().requestedAt() == null
                                ? 0
                                : Math.max(
                                        0,
                                        java.time.Duration.between(
                                                        batch.egress().requestedAt(), now)
                                                .getSeconds()))
                .detail("completionSource", EgressDetails.CompletionSource.PRIOR_ATTEMPT.name())
                .detail("priorCorrelationId", batch.egress().correlationId())
                .detail("fileProducedBy", object.producedBy())
                .detail("bytes", object.size())
                .build();
        boolean moved = transactions.run(session -> {
            boolean first = batches.transition(
                    session, batch.id(), BatchState.SCHEDULED, BatchState.EGRESS_REQUESTED, now, null);
            boolean second = first
                    && batches.transition(
                            session,
                            batch.id(),
                            BatchState.EGRESS_REQUESTED,
                            BatchState.EGRESS_COMPLETED,
                            now,
                            updates);
            if (second) {
                audit.write(session, event);
            }
            return second;
        });
        if (!moved) {
            throw new IllegalStateException(batch.id() + " is no longer SCHEDULED; EGRESS_COMPLETED not written");
        }
        UUID jobId = enqueuer.finish(batch.id(), batch.attempt());
        metrics.completion(EgressDetails.CompletionSource.PRIOR_ATTEMPT.name());
        LOG.infof(
                "%s: prior attempt %s already delivered %s; finish job %s",
                batch.id(), batch.egress().correlationId(), object.objectName(), jobId);
        return jobId;
    }

    /** Egress refused the request; counted so the dashboard sees it before the retries run out. */
    public void egressRefused(ExportBatch batch, String reason) {
        metrics.egressFailed(batch.tenantId(), reason);
    }

    /**
     * Egress is already running the tenant's maximum of selection exports ({@code 429}): a new
     * egress request job is scheduled after egress's {@code Retry-After}, so the wait spends none of
     * JobRunr's retries. The batch stays {@code SCHEDULED}.
     *
     * @param batch the batch
     * @param wait the number of this wait, from 1
     * @param at when to ask again
     * @return the scheduled job id
     */
    public UUID waitForEgressQuota(ExportBatch batch, int wait, Instant at) {
        egressRefused(batch, "SELECTION_QUOTA_EXCEEDED");
        UUID jobId = enqueuer.requestEgressAfterQuota(batch.id(), batch.attempt(), wait, at);
        LOG.infof(
                "%s: egress selection quota reached; wait %d, asking again at %s (job %s)",
                batch.id(), wait, at, jobId);
        return jobId;
    }
}
