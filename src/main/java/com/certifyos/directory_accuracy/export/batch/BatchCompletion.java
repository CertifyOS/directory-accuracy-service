package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.clients.EgressFailureReason;
import com.certifyos.directory_accuracy.clients.EgressSelection;
import com.certifyos.directory_accuracy.export.jobs.JobEnqueuer;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The transitions after the egress request, shared by the event handler, the deadline check and
 * the finish job: egress completed, egress or finish failed, delivered. Each is one transaction
 * with its events; the companion events a caller adds (the received or missed event) commit with
 * it. Jobs are enqueued or deleted after the commit.
 */
@ApplicationScoped
public class BatchCompletion {

    /** {@code cause} values on {@code EXPORT_BATCH_FAILED} written here. */
    public static final String EGRESS_FAILED = "EGRESS_FAILED";

    public static final String EGRESS_DID_NOT_FINISH = "EGRESS_DID_NOT_FINISH";
    public static final String FILE_NOT_FOUND = "FILE_NOT_FOUND";
    public static final String EGRESS_REFUSED = "EGRESS_REFUSED";
    public static final String SELECTION_COUNTS_MISSING = "SELECTION_COUNTS_MISSING";

    private static final Logger LOG = Logger.getLogger(BatchCompletion.class);

    private final ExportBatchRepository batches;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final JobEnqueuer enqueuer;
    private final DirectoryAccuracyMetrics metrics;

    public BatchCompletion(
            ExportBatchRepository batches,
            AuditRepository audit,
            Transactions transactions,
            JobEnqueuer enqueuer,
            DirectoryAccuracyMetrics metrics) {
        this.batches = batches;
        this.audit = audit;
        this.transactions = transactions;
        this.enqueuer = enqueuer;
        this.metrics = metrics;
    }

    /**
     * What egress reported on completion.
     *
     * @param outputUri where the file is
     * @param totalRecords practitioners in the file
     * @param totalRows rows in the file
     * @param completedAt when egress finished, or null
     * @param selection egress's selection block, or null when the event or status carried none
     */
    public record Outcome(
            String outputUri, Long totalRecords, Long totalRows, Instant completedAt, EgressSelection selection) {}

    /**
     * Egress finished the file: {@code EGRESS_REQUESTED} to {@code EGRESS_COMPLETED} with the
     * completion source and time, egress's selection counts when reported,
     * {@code EXPORT_EGRESS_COMPLETED} and the companions in one
     * transaction; then the finish job, and the pending deadline check is deleted when the event
     * path completed (the deadline path is that job itself).
     *
     * @param batch the batch, in {@code EGRESS_REQUESTED}
     * @param source who learned of the completion
     * @param outcome what egress reported
     * @param companions events to commit with the transition
     * @param now the time of the write
     * @return the finish job id, or empty when the batch had already moved on
     */
    public java.util.Optional<UUID> egressCompleted(
            ExportBatch batch,
            EgressDetails.CompletionSource source,
            Outcome outcome,
            List<AuditEvent> companions,
            Instant now) {
        Instant completedAt = outcome.completedAt() == null ? now : outcome.completedAt();
        long waitSeconds = batch.egress().requestedAt() == null
                ? 0
                : Math.max(
                        0,
                        Duration.between(batch.egress().requestedAt(), completedAt)
                                .getSeconds());
        EgressSelection selection = outcome.selection();
        boolean counted = selection != null && selection.distinct() != null;
        List<Bson> changes = new java.util.ArrayList<>(List.of(
                Updates.set("egress.completedAt", completedAt), Updates.set("egress.completionSource", source.name())));
        if (counted) {
            changes.addAll(selectionUpdates(selection, now));
        }
        Bson updates = Updates.combine(changes);
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_EGRESS_COMPLETED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("outputUri", outcome.outputUri())
                .detail("totalRecords", outcome.totalRecords())
                .detail("totalRows", outcome.totalRows())
                .detail("waitSeconds", waitSeconds)
                .detail("completionSource", source.name())
                .build();
        if (!transition(
                batch, BatchState.EGRESS_REQUESTED, BatchState.EGRESS_COMPLETED, updates, event, companions, now)) {
            return java.util.Optional.empty();
        }
        if (source == EgressDetails.CompletionSource.EVENT) {
            deleteDeadline(batch);
        }
        UUID jobId = enqueuer.finish(batch.id(), batch.attempt());
        metrics.completion(source.name());
        metrics.egressWaitSeconds(batch.tenantId(), waitSeconds);
        if (counted) {
            metrics.selectionPractitioners(batch.tenantId(), batch.vendor(), selection.distinct());
        }
        LOG.infof("%s egress completed (%s) after %d s; finish job %s", batch.id(), source, waitSeconds, jobId);
        return java.util.Optional.of(jobId);
    }

    /**
     * The batch failed at the egress step (egress reported failure, never finished, or left no
     * file): {@code from} to {@code FAILED} with {@code failedStep = EGRESS}, the cause and
     * {@code EXPORT_BATCH_FAILED}; the pending deadline check is deleted.
     *
     * @param batch the batch
     * @param from the state it is in ({@code EGRESS_REQUESTED} or {@code EGRESS_COMPLETED})
     * @param cause one of the constants on this class
     * @param lastError what was observed
     * @param companions events to commit with the transition
     * @param now the time of the write
     * @return true when the batch was moved
     */
    public boolean failed(
            ExportBatch batch,
            BatchState from,
            String cause,
            String lastError,
            List<AuditEvent> companions,
            Instant now) {
        return failed(batch, from, cause, lastError, companions, now, true);
    }

    /**
     * As {@link #failed(ExportBatch, BatchState, String, String, List, Instant)}, choosing whether
     * the stored deadline job is deleted. The deadline check passes false: the stored id is the job
     * that is running, and deleting a processing JobRunr job makes its own success write fail.
     *
     * @param batch the batch
     * @param from the state it is in
     * @param cause one of the constants on this class
     * @param lastError what was observed
     * @param companions events to commit with the transition
     * @param now the time of the write
     * @param deleteDeadlineJob whether to delete the stored deadline job after the commit
     * @return true when the batch was moved
     */
    public boolean failed(
            ExportBatch batch,
            BatchState from,
            String cause,
            String lastError,
            List<AuditEvent> companions,
            Instant now,
            boolean deleteDeadlineJob) {
        Bson updates = Updates.combine(
                Updates.set("failedStep", ExportBatch.FailedStep.EGRESS.name()), Updates.set("lastError", lastError));
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_FAILED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("failedStep", ExportBatch.FailedStep.EGRESS.name())
                .detail("cause", cause)
                .detail("lastError", lastError)
                .build();
        if (!transition(batch, from, BatchState.FAILED, updates, event, companions, now)) {
            return false;
        }
        if (deleteDeadlineJob) {
            deleteDeadline(batch);
        }
        metrics.egressFailed(batch.tenantId(), cause);
        LOG.warnf("%s FAILED at EGRESS (%s): %s", batch.id(), cause, lastError);
        return true;
    }

    /**
     * The file is verified at the destination: {@code EGRESS_COMPLETED} to {@code DELIVERED} with
     * the file facts, the count reconciliation, who produced the file, {@code EXPORT_RECONCILED} and
     * {@code EXPORT_BATCH_DELIVERED}; the pending deadline check is deleted.
     *
     * @param batch the batch
     * @param file the delivered file's facts
     * @param reconciliation selected against in-file counts
     * @param fileProducedBy the correlation id egress stamped on the object, or null
     * @param countsFromStatus egress's selection block when the finish job had to read it from the
     *     status (the batch held no counts yet), recorded with the delivery; null otherwise
     * @param now the time of the write, which is {@code deliveredAt}
     * @return true when the batch was moved
     */
    public boolean delivered(
            ExportBatch batch,
            FileDetails file,
            Reconciliation reconciliation,
            String fileProducedBy,
            EgressSelection countsFromStatus,
            Instant now) {
        List<Bson> changes = new java.util.ArrayList<>(List.of(
                Updates.set("file", file.toDocument()),
                Updates.set("reconciliation", reconciliation.toDocument()),
                Updates.set("deliveredAt", now),
                Updates.set("egress.fileProducedBy", fileProducedBy)));
        if (countsFromStatus != null) {
            changes.addAll(selectionUpdates(countsFromStatus, now));
        }
        Bson updates = Updates.combine(changes);
        String source = batch.egress().completionSource() == null
                ? null
                : batch.egress().completionSource().name();
        AuditEvent reconciled = AuditEvent.forBatch(
                        AuditEventType.EXPORT_RECONCILED, batch.tenantId(), batch.vendor(), batch.id(), batch.attempt())
                .occurredAt(now)
                .detail("selected", reconciliation.selected())
                .detail("inFile", reconciliation.inFile())
                .detail("match", reconciliation.match())
                .detail("fileProducedBy", fileProducedBy)
                .build();
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_BATCH_DELIVERED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("deliveredAt", now)
                .detail("fileName", file.name())
                .detail("rowCount", file.rowCount())
                .detail("bytes", file.bytes())
                .detail("md5", file.md5())
                .detail("completionSource", source)
                .build();
        if (!transition(
                batch, BatchState.EGRESS_COMPLETED, BatchState.DELIVERED, updates, event, List.of(reconciled), now)) {
            return false;
        }
        deleteDeadline(batch);
        if (!reconciliation.match()) {
            metrics.reconcileMismatch(batch.tenantId());
            LOG.warnf(
                    "%s DELIVERED with a count mismatch: selected %d, in file %d",
                    batch.id(), reconciliation.selected(), reconciliation.inFile());
        } else {
            LOG.infof("%s DELIVERED, %d practitioners reconciled", batch.id(), reconciliation.selected());
        }
        return true;
    }

    /**
     * Egress reported the export failed: the reason decides the outcome.
     *
     * <ul>
     *   <li>{@code NO_MATCHING_PRACTITIONERS}: the saved filter matched nobody, {@code EMPTY} with
     *       {@code EXPORT_BATCH_EMPTY}; no job follows.
     *   <li>{@code REUSE_SOURCE_UNUSABLE}: a retry's reuse source was gone or empty after egress
     *       accepted it; see {@link #reuseRefused}.
     *   <li>Anything else: {@code FAILED} at {@code EGRESS} with {@code EGRESS_FAILED}.
     * </ul>
     *
     * @param batch the batch, in {@code EGRESS_REQUESTED}
     * @param failureReason egress's reason text ({@code "REASON"} or {@code "REASON (detail)"}), or null
     * @param lastError what was observed, for a failure
     * @param source how the outcome arrived
     * @param companions events to commit with the transition
     * @param now the time of the write
     * @param deleteDeadlineJob whether to delete the stored deadline job after the commit; the
     *     deadline check passes false, as for {@link #failed}
     * @return true when the batch was moved
     */
    public boolean egressFailed(
            ExportBatch batch,
            String failureReason,
            String lastError,
            EgressDetails.CompletionSource source,
            List<AuditEvent> companions,
            Instant now,
            boolean deleteDeadlineJob) {
        boolean moved;
        if (EgressFailureReason.is(failureReason, EgressFailureReason.NO_MATCHING_PRACTITIONERS)) {
            moved = markEmpty(batch, failureReason, source, companions, now);
        } else if (EgressFailureReason.is(failureReason, EgressFailureReason.REUSE_SOURCE_UNUSABLE)) {
            moved = reuseRefused(batch, failureReason, source, companions, now);
        } else {
            return failed(
                    batch, BatchState.EGRESS_REQUESTED, EGRESS_FAILED, lastError, companions, now, deleteDeadlineJob);
        }
        if (moved && deleteDeadlineJob) {
            deleteDeadline(batch);
        }
        return moved;
    }

    private boolean markEmpty(
            ExportBatch batch,
            String failureReason,
            EgressDetails.CompletionSource source,
            List<AuditEvent> companions,
            Instant now) {
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
                .detail("completionSource", source.name())
                .build();
        if (!transition(batch, BatchState.EGRESS_REQUESTED, BatchState.EMPTY, updates, event, companions, now)) {
            return false;
        }
        metrics.selectionPractitioners(batch.tenantId(), batch.vendor(), 0);
        LOG.infof(
                "%s: saved filter %s matched nobody; EMPTY",
                batch.id(), batch.selection().filterId());
        return true;
    }

    /**
     * Egress accepted a retry's {@code reuseSelectionFrom} and then found the earlier list gone or
     * empty. The same correlation id cannot be sent again (egress answers {@code 409} for a known
     * one), so the batch goes back to {@code SCHEDULED} as attempt + 1 with
     * {@code EXPORT_REUSE_REFUSED}, and the egress request job runs for the new attempt. That job
     * asks to reuse this attempt, egress refuses at once (its selection never finished), and the
     * job sends the filter id and records {@code selection.reEvaluated}.
     */
    private boolean reuseRefused(
            ExportBatch batch,
            String failureReason,
            EgressDetails.CompletionSource source,
            List<AuditEvent> companions,
            Instant now) {
        int attempt = batch.attempt() + 1;
        AuditEvent event = AuditEvent.forBatch(
                        AuditEventType.EXPORT_REUSE_REFUSED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("egressCorrelationId", batch.egress().correlationId())
                .detail("failureReason", failureReason)
                .detail("completionSource", source.name())
                .detail("newAttempt", attempt)
                .build();
        if (!transition(
                batch,
                BatchState.EGRESS_REQUESTED,
                BatchState.SCHEDULED,
                Updates.set("attempt", attempt),
                event,
                companions,
                now)) {
            return false;
        }
        UUID jobId = enqueuer.requestEgress(batch.id(), attempt);
        LOG.infof(
                "%s: egress could not reuse the list for %s; attempt %d asks again, job %s",
                batch.id(), batch.egress().correlationId(), attempt, jobId);
        return true;
    }

    /**
     * The writes that record egress's selection block on the batch.
     *
     * @param selection the block from the event or the status
     * @param now when it was recorded
     * @return the {@code selection.*} updates; the filter id and {@code reEvaluated} are left as they are
     */
    public static List<Bson> selectionUpdates(EgressSelection selection, Instant now) {
        return List.of(
                Updates.set("selection.requested", selection.requested()),
                Updates.set("selection.distinct", selection.distinct()),
                Updates.set("selection.unmatched", selection.unmatched()),
                Updates.set("selection.parts", selection.parts()),
                Updates.set("selection.reusedFrom", selection.reusedFrom()),
                Updates.set("selection.listUsedUri", selection.listUsedUri()),
                Updates.set("selection.completedAt", now));
    }

    /**
     * The deadline check found egress still running at check 1: a standalone fact, once per check.
     *
     * @param batch the batch
     * @param check the check number
     * @param phaseLastSeen egress's phase
     * @param hoursWaiting since the request
     * @param now the time
     */
    public void egressStale(ExportBatch batch, int check, String phaseLastSeen, long hoursWaiting, Instant now) {
        audit.write(AuditEvent.forBatch(
                        AuditEventType.EXPORT_EGRESS_STALE,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("check", check)
                .detail("phaseLastSeen", phaseLastSeen)
                .detail("hoursWaiting", hoursWaiting)
                .build());
    }

    /**
     * The deadline check scheduled its second check: the pending job id on the batch is replaced.
     *
     * @param batch the batch, in {@code EGRESS_REQUESTED}
     * @param deadlineJobId the second check's id
     * @param now the time
     */
    public void deadlineRescheduled(ExportBatch batch, UUID deadlineJobId, Instant now) {
        transactions.run(session -> batches.updateInState(
                session,
                batch.id(),
                BatchState.EGRESS_REQUESTED,
                now,
                Updates.set("egress.deadlineJobId", deadlineJobId.toString())));
    }

    /**
     * A push delivery that cannot be applied: a standalone service-scoped fact and a counter.
     *
     * @param messageId the Pub/Sub message id
     * @param reason {@code UNPARSEABLE}, {@code UNKNOWN_SCHEMA} or {@code TENANT_MISMATCH}
     * @param correlationId the correlation id in the message, when it could be read
     * @param now the time
     */
    public void eventRejected(String messageId, String reason, String correlationId, Instant now) {
        audit.write(AuditEvent.of(AuditEventType.EXPORT_EVENT_REJECTED, null, null)
                .occurredAt(now)
                .detail("messageId", messageId)
                .detail("reason", reason)
                .detail("correlationId", correlationId)
                .build());
        metrics.eventRejected(reason);
        LOG.warnf("egress event %s rejected: %s (correlationId=%s)", messageId, reason, correlationId);
    }

    private boolean transition(
            ExportBatch batch,
            BatchState from,
            BatchState to,
            Bson updates,
            AuditEvent event,
            List<AuditEvent> companions,
            Instant now) {
        return transactions.run(session -> {
            boolean moved = batches.transition(session, batch.id(), from, to, now, updates);
            if (moved) {
                // The facts that led to the transition (received, missed, reconciled) are written
                // before the transition's own event, so the trail reads in the order things happened.
                for (AuditEvent companion : companions) {
                    audit.write(session, companion);
                }
                audit.write(session, event);
            }
            return moved;
        });
    }

    private void deleteDeadline(ExportBatch batch) {
        if (batch.egress() != null && batch.egress().deadlineJobId() != null) {
            enqueuer.delete(UUID.fromString(batch.egress().deadlineJobId()));
        }
    }
}
