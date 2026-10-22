package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.clients.EgressCancelResponse;
import com.certifyos.directory_accuracy.clients.EgressClient;
import com.certifyos.directory_accuracy.clients.EgressStatusResponse;
import com.certifyos.directory_accuracy.export.batch.BatchCompletion;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.WebApplicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * Lifecycle step 5: the fallback when the completion event never arrives. Check 1 runs at request +
 * {@code deadlineHours}: a finished file completes the batch ({@code completionSource = DEADLINE})
 * with the status's selection counts, a failed job is settled by its selection reason (empty,
 * asked again, or failed), anything else is recorded as stale and check 2 is scheduled at request +
 * {@code abandonHours}. Check 2 fails a batch still running after a best-effort cancel. Only runs
 * while the batch is {@code EGRESS_REQUESTED}; egress errors propagate for JobRunr's retry.
 */
@ApplicationScoped
public class DeadlineCheckJob implements JobRequestHandler<DeadlineCheckJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "deadline-check";

    static final String PHASE_COMPLETED = "COMPLETED";
    static final String PHASE_FAILED = "FAILED";
    private static final Logger LOG = Logger.getLogger(DeadlineCheckJob.class);

    private final BatchJobSupport support;
    private final BatchCompletion completion;
    private final BatchLifecycle lifecycle;
    private final JobEnqueuer enqueuer;
    private final EgressClient egress;
    private final Clock clock;

    public DeadlineCheckJob(
            BatchJobSupport support,
            BatchCompletion completion,
            BatchLifecycle lifecycle,
            JobEnqueuer enqueuer,
            @RestClient EgressClient egress,
            Clock clock) {
        this.support = support;
        this.completion = completion;
        this.lifecycle = lifecycle;
        this.enqueuer = enqueuer;
        this.egress = egress;
        this.clock = clock;
    }

    /**
     * Runs one check for one batch.
     *
     * @param request the batch, expected in {@code EGRESS_REQUESTED}, the attempt and the check
     */
    @Override
    @Job(name = "deadline-check %0")
    public void run(DeadlineCheckJobRequest request) {
        try (JobLogContext ignored = JobLogContext.open(request.exportBatchId(), JobLogContext.currentJob())) {
            if (!support.enabled()) {
                // Not a silent no-op like the other jobs: this one-shot check is the only thing that
                // moves an EGRESS_REQUESTED batch whose event was missed, and the reconciler never
                // re-enqueues that state. Failing makes JobRunr retry it with backoff until the
                // service is enabled again; if the switch stays off past the retries, the final
                // failure filter marks the batch FAILED at EGRESS, where alert E4 and retry find it.
                throw new IllegalStateException("service disabled; deadline check " + request + " deferred to a retry");
            }
            Optional<ExportBatch> loaded =
                    support.loadExpecting(NAME, request.exportBatchId(), BatchState.EGRESS_REQUESTED);
            if (loaded.isEmpty()) {
                return;
            }
            check(loaded.get(), request.check());
        }
    }

    private void check(ExportBatch batch, int check) {
        Instant now = clock.instant();
        String correlationId = batch.egress().correlationId();
        long hours = batch.egress().requestedAt() == null
                ? 0
                : Duration.between(batch.egress().requestedAt(), now).toHours();
        EgressStatusResponse status = egress.status(batch.tenantId(), batch.tenantId(), correlationId);
        String phase = status.phase() == null ? status.state() : status.phase();
        LOG.infof(
                "%s deadline check %d: egress phase %s, gcs_complete %s, %d h after request",
                batch.id(), check, phase, status.gcsComplete(), hours);
        if (PHASE_COMPLETED.equals(phase) && Boolean.TRUE.equals(status.gcsComplete())) {
            completion.egressCompleted(
                    batch,
                    EgressDetails.CompletionSource.DEADLINE,
                    new BatchCompletion.Outcome(
                            status.gcsUri(), status.rowsWithData(), status.totalRows(), null, status.selection()),
                    List.of(missed(batch, check, phase, hours, now)),
                    now);
            return;
        }
        if (PHASE_FAILED.equals(phase)) {
            completion.egressFailed(
                    batch,
                    status.selection() == null ? null : status.selection().failureReason(),
                    "egress reported " + phase + " at deadline check " + check,
                    EgressDetails.CompletionSource.DEADLINE,
                    List.of(missed(batch, check, phase, hours, now)),
                    now,
                    false);
            return;
        }
        if (check == 1) {
            completion.egressStale(batch, check, phase, hours, now);
            Instant abandonAt = (batch.egress().requestedAt() == null
                            ? now
                            : batch.egress().requestedAt())
                    .plus(Duration.ofHours(support.config().egress().abandonHours()));
            UUID second =
                    enqueuer.deadlineCheck(batch.id(), batch.attempt(), 2, abandonAt.isAfter(now) ? abandonAt : now);
            completion.deadlineRescheduled(batch, second, now);
            LOG.warnf("%s still %s at check 1; check 2 %s at %s", batch.id(), phase, second, abandonAt);
            return;
        }
        cancel(batch, correlationId, now);
        completion.failed(
                batch,
                BatchState.EGRESS_REQUESTED,
                BatchCompletion.EGRESS_DID_NOT_FINISH,
                "egress still " + phase + " " + hours + " h after the request",
                List.of(),
                now,
                false);
    }

    private void cancel(ExportBatch batch, String correlationId, Instant now) {
        try {
            EgressCancelResponse cancelled = egress.cancel(batch.tenantId(), batch.tenantId(), correlationId);
            lifecycle.priorAttemptCancel(batch, correlationId, true, cancelled.status(), now);
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            lifecycle.priorAttemptCancel(batch, correlationId, false, "HTTP " + status, now);
        } catch (jakarta.ws.rs.ProcessingException unreachable) {
            // Best effort: an unreachable egress must not keep the batch from being abandoned.
            lifecycle.priorAttemptCancel(batch, correlationId, false, "unreachable: " + unreachable.getMessage(), now);
        }
    }

    private static com.certifyos.directory_accuracy.audit.AuditEvent missed(
            ExportBatch batch, int check, String phaseFound, long hours, Instant now) {
        return com.certifyos.directory_accuracy.audit.AuditEvent.forBatch(
                        com.certifyos.directory_accuracy.audit.AuditEventType.EXPORT_EVENT_MISSED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("check", check)
                .detail("phaseFound", phaseFound)
                .detail("hoursSinceRequest", hours)
                .build();
    }
}
