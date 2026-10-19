package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.clients.FileIngestionClient;
import com.certifyos.vendor_exchange.clients.FileIngestionStatus;
import com.certifyos.vendor_exchange.export.jobs.JobLogContext;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.staging.StagingLifecycle;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;
import org.jobrunr.jobs.annotations.Job;
import org.jobrunr.jobs.context.JobContext;
import org.jobrunr.jobs.lambdas.JobRequestHandler;

/**
 * The fallback of design step 4: when file-ingestion's completion event has not moved the entry by
 * request + {@code ingestionDeadlineHours}, ask file-ingestion once. A finished parse moves the entry
 * to {@code PARSED} ({@code completionSource = DEADLINE}) and, after that commit, enqueues the
 * stage-rows job and records it as the entry's job; an invalid file moves to {@code
 * INGESTION_FAILED}; anything else fails the entry. Only runs while the entry is {@code
 * INGESTION_REQUESTED}; file-ingestion errors propagate for JobRunr's retry. An entry still in
 * {@code ARCHIVING} throws for a retry too: the request job schedules this check before it commits
 * {@code INGESTION_REQUESTED}, and a rerun of the request job schedules under the same id, which
 * JobRunr keeps, so a no-op here could leave the committed entry with no pending check. Every other
 * state exits as a no-op.
 */
@ApplicationScoped
public class IngestionDeadlineCheckJob implements JobRequestHandler<IngestionDeadlineCheckJobRequest> {

    /** Job name used in ids and logs. */
    public static final String NAME = "ingestion-deadline";

    static final String DEADLINE = "DEADLINE";
    static final String STEP_PARSE = "PARSE";
    static final String DID_NOT_FINISH = "INGESTION_DID_NOT_FINISH";
    static final String COMPLETED = "COMPLETED";
    static final String PARTIAL = "PARTIAL";
    static final String INVALID = "INVALID";

    private static final Set<InboundFileState> EXPECTED =
            EnumSet.of(InboundFileState.INGESTION_REQUESTED, InboundFileState.ARCHIVING);
    private static final Logger LOG = Logger.getLogger(IngestionDeadlineCheckJob.class);

    private final InboundJobSupport support;
    private final InboundLifecycle lifecycle;
    private final StagingLifecycle stagingLifecycle;
    private final FileIngestionClient fileIngestion;
    private final IngestionJobEnqueuer enqueuer;
    private final Clock clock;

    public IngestionDeadlineCheckJob(
            InboundJobSupport support,
            InboundLifecycle lifecycle,
            StagingLifecycle stagingLifecycle,
            @RestClient FileIngestionClient fileIngestion,
            IngestionJobEnqueuer enqueuer,
            Clock clock) {
        this.support = support;
        this.lifecycle = lifecycle;
        this.stagingLifecycle = stagingLifecycle;
        this.fileIngestion = fileIngestion;
        this.enqueuer = enqueuer;
        this.clock = clock;
    }

    /**
     * Runs the check for one upload.
     *
     * @param request the batch, the upload (expected in {@code INGESTION_REQUESTED}) and the attempt
     */
    @Override
    @Job(name = "ingestion-deadline %0")
    public void run(IngestionDeadlineCheckJobRequest request) {
        JobContext job = JobLogContext.currentJob();
        try (IngestionLogContext ignored = IngestionLogContext.open(request.exportBatchId(), request.uploadId(), job)) {
            if (!support.enabled()) {
                // Not a silent no-op: this one-shot check is the only thing that moves an entry whose
                // completion event was missed. Failing makes JobRunr retry it until the switch is on.
                throw new IllegalStateException(
                        "service disabled; ingestion deadline check " + request + " deferred to a retry");
            }
            Optional<Located> loaded = support.loadExpecting(NAME, request.uploadId(), request.attempt(), EXPECTED);
            if (loaded.isEmpty()) {
                return;
            }
            if (loaded.get().entry().state() == InboundFileState.ARCHIVING) {
                throw new IllegalStateException("ingestion deadline check " + request
                        + " found the entry still ARCHIVING; the request job has not committed, retrying");
            }
            check(loaded.get(), InboundJobSupport.jobAttempts(job));
        }
    }

    private void check(Located located, int attempts) {
        String tenantId = located.batch().tenantId();
        String identifier = support.config().fileIngestion().consumerIdentifier();
        FileIngestionStatus status =
                fileIngestion.status(tenantId, identifier, located.entry().ingestionId());
        String found = status.status();
        LOG.infof(
                "%s upload %s deadline check: ingestion %s is %s",
                located.batch().id(),
                located.entry().uploadId(),
                located.entry().ingestionId(),
                found);
        Instant now = clock.instant();
        if (COMPLETED.equals(found) || PARTIAL.equals(found)) {
            if (lifecycle.parsed(located, status.counts(), found, DEADLINE, now)) {
                handToStaging(located);
            }
            return;
        }
        if (INVALID.equals(found)) {
            String reason = status.invalidReason() == null ? INVALID : status.invalidReason();
            lifecycle.ingestionFailed(located, reason, DEADLINE, now);
            return;
        }
        lifecycle.failed(
                located, InboundFileState.INGESTION_REQUESTED, STEP_PARSE, DID_NOT_FINISH, found, attempts, now);
    }

    /**
     * After the {@code PARSED} commit: enqueue the stage job, then record it on the entry. A stage job
     * that has already moved the entry to {@code STAGING} leaves the job id unrecorded, which is logged.
     */
    private void handToStaging(Located located) {
        UUID stageJobId = enqueuer.stageRows(
                located.batch().id(),
                located.entry().uploadId(),
                located.entry().attempt());
        stagingLifecycle.jobRecorded(located, stageJobId, clock.instant());
    }
}
