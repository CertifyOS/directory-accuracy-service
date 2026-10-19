package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.http.ProblemException;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.vendor_exchange.ingestion.jobs.StageRowsJob;
import com.certifyos.vendor_exchange.ingestion.staging.StagingLifecycle;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import org.jboss.logging.Logger;

/**
 * The completion event handler table (design step 4): file-ingestion's event for one ingestion,
 * read from the base64 message data and matched to its entry by {@code externalRef}. A finished
 * parse ({@code COMPLETED} or {@code PARTIAL}) moves the entry {@code INGESTION_REQUESTED → PARSED}
 * ({@code completionSource = EVENT}); after that commit the deadline check is deleted and the stage
 * job enqueued and recorded on the entry, as the deadline check does when it finds a finished parse.
 * A refusal ({@code INVALID}) moves it to {@code INGESTION_FAILED} and deletes the deadline check.
 * The guarded moves make a duplicate or late event a no-op.
 *
 * <p>Every outcome acknowledges but one: an entry still {@code ARCHIVING} means the request job has
 * registered the file but not yet committed {@code INGESTION_REQUESTED}, so the handler throws a 503
 * problem and Pub/Sub redelivers until the commit lands (should the dead-letter topic take it first,
 * the deadline check still learns the outcome). An unexpected exception reaches the 500 mapper and
 * is redelivered the same way.
 */
@ApplicationScoped
public class CompletionHandler {

    /** {@code completionSource} for an outcome learned from this event. */
    static final String EVENT = "EVENT";

    static final String COMPLETED = "COMPLETED";
    static final String PARTIAL = "PARTIAL";
    static final String INVALID = "INVALID";

    /** The problem code of the redelivery answer for an entry still {@code ARCHIVING}. */
    public static final String ARCHIVING_NOT_COMMITTED = "ARCHIVING_NOT_COMMITTED";

    private static final Logger LOG = Logger.getLogger(CompletionHandler.class);

    private final ObjectMapper mapper;
    private final InboundFileRepository inbound;
    private final InboundLifecycle lifecycle;
    private final StagingLifecycle stagingLifecycle;
    private final IngestionJobEnqueuer enqueuer;
    private final VendorExchangeConfig cfg;
    private final Clock clock;

    public CompletionHandler(
            ObjectMapper mapper,
            InboundFileRepository inbound,
            InboundLifecycle lifecycle,
            StagingLifecycle stagingLifecycle,
            IngestionJobEnqueuer enqueuer,
            VendorExchangeConfig cfg,
            Clock clock) {
        this.mapper = mapper;
        this.inbound = inbound;
        this.lifecycle = lifecycle;
        this.stagingLifecycle = stagingLifecycle;
        this.enqueuer = enqueuer;
        this.cfg = cfg;
        this.clock = clock;
    }

    /** What the handler did with a delivery, for the log and the tests. */
    public enum Outcome {
        /** The data was missing, not base64 or not the event's JSON object. */
        REJECTED_UNPARSEABLE,
        /** No external ref, or one no entry carries: not ours. */
        UNKNOWN_REF,
        /** The event names another tenant than the batch the ref points at, or none. */
        REJECTED_TENANT_MISMATCH,
        /** The entry has moved past {@code INGESTION_REQUESTED} (a duplicate or late event). */
        STATE_PASSED,
        /** {@code PARSED}, the deadline check deleted and the stage job enqueued. */
        PARSED,
        /** {@code INGESTION_FAILED}, the deadline check deleted. */
        INGESTION_FAILED,
        /** A status this handler does not act on; the deadline check still owns the entry. */
        IGNORED_STATUS,
        /** The service is switched off; acknowledged with nothing written. */
        DISABLED
    }

    /**
     * Applies one push delivery.
     *
     * @param message the Pub/Sub message; its data is the event body
     * @return what happened
     * @throws ProblemException 503 {@value #ARCHIVING_NOT_COMMITTED} when the entry is still {@code
     *     ARCHIVING}, so Pub/Sub redelivers
     */
    public Outcome handle(PubSubPushMessage.Message message) {
        String messageId = message.messageId();
        if (!cfg.enabled()) {
            LOG.infof("completion %s acknowledged, the service is disabled", messageId);
            return Outcome.DISABLED;
        }
        Optional<CompletionEvent> read = parse(message.data());
        if (read.isEmpty()) {
            LOG.warnf("completion %s has no readable event body, acknowledged", messageId);
            return Outcome.REJECTED_UNPARSEABLE;
        }
        CompletionEvent event = read.get();
        Optional<Located> found =
                event.externalRef() == null ? Optional.empty() : inbound.findByExternalRef(event.externalRef());
        if (found.isEmpty()) {
            LOG.infof(
                    "completion %s for ingestion %s matches no inbound file, acknowledged",
                    messageId, event.ingestionId());
            return Outcome.UNKNOWN_REF;
        }
        Located located = found.get();
        if (!located.batch().tenantId().equals(event.tenantId())) {
            LOG.warnf(
                    "completion %s names another tenant than batch %s upload %s, acknowledged and dropped",
                    messageId, located.batch().id(), located.entry().uploadId());
            return Outcome.REJECTED_TENANT_MISMATCH;
        }
        InboundFileState state = located.entry().state();
        if (state == InboundFileState.ARCHIVING) {
            LOG.infof(
                    "completion %s for %s upload %s came before INGESTION_REQUESTED was committed; redelivery asked",
                    messageId, located.batch().id(), located.entry().uploadId());
            throw ProblemException.unavailable(
                    ARCHIVING_NOT_COMMITTED, "the ingestion request is not committed yet; deliver again later");
        }
        if (state == InboundFileState.PARSED && !stageJobRecorded(located)) {
            // A redelivery after a delivery that committed PARSED and failed before the stage job
            // was recorded: the id is deterministic, so the enqueue is a no-op when the job exists.
            LOG.infof(
                    "completion %s for %s upload %s is PARSED without its stage job; handed over again",
                    messageId, located.batch().id(), located.entry().uploadId());
            handToStaging(located);
            return Outcome.STATE_PASSED;
        }
        if (state != InboundFileState.INGESTION_REQUESTED) {
            LOG.infof(
                    "completion %s for %s upload %s in %s acknowledged without effect",
                    messageId, located.batch().id(), located.entry().uploadId(), state);
            return Outcome.STATE_PASSED;
        }
        return apply(located, event, messageId);
    }

    private Outcome apply(Located located, CompletionEvent event, String messageId) {
        Instant now = clock.instant();
        String status = event.status();
        if (COMPLETED.equals(status) || PARTIAL.equals(status)) {
            if (!lifecycle.parsed(located, event.counts(), status, EVENT, now)) {
                return Outcome.STATE_PASSED;
            }
            // The stage job first: a failure here answers 500 and the redelivery finds the entry
            // PARSED and hands it over again. The delete is best effort, since a surviving deadline
            // check exits as a no-op on a PARSED entry.
            handToStaging(located);
            deleteDeadlineCheckQuietly(located);
            return Outcome.PARSED;
        }
        if (INVALID.equals(status)) {
            String reason = event.reason() == null ? INVALID : event.reason();
            if (!lifecycle.ingestionFailed(located, reason, EVENT, now)) {
                return Outcome.STATE_PASSED;
            }
            deleteDeadlineCheck(located);
            return Outcome.INGESTION_FAILED;
        }
        LOG.infof(
                "completion %s for %s upload %s has status %s; left to the deadline check",
                messageId, located.batch().id(), located.entry().uploadId(), status);
        return Outcome.IGNORED_STATUS;
    }

    private Optional<CompletionEvent> parse(String data) {
        if (data == null) {
            return Optional.empty();
        }
        try {
            return Optional.ofNullable(mapper.readValue(Base64.getDecoder().decode(data), CompletionEvent.class));
        } catch (IOException | IllegalArgumentException malformed) {
            return Optional.empty();
        }
    }

    /**
     * Deletes the job that owned the {@code INGESTION_REQUESTED} entry: the deadline check, or one
     * the reconciler re-enqueued. Runs after the commit. A job id that is not a UUID is logged and
     * left; that check would find the entry moved and exit as a no-op.
     */
    private void deleteDeadlineCheck(Located located) {
        InboundFile entry = located.entry();
        if (entry.jobId() == null) {
            return;
        }
        UUID deadline;
        try {
            deadline = UUID.fromString(entry.jobId());
        } catch (IllegalArgumentException notAUuid) {
            LOG.warnf(
                    "%s upload %s: job id is not a UUID; deadline check not deleted",
                    located.batch().id(), entry.uploadId());
            return;
        }
        enqueuer.delete(deadline);
    }

    /**
     * Whether a stage job already owns the entry: the first deterministic one, the reconciler's latest
     * re-enqueue, or any stage job JobRunr still holds under the recorded id. Only an entry that still
     * names the job that owned {@code INGESTION_REQUESTED} is handed over again, so a reconciled stage
     * job never gets a second one beside it.
     */
    private boolean stageJobRecorded(Located located) {
        InboundFile entry = located.entry();
        String recorded = entry.jobId();
        if (recorded == null) {
            return false;
        }
        String first = IngestionJobEnqueuer.stageRowsId(entry.uploadId(), entry.attempt())
                .toString();
        String reconciled = entry.reconciliations() == 0
                ? null
                : IngestionJobEnqueuer.reconciledId(
                                StageRowsJob.NAME, entry.uploadId(), entry.reconciliations(), entry.attempt())
                        .toString();
        return recorded.equals(first)
                || recorded.equals(reconciled)
                || enqueuer.isPending(recorded, StageRowsJob.class);
    }

    /** {@link #deleteDeadlineCheck} after the stage hand-over, where a failure must not undo the 204. */
    private void deleteDeadlineCheckQuietly(Located located) {
        try {
            deleteDeadlineCheck(located);
        } catch (RuntimeException exception) {
            LOG.warnf(
                    exception,
                    "%s upload %s: deadline check not deleted; it will exit as a no-op on the PARSED entry",
                    located.batch().id(),
                    located.entry().uploadId());
        }
    }

    /**
     * After the {@code PARSED} commit: enqueue the stage job, then record it on the entry, the
     * hand-over the deadline check makes too. A stage job that has already moved the entry to
     * {@code STAGING} leaves the job id unrecorded, which is logged.
     */
    private void handToStaging(Located located) {
        UUID stageJobId = enqueuer.stageRows(
                located.batch().id(),
                located.entry().uploadId(),
                located.entry().attempt());
        stagingLifecycle.jobRecorded(located, stageJobId, clock.instant());
    }
}
