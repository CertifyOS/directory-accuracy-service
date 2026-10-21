package com.certifyos.directory_accuracy.export.events;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.export.batch.BatchCompletion;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoWriteException;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * The completion event handler table (design step 4). Every outcome but an unexpected exception
 * acknowledges: Pub/Sub would redeliver anything else until the dead-letter limit, and nothing in
 * the table improves on redelivery. The state check runs first, so a redelivered or late message
 * for a batch that already moved on is a no-op; the partial unique index on {@code messageId}
 * closes the remaining race between two deliveries of one message.
 */
@ApplicationScoped
public class EgressEventHandler {

    /** The one schema this handler understands. */
    public static final String SCHEMA_VERSION = "egress-export-event-v1";

    static final String PHASE_COMPLETED = "COMPLETED";
    static final String PHASE_FAILED = "FAILED";
    private static final Logger LOG = Logger.getLogger(EgressEventHandler.class);

    private final ObjectMapper mapper;
    private final ExportBatchRepository batches;
    private final BatchCompletion completion;
    private final Clock clock;

    public EgressEventHandler(
            ObjectMapper mapper, ExportBatchRepository batches, BatchCompletion completion, Clock clock) {
        this.mapper = mapper;
        this.batches = batches;
        this.completion = completion;
        this.clock = clock;
    }

    /** What the handler did with a delivery, for the log and the tests. */
    public enum Outcome {
        REJECTED_UNPARSEABLE,
        REJECTED_UNKNOWN_SCHEMA,
        UNKNOWN_CORRELATION,
        REJECTED_TENANT_MISMATCH,
        STATE_PASSED,
        IGNORED_PHASE,
        COMPLETED,
        FAILED,
        DUPLICATE
    }

    /**
     * Applies one push delivery.
     *
     * @param message the Pub/Sub message
     * @return what happened
     */
    public Outcome handle(PubSubPushMessage.Message message) {
        Instant now = clock.instant();
        String messageId = message.messageId();
        EgressEvent event = parse(message.data());
        if (event == null) {
            completion.eventRejected(messageId, "UNPARSEABLE", null, now);
            return Outcome.REJECTED_UNPARSEABLE;
        }
        if (!SCHEMA_VERSION.equals(event.schemaVersion())) {
            completion.eventRejected(messageId, "UNKNOWN_SCHEMA", event.correlationId(), now);
            return Outcome.REJECTED_UNKNOWN_SCHEMA;
        }
        Optional<ExportBatch> found =
                event.correlationId() == null ? Optional.empty() : batches.findByCorrelationId(event.correlationId());
        if (found.isEmpty()) {
            LOG.infof("egress event %s for unknown correlation %s acknowledged", messageId, event.correlationId());
            return Outcome.UNKNOWN_CORRELATION;
        }
        ExportBatch batch = found.get();
        if (!batch.tenantId().equals(event.tenantId())) {
            completion.eventRejected(messageId, "TENANT_MISMATCH", event.correlationId(), now);
            return Outcome.REJECTED_TENANT_MISMATCH;
        }
        if (batch.state() != BatchState.EGRESS_REQUESTED) {
            LOG.infof("egress event %s for %s in %s acknowledged without effect", messageId, batch.id(), batch.state());
            return Outcome.STATE_PASSED;
        }
        try {
            return apply(batch, event, message, now);
        } catch (MongoWriteException write) {
            if (Documents.isDuplicateKey(write)) {
                LOG.infof("egress event %s already applied to %s", messageId, batch.id());
                return Outcome.DUPLICATE;
            }
            throw write;
        }
    }

    private EgressEvent parse(String data) {
        try {
            return mapper.readValue(Base64.getDecoder().decode(data), EgressEvent.class);
        } catch (IOException | IllegalArgumentException malformed) {
            return null;
        }
    }

    private Outcome apply(ExportBatch batch, EgressEvent event, PubSubPushMessage.Message message, Instant now) {
        AuditEvent received = AuditEvent.forBatch(
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        batch.tenantId(),
                        batch.vendor(),
                        batch.id(),
                        batch.attempt())
                .occurredAt(now)
                .detail("messageId", message.messageId())
                .detail("publishTime", message.publishTime())
                .detail("phase", event.phase())
                .detail("outputUri", event.outputUri())
                .detail("totalRecords", event.totalRecords())
                .detail("totalRows", event.totalRows())
                .detail("failureMessage", event.failureMessage())
                .build();
        if (PHASE_COMPLETED.equals(event.phase())) {
            boolean moved = completion
                    .egressCompleted(
                            batch,
                            EgressDetails.CompletionSource.EVENT,
                            new BatchCompletion.Outcome(
                                    event.outputUri(), event.totalRecords(), event.totalRows(), event.completedAt()),
                            List.of(received),
                            now)
                    .isPresent();
            return moved ? Outcome.COMPLETED : Outcome.STATE_PASSED;
        }
        if (PHASE_FAILED.equals(event.phase())) {
            String cause = event.failureMessage() == null ? "egress reported FAILED" : event.failureMessage();
            boolean moved = completion.failed(
                    batch, BatchState.EGRESS_REQUESTED, BatchCompletion.EGRESS_FAILED, cause, List.of(received), now);
            return moved ? Outcome.FAILED : Outcome.STATE_PASSED;
        }
        LOG.infof(
                "egress event %s phase %s for %s acknowledged without effect",
                message.messageId(), event.phase(), batch.id());
        return Outcome.IGNORED_PHASE;
    }
}
