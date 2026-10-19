package com.certifyos.vendor_exchange.ingestion.review;

import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus.Action;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * The write half of a reviewer decision: one transaction holding the guarded update of the field
 * recommendation and its {@code FIELD_*} event. If the guard matches nothing (another writer moved
 * the document, or the version is stale) nothing is written and the caller answers 409.
 */
@ApplicationScoped
public class DecisionWriter {

    private final Transactions transactions;
    private final FieldRecommendationRepository recommendations;
    private final AuditRepository audit;

    public DecisionWriter(
            Transactions transactions, FieldRecommendationRepository recommendations, AuditRepository audit) {
        this.transactions = transactions;
        this.recommendations = recommendations;
        this.audit = audit;
    }

    /**
     * Records one decision.
     *
     * @param current the document as the caller read it; its version is the guard
     * @param action {@code APPROVE}, {@code REJECT} or {@code SKIP}
     * @param comment the reviewer's comment, or null
     * @param actor the reviewer
     * @param now the decision time, millisecond precision
     * @return true when the document moved and the event was written; false when the guard missed
     */
    public boolean record(FieldRecommendation current, Action action, String comment, String actor, Instant now) {
        return transactions.run(session -> record(session, current, action, comment, actor, now));
    }

    private boolean record(
            ClientSession session,
            FieldRecommendation current,
            Action action,
            String comment,
            String actor,
            Instant now) {
        FieldRecommendation.HistoryEntry entry =
                new FieldRecommendation.HistoryEntry(action.name(), actor, now, comment);
        boolean moved = recommendations.decide(
                session,
                current.tenantId(),
                current.id(),
                current.version(),
                startingStates(action),
                target(action),
                Updates.push("reviewHistory", entry.toDocument()),
                now);
        if (!moved) {
            return false;
        }
        audit.write(session, event(current, action, comment, actor, now));
        return true;
    }

    /**
     * The event of a decision. Detail: the field, the location for a location-level field, the state
     * the decision started from, and the version the decision produced (the version the document now
     * carries); the comment when there is one. A rejection also names {@code rejectedValueId} and
     * {@code rejectionCount}, null until the rejected-value memory exists.
     */
    static AuditEvent event(FieldRecommendation current, Action action, String comment, String actor, Instant now) {
        AuditEvent.Builder builder = AuditEvent.of(eventType(action), current.tenantId(), current.source())
                .batch(current.exportBatchId())
                .upload(
                        current.sourceFile() == null
                                ? null
                                : current.sourceFile().uploadId())
                .field(current.id(), current.practitioner().certifyPractitionerId())
                .actor(actor)
                .occurredAt(now)
                .detail("field", current.field())
                .detail(
                        "certifyLocationId",
                        current.location() == null ? null : current.location().certifyLocationId())
                .detail("fromStatus", current.reviewStatus().name())
                .detail("version", current.version() + 1)
                .detail("comment", comment);
        if (action == Action.REJECT) {
            builder.detail("rejectedValueId", null).detail("rejectionCount", null);
        }
        return builder.build();
    }

    /**
     * The state a decision moves to.
     *
     * @param action {@code APPROVE}, {@code REJECT} or {@code SKIP}
     * @return {@code APPROVED}, {@code REJECTED} or {@code SKIPPED}
     * @throws IllegalArgumentException for an action that is not a decision
     */
    static ReviewStatus target(Action action) {
        return switch (action) {
            case APPROVE -> ReviewStatus.APPROVED;
            case REJECT -> ReviewStatus.REJECTED;
            case SKIP -> ReviewStatus.SKIPPED;
            case ROLLBACK, RELEASE -> throw new IllegalArgumentException(action + " is not a review decision");
        };
    }

    private static AuditEventType eventType(Action action) {
        return switch (target(action)) {
            case APPROVED -> AuditEventType.FIELD_APPROVED;
            case REJECTED -> AuditEventType.FIELD_REJECTED;
            default -> AuditEventType.FIELD_SKIPPED;
        };
    }

    /** Every state {@link ReviewStatus#allows} the action from: the guard's {@code reviewStatus in}. */
    private static Set<ReviewStatus> startingStates(Action action) {
        Set<ReviewStatus> from = EnumSet.noneOf(ReviewStatus.class);
        for (ReviewStatus status : ReviewStatus.values()) {
            if (status.allows(action)) {
                from.add(status);
            }
        }
        return from;
    }
}
