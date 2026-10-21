package com.certifyos.directory_accuracy.ingestion.review;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus.Action;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Set;

/**
 * The write half of a reviewer decision: one transaction holding the guarded update of the field
 * recommendation, on {@code REJECT} the upsert of the rejected value into {@code
 * vendor_rejected_values} (design step 7), and its {@code FIELD_*} event. If the guard matches
 * nothing (another writer moved the document, or the version is stale) nothing is written and the
 * caller answers 409; if the memory write fails, the decision rolls back with it.
 */
@ApplicationScoped
public class DecisionWriter {

    private final Transactions transactions;
    private final FieldRecommendationRepository recommendations;
    private final AuditRepository audit;
    private final RejectedValueRepository rejectedValues;
    private final VendorExchangeConfig config;

    public DecisionWriter(
            Transactions transactions,
            FieldRecommendationRepository recommendations,
            AuditRepository audit,
            RejectedValueRepository rejectedValues,
            VendorExchangeConfig config) {
        this.transactions = transactions;
        this.recommendations = recommendations;
        this.audit = audit;
        this.rejectedValues = rejectedValues;
        this.config = config;
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
        RejectedValueRepository.Upserted remembered = action == Action.REJECT
                ? rejectedValues.upsert(
                        session,
                        current,
                        actor,
                        comment,
                        now,
                        config.ingestion().rejectedValueTtlMonths())
                : null;
        audit.write(session, event(current, action, comment, actor, now, remembered));
        return true;
    }

    /**
     * The event of a decision. Detail: the field, the location for a location-level field, the state
     * the decision started from, and the version the decision produced (the version the document now
     * carries); the comment when there is one. A rejection also carries the rejected-value row it
     * landed on, {@code rejectedValueId} and {@code rejectionCount}.
     */
    static AuditEvent event(
            FieldRecommendation current,
            Action action,
            String comment,
            String actor,
            Instant now,
            RejectedValueRepository.Upserted remembered) {
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
            builder.detail("rejectedValueId", remembered == null ? null : remembered.id())
                    .detail("rejectionCount", remembered == null ? null : remembered.rejectionCount());
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
