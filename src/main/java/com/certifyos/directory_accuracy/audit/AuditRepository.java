package com.certifyos.directory_accuracy.audit;

import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * The audit trail: append-only, one document per event, kept seven years. Field events ({@link
 * AuditEventType.Scope#FIELD}) go to {@code vendor_review_events}, every other event to {@code
 * vendor_export_events}. Three ways to write, one per commit rule in the design: inside a
 * transition's transaction, standalone, or standalone and idempotent on a natural key (a received
 * message id). Nothing here ever updates or deletes.
 */
@ApplicationScoped
public class AuditRepository {

    /** The logger the structured audit line goes to; alerts count these lines in Cloud Logging. */
    public static final String AUDIT_LOGGER = "audit";

    /** The detail keys the designs' alerts evaluate on; echoed on the line when the event carries them. */
    static final List<String> ALERT_KEYS = List.of(
            "cause",
            "completionSource",
            "match",
            "reason",
            "check",
            "trigger",
            "failureReason",
            "outcome",
            "status",
            "action",
            "writePath");

    /**
     * An alert value reaches the line only as one token: letters, digits and {@code _ . : / -}, at
     * most 64 characters. Free text (an exception message as an outcome) stays in the stored detail.
     */
    private static final Pattern TOKEN = Pattern.compile("^[A-Za-z0-9_.:/-]{1,64}$");

    private static final Logger AUDIT = Logger.getLogger(AUDIT_LOGGER);

    private final Collections collections;

    public AuditRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * The one line per audit event, after the write: {@code AUDIT type=... tenantId=... vendor=...
     * exportBatchId=... attempt=... actor=...}, then the ingestion ids the event carries ({@code
     * uploadId}, {@code fieldRecommendationId}, {@code certifyPractitionerId}, {@code releaseId}),
     * then the alert keys the event carries whose value is a single token. Ids and enumerations
     * only, never an NPI, a field value or free text.
     *
     * @param event the event written
     * @return the line
     */
    static String line(AuditEvent event) {
        StringBuilder out = new StringBuilder("AUDIT type=").append(event.type());
        out.append(" tenantId=").append(event.tenantId());
        out.append(" vendor=").append(event.vendor());
        out.append(" exportBatchId=").append(event.exportBatchId());
        out.append(" attempt=").append(event.attempt());
        out.append(" actor=").append(event.actor());
        appendWhenSet(out, "uploadId", event.uploadId());
        appendWhenSet(out, "fieldRecommendationId", event.fieldRecommendationId());
        appendWhenSet(out, "certifyPractitionerId", event.certifyPractitionerId());
        appendWhenSet(out, "releaseId", event.releaseId());
        for (String key : ALERT_KEYS) {
            Object value = event.detail().get(key);
            if (value != null && TOKEN.matcher(value.toString()).matches()) {
                appendWhenSet(out, key, value);
            }
        }
        return out.toString();
    }

    private static void appendWhenSet(StringBuilder out, String key, Object value) {
        if (value != null) {
            out.append(' ').append(key).append('=').append(value);
        }
    }

    private static void log(AuditEvent event) {
        AUDIT.info(line(event));
    }

    /** The collection an event belongs in: review events for field events, export events otherwise. */
    private MongoCollection<Document> collectionFor(AuditEvent event) {
        return event.type().scope() == AuditEventType.Scope.FIELD ? collections.reviewEvents() : collections.events();
    }

    /**
     * Writes an event inside a transaction, so it commits or rolls back with the state change.
     *
     * @param session the transaction
     * @param event the event
     */
    public void write(ClientSession session, AuditEvent event) {
        collectionFor(event).insertOne(session, event.toDocument());
        log(event);
    }

    /**
     * Writes a standalone fact, outside any transaction.
     *
     * @param event the event
     */
    public void write(AuditEvent event) {
        collectionFor(event).insertOne(event.toDocument());
        log(event);
    }

    /**
     * Writes a standalone fact that is idempotent on a natural key held in {@code detail} and
     * enforced by a partial unique index ({@code EXPORT_EVENT_RECEIVED} on {@code detail.messageId}).
     * A duplicate is swallowed and reported as {@code false}; any other failure propagates.
     *
     * @param event the event
     * @return true when the event was new, false when the same key was already recorded
     */
    public boolean writeIdempotent(AuditEvent event) {
        try {
            collectionFor(event).insertOne(event.toDocument());
            log(event);
            return true;
        } catch (MongoWriteException failure) {
            if (Documents.isDuplicateKey(failure)) {
                return false;
            }
            throw failure;
        }
    }

    /**
     * A batch's events in the order they happened.
     *
     * @param exportBatchId the batch
     * @param limit the most rows to return
     * @return the events, oldest first
     */
    public List<AuditEvent> findForBatch(String exportBatchId, int limit) {
        return toList(Filters.eq("exportBatchId", exportBatchId), limit);
    }

    /**
     * A schedule's events, including those of its batches, newest first.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param limit the most rows to return
     * @return the events, newest first
     */
    public List<AuditEvent> findForSchedule(String tenantId, String vendor, int limit) {
        Bson filter = Filters.and(Filters.eq("tenantId", tenantId), Filters.eq("vendor", vendor));
        List<AuditEvent> events = new ArrayList<>();
        for (Document doc : collections
                .events()
                .find(filter)
                .sort(Sorts.descending("occurredAt"))
                .limit(limit)) {
            events.add(AuditEvent.fromDocument(doc));
        }
        return events;
    }

    /**
     * A batch's whole trail in the order it was written: the natural {@code _id} order, which
     * keeps events that share an {@code occurredAt} (one transaction) in write order.
     *
     * @param exportBatchId the batch
     * @param limit the most rows to return
     * @return the events, oldest first
     */
    public List<AuditEvent> trailForBatch(String exportBatchId, int limit) {
        List<AuditEvent> events = new ArrayList<>();
        for (Document doc : collections
                .events()
                .find(Filters.eq("exportBatchId", exportBatchId))
                .sort(Sorts.ascending("_id"))
                .limit(limit)) {
            events.add(AuditEvent.fromDocument(doc));
        }
        return events;
    }

    /**
     * A schedule's own events (those with no batch), newest first.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param limit the most rows to return
     * @return the events
     */
    public List<AuditEvent> trailForSchedule(String tenantId, String vendor, int limit) {
        Bson filter = Filters.and(
                Filters.eq("tenantId", tenantId), Filters.eq("vendor", vendor), Filters.exists("exportBatchId", false));
        List<AuditEvent> events = new ArrayList<>();
        for (Document doc : collections
                .events()
                .find(filter)
                .sort(Sorts.orderBy(Sorts.descending("occurredAt"), Sorts.descending("_id")))
                .limit(limit)) {
            events.add(AuditEvent.fromDocument(doc));
        }
        return events;
    }

    /**
     * A field recommendation's whole trail in the order it was written (natural {@code _id} order,
     * as {@link #trailForBatch}). The id arrives from a caller's URL, so the read is tenant-scoped:
     * another tenant's id reads nothing. The {@code (fieldRecommendationId, _id)} index serves it.
     *
     * @param tenantId the tenant
     * @param fieldRecommendationId the field recommendation
     * @param limit the most rows to return
     * @return the events, oldest first
     */
    public List<AuditEvent> trailForField(String tenantId, String fieldRecommendationId, int limit) {
        return reviewEvents(
                Filters.and(
                        Filters.eq("fieldRecommendationId", fieldRecommendationId), Filters.eq("tenantId", tenantId)),
                limit);
    }

    /**
     * The field events of one release, in the order they were written.
     *
     * @param tenantId the tenant
     * @param releaseId the release
     * @param limit the most rows to return
     * @return the events, oldest first
     */
    public List<AuditEvent> findForRelease(String tenantId, String releaseId, int limit) {
        return reviewEvents(Filters.and(Filters.eq("tenantId", tenantId), Filters.eq("releaseId", releaseId)), limit);
    }

    private List<AuditEvent> reviewEvents(Bson filter, int limit) {
        List<AuditEvent> events = new ArrayList<>();
        for (Document doc : collections
                .reviewEvents()
                .find(filter)
                .sort(Sorts.ascending("_id"))
                .limit(limit)) {
            events.add(AuditEvent.fromDocument(doc));
        }
        return events;
    }

    private List<AuditEvent> toList(Bson filter, int limit) {
        List<AuditEvent> events = new ArrayList<>();
        for (Document doc : collections
                .events()
                .find(filter)
                .sort(Sorts.ascending("occurredAt"))
                .limit(limit)) {
            events.add(AuditEvent.fromDocument(doc));
        }
        return events;
    }
}
