package com.certifyos.vendor_exchange.audit;

import com.certifyos.vendor_exchange.persistence.Documents;
import com.certifyos.vendor_exchange.persistence.Ids;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.bson.Document;

/**
 * The common audit envelope from the design. Append-only; written in the same transaction as the
 * state change where one exists, otherwise as a standalone fact. {@code detail} is the per-type
 * payload the design's table lists; it carries ids and counts, never field values or PII.
 *
 * @param id {@code ev-<uuid v7>}
 * @param type the event type
 * @param tenantId the tenant; null only for {@link AuditEventType.Scope#SERVICE} events
 * @param vendor the vendor; null only for service events
 * @param exportBatchId the batch; required for batch events, null for schedule events
 * @param attempt the batch attempt the event belongs to, or null
 * @param actor who caused it: one of {@link Actors} or an operator id
 * @param jobId the JobRunr job that wrote it, or null for HTTP handlers
 * @param uploadId the vendor upload a file or field event belongs to, or null
 * @param fieldRecommendationId the field recommendation; required for {@link AuditEventType.Scope#FIELD}
 *     events, null otherwise
 * @param certifyPractitionerId the practitioner a field event is about, or null
 * @param releaseId the release a field event belongs to, set from the release request on, or null
 * @param occurredAt when it happened
 * @param detail the per-type payload, immutable
 */
public record AuditEvent(
        String id,
        AuditEventType type,
        String tenantId,
        String vendor,
        String exportBatchId,
        Integer attempt,
        String actor,
        String jobId,
        String uploadId,
        String fieldRecommendationId,
        String certifyPractitionerId,
        String releaseId,
        Instant occurredAt,
        Map<String, Object> detail) {

    public AuditEvent {
        requireActor(type, actor);
        requireIds(type, tenantId, vendor, exportBatchId);
        if (type.requiresField() && fieldRecommendationId == null) {
            throw new IllegalArgumentException(type + " requires fieldRecommendationId");
        }
        detail = detail == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(detail));
    }

    private static void requireActor(AuditEventType type, String actor) {
        if (type == null) {
            throw new IllegalArgumentException("audit event type is required");
        }
        if (actor == null || actor.isBlank()) {
            throw new IllegalArgumentException("audit event actor is required for " + type);
        }
    }

    private static void requireIds(AuditEventType type, String tenantId, String vendor, String exportBatchId) {
        if (type.requiresTenant() && (tenantId == null || vendor == null)) {
            throw new IllegalArgumentException(type + " requires tenantId and vendor");
        }
        if (type.requiresBatch() && exportBatchId == null) {
            throw new IllegalArgumentException(type + " requires exportBatchId");
        }
        if (type.scope() == AuditEventType.Scope.SCHEDULE && exportBatchId != null) {
            throw new IllegalArgumentException(type + " is a schedule event and carries no exportBatchId");
        }
    }

    /**
     * Starts an event for a schedule, the service or a field recommendation.
     *
     * @param type the type
     * @param tenantId the tenant, or null for a service event
     * @param vendor the vendor, or null for a service event
     * @return a builder with the system actor and the current time
     */
    public static Builder of(AuditEventType type, String tenantId, String vendor) {
        return new Builder(type, tenantId, vendor);
    }

    /**
     * Starts an event for a batch.
     *
     * @param type the type
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param exportBatchId the batch
     * @param attempt the attempt
     * @return a builder with the system actor and the current time
     */
    public static Builder forBatch(
            AuditEventType type, String tenantId, String vendor, String exportBatchId, int attempt) {
        return new Builder(type, tenantId, vendor).batch(exportBatchId, attempt);
    }

    /** The BSON document; the driver adds its own {@code _id}. */
    public Document toDocument() {
        Document doc = new Document("id", id)
                .append("type", type.name())
                .append("actor", actor)
                .append("occurredAt", occurredAt);
        Documents.put(doc, "tenantId", tenantId);
        Documents.put(doc, "vendor", vendor);
        Documents.put(doc, "exportBatchId", exportBatchId);
        Documents.put(doc, "attempt", attempt);
        Documents.put(doc, "jobId", jobId);
        Documents.put(doc, "uploadId", uploadId);
        Documents.put(doc, "fieldRecommendationId", fieldRecommendationId);
        Documents.put(doc, "certifyPractitionerId", certifyPractitionerId);
        Documents.put(doc, "releaseId", releaseId);
        if (!detail.isEmpty()) {
            doc.append("detail", new Document(detail));
        }
        return doc;
    }

    /**
     * Reads a stored document.
     *
     * @param doc the document
     * @return the event
     */
    public static AuditEvent fromDocument(Document doc) {
        Document detail = doc.get("detail", Document.class);
        return new AuditEvent(
                doc.getString("id"),
                AuditEventType.valueOf(doc.getString("type")),
                doc.getString("tenantId"),
                doc.getString("vendor"),
                doc.getString("exportBatchId"),
                Documents.intValue(doc, "attempt"),
                doc.getString("actor"),
                doc.getString("jobId"),
                doc.getString("uploadId"),
                doc.getString("fieldRecommendationId"),
                doc.getString("certifyPractitionerId"),
                doc.getString("releaseId"),
                Documents.instant(doc, "occurredAt"),
                detail == null ? Map.of() : detail);
    }

    /** Builds an event; the id and time are set at {@link #build()}. */
    public static final class Builder {
        private final AuditEventType type;
        private final String tenantId;
        private final String vendor;
        private String exportBatchId;
        private Integer attempt;
        private String actor = Actors.SYSTEM;
        private String jobId;
        private String uploadId;
        private String fieldRecommendationId;
        private String certifyPractitionerId;
        private String releaseId;
        private Instant occurredAt;
        private final Map<String, Object> detail = new LinkedHashMap<>();
        private final java.util.Set<String> named = new java.util.LinkedHashSet<>();

        private Builder(AuditEventType type, String tenantId, String vendor) {
            this.type = type;
            this.tenantId = tenantId;
            this.vendor = vendor;
        }

        /** Sets the batch and attempt. */
        public Builder batch(String batchId, int batchAttempt) {
            this.exportBatchId = batchId;
            this.attempt = batchAttempt;
            return this;
        }

        /** Sets the batch and leaves the attempt unset; for file and field events. */
        public Builder batch(String batchId) {
            this.exportBatchId = batchId;
            return this;
        }

        /** Sets the vendor upload the event belongs to. */
        public Builder upload(String upload) {
            this.uploadId = upload;
            return this;
        }

        /** Sets the field recommendation and the practitioner it is about. */
        public Builder field(String recommendationId, String practitionerId) {
            this.fieldRecommendationId = recommendationId;
            this.certifyPractitionerId = practitionerId;
            return this;
        }

        /** Sets the release the event belongs to. */
        public Builder release(String release) {
            this.releaseId = release;
            return this;
        }

        /** Sets the actor; an operator's user id or one of {@link Actors}. */
        public Builder actor(String who) {
            this.actor = who;
            return this;
        }

        /** Sets the JobRunr job id that is writing the event. */
        public Builder jobId(String id) {
            this.jobId = id;
            return this;
        }

        /** Sets the time; defaults to now at build. */
        public Builder occurredAt(Instant when) {
            this.occurredAt = when;
            return this;
        }

        /** Adds one detail field; a null value is left out. */
        public Builder detail(String key, Object value) {
            named.add(key);
            if (value != null) {
                detail.put(key, value);
            }
            return this;
        }

        /** Adds every entry of a detail map. */
        public Builder details(Map<String, ?> values) {
            values.forEach(this::detail);
            return this;
        }

        /** Builds the event with a fresh v7 id. */
        public AuditEvent build() {
            AuditDetailContract.check(type, named);
            return new AuditEvent(
                    Ids.eventId(),
                    type,
                    tenantId,
                    vendor,
                    exportBatchId,
                    attempt,
                    actor,
                    jobId,
                    uploadId,
                    fieldRecommendationId,
                    certifyPractitionerId,
                    releaseId,
                    occurredAt == null ? Instant.now() : occurredAt,
                    detail);
        }
    }
}
