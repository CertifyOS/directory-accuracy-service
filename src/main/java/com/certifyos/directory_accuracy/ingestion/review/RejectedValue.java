package com.certifyos.directory_accuracy.ingestion.review;

import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
import java.time.Instant;
import java.util.List;
import org.bson.Document;

/**
 * One {@code vendor_rejected_values} row: one distinct rejected value of one practitioner, location
 * (or none) and field, across every cycle (design step 7, D16). The {@code _id} has no batch id, so
 * the same value rejected in October and again in December is this one row with
 * {@code rejectionCount = 2}. Kept until {@code expiresAt}, twelve months after the latest rejection
 * by default, through the service's only TTL index; the audit trail keeps every rejection seven
 * years. Nothing reads it in the MVP.
 *
 * @param id {@link Ids#rejectedValueId}
 * @param tenantId the tenant
 * @param certifyPractitionerId the practitioner
 * @param certifyLocationId the location, null for a practitioner-level field
 * @param field the field
 * @param normalizedValue {@link ValueNormalizer#normalize} of the vendor's value, or {@link #NO_VALUE}
 * @param normalizationVersion the {@link ValueNormalizer#VERSION} the row was keyed with
 * @param rawValue the vendor's value as first rejected
 * @param rejectionCount how many times a reviewer rejected it
 * @param firstRejectedAt the first rejection
 * @param lastRejectedAt the latest rejection
 * @param rejections the latest rejections, oldest first, at most {@link #HISTORY_LIMIT}
 * @param expiresAt when the TTL index removes the row
 */
public record RejectedValue(
        String id,
        String tenantId,
        String certifyPractitionerId,
        String certifyLocationId,
        String field,
        String normalizedValue,
        int normalizationVersion,
        String rawValue,
        long rejectionCount,
        Instant firstRejectedAt,
        Instant lastRejectedAt,
        List<Rejection> rejections,
        Instant expiresAt) {

    /**
     * The key value of a rejected removal: the vendor sent no value, or one that normalises to
     * nothing. No version-1 rule can produce it (rules yield digits, upper-case words, {@code ;},
     * {@code |}, {@code true} or {@code false}), so it never collides with a real value.
     */
    public static final String NO_VALUE = "<none>";

    /** The most rejections a row keeps in {@code rejections[]}; {@code rejectionCount} counts them all. */
    public static final int HISTORY_LIMIT = 20;

    /**
     * One rejection.
     *
     * @param exportBatchId the batch the rejected document belonged to
     * @param by the reviewer
     * @param at when
     * @param reason the reviewer's comment
     */
    public record Rejection(String exportBatchId, String by, Instant at, String reason) {
        Document toDocument() {
            Document doc = new Document();
            Documents.put(doc, "exportBatchId", exportBatchId);
            Documents.put(doc, "by", by);
            Documents.put(doc, "at", at);
            Documents.put(doc, "reason", reason);
            return doc;
        }

        static Rejection fromDocument(Document doc) {
            return new Rejection(
                    doc.getString("exportBatchId"),
                    doc.getString("by"),
                    Documents.instant(doc, "at"),
                    doc.getString("reason"));
        }
    }

    public RejectedValue {
        rejections = rejections == null ? List.of() : List.copyOf(rejections);
    }

    /**
     * The normalised value a rejection of {@code recommendation} is remembered under.
     *
     * @param recommendation the field recommendation being rejected
     * @return its vendor value normalised, or {@link #NO_VALUE} when that is empty
     */
    public static String keyValue(FieldRecommendation recommendation) {
        String normalized = ValueNormalizer.normalize(recommendation.field(), recommendation.vendorValue());
        return normalized.isEmpty() ? NO_VALUE : normalized;
    }

    /**
     * The row id a rejection of {@code recommendation} lands on.
     *
     * @param recommendation the field recommendation being rejected
     * @return {@code tenant|practitioner|location or -|field|sha256(normalizedValue)}
     */
    public static String idFor(FieldRecommendation recommendation) {
        return Ids.rejectedValueId(
                recommendation.tenantId(),
                recommendation.practitioner().certifyPractitionerId(),
                recommendation.location() == null
                        ? null
                        : recommendation.location().certifyLocationId(),
                recommendation.field(),
                keyValue(recommendation));
    }

    /**
     * Reads a stored row.
     *
     * @param doc one {@code vendor_rejected_values} document
     * @return the row
     */
    public static RejectedValue fromDocument(Document doc) {
        Integer version = Documents.intValue(doc, "normalizationVersion");
        Long count = Documents.longValue(doc, "rejectionCount");
        List<Document> history = doc.getList("rejections", Document.class);
        return new RejectedValue(
                doc.getString("_id"),
                doc.getString("tenantId"),
                doc.getString("certifyPractitionerId"),
                doc.getString("certifyLocationId"),
                doc.getString("field"),
                doc.getString("normalizedValue"),
                version == null ? 0 : version,
                doc.getString("rawValue"),
                count == null ? 0 : count,
                Documents.instant(doc, "firstRejectedAt"),
                Documents.instant(doc, "lastRejectedAt"),
                history == null
                        ? List.of()
                        : history.stream().map(Rejection::fromDocument).toList(),
                Documents.instant(doc, "expiresAt"));
    }
}
