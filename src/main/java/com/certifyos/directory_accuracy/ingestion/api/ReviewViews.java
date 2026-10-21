package com.certifyos.directory_accuracy.ingestion.api;

import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Field recommendations as the reviewer UI reads them: the stored document as is, {@code version}
 * included, because the UI sends it back with a decision. {@code _id} reads as {@code id}, as every
 * other view of this service names it.
 */
public final class ReviewViews {

    private ReviewViews() {}

    /** One field recommendation, the design's document field for field. */
    public record FieldView(
            String id,
            String tenantId,
            String sourceType,
            String source,
            String exportBatchId,
            FieldRecommendation.Practitioner practitioner,
            FieldRecommendation.Location location,
            String field,
            String sentValue,
            String vendorValue,
            String vendorStatus,
            List<String> vendorReason,
            String vendorConfidence,
            List<Map<String, Object>> vendorEvidence,
            String reviewStatus,
            List<FieldRecommendation.HistoryEntry> reviewHistory,
            long version,
            FieldRecommendation.GoldenRecordUpdate goldenRecordUpdate,
            FieldRecommendation.SourceFile sourceFile,
            Instant stagedAt,
            Instant updatedAt) {

        public FieldView {
            vendorReason = vendorReason == null ? List.of() : List.copyOf(vendorReason);
            vendorEvidence = vendorEvidence == null ? List.of() : List.copyOf(vendorEvidence);
            reviewHistory = reviewHistory == null ? List.of() : List.copyOf(reviewHistory);
        }

        /** Builds the view; null for null, so a per-item result without a document stays null. */
        public static FieldView of(FieldRecommendation doc) {
            if (doc == null) {
                return null;
            }
            return new FieldView(
                    doc.id(),
                    doc.tenantId(),
                    doc.sourceType(),
                    doc.source(),
                    doc.exportBatchId(),
                    doc.practitioner(),
                    doc.location(),
                    doc.field(),
                    doc.sentValue(),
                    doc.vendorValue(),
                    doc.vendorStatus(),
                    doc.vendorReason(),
                    doc.vendorConfidence(),
                    doc.vendorEvidence(),
                    doc.reviewStatus() == null ? null : doc.reviewStatus().name(),
                    doc.reviewHistory(),
                    doc.version(),
                    doc.goldenRecordUpdate(),
                    doc.sourceFile(),
                    doc.stagedAt(),
                    doc.updatedAt());
        }

        /** Builds the views of a list. */
        public static List<FieldView> of(List<FieldRecommendation> docs) {
            return docs.stream().map(FieldView::of).toList();
        }
    }

    /**
     * Counts per field and review state, for the filter chips.
     *
     * @param fields field to state to count, fields in name order
     * @param exportBatchState the batch's state when the request named one, so a {@code SUPERSEDED}
     *     cycle shows on the review screen; null otherwise
     */
    public record SummaryView(Map<String, Map<String, Long>> fields, String exportBatchState) {

        public SummaryView {
            fields = fields == null ? Map.of() : java.util.Collections.unmodifiableMap(new TreeMap<>(fields));
        }

        /** Builds the view from the repository's counts. */
        static SummaryView of(Map<String, Map<ReviewStatus, Long>> counts, String exportBatchState) {
            Map<String, Map<String, Long>> fields = new TreeMap<>();
            counts.forEach((field, byStatus) -> {
                Map<String, Long> named = new TreeMap<>();
                byStatus.forEach((status, count) -> named.put(status.name(), count));
                fields.put(field, java.util.Collections.unmodifiableMap(named));
            });
            return new SummaryView(fields, exportBatchState);
        }
    }

    /**
     * One practitioner's documents in one batch.
     *
     * @param practitioner the practitioner, from its newest document
     * @param exportBatchId the batch the documents belong to: the one asked for, or the latest
     * @param fields the documents in id order, which keeps practitioner-level fields first and then
     *     groups them by location
     */
    public record PractitionerFieldsView(
            FieldRecommendation.Practitioner practitioner, String exportBatchId, List<FieldView> fields) {

        public PractitionerFieldsView {
            fields = fields == null ? List.of() : List.copyOf(fields);
        }
    }

    /**
     * {@code decision} body.
     *
     * @param action {@code APPROVE}, {@code REJECT} or {@code SKIP}
     * @param comment the reviewer's reason, required on {@code REJECT}
     * @param version the {@code version} the reviewer read
     */
    public record DecisionBody(String action, String comment, Long version) {}

    /**
     * One item of a bulk decision.
     *
     * @param id the field recommendation
     * @param action {@code APPROVE}, {@code REJECT} or {@code SKIP}
     * @param comment the reviewer's reason, required on {@code REJECT}
     * @param version the {@code version} the reviewer read
     */
    public record DecisionItem(String id, String action, String comment, Long version) {}

    /** {@code decisions} body. */
    public record BulkDecisionBody(List<DecisionItem> decisions) {}

    /**
     * One item's outcome.
     *
     * @param id the field recommendation
     * @param status 200, 400, 404 or 409, as the single decision endpoint would answer
     * @param code the problem code when not 200
     * @param document the current document on 200 and 409, else null
     */
    public record DecisionResultView(String id, int status, String code, FieldView document) {}

    /** {@code decisions} answer, one result per item in request order. */
    public record BulkDecisionView(List<DecisionResultView> results) {

        public BulkDecisionView {
            results = results == null ? List.of() : List.copyOf(results);
        }
    }
}
