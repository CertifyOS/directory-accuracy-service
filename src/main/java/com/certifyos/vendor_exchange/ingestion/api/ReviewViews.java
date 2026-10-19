package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Field recommendations as reviewers read them. {@link FieldView} is the stored document as is,
 * {@code version} included, so the reviewer's next write can carry it.
 */
public final class ReviewViews {

    private ReviewViews() {}

    /** One {@code field_recommendations} document, every field. */
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

        /** Builds the view. */
        public static FieldView of(FieldRecommendation doc) {
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
    }
}
