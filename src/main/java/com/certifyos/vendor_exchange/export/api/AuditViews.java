package com.certifyos.vendor_exchange.export.api;

import com.certifyos.vendor_exchange.audit.AuditEvent;
import java.time.Instant;
import java.util.Map;

/**
 * The audit envelope as the API shows it: the stored document, {@code jobId} included (it links to the
 * JobRunr entry), and the ingestion ids a file or field event carries ({@code uploadId},
 * {@code fieldRecommendationId}, {@code certifyPractitionerId}, {@code releaseId}), null on export events.
 */
public final class AuditViews {

    private AuditViews() {}

    /** One audit event. */
    public record EventView(
            String id,
            String type,
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

        /** Builds the view. */
        public static EventView of(AuditEvent event) {
            return new EventView(
                    event.id(),
                    event.type().name(),
                    event.tenantId(),
                    event.vendor(),
                    event.exportBatchId(),
                    event.attempt(),
                    event.actor(),
                    event.jobId(),
                    event.uploadId(),
                    event.fieldRecommendationId(),
                    event.certifyPractitionerId(),
                    event.releaseId(),
                    event.occurredAt(),
                    event.detail());
        }
    }
}
