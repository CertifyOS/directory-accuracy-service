package com.certifyos.vendor_exchange.ingestion;

import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.Location;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.Practitioner;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.SourceFile;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Ids;
import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Field recommendations for the review and rejected-value tests, built the way staging builds them
 * and written through {@code FieldRecommendationRepository.upsertAll}, so these tests never depend on
 * the staging job.
 */
public final class ReviewFixtures {

    public static final String SHEET = "Physicians Directory Accuracy";
    public static final Instant STAGED = Instant.parse("2026-10-15T09:15:10Z");

    private ReviewFixtures() {}

    /**
     * A {@code PENDING} recommendation, version 1, no history, as staging writes it.
     *
     * @param tenant the tenant
     * @param exportBatchId the batch
     * @param practitionerId the practitioner
     * @param npi the NPI
     * @param locationId the location, or null for a practitioner-level field
     * @param field the field
     * @param vendorValue the vendor's value
     * @param stagedAt when staging wrote it
     * @return the recommendation
     */
    public static FieldRecommendation pending(
            String tenant,
            String exportBatchId,
            String practitionerId,
            String npi,
            String locationId,
            String field,
            String vendorValue,
            Instant stagedAt) {
        return new FieldRecommendation(
                Ids.fieldRecommendationId(tenant, "candor", exportBatchId, practitionerId, locationId, field),
                tenant,
                FieldRecommendation.SOURCE_TYPE,
                "candor",
                exportBatchId,
                new Practitioner(practitionerId, npi),
                locationId == null ? null : new Location(locationId),
                field,
                "sent",
                vendorValue,
                "INVALID",
                List.of("direct_outreach"),
                "VERY HIGH",
                List.of(Map.of("reason", "direct_outreach", "evidence", "date", "value", "2026-06-17")),
                ReviewStatus.PENDING,
                List.of(),
                1L,
                null,
                new SourceFile("vendor-sftp|to/" + tenant + "/f.csv|1760520000123456", SHEET, 913),
                stagedAt,
                stagedAt);
    }
}
