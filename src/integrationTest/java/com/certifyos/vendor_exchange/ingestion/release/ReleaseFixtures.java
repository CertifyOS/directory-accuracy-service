package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.HistoryEntry;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.github.tomakehurst.wiremock.client.WireMock;
import java.time.Instant;
import java.util.List;

/**
 * Field recommendations in any review state, inserted straight into {@code field_recommendations}
 * (the staging upsert only ever writes {@code PENDING}, and the decision endpoints belong to another
 * step), and the DAL's tenant configuration stubbed with or without a candor ranking.
 */
public final class ReleaseFixtures {

    public static final String VENDOR = "candor";
    public static final Instant STAGED = Instant.parse("2026-10-15T09:15:10Z");
    public static final String UPLOAD = "test-vendor-bucket|to/x/x.csv|1760520000123456";
    public static final String REVIEWER = "reviewer@certifyos.com";

    private ReleaseFixtures() {}

    /** The batch id fields of a tenant are staged under. */
    public static String batchId(String tenant) {
        return tenant + "-candor-2026-10-001";
    }

    /**
     * A field recommendation of the tenant's batch.
     *
     * @param tenant the tenant
     * @param practitionerId the practitioner
     * @param npi the NPI
     * @param locationId the location, or null for a practitioner-level field
     * @param field the field
     * @param vendorValue the vendor's value
     * @param status the review state
     * @param version the version
     * @param update the release record, or null
     * @return the document, not yet stored
     */
    public static FieldRecommendation field(
            String tenant,
            String practitionerId,
            String npi,
            String locationId,
            String field,
            String vendorValue,
            ReviewStatus status,
            long version,
            GoldenRecordUpdate update) {
        String batch = batchId(tenant);
        List<HistoryEntry> history = status == ReviewStatus.PENDING
                ? List.of()
                : List.of(new HistoryEntry("APPROVE", REVIEWER, STAGED.plusSeconds(60), null));
        return new FieldRecommendation(
                Ids.fieldRecommendationId(tenant, VENDOR, batch, practitionerId, locationId, field),
                tenant,
                FieldRecommendation.SOURCE_TYPE,
                VENDOR,
                batch,
                new FieldRecommendation.Practitioner(practitionerId, npi),
                locationId == null ? null : new FieldRecommendation.Location(locationId),
                field,
                "sent-" + field,
                vendorValue,
                "INVALID",
                List.of("direct_outreach"),
                "HIGH",
                List.of(),
                status,
                history,
                version,
                update,
                new FieldRecommendation.SourceFile(UPLOAD, "Physicians Directory Accuracy", 913),
                STAGED,
                STAGED.plusSeconds(60));
    }

    /** An {@code APPROVED} field at version 2, the state a reviewer's approval leaves. */
    public static FieldRecommendation approved(
            String tenant, String practitionerId, String npi, String locationId, String field, String vendorValue) {
        return field(tenant, practitionerId, npi, locationId, field, vendorValue, ReviewStatus.APPROVED, 2L, null);
    }

    /** The release record a release request writes. */
    public static GoldenRecordUpdate requested(String releaseId, String resource) {
        return new GoldenRecordUpdate(
                releaseId,
                REVIEWER,
                STAGED.plusSeconds(120),
                "SLICE",
                resource,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /** The release record once the release job's DAL write was accepted; sent one second before. */
    public static GoldenRecordUpdate written(
            String releaseId,
            String resource,
            String crosswalkId,
            String sliceId,
            Instant writtenAt,
            String deadlineJobId) {
        return new GoldenRecordUpdate(
                releaseId,
                REVIEWER,
                STAGED.plusSeconds(120),
                "SLICE",
                resource,
                crosswalkId,
                sliceId,
                writtenAt == null ? null : writtenAt.minusSeconds(1),
                writtenAt,
                deadlineJobId,
                null,
                null,
                null,
                null);
    }

    /** Stores the documents and returns them. */
    public static List<FieldRecommendation> insert(Collections collections, FieldRecommendation... docs) {
        for (FieldRecommendation doc : docs) {
            collections.fieldRecommendations().insertOne(doc.toDocument());
        }
        return List.of(docs);
    }

    /**
     * Stubs the tenant's configuration page: a survivorship configuration whose {@code sourceRanking}
     * rule lists candor ({@code ranked}) or only roster and tenant sources.
     */
    public static void stubRanking(String tenant, boolean ranked) {
        String sources = ranked
                ? "{\"sourceType\": \"candor\", \"priority\": 1}, {\"sourceType\": \"roster\", \"priority\": 3}"
                : "{\"sourceType\": \"tenant\", \"priority\": 1}, {\"sourceType\": \"roster\", \"priority\": 3}";
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/tenants/" + tenant + "/configurations"))
                .willReturn(WireMock.okJson("{\"data\": [{\"id\": \"cfg-1\", \"tenantId\": \"" + tenant + "\","
                        + " \"namespace\": \"mdm\", \"configurationTypeId\": \"survivorship\", \"isEnabled\": true,"
                        + " \"configuration\": {\"rules\": [{\"type\": \"sourceRanking\", \"fieldRules\": ["
                        + "{\"fieldPattern\": \"*\", \"sourcePriority\": [" + sources + "]}]}]}}],"
                        + " \"links\": {}, \"totalCount\": 1}")));
    }
}
