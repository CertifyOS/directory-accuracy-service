package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.ingestion.release.ReleaseFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The read-back deadline (plan P13): a release whose {@code mdm.slice.ov_generated} event never came is
 * read once at the deadline; a read that fails there marks the fields {@code UNKNOWN}. Run directly on
 * the api role. Every tenant here starts with {@code rbdl-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class ReleaseReadBackDeadlineJobIT {

    static final Instant WRITTEN = Instant.parse("2026-10-20T14:00:00Z");

    @Inject
    ReleaseReadBackDeadlineJob job;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
    }

    private static FieldRecommendation written(
            String tenant, String practitionerId, String locationId, String field, String releaseId, String sliceId) {
        return ReleaseFixtures.field(
                tenant,
                practitionerId,
                "1003002890",
                locationId,
                field,
                "v-" + field,
                ReviewStatus.RELEASING,
                4L,
                ReleaseFixtures.written(
                        releaseId,
                        locationId == null ? "CorePractitioner" : "CoreLocation",
                        locationId == null ? "1003002890" : "CLINIC_hash",
                        sliceId,
                        WRITTEN,
                        IngestionJobEnqueuer.readBackDeadlineId(releaseId).toString()));
    }

    private FieldRecommendation read(FieldRecommendation doc) {
        return repository.find(doc.tenantId(), doc.id()).orElseThrow();
    }

    private static void stubOv(String path, String body) {
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo(path))
                .withQueryParam("includeLineage", WireMock.equalTo("true"))
                .willReturn(WireMock.okJson(body)));
    }

    @Test
    void deadlineReadsOnceAndReleases() {
        String tenant = "rbdl-read";
        String release = "rel-rbdl-read";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                written(tenant, "cert-rbdl-1", null, "first_name", release, "slice-p"),
                written(tenant, "cert-rbdl-1", "loc-rbdl-1", "fax", release, "slice-l"));
        stubOv(
                "/practitioners/ov/cert-rbdl-1",
                "{\"certifyId\": \"cert-rbdl-1\", \"contributingSlices\": [\"slice-p\"],"
                        + " \"contributionMap\": {\"firstName\": \"candor:" + tenant + "\"}}");
        stubOv(
                "/locations/ov/loc-rbdl-1",
                "{\"certifyLocationId\": \"loc-rbdl-1\", \"contributingSlices\": [\"slice-other\"],"
                        + " \"contributionMap\": {}}");

        job.run(new ReleaseReadBackDeadlineJobRequest(tenant, "cert-rbdl-1", release));

        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(0)).reviewStatus());
        Assertions.assertEquals("WON", read(docs.get(0)).goldenRecordUpdate().outcome());
        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(1)).reviewStatus());
        Assertions.assertEquals(
                "NOT_APPLIED", read(docs.get(1)).goldenRecordUpdate().outcome());
        List<AuditEvent> events = audit.findForRelease(tenant, release, 10);
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals("DEADLINE", events.get(0).detail().get("completionSource"));
        WireMockDal.verify(1, WireMock.getRequestedFor(WireMock.urlPathEqualTo("/practitioners/ov/cert-rbdl-1")));
    }

    @Test
    void deadlineFinishesOnlyItsOwnReleaseAtASharedLocation() {
        String tenant = "rbdl-shared";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                written(tenant, "cert-rbdl-a", "loc-rbdl-s", "phone", "rel-rbdl-a", "slice-s"),
                written(tenant, "cert-rbdl-b", "loc-rbdl-s", "fax", "rel-rbdl-b", "slice-s"));
        stubOv(
                "/locations/ov/loc-rbdl-s",
                "{\"certifyLocationId\": \"loc-rbdl-s\", \"contributingSlices\": [\"slice-s\"],"
                        + " \"contributionMap\": {\"contacts\": \"candor:" + tenant + "\"}}");

        job.run(new ReleaseReadBackDeadlineJobRequest(tenant, "cert-rbdl-a", "rel-rbdl-a"));

        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(0)).reviewStatus());
        Assertions.assertEquals(
                ReviewStatus.RELEASING,
                read(docs.get(1)).reviewStatus(),
                "the other practitioner's release waits for its own event or deadline");
        Assertions.assertTrue(audit.findForRelease(tenant, "rel-rbdl-b", 10).isEmpty());
    }

    @Test
    void deadlineReadFailureMarksUnknown() {
        String tenant = "rbdl-fail";
        String release = "rel-rbdl-fail";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, written(tenant, "cert-rbdl-2", null, "last_name", release, "slice-p"))
                .get(0);
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/practitioners/ov/cert-rbdl-2"))
                .willReturn(WireMock.aResponse().withStatus(503)));

        job.run(new ReleaseReadBackDeadlineJobRequest(tenant, "cert-rbdl-2", release));

        FieldRecommendation after = read(doc);
        Assertions.assertEquals(ReviewStatus.RELEASED, after.reviewStatus());
        Assertions.assertEquals("UNKNOWN", after.goldenRecordUpdate().outcome());
        Assertions.assertNull(after.goldenRecordUpdate().winningSource());
        AuditEvent event = audit.findForRelease(tenant, release, 10).get(0);
        Assertions.assertEquals("UNKNOWN", event.detail().get("outcome"));
        Assertions.assertEquals("DEADLINE", event.detail().get("completionSource"));
    }

    @Test
    void releaseAlreadyReadBackIsANoOp() {
        String tenant = "rbdl-done";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-rbdl-3",
                                "1003002890",
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASED,
                                5L,
                                ReleaseFixtures.requested("rel-rbdl-done", "CorePractitioner")))
                .get(0);

        job.run(new ReleaseReadBackDeadlineJobRequest(tenant, "cert-rbdl-3", "rel-rbdl-done"));

        Assertions.assertEquals(doc, read(doc));
        WireMockDal.verify(0, WireMock.getRequestedFor(WireMock.urlPathEqualTo("/practitioners/ov/cert-rbdl-3")));
    }
}
