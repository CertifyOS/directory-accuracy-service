package com.certifyos.directory_accuracy.ingestion.release;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.jobs.ReleaseJob;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Release requests, bulk release, rollback, the release read, the ranking guard and the claim the
 * release job starts with. Every tenant here starts with {@code relsvc-}; the ranking answer is
 * cached per tenant, so each test that needs a different answer has its own tenant.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class ReleaseServiceIT {

    static final int LIMIT = 100;

    @Inject
    ReleaseService service;

    @Inject
    ReleaseLifecycle lifecycle;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    @Inject
    StorageProvider storage;

    @InjectMock
    GoogleIdTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
    }

    private FieldRecommendation read(FieldRecommendation doc) {
        return repository.find(doc.tenantId(), doc.id()).orElseThrow();
    }

    private static ProblemException refused(int status, String code, Runnable call) {
        ProblemException refusal = Assertions.assertThrows(ProblemException.class, call::run);
        Assertions.assertEquals(status, refusal.status(), refusal.getMessage());
        Assertions.assertEquals(code, refusal.code());
        return refusal;
    }

    @Test
    void requestMovesFieldAndEnqueues() {
        String tenant = "relsvc-request";
        ReleaseFixtures.stubRanking(tenant, true);
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"))
                .get(0);

        ReleaseService.Requested requested = service.request(tenant, doc.id(), 2L, ReleaseFixtures.REVIEWER, false);

        Assertions.assertTrue(requested.releaseId().startsWith("rel-"), requested.releaseId());
        Assertions.assertEquals("cert-1", requested.certifyPractitionerId());
        Assertions.assertEquals(List.of(doc.id()), requested.fieldRecommendationIds());
        FieldRecommendation after = read(doc);
        Assertions.assertEquals(ReviewStatus.RELEASE_REQUESTED, after.reviewStatus());
        Assertions.assertEquals(3L, after.version());
        GoldenRecordUpdate update = after.goldenRecordUpdate();
        Assertions.assertEquals(requested.releaseId(), update.releaseId());
        Assertions.assertEquals(ReleaseFixtures.REVIEWER, update.requestedBy());
        Assertions.assertNotNull(update.requestedAt());
        Assertions.assertEquals("SLICE", update.writePath());
        Assertions.assertEquals("CorePractitioner", update.resource());
        FieldRecommendation.HistoryEntry last =
                after.reviewHistory().get(after.reviewHistory().size() - 1);
        Assertions.assertEquals("RELEASE", last.action());
        Assertions.assertEquals(ReleaseFixtures.REVIEWER, last.by());
        Assertions.assertEquals(
                StateName.ENQUEUED,
                storage.getJobById(JobIds.of(ReleaseJob.NAME, requested.releaseId(), 1))
                        .getState());
        List<AuditEvent> events = audit.findForRelease(tenant, requested.releaseId(), LIMIT);
        Assertions.assertEquals(1, events.size());
        AuditEvent event = events.get(0);
        Assertions.assertEquals(AuditEventType.FIELD_RELEASE_REQUESTED, event.type());
        Assertions.assertEquals(requested.releaseId(), event.releaseId());
        Assertions.assertEquals(doc.id(), event.fieldRecommendationId());
        Assertions.assertEquals("cert-1", event.certifyPractitionerId());
        Assertions.assertEquals(ReleaseFixtures.UPLOAD, event.uploadId());
        Assertions.assertEquals(doc.exportBatchId(), event.exportBatchId());
        Assertions.assertEquals(ReleaseFixtures.REVIEWER, event.actor());
        Assertions.assertEquals(requested.releaseId(), event.detail().get("releaseId"));
        Assertions.assertFalse((Boolean) event.detail().get("bulk"));
        Assertions.assertEquals(
                List.of(event.id()),
                audit.trailForField(tenant, doc.id(), LIMIT).stream()
                        .map(AuditEvent::id)
                        .toList(),
                "the event is in vendor_review_events");
    }

    @Test
    void secondFieldOfSamePractitionerJoinsTheOpenRelease() {
        String tenant = "relsvc-join";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", "loc-1", "phone", "3035550100"),
                ReleaseFixtures.approved(tenant, "cert-2", "1003002891", null, "last_name", "Lee"));

        String first = service.request(tenant, docs.get(0).id(), 2L, ReleaseFixtures.REVIEWER, false)
                .releaseId();
        String second = service.request(tenant, docs.get(1).id(), 2L, ReleaseFixtures.REVIEWER, false)
                .releaseId();
        String other = service.request(tenant, docs.get(2).id(), 2L, ReleaseFixtures.REVIEWER, false)
                .releaseId();

        Assertions.assertEquals(first, second, "the open release of the practitioner is joined");
        Assertions.assertNotEquals(first, other, "another practitioner has its own release");
        Assertions.assertEquals(
                Set.of(docs.get(0).id(), docs.get(1).id()),
                service.release(tenant, first).stream()
                        .map(FieldRecommendation::id)
                        .collect(Collectors.toSet()));
        Assertions.assertEquals(
                "CoreLocation", read(docs.get(1)).goldenRecordUpdate().resource());
    }

    @Test
    void requestAfterTheOpenReleaseWasClaimedMintsANewId() {
        String tenant = "relsvc-claimed";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "last_name", "Lee"));
        String first = service.request(tenant, docs.get(0).id(), 2L, ReleaseFixtures.REVIEWER, false)
                .releaseId();
        Assertions.assertTrue(
                lifecycle.claim(tenant, "cert-1", first, Instant.now()).isPresent());

        String second = service.request(tenant, docs.get(1).id(), 2L, ReleaseFixtures.REVIEWER, false)
                .releaseId();

        Assertions.assertNotEquals(first, second, "a claimed release is not joined: its job would never see the field");
        Assertions.assertEquals(ReviewStatus.RELEASING, read(docs.get(0)).reviewStatus());
        Assertions.assertEquals(
                ReviewStatus.RELEASE_REQUESTED, read(docs.get(1)).reviewStatus());
    }

    @Test
    void inPlaceFieldIs422() {
        String tenant = "relsvc-inplace";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", "loc-1", "practice_name", "Clinic"),
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "npi", "1999999999"));

        refused(
                422,
                "IN_PLACE_RELEASE_UNSUPPORTED",
                () -> service.request(tenant, docs.get(0).id(), 2L, ReleaseFixtures.REVIEWER, false));
        refused(
                422,
                "REMOVAL_NOT_RELEASED",
                () -> service.request(tenant, docs.get(1).id(), 2L, ReleaseFixtures.REVIEWER, false));

        for (FieldRecommendation doc : docs) {
            Assertions.assertEquals(doc, read(doc), "nothing written");
            Assertions.assertTrue(audit.trailForField(tenant, doc.id(), LIMIT).isEmpty());
        }
    }

    @Test
    void staleVersionAndWrongStateAre409AndAnotherTenantIs404() {
        String tenant = "relsvc-stale";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.field(
                        tenant, "cert-2", "1003002891", null, "first_name", "Ann", ReviewStatus.PENDING, 1L, null));

        refused(
                409,
                "RELEASE_NOT_ALLOWED",
                () -> service.request(tenant, docs.get(0).id(), 1L, ReleaseFixtures.REVIEWER, false));
        refused(
                409,
                "RELEASE_NOT_ALLOWED",
                () -> service.request(tenant, docs.get(1).id(), 1L, ReleaseFixtures.REVIEWER, false));
        refused(
                404,
                "FIELD_NOT_FOUND",
                () -> service.request("relsvc-someone-else", docs.get(0).id(), 2L, ReleaseFixtures.REVIEWER, false));

        Assertions.assertEquals(docs.get(0), read(docs.get(0)));
    }

    @Test
    void rankingMissingIs422AndWritesNothing() {
        String tenant = "relsvc-unranked";
        ReleaseFixtures.stubRanking(tenant, false);
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"))
                .get(0);

        refused(422, "RANKING_MISSING", () -> service.request(tenant, doc.id(), 2L, ReleaseFixtures.REVIEWER, false));
        refused(
                422,
                "RANKING_MISSING",
                () -> service.requestAll(tenant, List.of(doc.id()), Map.of(doc.id(), 2L), ReleaseFixtures.REVIEWER));

        Assertions.assertEquals(doc, read(doc), "still APPROVED, no release record");
        Assertions.assertTrue(audit.trailForField(tenant, doc.id(), LIMIT).isEmpty());
    }

    @Test
    void rankingUnreadableIs503() {
        String tenant = "relsvc-dal-down";
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/tenants/" + tenant + "/configurations"))
                .willReturn(WireMock.aResponse().withStatus(500)));
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"))
                .get(0);

        refused(503, "DAL_UNAVAILABLE", () -> service.request(tenant, doc.id(), 2L, ReleaseFixtures.REVIEWER, false));

        ReleaseFixtures.stubRanking(tenant, true);
        Assertions.assertNotNull(
                service.request(tenant, doc.id(), 2L, ReleaseFixtures.REVIEWER, false)
                        .releaseId(),
                "a failed read is not cached");
    }

    @Test
    void rollbackFromApprovedThenRefusedOnceRequested() {
        String tenant = "relsvc-rollback";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-1", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.approved(tenant, "cert-2", "1003002891", null, "last_name", "Lee"));

        FieldRecommendation rolledBack =
                service.rollback(tenant, docs.get(0).id(), 2L, "approved the wrong row", ReleaseFixtures.REVIEWER);
        service.request(tenant, docs.get(1).id(), 2L, ReleaseFixtures.REVIEWER, false);

        Assertions.assertEquals(ReviewStatus.PENDING, rolledBack.reviewStatus());
        Assertions.assertEquals(3L, rolledBack.version());
        FieldRecommendation.HistoryEntry last =
                rolledBack.reviewHistory().get(rolledBack.reviewHistory().size() - 1);
        Assertions.assertEquals("ROLLBACK", last.action());
        Assertions.assertEquals("approved the wrong row", last.comment());
        Assertions.assertEquals(2, rolledBack.reviewHistory().size(), "history kept");
        List<AuditEvent> trail = audit.trailForField(tenant, docs.get(0).id(), LIMIT);
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_ROLLED_BACK),
                trail.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("approved the wrong row", trail.get(0).detail().get("reason"));
        Assertions.assertEquals(2L, ((Number) trail.get(0).detail().get("version")).longValue());
        refused(
                409,
                "ROLLBACK_NOT_ALLOWED",
                () -> service.rollback(tenant, docs.get(1).id(), 3L, "too late", ReleaseFixtures.REVIEWER));
        refused(
                409,
                "ROLLBACK_NOT_ALLOWED",
                () -> service.rollback(tenant, docs.get(0).id(), 2L, "stale", ReleaseFixtures.REVIEWER));
        refused(
                400,
                "REASON_REQUIRED",
                () -> service.rollback(tenant, docs.get(0).id(), 3L, " ", ReleaseFixtures.REVIEWER));
        refused(
                404,
                "FIELD_NOT_FOUND",
                () -> service.rollback("relsvc-someone-else", docs.get(0).id(), 3L, "x", ReleaseFixtures.REVIEWER));
    }

    @Test
    void requestBatchTakesEverySliceField() {
        String tenant = "relsvc-batch";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-a", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.approved(tenant, "cert-a", "1003002890", "loc-1", "phone", "3035550100"),
                ReleaseFixtures.approved(tenant, "cert-b", "1003002891", null, "last_name", "Lee"),
                ReleaseFixtures.approved(tenant, "cert-b", "1003002891", "loc-2", "practice_name", "Clinic"),
                ReleaseFixtures.field(
                        tenant, "cert-c", "1003002892", "loc-3", "fax", "3039998888", ReviewStatus.PENDING, 1L, null));

        ReleaseService.Bulk bulk =
                service.requestBatch(tenant, ReleaseFixtures.batchId(tenant), ReleaseFixtures.REVIEWER);

        Assertions.assertEquals(2, bulk.releases().size());
        Assertions.assertEquals(
                List.of(new ReleaseService.Skipped(docs.get(3).id(), "IN_PLACE_RELEASE_UNSUPPORTED")),
                bulk.skipped(),
                "an approved in-place field is listed, not silently left out");
        Map<String, Set<String>> byPractitioner = bulk.releases().stream()
                .collect(Collectors.toMap(
                        ReleaseService.Requested::certifyPractitionerId,
                        release -> Set.copyOf(release.fieldRecommendationIds())));
        Assertions.assertEquals(
                Map.of(
                        "cert-a", Set.of(docs.get(0).id(), docs.get(1).id()),
                        "cert-b", Set.of(docs.get(2).id())),
                byPractitioner);
        Assertions.assertEquals(ReviewStatus.APPROVED, read(docs.get(3)).reviewStatus(), "in-place fields wait");
        Assertions.assertEquals(ReviewStatus.PENDING, read(docs.get(4)).reviewStatus());
        AuditEvent event = audit.trailForField(tenant, docs.get(0).id(), LIMIT).get(0);
        Assertions.assertTrue((Boolean) event.detail().get("bulk"));
        Assertions.assertTrue(service.requestBatch(tenant, "relsvc-no-such-batch", ReleaseFixtures.REVIEWER)
                .releases()
                .isEmpty());
    }

    @Test
    void requestAllGroupsPerPractitionerAndSkipsPerItem() {
        String tenant = "relsvc-bulk";
        ReleaseFixtures.stubRanking(tenant, true);
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.approved(tenant, "cert-a", "1003002890", null, "first_name", "Dan"),
                ReleaseFixtures.approved(tenant, "cert-a", "1003002890", null, "last_name", "Lee"),
                ReleaseFixtures.approved(tenant, "cert-b", "1003002891", "loc-1", "fax", "3039998888"),
                ReleaseFixtures.approved(tenant, "cert-b", "1003002891", "loc-1", "phone", "3035550100"),
                ReleaseFixtures.approved(tenant, "cert-b", "1003002891", "loc-1", "practice_name", "Clinic"),
                ReleaseFixtures.approved(tenant, "cert-c", "1003002892", null, "first_name", "Kim"));
        List<String> ids = List.of(
                docs.get(0).id(),
                docs.get(1).id(),
                docs.get(2).id(),
                docs.get(3).id(),
                docs.get(4).id(),
                docs.get(5).id(),
                "relsvc-bulk|candor|nope|cert-x|-|first_name");
        Map<String, Long> versions = Map.of(
                docs.get(0).id(),
                2L,
                docs.get(1).id(),
                2L,
                docs.get(2).id(),
                2L,
                docs.get(3).id(),
                1L,
                docs.get(4).id(),
                2L,
                "relsvc-bulk|candor|nope|cert-x|-|first_name",
                2L);

        ReleaseService.Bulk bulk = service.requestAll(tenant, ids, versions, ReleaseFixtures.REVIEWER);

        Assertions.assertEquals(
                Map.of(
                        "cert-a", List.of(docs.get(0).id(), docs.get(1).id()),
                        "cert-b", List.of(docs.get(2).id())),
                bulk.releases().stream()
                        .collect(Collectors.toMap(
                                ReleaseService.Requested::certifyPractitionerId,
                                ReleaseService.Requested::fieldRecommendationIds)));
        Assertions.assertEquals(
                List.of(
                        new ReleaseService.Skipped(docs.get(3).id(), "RELEASE_NOT_ALLOWED"),
                        new ReleaseService.Skipped(docs.get(4).id(), "IN_PLACE_RELEASE_UNSUPPORTED"),
                        new ReleaseService.Skipped(docs.get(5).id(), "VERSION_REQUIRED"),
                        new ReleaseService.Skipped("relsvc-bulk|candor|nope|cert-x|-|first_name", "FIELD_NOT_FOUND")),
                bulk.skipped());
    }

    @Test
    void reReleaseFromReleaseFailedReplacesTheGoldenRecordUpdate() {
        String tenant = "relsvc-rerelease";
        ReleaseFixtures.stubRanking(tenant, true);
        GoldenRecordUpdate failed = new GoldenRecordUpdate(
                "rel-old",
                ReleaseFixtures.REVIEWER,
                ReleaseFixtures.STAGED,
                "SLICE",
                "CorePractitioner",
                "1003002890",
                "slice-old",
                null,
                ReleaseFixtures.STAGED.plusSeconds(5),
                "job-old",
                null,
                null,
                null,
                "EXHAUSTED: java.net.ConnectException");
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-1",
                                "1003002890",
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASE_FAILED,
                                5L,
                                failed))
                .get(0);

        String releaseId = service.request(tenant, doc.id(), 5L, ReleaseFixtures.REVIEWER, false)
                .releaseId();

        GoldenRecordUpdate update = read(doc).goldenRecordUpdate();
        Assertions.assertEquals(
                ReleaseFixtures.requested(releaseId, "CorePractitioner").releaseId(), update.releaseId());
        Assertions.assertNull(update.error(), "the old failure does not linger");
        Assertions.assertNull(update.writtenAt());
        Assertions.assertNull(update.sliceId());
        Assertions.assertNull(update.deadlineJobId());
    }

    @Test
    void releaseReadsItsFieldsOr404() {
        String tenant = "relsvc-read";
        ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "first_name",
                        "Dan",
                        ReviewStatus.RELEASING,
                        3L,
                        ReleaseFixtures.requested("rel-read", "CorePractitioner")));

        Assertions.assertEquals(1, service.release(tenant, "rel-read").size());
        refused(404, "RELEASE_NOT_FOUND", () -> service.release(tenant, "rel-none"));
        refused(404, "RELEASE_NOT_FOUND", () -> service.release("relsvc-someone-else", "rel-read"));
    }

    @Test
    void claimBacksOffWhenAnotherReleaseIsRunning() {
        String tenant = "relsvc-backoff";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "first_name",
                        "Dan",
                        ReviewStatus.RELEASING,
                        3L,
                        ReleaseFixtures.requested("rel-running", "CorePractitioner")),
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        "loc-1",
                        "phone",
                        "3035550100",
                        ReviewStatus.RELEASE_REQUESTED,
                        3L,
                        ReleaseFixtures.requested("rel-mine", "CoreLocation")));

        Assertions.assertThrows(
                ReleaseInProgressException.class, () -> lifecycle.claim(tenant, "cert-1", "rel-mine", Instant.now()));

        Assertions.assertEquals(
                ReviewStatus.RELEASE_REQUESTED, read(docs.get(1)).reviewStatus());
        Assertions.assertEquals(ReviewStatus.RELEASING, read(docs.get(0)).reviewStatus());
    }

    @Test
    void claimPicksUpItsOwnReleasingFieldsOnARetry() {
        String tenant = "relsvc-retry";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "first_name",
                        "Dan",
                        ReviewStatus.RELEASING,
                        3L,
                        ReleaseFixtures.requested("rel-retry", "CorePractitioner")),
                ReleaseFixtures.field(
                        tenant,
                        "cert-1",
                        "1003002890",
                        null,
                        "last_name",
                        "Lee",
                        ReviewStatus.RELEASE_REQUESTED,
                        3L,
                        ReleaseFixtures.requested("rel-retry", "CorePractitioner")));

        ReleaseLifecycle.Claimed claimed =
                lifecycle.claim(tenant, "cert-1", "rel-retry", Instant.now()).orElseThrow();

        Assertions.assertEquals(
                Set.of(docs.get(0).id(), docs.get(1).id()),
                claimed.fields().stream().map(FieldRecommendation::id).collect(Collectors.toSet()));
        Assertions.assertEquals(
                Set.of(ReviewStatus.RELEASING),
                claimed.fields().stream().map(FieldRecommendation::reviewStatus).collect(Collectors.toSet()));
        Assertions.assertTrue(
                lifecycle.claim(tenant, "cert-1", "rel-gone", Instant.now()).isEmpty());
    }
}
