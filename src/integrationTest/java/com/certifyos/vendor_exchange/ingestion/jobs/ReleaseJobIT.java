package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import com.certifyos.vendor_exchange.auth.GoogleIdTokenService;
import com.certifyos.vendor_exchange.auth.WireMockDal;
import com.certifyos.vendor_exchange.ingestion.release.ReadBackService;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseFixtures;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseInProgressException;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseLifecycle;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.awaitility.Awaitility;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The release job, run directly on the api role against a WireMock DAL: claim, ranking, resolve, the
 * name guard, read our slices, merge, one transactional {@code /batch}, written, the read-back
 * deadline. Every tenant here starts with {@code reljob-}, one per test, so the ranking answer the
 * guard caches is the one the test stubbed. Fields are seeded {@code RELEASE_REQUESTED} directly.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class ReleaseJobIT {

    static final String NPI = "1003002890";
    static final String BATCH = "/batch";

    @Inject
    ReleaseJob job;

    @Inject
    ReadBackService readBack;

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

    private static FieldRecommendation requested(
            String tenant, String practitionerId, String locationId, String field, String value, String releaseId) {
        return ReleaseFixtures.field(
                tenant,
                practitionerId,
                NPI,
                locationId,
                field,
                value,
                ReviewStatus.RELEASE_REQUESTED,
                3L,
                ReleaseFixtures.requested(releaseId, locationId == null ? "CorePractitioner" : "CoreLocation"));
    }

    private FieldRecommendation read(FieldRecommendation doc) {
        return repository.find(doc.tenantId(), doc.id()).orElseThrow();
    }

    private void run(String tenant, String practitionerId, String releaseId) {
        job.run(new ReleaseJobRequest(tenant, practitionerId, releaseId));
    }

    static void stubPractitionerOv(String practitionerId, String firstName, String lastName) {
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/practitioners/ov/" + practitionerId))
                .willReturn(WireMock.okJson("{\"certifyId\": \"" + practitionerId + "\", \"data\": {\"npi\": \"" + NPI
                        + "\", \"firstName\": \"" + firstName + "\", \"lastName\": \"" + lastName + "\"},"
                        + " \"contributingSlices\": [\"slice-roster\"], \"contributionMap\": {}}")));
    }

    static void stubNoOwnPractitionerSlice(String tenant) {
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:" + tenant + "/" + NPI))
                .willReturn(WireMock.aResponse().withStatus(404)));
    }

    static void stubBatch(String tenant, String result) {
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant))
                .willReturn(WireMock.okJson(result)));
    }

    private static RequestPatternBuilder batchFor(String tenant) {
        return WireMock.postRequestedFor(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant));
    }

    private static Instant deadlineOf(StorageProvider storage, String releaseId) {
        Job deadline = storage.getJobById(IngestionJobEnqueuer.readBackDeadlineId(releaseId));
        Assertions.assertEquals(StateName.SCHEDULED, deadline.getState());
        return ((ScheduledState) deadline.getJobState()).getScheduledAt();
    }

    private AuditEvent failure(FieldRecommendation doc) {
        List<AuditEvent> trail = audit.trailForField(doc.tenantId(), doc.id(), 10);
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_RELEASE_FAILED),
                trail.stream().map(AuditEvent::type).toList());
        return trail.get(0);
    }

    @Test
    void releasesAPractitionerFieldEndToEnd() {
        String tenant = "reljob-e2e";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-e2e", "Danielle", "Smith");
        stubNoOwnPractitionerSlice(tenant);
        stubBatch(tenant, "[{\"alias\": \"practitioner\", \"result\": {\"certifyId\": \"slice-e2e\"}}]");
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-e2e", null, "first_name", "Daniel", "rel-e2e"))
                .get(0);
        Instant before = Instant.now();

        run(tenant, "cert-e2e", "rel-e2e");

        WireMockDal.verify(
                1,
                batchFor(tenant)
                        .withHeader("requesting-user-id", WireMock.equalTo(DalUserClient.REQUESTING_USER_ID))
                        .withHeader("Authorization", WireMock.equalTo("Bearer dal-id-token"))
                        .withRequestBody(WireMock.equalToJson("{\"transaction\":true,\"operations\":[{\"resource\":"
                                + "\"CorePractitioner\",\"alias\":\"practitioner\",\"operation\":\"UPSERT\",\"body\":"
                                + "{\"sourceType\":\"candor:" + tenant + "\",\"crosswalkId\":\"" + NPI + "\",\"data\":"
                                + "{\"npi\":\"" + NPI + "\",\"firstName\":\"Daniel\",\"lastName\":\"Smith\"}},"
                                + "\"filters\":{},\"returning\":[]}]}")));
        FieldRecommendation after = read(doc);
        GoldenRecordUpdate update = after.goldenRecordUpdate();
        Assertions.assertEquals(ReviewStatus.RELEASING, after.reviewStatus());
        Assertions.assertNotNull(update.writtenAt());
        Assertions.assertEquals("slice-e2e", update.sliceId());
        Assertions.assertEquals(NPI, update.crosswalkId());
        Assertions.assertEquals("CorePractitioner", update.resource());
        Assertions.assertEquals(
                IngestionJobEnqueuer.readBackDeadlineId("rel-e2e").toString(), update.deadlineJobId());
        Instant deadline = deadlineOf(storage, "rel-e2e");
        Assertions.assertFalse(deadline.isBefore(before.plus(Duration.ofMinutes(30))), deadline.toString());
        Assertions.assertTrue(deadline.isBefore(Instant.now().plus(Duration.ofMinutes(31))), deadline.toString());
        Assertions.assertTrue(audit.trailForField(tenant, doc.id(), 10).isEmpty(), "written writes no event");
    }

    @Test
    void secondReleaseKeepsLastMonthsValue() {
        String tenant = "reljob-second";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-second", "Dan", "Whitfield");
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:" + tenant + "/" + NPI))
                .willReturn(WireMock.okJson("{\"certifyId\": \"slice-second\", \"sourceType\": \"candor:" + tenant
                        + "\", \"crosswalkId\": \"" + NPI + "\", \"data\": {\"npi\": \"" + NPI
                        + "\", \"firstName\": \"Dan\", \"lastName\": \"Whitfield-Lee\"}}")));
        stubBatch(tenant, "[{\"alias\": \"practitioner\", \"result\": {}}]");
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-second", null, "first_name", "Danny", "rel-second"))
                .get(0);

        run(tenant, "cert-second", "rel-second");

        WireMockDal.verify(
                1,
                batchFor(tenant)
                        .withRequestBody(WireMock.matchingJsonPath(
                                "$.operations[0].body.data",
                                WireMock.equalToJson("{\"npi\":\"" + NPI
                                        + "\",\"firstName\":\"Danny\",\"lastName\":\"Whitfield-Lee\"}"))));
        Assertions.assertEquals(
                "slice-second",
                read(doc).goldenRecordUpdate().sliceId(),
                "an answer without the id: the slice id comes from the slice read after the write");
    }

    @Test
    void locationReleaseWritesThreeSlicesAndTheLink() {
        String tenant = "reljob-location";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-loc", "Dana", "Whitfield");
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/locations/ov/loc-77"))
                .willReturn(WireMock.okJson("{\"certifyLocationId\": \"loc-77\", \"data\": {\"name\": \"Greenwood"
                        + " Village Clinic\", \"contacts\": [{\"type\": \"phone\", \"value\": \"3036790177\"},"
                        + " {\"type\": \"fax\", \"value\": \"3036790178\"}], \"addresses\": [{\"addressLine1\":"
                        + " \"5351 S. Roslyn St\", \"addressLine2\": \"Suite 200\", \"city\": \"Greenwood Village\","
                        + " \"state\": \"CO\", \"zip\": \"80111\", \"addressType\": \"service\"}]},"
                        + " \"contributingSlices\": [], \"contributionMap\": {}}")));
        String crosswalk = "GREENWOOD_VILLAGE_CLINIC_afc25220a8c9a4431425f9de2eeb87d9be1f14f8453a66847f7a9ff5973670b9";
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/locations/candor:" + tenant + "/" + crosswalk))
                .willReturn(WireMock.aResponse().withStatus(404)));
        stubBatch(
                tenant,
                "[{\"alias\": \"location1\", \"result\": {\"certifyLocationId\": \"slice-loc\"}},"
                        + " {\"alias\": \"address1\", \"result\": {\"certifyId\": \"slice-addr\"}},"
                        + " {\"alias\": \"link1\", \"result\": {\"id\": \"link-1\"}}]");
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-loc", "loc-77", "phone", "3035550100", "rel-location"))
                .get(0);

        run(tenant, "cert-loc", "rel-location");

        String address = "{\"addressLine1\": \"5351 S. Roslyn St\", \"addressLine2\": \"Suite 200\", \"city\":"
                + " \"Greenwood Village\", \"state\": \"CO\", \"zip\": \"80111\", \"addressType\": \"service\"}";
        WireMockDal.verify(
                1,
                batchFor(tenant)
                        .withRequestBody(WireMock.equalToJson("{\"transaction\": true, \"operations\": ["
                                + "{\"resource\": \"CoreLocation\", \"alias\": \"location1\", \"operation\": \"UPSERT\","
                                + " \"body\": {\"sourceType\": \"candor:" + tenant + "\", \"crosswalkId\": \""
                                + crosswalk
                                + "\", \"data\": {\"name\": \"Greenwood Village Clinic\", \"contacts\": ["
                                + "{\"type\": \"phone\", \"value\": \"3035550100\"},"
                                + " {\"type\": \"fax\", \"value\": \"3036790178\"}]}}, \"filters\": {}, \"returning\": []},"
                                + "{\"resource\": \"CoreEntityAddress\", \"alias\": \"address1\", \"operation\": \"UPSERT\","
                                + " \"body\": {\"sourceType\": \"candor:" + tenant + "\", \"crosswalkId\":"
                                + " \"afc25220a8c9a4431425f9de2eeb87d9be1f14f8453a66847f7a9ff5973670b9\", \"data\": "
                                + address + "}, \"filters\": {}, \"returning\": []},"
                                + "{\"resource\": \"LocationEntityAddress\", \"alias\": \"link1\", \"operation\": \"UPSERT\","
                                + " \"body\": {\"locationId\": \"${location1.certifyLocationId}\", \"entityAddressId\":"
                                + " \"${address1.certifyId}\", \"data\": {\"addressType\": \"service\"}},"
                                + " \"filters\": {\"locationId\": \"${location1.certifyLocationId}\", \"entityAddressId\":"
                                + " \"${address1.certifyId}\"}, \"returning\": []}]}")));
        GoldenRecordUpdate update = read(doc).goldenRecordUpdate();
        Assertions.assertEquals("CoreLocation", update.resource());
        Assertions.assertEquals(crosswalk, update.crosswalkId());
        Assertions.assertEquals("slice-loc", update.sliceId());
        Assertions.assertEquals(ReviewStatus.RELEASING, read(doc).reviewStatus());
    }

    @Test
    void rankingMissingWritesNothing() {
        String tenant = "reljob-unranked";
        ReleaseFixtures.stubRanking(tenant, false);
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-unranked", null, "last_name", "Smith", "rel-unranked"))
                .get(0);

        run(tenant, "cert-unranked", "rel-unranked");

        WireMockDal.verify(0, batchFor(tenant));
        FieldRecommendation after = read(doc);
        Assertions.assertEquals(ReviewStatus.APPROVED, after.reviewStatus(), "back to APPROVED for a retry");
        Assertions.assertTrue(after.goldenRecordUpdate().error().startsWith("RANKING_MISSING"));
        AuditEvent event = failure(doc);
        Assertions.assertEquals("RANKING_MISSING", event.detail().get("reason"));
        Assertions.assertEquals(1, ((Number) event.detail().get("jobAttempts")).intValue());
        Assertions.assertEquals("rel-unranked", event.releaseId());
    }

    @Test
    void unknownEntityWritesNothing() {
        String tenant = "reljob-unknown";
        ReleaseFixtures.stubRanking(tenant, true);
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/practitioners/ov/cert-gone"))
                .willReturn(WireMock.aResponse().withStatus(404)));
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-gone", null, "first_name", "Dan", "rel-unknown"))
                .get(0);

        run(tenant, "cert-gone", "rel-unknown");

        WireMockDal.verify(0, batchFor(tenant));
        Assertions.assertEquals(ReviewStatus.APPROVED, read(doc).reviewStatus());
        Assertions.assertEquals("UNKNOWN_ENTITY", failure(doc).detail().get("reason"));
    }

    @Test
    void locationWithoutServiceAddressWritesNothing() {
        String tenant = "reljob-noaddress";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-noaddr", "Dana", "Whitfield");
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/locations/ov/loc-noaddr"))
                .willReturn(
                        WireMock.okJson("{\"certifyLocationId\": \"loc-noaddr\", \"data\": {\"name\": \"Clinic\"}}")));
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-noaddr", "loc-noaddr", "fax", "3039998888", "rel-noaddr"))
                .get(0);

        run(tenant, "cert-noaddr", "rel-noaddr");

        WireMockDal.verify(0, batchFor(tenant));
        Assertions.assertEquals(ReviewStatus.APPROVED, read(doc).reviewStatus());
        Assertions.assertEquals("UNKNOWN_ENTITY", failure(doc).detail().get("reason"));
    }

    @Test
    void nameSoundChangedWritesNothing() {
        String tenant = "reljob-sound";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-sound", "Bob", "Smith");
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo("/locations/ov/loc-1"))
                .willReturn(WireMock.okJson("{\"certifyLocationId\": \"loc-1\", \"data\": {\"name\": \"Clinic\","
                        + " \"serviceAddress\": {\"addressLine1\": \"1 Main St\", \"city\": \"Denver\"}}}")));
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                requested(tenant, "cert-sound", null, "first_name", "Robert", "rel-sound"),
                requested(tenant, "cert-sound", "loc-1", "phone", "3035550100", "rel-sound"));

        run(tenant, "cert-sound", "rel-sound");

        WireMockDal.verify(0, batchFor(tenant));
        for (FieldRecommendation doc : docs) {
            Assertions.assertEquals(ReviewStatus.APPROVED, read(doc).reviewStatus(), "the whole release is refused");
            Assertions.assertEquals("NAME_SOUND_CHANGED", failure(doc).detail().get("reason"));
        }
    }

    @Test
    void dalRejectsWithFourHundred() {
        String tenant = "reljob-rejected";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-rej", "Dana", "Whitfield");
        stubNoOwnPractitionerSlice(tenant);
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant))
                .willReturn(WireMock.aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"errors\": [{\"reason\": \"Bad Request\", \"detail\": \"source not found\"}]}")));
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-rej", null, "last_name", "Whitfield-Lee", "rel-rejected"))
                .get(0);

        run(tenant, "cert-rej", "rel-rejected");

        FieldRecommendation after = read(doc);
        Assertions.assertEquals(ReviewStatus.APPROVED, after.reviewStatus());
        Assertions.assertTrue(
                after.goldenRecordUpdate().error().contains("source not found"),
                after.goldenRecordUpdate().error());
        Assertions.assertTrue(after.goldenRecordUpdate().error().startsWith("DAL_REJECTED: HTTP 400"));
        Assertions.assertEquals("DAL_REJECTED", failure(doc).detail().get("reason"));
    }

    @Test
    void dalDownPropagates() {
        String tenant = "reljob-down";
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-down", "Dana", "Whitfield");
        stubNoOwnPractitionerSlice(tenant);
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant))
                .willReturn(WireMock.aResponse().withStatus(503)));
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections, requested(tenant, "cert-down", null, "first_name", "Danna", "rel-down"))
                .get(0);

        WebApplicationException down =
                Assertions.assertThrows(WebApplicationException.class, () -> run(tenant, "cert-down", "rel-down"));

        Assertions.assertEquals(503, down.getResponse().getStatus());
        Assertions.assertEquals(ReviewStatus.RELEASING, read(doc).reviewStatus(), "kept for JobRunr's retry");
        Assertions.assertNull(read(doc).goldenRecordUpdate().writtenAt());
        stubBatch(tenant, "[{\"alias\": \"practitioner\", \"result\": {\"certifyId\": \"slice-down\"}}]");
        run(tenant, "cert-down", "rel-down");
        Assertions.assertEquals("slice-down", read(doc).goldenRecordUpdate().sliceId(), "the retry writes");
        WireMockDal.verify(2, batchFor(tenant));
    }

    @Test
    void retryAfterWriteSchedulesTheDeadlineOnly() {
        String tenant = "reljob-retry";
        Instant writtenAt = Instant.parse("2026-10-20T14:00:00Z");
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-retry",
                                NPI,
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASING,
                                5L,
                                ReleaseFixtures.written(
                                        "rel-retry", "CorePractitioner", NPI, "slice-retry", writtenAt, null)))
                .get(0);

        run(tenant, "cert-retry", "rel-retry");

        WireMockDal.verify(0, batchFor(tenant));
        WireMockDal.verify(
                0, WireMock.getRequestedFor(WireMock.urlPathEqualTo("/tenants/" + tenant + "/configurations")));
        Assertions.assertEquals(writtenAt.plus(Duration.ofMinutes(30)), deadlineOf(storage, "rel-retry"));
        Assertions.assertEquals(doc, read(doc), "the fields are left as they were");
    }

    /**
     * Review focus 4: two releases of one practitioner at once. The first job's {@code /batch} is slow;
     * the second job claims while it runs, sees the first release {@code RELEASING} and backs off. Once
     * the first is read back, the second runs and its merge carries the first's value, which the own-slice
     * read (a WireMock scenario) now returns.
     */
    @Test
    void concurrentJobsForOnePractitionerSerialise() throws InterruptedException, ExecutionException, TimeoutException {
        String tenant = "reljob-concurrent";
        String scenario = "slice-" + tenant;
        ReleaseFixtures.stubRanking(tenant, true);
        stubPractitionerOv("cert-conc", "Dan", "Smith");
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:" + tenant + "/" + NPI))
                .inScenario(scenario)
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(WireMock.aResponse().withStatus(404)));
        WireMockDal.stubFor(WireMock.get(WireMock.urlEqualTo("/core/practitioner/candor:" + tenant + "/" + NPI))
                .inScenario(scenario)
                .whenScenarioStateIs("written")
                .willReturn(WireMock.okJson("{\"certifyId\": \"slice-conc\", \"data\": {\"npi\": \"" + NPI
                        + "\", \"firstName\": \"Danny\", \"lastName\": \"Smith\"}}")));
        String answer = "[{\"alias\": \"practitioner\", \"result\": {\"certifyId\": \"slice-conc\"}}]";
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant))
                .inScenario(scenario)
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("written")
                .willReturn(WireMock.okJson(answer).withFixedDelay(1500)));
        WireMockDal.stubFor(WireMock.post(WireMock.urlEqualTo(BATCH))
                .withHeader(DalUserClient.ORGANIZATION_HEADER, WireMock.equalTo(tenant))
                .inScenario(scenario)
                .whenScenarioStateIs("written")
                .willReturn(WireMock.okJson(answer)));
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                requested(tenant, "cert-conc", null, "first_name", "Danny", "rel-conc-1"),
                requested(tenant, "cert-conc", null, "last_name", "Smyth", "rel-conc-2"));

        CompletableFuture<Void> first = CompletableFuture.runAsync(() -> run(tenant, "cert-conc", "rel-conc-1"));
        // The first job reads its own slice only after its claim and its count, so once WireMock has seen
        // that read the first job is past the point where a second claim could make it back off.
        Awaitility.await()
                .atMost(10, TimeUnit.SECONDS)
                .ignoreExceptions()
                .untilAsserted(() -> WireMockDal.verify(
                        1,
                        WireMock.getRequestedFor(
                                WireMock.urlEqualTo("/core/practitioner/candor:" + tenant + "/" + NPI))));
        Assertions.assertEquals(ReviewStatus.RELEASING, read(docs.get(0)).reviewStatus());
        ReleaseInProgressException backedOff =
                Assertions.assertThrows(ReleaseInProgressException.class, () -> run(tenant, "cert-conc", "rel-conc-2"));
        first.get(10, TimeUnit.SECONDS);

        Assertions.assertTrue(backedOff.getMessage().contains("rel-conc-2"));
        Assertions.assertEquals(
                ReviewStatus.RELEASE_REQUESTED, read(docs.get(1)).reviewStatus());
        WireMockDal.verify(1, batchFor(tenant));
        Assertions.assertThrows(
                ReleaseInProgressException.class,
                () -> run(tenant, "cert-conc", "rel-conc-2"),
                "the first release is RELEASING until it is read back");
        readBack.markUnknown(tenant, List.of(read(docs.get(0))), ReleaseLifecycle.EVENT, Instant.now());
        run(tenant, "cert-conc", "rel-conc-2");

        WireMockDal.verify(2, batchFor(tenant));
        WireMockDal.verify(
                1,
                batchFor(tenant)
                        .withRequestBody(WireMock.matchingJsonPath(
                                "$.operations[0].body.data",
                                WireMock.equalToJson(
                                        "{\"npi\":\"" + NPI + "\",\"firstName\":\"Danny\",\"lastName\":\"Smyth\"}"))));
        Assertions.assertEquals(ReviewStatus.RELEASING, read(docs.get(1)).reviewStatus());
        Assertions.assertEquals(FieldLevel.PRACTITIONER, docs.get(1).level());
    }
}
