package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.directory_accuracy.ingestion.release.ReadBackService;
import com.certifyos.directory_accuracy.ingestion.release.ReleaseFixtures;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The {@code mdm.slice.ov_generated} handler table (design step 8, plan P9): a run our candor slice
 * triggered reads the operational value with lineage and finishes the written {@code RELEASING}
 * fields of that entity; anything else is acknowledged with nothing changed. WireMock stands in for
 * the DAL's operational values. Every tenant here starts with {@code ovgen-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@TestProfile(ApiTestProfile.class)
class OvGeneratedHandlerIT {

    // Two minutes ago, so the read-back time the event records is positive.
    static final Instant WRITTEN = Instant.now().truncatedTo(ChronoUnit.MILLIS).minusSeconds(120);
    static final int LIMIT = 100;

    @Inject
    OvGeneratedHandler handler;

    @Inject
    FieldRecommendationRepository repository;

    @Inject
    AuditRepository audit;

    @Inject
    Collections collections;

    @Inject
    IngestionJobEnqueuer enqueuer;

    @Inject
    StorageProvider storage;

    @Inject
    MeterRegistry registry;

    @InjectMock
    GoogleIdTokenService tokens;

    @Inject
    ObjectMapper mapper;

    @BeforeEach
    void before() {
        Mockito.when(tokens.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        WireMockDal.resetRequests();
    }

    /** Survivorship's envelope for one entity, as the push subscription delivers it. */
    static PubSubPushMessage.Message message(
            String tenant, String certifyId, String entityType, String triggeringSourceType) {
        return message(tenant, certifyId, entityType, triggeringSourceType, "slice-x", Instant.now());
    }

    /** The envelope with one triggering slice: its id and when it changed. */
    static PubSubPushMessage.Message message(
            String tenant,
            String certifyId,
            String entityType,
            String triggeringSourceType,
            String triggeringSliceId,
            Instant changedAt) {
        String envelope = "{\"eventId\": \"ev-" + certifyId + "\", \"serviceSource\": \"survivorship\","
                + " \"payload\": {\"certifyId\": \"" + certifyId + "\", \"tenantId\": \"" + tenant + "\","
                + " \"entityType\": \"" + entityType + "\", \"crosswalks\": [],"
                + " \"triggeringCrosswalks\": [{\"certifyId\": \"" + triggeringSliceId
                + "\", \"crosswalkId\": \"cw-x\","
                + " \"sourceType\": \"" + triggeringSourceType + "\", \"updatedAt\": \"" + changedAt + "\"}]}}";
        return new PubSubPushMessage.Message(
                Base64.getEncoder().encodeToString(envelope.getBytes(StandardCharsets.UTF_8)),
                "m-" + certifyId,
                "2026-10-20T14:00:06Z",
                Map.of("entityType", entityType, "triggeringSourceTypes", triggeringSourceType));
    }

    static void stubOv(String path, String contributingSlices, String contributionMap) {
        WireMockDal.stubFor(WireMock.get(WireMock.urlPathEqualTo(path))
                .withQueryParam("includeLineage", WireMock.equalTo("true"))
                .willReturn(WireMock.okJson("{\"certifyId\": \"x\", \"tenantId\": \"t\", \"data\": {},"
                        + " \"contributingSlices\": " + contributingSlices + ", \"contributionMap\": "
                        + contributionMap + "}")));
    }

    private FieldRecommendation writtenField(
            String tenant, String locationId, String field, String releaseId, String resource, String sliceId) {
        GoldenRecordUpdate update = ReleaseFixtures.written(
                releaseId,
                resource,
                locationId == null ? "1003002890" : "CLINIC_hash",
                sliceId,
                WRITTEN,
                IngestionJobEnqueuer.readBackDeadlineId(releaseId).toString());
        return ReleaseFixtures.field(
                tenant, "cert-1", "1003002890", locationId, field, "v-" + field, ReviewStatus.RELEASING, 4L, update);
    }

    private FieldRecommendation read(FieldRecommendation doc) {
        return repository.find(doc.tenantId(), doc.id()).orElseThrow();
    }

    private UUID scheduleDeadline(String tenant, String releaseId) {
        return enqueuer.readBackDeadline(tenant, "cert-1", releaseId, WRITTEN.plusSeconds(1800));
    }

    private double notOurs() {
        Counter found = registry.find(DirectoryAccuracyMetrics.INGESTION_OV_EVENTS_NOT_OURS)
                .counter();
        return found == null ? 0 : found.count();
    }

    @Test
    void candorTriggeredEventReleasesTheFields() {
        String tenant = "ovgen-practitioner";
        String release = "rel-ovgen-practitioner";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                writtenField(tenant, null, "first_name", release, "CorePractitioner", "slice-candor-1"),
                writtenField(tenant, null, "last_name", release, "CorePractitioner", "slice-candor-1"));
        UUID deadline = scheduleDeadline(tenant, release);
        stubOv(
                "/practitioners/ov/cert-1",
                "[\"slice-roster\", \"slice-candor-1\"]",
                "{\"firstName\": \"candor:" + tenant + "\", \"lastName\": \"roster:" + tenant + "\"}");

        OvGeneratedHandler.Outcome outcome = handler.handle(
                message(tenant, "cert-1", "practitioner", "candor:" + tenant, "slice-candor-1", Instant.now()));

        Assertions.assertEquals(OvGeneratedHandler.Outcome.RELEASED, outcome);
        GoldenRecordUpdate first = read(docs.get(0)).goldenRecordUpdate();
        GoldenRecordUpdate last = read(docs.get(1)).goldenRecordUpdate();
        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(0)).reviewStatus());
        Assertions.assertEquals("WON", first.outcome());
        Assertions.assertEquals("candor:" + tenant, first.winningSource());
        Assertions.assertNotNull(first.releasedAt());
        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(1)).reviewStatus());
        Assertions.assertEquals("OUTRANKED", last.outcome());
        Assertions.assertEquals("roster:" + tenant, last.winningSource());
        Assertions.assertEquals(5L, read(docs.get(0)).version());
        List<AuditEvent> events = audit.findForRelease(tenant, release, LIMIT);
        Assertions.assertEquals(
                List.of(AuditEventType.FIELD_RELEASED, AuditEventType.FIELD_RELEASED),
                events.stream().map(AuditEvent::type).toList());
        Map<String, Object> detail = events.get(0).detail();
        Assertions.assertEquals("WON", detail.get("outcome"));
        Assertions.assertEquals("candor:" + tenant, detail.get("winningSource"));
        Assertions.assertEquals("SLICE", detail.get("writePath"));
        Assertions.assertEquals("CorePractitioner", detail.get("resource"));
        Assertions.assertEquals("1003002890", detail.get("crosswalkId"));
        Assertions.assertEquals("EVENT", detail.get("completionSource"));
        Assertions.assertTrue(((Number) detail.get("readBackSeconds")).longValue() > 0);
        Assertions.assertEquals(release, events.get(0).releaseId());
        Assertions.assertEquals(
                StateName.DELETED, storage.getJobById(deadline).getState(), "the deadline check is no longer needed");
    }

    @Test
    void notOursIsAcknowledged() {
        String tenant = "ovgen-notours";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        writtenField(tenant, null, "first_name", "rel-ovgen-notours", "CorePractitioner", "slice-c"))
                .get(0);
        double before = notOurs();

        OvGeneratedHandler.Outcome outcome =
                handler.handle(message(tenant, "cert-1", "practitioner", "roster:" + tenant));

        Assertions.assertEquals(OvGeneratedHandler.Outcome.NOT_OURS, outcome);
        Assertions.assertEquals(doc, read(doc), "nothing changes");
        Assertions.assertEquals(before + 1, notOurs());
        WireMockDal.verify(0, WireMock.getRequestedFor(WireMock.urlPathMatching("/practitioners/ov/.*")));
    }

    @Test
    void locationEventReadsTheLocationOv() {
        String tenant = "ovgen-location";
        String release = "rel-ovgen-location";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                writtenField(tenant, "loc-1", "phone", release, "CoreLocation", "slice-candor-loc"),
                writtenField(tenant, null, "first_name", release, "CorePractitioner", "slice-candor-1"));
        UUID deadline = scheduleDeadline(tenant, release);
        stubOv("/locations/ov/loc-1", "[\"slice-candor-loc\"]", "{\"contacts\": \"candor:" + tenant + "\"}");

        OvGeneratedHandler.Outcome outcome = handler.handle(
                message(tenant, "loc-1", "location", "candor:" + tenant, "slice-candor-loc", Instant.now()));

        Assertions.assertEquals(OvGeneratedHandler.Outcome.RELEASED, outcome);
        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(0)).reviewStatus());
        Assertions.assertEquals("WON", read(docs.get(0)).goldenRecordUpdate().outcome());
        Assertions.assertEquals(
                ReviewStatus.RELEASING,
                read(docs.get(1)).reviewStatus(),
                "the practitioner field waits for its own event");
        Assertions.assertEquals(
                StateName.SCHEDULED,
                storage.getJobById(deadline).getState(),
                "the release still has a RELEASING field, so its deadline check stays");
        WireMockDal.verify(0, WireMock.getRequestedFor(WireMock.urlPathMatching("/practitioners/ov/.*")));
    }

    @Test
    void ourSliceAbsentFromTheOvIsNotApplied() {
        String tenant = "ovgen-notapplied";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        writtenField(tenant, null, "last_name", "rel-ovgen-notapplied", "CorePractitioner", "slice-c"))
                .get(0);
        stubOv("/practitioners/ov/cert-1", "[\"slice-roster\"]", "{\"lastName\": \"roster:" + tenant + "\"}");

        handler.handle(message(tenant, "cert-1", "practitioner", "candor:" + tenant, "slice-c", Instant.now()));

        Assertions.assertEquals(ReviewStatus.RELEASED, read(doc).reviewStatus());
        Assertions.assertEquals("NOT_APPLIED", read(doc).goldenRecordUpdate().outcome());
    }

    @Test
    void anEarlierRunOrAnotherSliceFinishesNothing() {
        String tenant = "ovgen-stale";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        writtenField(tenant, "loc-1", "phone", "rel-ovgen-stale", "CoreLocation", "slice-candor-loc"))
                .get(0);

        // a run triggered by our slice before this write was sent (a redelivery, or another
        // practitioner's write at the same location), then a run for a slice that is not this write's
        OvGeneratedHandler.Outcome earlier = handler.handle(
                message(tenant, "loc-1", "location", "candor:" + tenant, "slice-candor-loc", WRITTEN.minusSeconds(60)));
        OvGeneratedHandler.Outcome elsewhere =
                handler.handle(message(tenant, "loc-1", "location", "candor:" + tenant, "slice-other", Instant.now()));

        Assertions.assertEquals(OvGeneratedHandler.Outcome.NOT_COVERED, earlier);
        Assertions.assertEquals(OvGeneratedHandler.Outcome.NOT_COVERED, elsewhere);
        Assertions.assertEquals(doc, read(doc), "the field waits for the run that includes its write");
        WireMockDal.verify(0, WireMock.getRequestedFor(WireMock.urlPathMatching("/locations/ov/.*")));
    }

    @Test
    void coveredFieldsFinishAndAnUnwrittenSiblingAsksForRedelivery() {
        String tenant = "ovgen-sibling";
        List<FieldRecommendation> docs = ReleaseFixtures.insert(
                collections,
                writtenField(tenant, "loc-1", "phone", "rel-ovgen-sib-1", "CoreLocation", "slice-candor-loc"),
                ReleaseFixtures.field(
                        tenant,
                        "cert-2",
                        "1003002891",
                        "loc-1",
                        "fax",
                        "3035550199",
                        ReviewStatus.RELEASING,
                        4L,
                        ReleaseFixtures.requested("rel-ovgen-sib-2", "CoreLocation")));
        stubOv("/locations/ov/loc-1", "[\"slice-candor-loc\"]", "{\"contacts\": \"candor:" + tenant + "\"}");

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> handler.handle(
                        message(tenant, "loc-1", "location", "candor:" + tenant, "slice-candor-loc", Instant.now())),
                "the sibling's write may be in this run; Pub/Sub redelivers");

        Assertions.assertEquals(ReviewStatus.RELEASED, read(docs.get(0)).reviewStatus(), "the covered field is done");
        Assertions.assertEquals(ReviewStatus.RELEASING, read(docs.get(1)).reviewStatus());
    }

    @Test
    void aSwitchedOffServiceRefusesForRedelivery() {
        DirectoryAccuracyConfig disabled = Mockito.mock(DirectoryAccuracyConfig.class);
        Mockito.when(disabled.enabled()).thenReturn(false);
        OvGeneratedHandler off = new OvGeneratedHandler(
                Mockito.mock(ReadBackService.class),
                Mockito.mock(DirectoryAccuracyMetrics.class),
                mapper,
                disabled,
                java.time.Clock.systemUTC());

        ProblemException refused = Assertions.assertThrows(
                ProblemException.class,
                () -> off.handle(message("ovgen-off", "cert-1", "practitioner", "candor:ovgen-off")));

        Assertions.assertEquals(503, refused.status());
        Assertions.assertEquals(OvGeneratedHandler.SERVICE_DISABLED, refused.code());
    }

    @Test
    void nothingReleasingIsAcknowledged() {
        Assertions.assertEquals(
                OvGeneratedHandler.Outcome.NOTHING_RELEASING,
                handler.handle(message("ovgen-nothing", "cert-1", "practitioner", "candor:ovgen-nothing")));
        WireMockDal.verify(0, WireMock.getRequestedFor(WireMock.urlPathMatching("/practitioners/ov/.*")));
    }

    @Test
    void unparseableIsAcknowledged() {
        PubSubPushMessage.Message garbage = new PubSubPushMessage.Message(
                Base64.getEncoder().encodeToString("not json".getBytes(StandardCharsets.UTF_8)), "m-x", null, Map.of());
        PubSubPushMessage.Message notBase64 = new PubSubPushMessage.Message("%%%", "m-y", null, Map.of());
        PubSubPushMessage.Message empty = new PubSubPushMessage.Message(null, "m-z", null, Map.of());

        for (PubSubPushMessage.Message message : List.of(garbage, notBase64, empty)) {
            Assertions.assertEquals(OvGeneratedHandler.Outcome.REJECTED_UNPARSEABLE, handler.handle(message));
        }
    }

    @Test
    void unknownEntityTypeIsAcknowledged() {
        Assertions.assertEquals(
                OvGeneratedHandler.Outcome.UNKNOWN_ENTITY_TYPE,
                handler.handle(message("ovgen-facility", "fac-1", "facility", "candor:ovgen-facility")));
    }

    @Test
    void eventBeforeTheWriteIsRecordedIsRedelivered() {
        String tenant = "ovgen-early";
        FieldRecommendation doc = ReleaseFixtures.insert(
                        collections,
                        ReleaseFixtures.field(
                                tenant,
                                "cert-1",
                                "1003002890",
                                null,
                                "first_name",
                                "Dan",
                                ReviewStatus.RELEASING,
                                4L,
                                ReleaseFixtures.requested("rel-ovgen-early", "CorePractitioner")))
                .get(0);

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> handler.handle(message(tenant, "cert-1", "practitioner", "candor:" + tenant)),
                "the job has written but not yet recorded the slice id; a 5xx makes Pub/Sub redeliver");
        Assertions.assertEquals(doc, read(doc));
    }
}
