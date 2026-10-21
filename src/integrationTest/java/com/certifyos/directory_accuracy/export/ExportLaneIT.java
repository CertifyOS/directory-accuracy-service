package com.certifyos.directory_accuracy.export;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.auth.UserContextFilter;
import com.certifyos.directory_accuracy.auth.WireMockDal;
import com.certifyos.directory_accuracy.clients.ApiLayerTokenService;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.jobs.DeadlineCheckJob;
import com.certifyos.directory_accuracy.export.jobs.DeadlineCheckJobRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.export.schedule.TemplateProvisioner;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.security.TestSecurity;
import io.quarkus.test.security.jwt.Claim;
import io.quarkus.test.security.jwt.JwtSecurity;
import io.restassured.RestAssured;
import io.restassured.specification.RequestSpecification;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.awaitility.Awaitility;
import org.bson.Document;
import org.hamcrest.Matchers;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The whole export lane on the worker role, with JobRunr executing the jobs: an operator creates a
 * schedule with a saved filter id and runs it now, the egress request job pins the template and
 * asks egress to select the filter's practitioners and build the file, the completion arrives (by push event, by the deadline check, or
 * after a failure and a retry), the finish job verifies the file and the batch is DELIVERED. The
 * three external systems are WireMock (DAL, api-layer, egress) and a mocked bucket. One tenant,
 * one vendor per scenario, so the batch ids never collide.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockDal.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(WorkerTestProfile.class)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExportLaneIT {

    static final String EMAIL = UserContextFilter.EMAIL_CLAIM;
    static final String TENANT = "org-lane";
    static final String FILTER_ID = "flt-lane-direct";
    static final String SCHEDULES = "/v1/vendor-exports/schedules/" + TENANT + "/";
    static final String BATCHES = "/v1/vendor-exports/";
    static final String EVENTS = "/internal/vendor-exports/egress-events";
    static final String TEMPLATES = "/api/v1/egress-templates";
    static final String EXPORT = "/api/v1/egress/export";
    static final Duration HOP = Duration.ofSeconds(60);
    static final String SCHEDULE_BODY = "{\"cadence\":{\"type\":\"monthly\",\"dayOfMonth\":1},\"timezone\":\"UTC\","
            + "\"filterId\":\"" + ExportLaneIT.FILTER_ID + "\"}";
    static final String TEMPLATE = "{\"id\":\"tpl-lane\",\"tenantId\":\"" + TENANT + "\",\"templateName\":\""
            + TemplateProvisioner.TEMPLATE_NAME
            + "\",\"entityType\":\"practitioner\",\"status\":\"active\",\"version\":2,"
            + "\"mappingsCsvUrl\":\"gs://egress-templates/" + TENANT
            + "/tpl-lane/v2/mappings.csv\",\"separator\":\";\","
            + "\"outputFormat\":\"csv\",\"rowExpansionKeys\":[\"locations\"]}";
    /** Egress's selection block for the three practitioners the saved filter matches. */
    static final String SELECTION_BLOCK = "\"selection\":{\"source\":\"SAVED_FILTER\",\"state\":\"RESOLVED\","
            + "\"requested\":3,\"distinct\":3,\"matched\":3,\"unmatched\":0,\"parts\":1,"
            + "\"listUsedUri\":\"gs://egress-list-used/lane.csv\",\"reusedFrom\":null,\"attempts\":1,"
            + "\"failureReason\":null}";

    /** Objects the mocked bucket answers, by object name. */
    static final Map<String, VendorBucket.ObjectInfo> OBJECTS = new ConcurrentHashMap<>();

    @Inject
    ExportBatchRepository batches;

    @Inject
    ScheduleRepository schedules;

    @Inject
    Collections collections;

    @Inject
    StorageProvider storage;

    @Inject
    DeadlineCheckJob deadlineCheck;

    @InjectMock
    GoogleIdTokenService google;

    @InjectMock
    ApiLayerTokenService apiLayerTokens;

    @InjectMock
    PushTokenVerifier verifier;

    @InjectMock
    VendorBucket bucket;

    @BeforeEach
    void before() {
        // The journal is shared across scenarios on one server; counts below are per scenario.
        WireMockUpstreams.resetRequests();
        Mockito.when(google.idToken("test-dal-iap-client-id")).thenReturn("dal-id-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
        Mockito.when(google.idToken("test-egress-iap-client-id")).thenReturn("egress-token");
        Mockito.when(apiLayerTokens.accessToken()).thenReturn("api-layer-token");
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
        Mockito.when(bucket.head(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenAnswer(invocation -> Optional.ofNullable(OBJECTS.get(invocation.getArgument(1, String.class))));
        WireMockDal.stubMember("lane@certifyos.com", TENANT);
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES))
                .withHeader("tenant-id", WireMock.equalTo(TENANT))
                .willReturn(WireMock.okJson("{\"templates\":[" + TEMPLATE + "],\"total\":1,\"page\":0,\"size\":10}")));
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo(TEMPLATES + "/tpl-lane"))
                .withHeader("tenant-id", WireMock.equalTo(TENANT))
                .willReturn(WireMock.okJson(TEMPLATE)));
    }

    /** The first batch id run-now creates for a vendor this month (schedules here are in UTC). */
    private static String batchIdFor(String vendor) {
        return TENANT + "-" + vendor + "-" + java.time.YearMonth.now(ZoneId.of("UTC")) + "-001";
    }

    /** Egress's status for a finished export, with the selection block the finish job reconciles against. */
    private static void stubStatusCompleted(String correlationId) {
        WireMockUpstreams.stubFor(
                WireMock.get(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/" + TENANT + "/" + correlationId))
                        .willReturn(WireMock.okJson(
                                "{\"state\":\"COMPLETE\",\"phase\":\"COMPLETED\",\"total_rows\":4,\"rows_with_data\":3,"
                                        + "\"gcs_uri\":\"gs://test-vendor-bucket/from/x.csv\",\"gcs_complete\":true,"
                                        + SELECTION_BLOCK + "}")));
    }

    private static RequestSpecification member() {
        return RestAssured.given().header("tenant-id", TENANT).contentType("application/json");
    }

    private static void stubExportAccepted(String correlationId) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withRequestBody(WireMock.matchingJsonPath("$.correlationId", WireMock.equalTo(correlationId)))
                .willReturn(WireMock.jsonResponse(
                        "{\"type\":\"practitioner\",\"tenant_id\":\"" + TENANT + "\",\"correlation_id\":\""
                                + correlationId + "\",\"table\":\"practitioner:" + TENANT + ":" + correlationId
                                + "\",\"ttl_days\":7,\"state\":\"PENDING\"}",
                        202)));
    }

    private static void pushEvent(String messageId, String correlationId, String phase) {
        String event =
                "{\"schemaVersion\":\"egress-export-event-v1\",\"type\":\"practitioner\",\"tenantId\":\"" + TENANT
                        + "\",\"correlationId\":\"" + correlationId + "\",\"phase\":\"" + phase + "\","
                        + "\"outputUri\":\"gs://test-vendor-bucket/from/" + TENANT
                        + "/x.csv\",\"totalRecords\":3,\"totalRows\":4,"
                        + (phase.equals("FAILED") ? "\"failureMessage\":\"dataflow died\"," : "")
                        + "\"completedAt\":\"2026-10-01T07:12:00Z\"}";
        String data = Base64.getEncoder().encodeToString(event.getBytes(StandardCharsets.UTF_8));
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body("{\"message\":{\"data\":\"" + data + "\",\"messageId\":\"" + messageId
                        + "\",\"publishTime\":\"2026-10-01T07:12:01Z\",\"attributes\":{}},\"subscription\":\"s\"}")
                .post(EVENTS)
                .then()
                .statusCode(204);
    }

    private static void fileAt(ExportBatch batch, String producedBy) {
        String name = batch.egress().destination().objectName();
        OBJECTS.put(
                name,
                new VendorBucket.ObjectInfo(
                        "test-vendor-bucket",
                        name,
                        4096,
                        Map.of("complete", "true", "totalRecords", "3", "totalRows", "4", "correlationId", producedBy),
                        "md5-" + "x"));
    }

    private ExportBatch awaitState(String exportBatchId, BatchState state) {
        Awaitility.await()
                .atMost(HOP)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> batches.find(exportBatchId).map(ExportBatch::state).orElse(null) == state);
        return batches.find(exportBatchId).orElseThrow();
    }

    private List<AuditEventType> trail(String exportBatchId) {
        List<AuditEventType> types = new ArrayList<>();
        for (Document doc : collections
                .events()
                .find(Filters.eq("exportBatchId", exportBatchId))
                .sort(Sorts.ascending("_id"))) {
            types.add(AuditEvent.fromDocument(doc).type());
        }
        return types;
    }

    private String runNow(String vendor) {
        member().body(SCHEDULE_BODY)
                .put(SCHEDULES + vendor)
                .then()
                .statusCode(201)
                .body("egressTemplateId", Matchers.equalTo("tpl-lane"));
        return member().post(SCHEDULES + vendor + "/run-now")
                .then()
                .statusCode(201)
                .extract()
                .path("exportBatchId");
    }

    @Test
    @Order(1)
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "lane@certifyos.com")})
    void runNowToDeliveredThroughTheCompletionEvent() {
        String correlationId = batchIdFor("candor") + "-r1";
        stubExportAccepted(correlationId);
        stubStatusCompleted(correlationId);
        String batchId = runNow("candor");

        ExportBatch requested = awaitState(batchId, BatchState.EGRESS_REQUESTED);

        String day = LocalDate.now(ZoneId.of("UTC")).toString().replace("-", "");
        String objectName = "from/" + TENANT + "/" + TENANT + "_" + batchId + "_" + day + ".csv";
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withHeader("Authorization", WireMock.equalTo("Bearer egress-token"))
                        .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(TENANT))
                        .withRequestBody(WireMock.equalToJson(
                                "{\"type\":\"practitioner\",\"tenantId\":\"" + TENANT + "\",\"correlationId\":\""
                                        + correlationId + "\","
                                        + "\"templateId\":\"tpl-lane\",\"mappingsCsvUrl\":\"gs://egress-templates/"
                                        + TENANT + "/tpl-lane/v2/mappings.csv\","
                                        + "\"separator\":\";\",\"outputFormat\":\"csv\",\"rowExpansionKeys\":\"[\\\"locations\\\"]\","
                                        + "\"selection\":{\"source\":\"SAVED_FILTER\",\"filterId\":\"" + FILTER_ID
                                        + "\",\"listUsed\":true},\"userId\":\"directory-accuracy-worker\","
                                        + "\"destination\":{\"bucket\":\"test-vendor-bucket\",\"objectName\":\""
                                        + objectName + "\"}}",
                                true,
                                false)));
        Assertions.assertEquals(FILTER_ID, requested.selection().filterId());
        Assertions.assertFalse(requested.selection().hasCounts(), "egress has not reported yet");
        Assertions.assertEquals(2, requested.egress().templateVersion());
        Assertions.assertEquals(
                StateName.SCHEDULED,
                storage.getJobById(java.util.UUID.fromString(requested.egress().deadlineJobId()))
                        .getState());

        fileAt(requested, correlationId);
        pushEvent("lane-m-1", correlationId, "COMPLETED");
        ExportBatch delivered = awaitState(batchId, BatchState.DELIVERED);

        // The batch document against the design's sample after delivery.
        Assertions.assertEquals(1, delivered.attempt());
        Assertions.assertEquals(1, delivered.seq());
        Assertions.assertEquals(correlationId, delivered.egress().correlationId());
        Assertions.assertEquals(
                "practitioner:" + TENANT + ":" + correlationId,
                delivered.egress().jobReference());
        Assertions.assertEquals("tpl-lane", delivered.egress().templateId());
        Assertions.assertEquals(List.of("locations"), delivered.egress().rowExpansionKeys());
        Assertions.assertEquals(
                new EgressDetails.Destination("test-vendor-bucket", objectName),
                delivered.egress().destination());
        Assertions.assertEquals(
                EgressDetails.CompletionSource.EVENT, delivered.egress().completionSource());
        Assertions.assertEquals(correlationId, delivered.egress().fileProducedBy());
        Assertions.assertEquals(
                TENANT + "_" + batchId + "_" + day + ".csv", delivered.file().name());
        Assertions.assertEquals(
                "gs://test-vendor-bucket/" + objectName, delivered.file().path());
        Assertions.assertEquals(4L, delivered.file().rowCount());
        Assertions.assertEquals(4096L, delivered.file().bytes());
        Assertions.assertEquals("certify-export-v1", delivered.file().schemaVersion());
        Assertions.assertTrue(delivered.reconciliation().match());
        Assertions.assertEquals(3, delivered.reconciliation().selected());
        Assertions.assertEquals(3, delivered.reconciliation().inFile());
        Assertions.assertEquals(3L, delivered.selection().distinct());
        Assertions.assertEquals(
                "gs://egress-list-used/lane.csv", delivered.selection().listUsedUri());
        Assertions.assertNotNull(delivered.deliveredAt());
        Assertions.assertEquals(
                StateName.DELETED,
                storage.getJobById(java.util.UUID.fromString(delivered.egress().deadlineJobId()))
                        .getState());
        Assertions.assertEquals(
                batchId, schedules.find(TENANT, "candor").orElseThrow().lastBatchId());

        Assertions.assertEquals(
                List.of(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        AuditEventType.EXPORT_EGRESS_REQUESTED,
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        AuditEventType.EXPORT_EGRESS_COMPLETED,
                        AuditEventType.EXPORT_RECONCILED,
                        AuditEventType.EXPORT_BATCH_DELIVERED),
                trail(batchId));

        member().get(BATCHES + batchId)
                .then()
                .statusCode(200)
                .body("state", Matchers.equalTo("DELIVERED"))
                .body("reconciliation.match", Matchers.equalTo(true))
                .body("selection.filterId", Matchers.equalTo(FILTER_ID))
                .body("selection.distinct", Matchers.equalTo(3))
                .body("egress", Matchers.not(Matchers.hasKey("deadlineJobId")));
        member().get(BATCHES + batchId + "/events")
                .then()
                .statusCode(200)
                .body("items.size()", Matchers.equalTo(6))
                .body("items[0].type", Matchers.equalTo("EXPORT_BATCH_SCHEDULED"))
                .body("items[0].actor", Matchers.equalTo("lane@certifyos.com"))
                .body("items[0].detail.trigger", Matchers.equalTo("MANUAL"))
                .body("items[5].type", Matchers.equalTo("EXPORT_BATCH_DELIVERED"))
                .body(
                        "items[5].detail.fileName",
                        Matchers.equalTo(delivered.file().name()));
        member().get(SCHEDULES + "candor/events")
                .then()
                .statusCode(200)
                .body("items.type", Matchers.hasItems("SCHEDULE_RUN_NOW", "SCHEDULE_CREATED"))
                .body("items.type", Matchers.not(Matchers.hasItem("EXPORT_BATCH_SCHEDULED")));
    }

    @Test
    @Order(2)
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "lane@certifyos.com")})
    void withTheEventWithheldTheDeadlineCheckDelivers() {
        String correlationId = batchIdFor("lane-deadline") + "-r1";
        stubExportAccepted(correlationId);
        String batchId = runNow("lane-deadline");
        ExportBatch requested = awaitState(batchId, BatchState.EGRESS_REQUESTED);
        stubStatusCompleted(correlationId);
        fileAt(requested, correlationId);

        // The real check is scheduled six hours out; running its body now is the same code path.
        deadlineCheck.run(new DeadlineCheckJobRequest(batchId, 1, 1));

        ExportBatch delivered = awaitState(batchId, BatchState.DELIVERED);
        Assertions.assertEquals(
                EgressDetails.CompletionSource.DEADLINE, delivered.egress().completionSource());
        Assertions.assertTrue(delivered.reconciliation().match());
        Assertions.assertEquals(
                List.of(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        AuditEventType.EXPORT_EGRESS_REQUESTED,
                        AuditEventType.EXPORT_EVENT_MISSED,
                        AuditEventType.EXPORT_EGRESS_COMPLETED,
                        AuditEventType.EXPORT_RECONCILED,
                        AuditEventType.EXPORT_BATCH_DELIVERED),
                trail(batchId));
    }

    @Test
    @Order(3)
    @TestSecurity(user = "ops")
    @JwtSecurity(claims = {@Claim(key = EMAIL, value = "lane@certifyos.com")})
    void aFailedFirstAttemptIsRetriedUnderR2WithThePriorCancelledAndItsListReused() {
        String first = batchIdFor("lane-retry") + "-r1";
        String second = batchIdFor("lane-retry") + "-r2";
        stubExportAccepted(first);
        stubExportAccepted(second);
        stubStatusCompleted(second);
        WireMockUpstreams.stubFor(WireMock.post(
                        WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/" + TENANT + "/" + first + "/cancel"))
                .willReturn(WireMock.jsonResponse("{\"status\":\"cancellation_requested\"}", 202)));
        String batchId = runNow("lane-retry");
        awaitState(batchId, BatchState.EGRESS_REQUESTED);

        pushEvent("lane-m-fail", first, "FAILED");
        ExportBatch failed = awaitState(batchId, BatchState.FAILED);
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, failed.failedStep());
        Assertions.assertEquals("dataflow died", failed.lastError());

        member().body("{\"reason\":\"egress fixed the pipeline\"}")
                .post(BATCHES + batchId + "/retry")
                .then()
                .statusCode(202)
                .body("attempt", Matchers.equalTo(2));
        Awaitility.await().atMost(HOP).pollInterval(Duration.ofMillis(500)).until(() -> batches.find(batchId)
                .map(batch -> batch.state() == BatchState.EGRESS_REQUESTED
                        && second.equals(batch.egress().correlationId()))
                .orElse(false));
        ExportBatch requestedAgain = batches.find(batchId).orElseThrow();
        Assertions.assertEquals(2, requestedAgain.attempt());
        Assertions.assertEquals("tpl-lane", requestedAgain.egress().templateId(), "the pin from attempt 1 is kept");
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(
                                "/api/v1/egress/jobs/practitioner/" + TENANT + "/" + first + "/cancel"))
                        .withHeader("x-user-id", WireMock.equalTo("directory-accuracy-worker")));
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withRequestBody(WireMock.matchingJsonPath("$.correlationId", WireMock.equalTo(second)))
                        .withRequestBody(
                                WireMock.matchingJsonPath("$.selection.reuseSelectionFrom", WireMock.equalTo(first))));
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo(TEMPLATES + "/tpl-lane"))
                        .withHeader("tenant-id", WireMock.equalTo(TENANT)));

        fileAt(requestedAgain, second);
        pushEvent("lane-m-done", second, "COMPLETED");
        ExportBatch delivered = awaitState(batchId, BatchState.DELIVERED);
        Assertions.assertEquals(2, delivered.attempt());
        Assertions.assertEquals(second, delivered.egress().fileProducedBy());
        Assertions.assertEquals(
                List.of(
                        AuditEventType.EXPORT_BATCH_SCHEDULED,
                        AuditEventType.EXPORT_EGRESS_REQUESTED,
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        AuditEventType.EXPORT_BATCH_FAILED,
                        AuditEventType.EXPORT_RETRY_REQUESTED,
                        AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED,
                        AuditEventType.EXPORT_EGRESS_REQUESTED,
                        AuditEventType.EXPORT_EVENT_RECEIVED,
                        AuditEventType.EXPORT_EGRESS_COMPLETED,
                        AuditEventType.EXPORT_RECONCILED,
                        AuditEventType.EXPORT_BATCH_DELIVERED),
                trail(batchId));
    }

    @Test
    @Order(4)
    void theMetricsEndpointReflectsTheThreeScenarios() {
        // The design's names as Prometheus renders them, with the labels and the values the three
        // scenarios produced: three batches, two completions by event and one by the deadline path,
        // one egress failure, no count mismatch. Tags render in
        // alphabetical order. The four names the lane did not touch here (event.rejected,
        // reconcile.mismatch, jobs.retries, jobs.final_failures) are covered by MetricsEndpointIT.
        String body =
                RestAssured.get("/q/metrics").then().statusCode(200).extract().asString();
        for (String expected : List.of(
                "vendor_export_tick_duration_seconds",
                "vendor_export_tick_schedules_due",
                "vendor_export_schedules_by_state{state=\"ENABLED\"}",
                "vendor_export_batches_by_state{state=\"DELIVERED\"}",
                "vendor_export_batches_created_total{tenant=\"" + TENANT + "\",vendor=\"candor\"} 1.0",
                "vendor_export_batches_created_total{tenant=\"" + TENANT + "\",vendor=\"lane-deadline\"} 1.0",
                "vendor_export_batches_created_total{tenant=\"" + TENANT + "\",vendor=\"lane-retry\"} 1.0",
                "vendor_export_egress_wait_seconds{tenant=\"" + TENANT + "\"}",
                "vendor_export_egress_failed_total{reason=\"EGRESS_FAILED\",tenant=\"" + TENANT + "\"} 1.0",
                "vendor_export_completion_source_total{source=\"EVENT\"} 2.0",
                "vendor_export_completion_source_total{source=\"DEADLINE\"} 1.0")) {
            Assertions.assertTrue(body.contains(expected), "missing or wrong: " + expected);
        }
        Assertions.assertFalse(
                body.contains("vendor_export_reconcile_mismatch_total{tenant=\"" + TENANT + "\"}"), "no mismatch ran");
        Assertions.assertFalse(body.contains("npi=\""), "no label named npi");
    }
}
