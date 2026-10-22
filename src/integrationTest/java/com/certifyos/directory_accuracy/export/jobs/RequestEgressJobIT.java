package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.clients.ApiLayerTokenService;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.schedule.Cadence;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.bson.Document;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/** The egress request body, run directly on the api role against WireMock egress and api-layer. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class RequestEgressJobIT {

    static final String FILTER_ID = "flt-direct";
    static final YearMonth PERIOD = YearMonth.of(2026, 10);
    static final ZoneId NEW_YORK = ZoneId.of("America/New_York");
    static final String EXPORT = "/api/v1/egress/export";
    static final String TEMPLATE_JSON =
            "{\"id\":\"tpl-1\",\"tenantId\":\"%s\",\"templateName\":\"directory-accuracy candor certify-export-v1\","
                    + "\"entityType\":\"practitioner\",\"status\":\"active\",\"version\":3,"
                    + "\"mappingsCsvUrl\":\"gs://egress-templates/%s/tpl-1/v3/mappings.csv\",\"separator\":\";\","
                    + "\"outputFormat\":\"csv\",\"rowExpansionKeys\":[\"locations\"]}";
    static final String ACCEPTED = "{\"type\":\"practitioner\",\"tenant_id\":\"%s\",\"correlation_id\":\"%s\","
            + "\"table\":\"practitioner:%s:%s\",\"ttl_days\":7,\"state\":\"PENDING\"}";

    @Inject
    RequestEgressJob job;

    @Inject
    ExportBatchRepository batches;

    @Inject
    ScheduleRepository schedules;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    StorageProvider storage;

    @InjectMock
    ApiLayerTokenService tokens;

    @InjectMock
    GoogleIdTokenService google;

    @InjectMock
    VendorBucket bucket;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
        Mockito.when(google.idToken("test-egress-iap-client-id")).thenReturn("egress-token");
        Mockito.when(bucket.head(ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(Optional.empty());
        WireMockUpstreams.resetRequests();
    }

    private ExportBatch scheduled(String tenant, int attempt, EgressDetails priorEgress) {
        Schedule schedule = Schedule.create(
                tenant,
                "candor",
                Cadence.monthly(1),
                NEW_YORK,
                FILTER_ID,
                "tpl-1",
                Instant.now().plusSeconds(86400),
                "user:ops",
                Instant.now());
        ExportBatch batch = ExportBatch.scheduled(tenant, "candor", PERIOD, 1, FILTER_ID, Instant.now());
        transactions.run(session -> {
            schedules.insert(session, schedule);
            batches.insert(session, batch);
            Document egress = priorEgress == null ? null : priorEgress.toDocument();
            batches.updateInState(
                    session,
                    batch.id(),
                    BatchState.SCHEDULED,
                    Instant.now(),
                    egress == null
                            ? Updates.set("attempt", attempt)
                            : Updates.combine(Updates.set("attempt", attempt), Updates.set("egress", egress)));
            return null;
        });
        return batches.find(batch.id()).orElseThrow();
    }

    private static EgressDetails pinned(String tenant, String batchId, String correlationId) {
        return new EgressDetails(
                correlationId,
                "practitioner:" + tenant + ":" + correlationId,
                "tpl-1",
                3,
                "gs://egress-templates/" + tenant + "/tpl-1/v3/mappings.csv",
                ";",
                "csv",
                List.of("locations"),
                new EgressDetails.Destination(
                        "test-vendor-bucket", "from/" + tenant + "/" + tenant + "_" + batchId + "_20261001.csv"),
                Instant.parse("2026-10-01T06:00:00Z"),
                null,
                null,
                null,
                null);
    }

    private static void stubTemplate(String tenant) {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlPathEqualTo("/api/v1/egress-templates/tpl-1"))
                .withHeader("tenant-id", WireMock.equalTo(tenant))
                .willReturn(WireMock.okJson(String.format(TEMPLATE_JSON, tenant, tenant))));
    }

    private static void stubExportAccepted(String tenant, String correlationId) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .withRequestBody(WireMock.matchingJsonPath("$.correlationId", WireMock.equalTo(correlationId)))
                .willReturn(WireMock.jsonResponse(
                        String.format(ACCEPTED, tenant, correlationId, tenant, correlationId), 202)));
    }

    /** The first attempt's selection: the saved filter, list-used file on. */
    static final String BY_FILTER =
            "{\"source\":\"SAVED_FILTER\",\"filterId\":\"" + FILTER_ID + "\",\"listUsed\":true}";

    /** A retry's selection: the prior attempt's frozen list, list-used file on. */
    private static String reusing(String priorCorrelationId) {
        return "{\"listUsed\":true,\"reuseSelectionFrom\":\"" + priorCorrelationId + "\"}";
    }

    private static String expectedBody(String tenant, String correlationId, String objectName, String selection) {
        return "{\"type\":\"practitioner\",\"tenantId\":\"" + tenant + "\",\"correlationId\":\"" + correlationId + "\","
                + "\"templateId\":\"tpl-1\",\"mappingsCsvUrl\":\"gs://egress-templates/" + tenant
                + "/tpl-1/v3/mappings.csv\","
                + "\"separator\":\";\",\"outputFormat\":\"csv\",\"rowExpansionKeys\":\"[\\\"locations\\\"]\","
                + "\"selection\":" + selection + ",\"userId\":\"directory-accuracy-worker\","
                + "\"destination\":{\"bucket\":\"test-vendor-bucket\",\"objectName\":\"" + objectName + "\"}}";
    }

    private static void stubCancelAccepted(String tenant, String priorCorrelationId) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(
                        "/api/v1/egress/jobs/practitioner/" + tenant + "/" + priorCorrelationId + "/cancel"))
                .willReturn(WireMock.jsonResponse("{\"status\":\"cancellation_requested\"}", 202)));
    }

    private static void stubExportRefused(String tenant, int status, String error, String message) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse(
                        "{\"error\":\"" + error + "\",\"message\":\"" + message + "\",\"details\":null,"
                                + "\"retryable\":false}",
                        status)));
    }

    private List<AuditEventType> auditTypes(ExportBatch batch) {
        return audit.findForBatch(batch.id(), 20).stream().map(AuditEvent::type).toList();
    }

    @Test
    void firstAttemptPinsRequestsSchedulesTheDeadlineAndAudits() {
        String tenant = "req-first";
        ExportBatch batch = scheduled(tenant, 1, null);
        String correlationId = batch.id() + "-r1";
        stubTemplate(tenant);
        stubExportAccepted(tenant, correlationId);
        Instant before = Instant.now();
        String today = LocalDate.now(NEW_YORK).toString().replace("-", "");
        String objectName = "from/" + tenant + "/" + tenant + "_" + batch.id() + "_" + today + ".csv";

        job.run(new RequestEgressJobRequest(batch.id(), 1, 0));

        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withHeader("Authorization", WireMock.equalTo("Bearer egress-token"))
                        .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                        .withRequestBody(WireMock.equalToJson(
                                expectedBody(tenant, correlationId, objectName, BY_FILTER), true, false)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(correlationId, after.egress().correlationId());
        Assertions.assertEquals(
                "practitioner:" + tenant + ":" + correlationId, after.egress().jobReference());
        Assertions.assertEquals("tpl-1", after.egress().templateId());
        Assertions.assertEquals(3, after.egress().templateVersion());
        Assertions.assertEquals(";", after.egress().separator());
        Assertions.assertEquals(List.of("locations"), after.egress().rowExpansionKeys());
        Assertions.assertEquals(
                new EgressDetails.Destination("test-vendor-bucket", objectName),
                after.egress().destination());
        Assertions.assertEquals(
                tenant + "_" + batch.id() + "_" + today + ".csv", after.file().name());
        Assertions.assertEquals(
                "gs://test-vendor-bucket/" + objectName, after.file().path());
        Assertions.assertNotNull(after.egress().requestedAt());

        UUID deadlineId = JobIds.deadline(batch.id(), 1, 1);
        Assertions.assertEquals(deadlineId.toString(), after.egress().deadlineJobId());
        org.jobrunr.jobs.Job deadline = storage.getJobById(deadlineId);
        Assertions.assertEquals(StateName.SCHEDULED, deadline.getState());
        Instant scheduledAt = ((ScheduledState) deadline.getJobState()).getScheduledAt();
        Duration fromNow =
                Duration.between(before.plus(Duration.ofHours(6)), scheduledAt).abs();
        Assertions.assertTrue(
                fromNow.compareTo(Duration.ofMinutes(2)) < 0, "deadline at now + 6 h, was " + scheduledAt);

        AuditEvent event = audit.findForBatch(batch.id(), 10).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EGRESS_REQUESTED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(correlationId, event.detail().get("egressCorrelationId"));
        Assertions.assertEquals("tpl-1", event.detail().get("templateId"));
        Assertions.assertEquals(FILTER_ID, event.detail().get("filterId"));
        Assertions.assertFalse(event.detail().containsKey("reuseSelectionFrom"), "a first attempt reuses nothing");
        Assertions.assertNull(after.selection().reEvaluated());
        Assertions.assertNotNull(event.detail().get("deadlineAt"));
    }

    @Test
    void secondAttemptCancelsThePriorCorrelationFirstKeepsThePinAndReusesItsList() {
        String tenant = "req-retry";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        stubCancelAccepted(tenant, batchId + "-r1");
        stubExportAccepted(tenant, batchId + "-r2");

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlPathMatching("/api/v1/egress/jobs/practitioner/.*/cancel"))
                        .withHeader("x-user-id", WireMock.equalTo("directory-accuracy-worker")));
        WireMockUpstreams.verify(0, WireMock.getRequestedFor(WireMock.urlPathMatching("/api/v1/egress-templates/.*")));
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withRequestBody(WireMock.equalToJson(
                                expectedBody(
                                        tenant,
                                        batchId + "-r2",
                                        "from/" + tenant + "/" + tenant + "_" + batchId + "_20261001.csv",
                                        reusing(batchId + "-r1")),
                                true,
                                false)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(batchId + "-r2", after.egress().correlationId());
        Assertions.assertEquals(
                JobIds.deadline(batchId, 2, 1).toString(), after.egress().deadlineJobId());
        Assertions.assertNull(after.selection().reEvaluated(), "the list was reused");
        List<AuditEventType> types = auditTypes(batch);
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED), types.toString());
        AuditEvent requested = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EGRESS_REQUESTED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(batchId + "-r1", requested.detail().get("reuseSelectionFrom"));
    }

    @Test
    void aNewAttemptClearsTheEarlierAttemptsSelectionCounts() {
        String tenant = "req-stale";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        transactions.run(session -> batches.updateInState(
                session,
                batch.id(),
                BatchState.SCHEDULED,
                Instant.now(),
                Updates.combine(
                        Updates.set("selection.distinct", 1240L),
                        Updates.set("selection.listUsedUri", "gs://egress-list-used/r1.csv"))));
        stubCancelAccepted(tenant, batchId + "-r1");
        stubExportAccepted(tenant, batchId + "-r2");

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertNull(after.selection().distinct(), "attempt 1's count never stands in for attempt 2's");
        Assertions.assertNull(after.selection().listUsedUri());
        Assertions.assertEquals(FILTER_ID, after.selection().filterId());
    }

    @Test
    void aConflictOnAReuseAsksEgressWhichSelectionItHolds() {
        String tenant = "req-held";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        stubCancelAccepted(tenant, batchId + "-r1");
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse("{\"error\":\"job exists\"}", 409)));
        WireMockUpstreams.stubFor(WireMock.get(
                        WireMock.urlEqualTo("/api/v1/egress/status/practitioner/" + tenant + "/" + batchId + "-r2"))
                .willReturn(WireMock.okJson("{\"state\":\"RUNNING\",\"phase\":\"SEEDING\","
                        + "\"selection\":{\"source\":\"SAVED_FILTER\",\"state\":\"RESOLVING\",\"reusedFrom\":null}}")));

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(
                Boolean.TRUE, after.selection().reEvaluated(), "the earlier run's filter request is what egress holds");
        AuditEvent requested = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EGRESS_REQUESTED)
                .findFirst()
                .orElseThrow();
        Assertions.assertFalse(requested.detail().containsKey("reuseSelectionFrom"));
    }

    @Test
    void aConflictOnAReuseEgressHeldAsAReuseStaysAReuse() {
        String tenant = "req-held-reuse";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        stubCancelAccepted(tenant, batchId + "-r1");
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse("{\"error\":\"job exists\"}", 409)));
        WireMockUpstreams.stubFor(WireMock.get(
                        WireMock.urlEqualTo("/api/v1/egress/status/practitioner/" + tenant + "/" + batchId + "-r2"))
                .willReturn(WireMock.okJson("{\"state\":\"RUNNING\",\"phase\":\"SEEDING\","
                        + "\"selection\":{\"source\":\"REUSE\",\"state\":\"RESOLVED\",\"reusedFrom\":\""
                        + batchId + "-r1\"}}")));

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertNull(after.selection().reEvaluated());
    }

    @Test
    void aReuseEgressCannotServeIsSentAgainWithTheFilter() {
        String tenant = "req-reuse-gone";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        stubCancelAccepted(tenant, batchId + "-r1");
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .withRequestBody(
                        WireMock.matchingJsonPath("$.selection.reuseSelectionFrom", WireMock.equalTo(batchId + "-r1")))
                .willReturn(WireMock.jsonResponse(
                        "{\"error\":\"INVALID_REQUEST\",\"message\":\"selection.reuse_selection_from names a "
                                + "selection whose list has expired\",\"details\":null,\"retryable\":false}",
                        400)));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .withRequestBody(WireMock.matchingJsonPath("$.selection.filterId", WireMock.equalTo(FILTER_ID)))
                .willReturn(WireMock.jsonResponse(
                        String.format(ACCEPTED, tenant, batchId + "-r2", tenant, batchId + "-r2"), 202)));

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        WireMockUpstreams.verify(2, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withRequestBody(WireMock.equalToJson(
                                expectedBody(
                                        tenant,
                                        batchId + "-r2",
                                        "from/" + tenant + "/" + tenant + "_" + batchId + "_20261001.csv",
                                        BY_FILTER),
                                true,
                                false)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(Boolean.TRUE, after.selection().reEvaluated());
    }

    @Test
    void aRequestFormErrorOnAReuseFailsTheBatchInsteadOfEvaluatingAgain() {
        String tenant = "req-reuse-form";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        stubCancelAccepted(tenant, batchId + "-r1");
        stubExportRefused(
                tenant, 400, "INVALID_REQUEST", "selection.reuse_selection_from must be 255 characters or less");

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        WireMockUpstreams.verify(1, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, after.failedStep());
        Assertions.assertTrue(after.lastError().startsWith("400 INVALID_REQUEST: "), after.lastError());
    }

    @Test
    void aDisabledSelectionSourceFailsTheBatchAtOnce() {
        String tenant = "req-disabled";
        ExportBatch batch = scheduled(tenant, 1, null);
        stubTemplate(tenant);
        stubExportRefused(
                tenant, 403, "SELECTION_SOURCE_DISABLED", "selection by saved filter is disabled in this environment");

        job.run(new RequestEgressJobRequest(batch.id(), 1, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, after.failedStep());
        Assertions.assertTrue(after.lastError().startsWith("403 SELECTION_SOURCE_DISABLED"), after.lastError());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EGRESS_REFUSED", failed.detail().get("cause"));
    }

    @Test
    void aQuotaRefusalSchedulesANewRequestAfterRetryAfter() {
        String tenant = "req-quota";
        ExportBatch batch = scheduled(tenant, 1, null);
        stubTemplate(tenant);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse(
                                "{\"error\":\"SELECTION_QUOTA_EXCEEDED\",\"message\":\"busy\",\"details\":null,"
                                        + "\"retryable\":true}",
                                429)
                        .withHeader("Retry-After", "300")));
        Instant before = Instant.now();

        job.run(new RequestEgressJobRequest(batch.id(), 1, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, after.state(), "waiting is not a failure");
        org.jobrunr.jobs.Job wait = storage.getJobById(JobIds.quotaWait(batch.id(), 1, 1));
        Assertions.assertEquals(StateName.SCHEDULED, wait.getState());
        Instant scheduledAt = ((ScheduledState) wait.getJobState()).getScheduledAt();
        Assertions.assertTrue(
                Duration.between(before.plusSeconds(300), scheduledAt).abs().compareTo(Duration.ofMinutes(1)) < 0,
                "asked again after Retry-After, was " + scheduledAt);
    }

    @Test
    void aQuotaRefusalAfterTheLastWaitIsLeftToJobRunr() {
        String tenant = "req-quota-out";
        ExportBatch batch = scheduled(tenant, 1, null);
        stubTemplate(tenant);
        stubExportRefused(tenant, 429, "SELECTION_QUOTA_EXCEEDED", "busy");

        WebApplicationException failure = Assertions.assertThrows(
                WebApplicationException.class,
                () -> job.run(new RequestEgressJobRequest(batch.id(), 1, RequestEgressJob.MAX_QUOTA_WAITS)));

        Assertions.assertEquals(429, failure.getResponse().getStatus());
        Assertions.assertEquals(
                BatchState.SCHEDULED, batches.find(batch.id()).orElseThrow().state());
    }

    @Test
    void aRefusedCancelWithACompleteFileFinishesWithoutAskingEgressAgain() {
        String tenant = "req-done";
        String batchId = tenant + "-candor-2026-10-001";
        EgressDetails prior = pinned(tenant, batchId, batchId + "-r1");
        ExportBatch batch = scheduled(tenant, 2, prior);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(
                        "/api/v1/egress/jobs/practitioner/" + tenant + "/" + batchId + "-r1/cancel"))
                .willReturn(WireMock.jsonResponse("{\"error\":\"terminal\"}", 409)));
        Mockito.when(bucket.head("test-vendor-bucket", prior.destination().objectName()))
                .thenReturn(Optional.of(new VendorBucket.ObjectInfo(
                        "test-vendor-bucket",
                        prior.destination().objectName(),
                        4096,
                        Map.of("complete", "true", "correlationId", batchId + "-r1"),
                        "md5-" + "x")));

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        WireMockUpstreams.verify(0, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_COMPLETED, after.state());
        Assertions.assertEquals(
                EgressDetails.CompletionSource.PRIOR_ATTEMPT, after.egress().completionSource());
        Assertions.assertEquals(batchId + "-r1", after.egress().fileProducedBy());
        Assertions.assertEquals(
                StateName.ENQUEUED,
                storage.getJobById(JobIds.of(FinishJob.NAME, batchId, 2)).getState());
        List<AuditEventType> types = auditTypes(batch);
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED), types.toString());
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_EGRESS_COMPLETED), types.toString());
    }

    @Test
    void aRefusedCancelWithoutAFileRequestsAgain() {
        String tenant = "req-nofile";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = scheduled(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(
                        "/api/v1/egress/jobs/practitioner/" + tenant + "/" + batchId + "-r1/cancel"))
                .willReturn(WireMock.jsonResponse("{\"error\":\"terminal\"}", 409)));
        stubExportAccepted(tenant, batchId + "-r2");

        job.run(new RequestEgressJobRequest(batch.id(), 2, 0));

        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
        WireMockUpstreams.verify(1, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
    }

    @Test
    void aConflictOnExportIsSuccess() {
        String tenant = "req-conflict";
        ExportBatch batch = scheduled(tenant, 1, null);
        stubTemplate(tenant);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse("{\"error\":\"job exists\"}", 409)));

        job.run(new RequestEgressJobRequest(batch.id(), 1, 0));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(
                batch.id() + "-r1", after.egress().jobReference(), "no body to read, the correlation id stands in");
        Assertions.assertEquals(
                StateName.SCHEDULED,
                storage.getJobById(JobIds.deadline(batch.id(), 1, 1)).getState());
    }

    @Test
    void thePinIsWrittenBeforeTheExportCallAndNotRepeatedOnRetry() {
        String tenant = "req-pin";
        ExportBatch batch = scheduled(tenant, 1, null);
        stubTemplate(tenant);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .inScenario("req-pin")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("up")
                .willReturn(WireMock.serviceUnavailable()));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .inScenario("req-pin")
                .whenScenarioStateIs("up")
                .willReturn(WireMock.jsonResponse(
                        String.format(ACCEPTED, tenant, batch.id() + "-r1", tenant, batch.id() + "-r1"), 202)));

        WebApplicationException failure = Assertions.assertThrows(
                WebApplicationException.class, () -> job.run(new RequestEgressJobRequest(batch.id(), 1, 0)));
        Assertions.assertEquals(503, failure.getResponse().getStatus(), "egress errors propagate so JobRunr retries");
        ExportBatch afterFailure = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, afterFailure.state());
        Assertions.assertEquals("tpl-1", afterFailure.egress().templateId(), "pinned before the call");
        Assertions.assertNull(afterFailure.egress().correlationId(), "no request accepted yet");

        job.run(new RequestEgressJobRequest(batch.id(), 1, 0));

        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/api/v1/egress-templates/tpl-1"))
                        .withHeader("tenant-id", WireMock.equalTo(tenant)));
        WireMockUpstreams.verify(
                2,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant)));
        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
    }
}
