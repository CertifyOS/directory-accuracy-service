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
import com.certifyos.directory_accuracy.export.batch.ExportNpi;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.schedule.Cadence;
import com.certifyos.directory_accuracy.export.schedule.Schedule;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
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

    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));
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
    ExportNpiRepository npis;

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

    private ExportBatch selected(String tenant, int attempt, EgressDetails priorEgress) {
        Schedule schedule = Schedule.create(
                tenant,
                "candor",
                Cadence.monthly(1),
                NEW_YORK,
                CRITERIA,
                "tpl-1",
                Instant.now().plusSeconds(86400),
                "user:ops",
                Instant.now());
        ExportBatch batch = ExportBatch.scheduled(tenant, "candor", PERIOD, 1, CRITERIA, Instant.now());
        transactions.run(session -> {
            schedules.insert(session, schedule);
            batches.insert(session, batch);
            npis.upsertAll(
                    session,
                    List.of(
                            ExportNpi.of(batch.id(), tenant, "2345678918", "p-2", Instant.now()),
                            ExportNpi.of(batch.id(), tenant, "1234567893", "p-1", Instant.now())));
            batches.transition(
                    session, batch.id(), BatchState.SCHEDULED, BatchState.NPIS_SELECTED, Instant.now(), null);
            Document egress = priorEgress == null ? null : priorEgress.toDocument();
            batches.updateInState(
                    session,
                    batch.id(),
                    BatchState.NPIS_SELECTED,
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

    private static String expectedBody(String tenant, String batchId, String correlationId, String objectName) {
        return "{\"type\":\"practitioner\",\"tenantId\":\"" + tenant + "\",\"correlationId\":\"" + correlationId + "\","
                + "\"templateId\":\"tpl-1\",\"mappingsCsvUrl\":\"gs://egress-templates/" + tenant
                + "/tpl-1/v3/mappings.csv\","
                + "\"separator\":\";\",\"outputFormat\":\"csv\",\"rowExpansionKeys\":\"[\\\"locations\\\"]\","
                + "\"npiFilter\":[\"1234567893\",\"2345678918\"],\"userId\":\"directory-accuracy-worker\","
                + "\"destination\":{\"bucket\":\"test-vendor-bucket\",\"objectName\":\"" + objectName + "\"}}";
    }

    @Test
    void firstAttemptPinsRequestsSchedulesTheDeadlineAndAudits() {
        String tenant = "req-first";
        ExportBatch batch = selected(tenant, 1, null);
        String correlationId = batch.id() + "-r1";
        stubTemplate(tenant);
        stubExportAccepted(tenant, correlationId);
        Instant before = Instant.now();
        String today = LocalDate.now(NEW_YORK).toString().replace("-", "");
        String objectName = "from/" + tenant + "/" + tenant + "_" + batch.id() + "_" + today + ".csv";

        job.run(new RequestEgressJobRequest(batch.id(), 1));

        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT))
                        .withHeader("Authorization", WireMock.equalTo("Bearer egress-token"))
                        .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                        .withRequestBody(WireMock.equalToJson(
                                expectedBody(tenant, batch.id(), correlationId, objectName), true, false)));
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
        Assertions.assertEquals(2, event.detail().get("npiCount"));
        Assertions.assertNotNull(event.detail().get("deadlineAt"));
    }

    @Test
    void secondAttemptCancelsThePriorCorrelationFirstAndKeepsThePin() {
        String tenant = "req-retry";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = selected(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(
                        "/api/v1/egress/jobs/practitioner/" + tenant + "/" + batchId + "-r1/cancel"))
                .willReturn(WireMock.jsonResponse("{\"status\":\"cancellation_requested\"}", 202)));
        stubExportAccepted(tenant, batchId + "-r2");

        job.run(new RequestEgressJobRequest(batch.id(), 2));

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
                                        batchId,
                                        batchId + "-r2",
                                        "from/" + tenant + "/" + tenant + "_" + batchId + "_20261001.csv"),
                                true,
                                false)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        Assertions.assertEquals(batchId + "-r2", after.egress().correlationId());
        Assertions.assertEquals(
                JobIds.deadline(batchId, 2, 1).toString(), after.egress().deadlineJobId());
        List<AuditEventType> types = audit.findForBatch(batch.id(), 10).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED), types.toString());
    }

    @Test
    void aRefusedCancelWithACompleteFileFinishesWithoutAskingEgressAgain() {
        String tenant = "req-done";
        String batchId = tenant + "-candor-2026-10-001";
        EgressDetails prior = pinned(tenant, batchId, batchId + "-r1");
        ExportBatch batch = selected(tenant, 2, prior);
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

        job.run(new RequestEgressJobRequest(batch.id(), 2));

        WireMockUpstreams.verify(0, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_COMPLETED, after.state());
        Assertions.assertEquals(
                EgressDetails.CompletionSource.PRIOR_ATTEMPT, after.egress().completionSource());
        Assertions.assertEquals(batchId + "-r1", after.egress().fileProducedBy());
        Assertions.assertEquals(
                StateName.ENQUEUED,
                storage.getJobById(JobIds.of(FinishJob.NAME, batchId, 2)).getState());
        List<AuditEventType> types = audit.findForBatch(batch.id(), 10).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED), types.toString());
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_EGRESS_COMPLETED), types.toString());
    }

    @Test
    void aRefusedCancelWithoutAFileRequestsAgain() {
        String tenant = "req-nofile";
        String batchId = tenant + "-candor-2026-10-001";
        ExportBatch batch = selected(tenant, 2, pinned(tenant, batchId, batchId + "-r1"));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(
                        "/api/v1/egress/jobs/practitioner/" + tenant + "/" + batchId + "-r1/cancel"))
                .willReturn(WireMock.jsonResponse("{\"error\":\"terminal\"}", 409)));
        stubExportAccepted(tenant, batchId + "-r2");

        job.run(new RequestEgressJobRequest(batch.id(), 2));

        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
        WireMockUpstreams.verify(1, WireMock.postRequestedFor(WireMock.urlEqualTo(EXPORT)));
    }

    @Test
    void aConflictOnExportIsSuccess() {
        String tenant = "req-conflict";
        ExportBatch batch = selected(tenant, 1, null);
        stubTemplate(tenant);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(EXPORT))
                .withHeader("X-Forwarding-Tenant-Id", WireMock.equalTo(tenant))
                .willReturn(WireMock.jsonResponse("{\"error\":\"job exists\"}", 409)));

        job.run(new RequestEgressJobRequest(batch.id(), 1));

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
        ExportBatch batch = selected(tenant, 1, null);
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
                WebApplicationException.class, () -> job.run(new RequestEgressJobRequest(batch.id(), 1)));
        Assertions.assertEquals(503, failure.getResponse().getStatus(), "egress errors propagate so JobRunr retries");
        ExportBatch afterFailure = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.NPIS_SELECTED, afterFailure.state());
        Assertions.assertEquals("tpl-1", afterFailure.egress().templateId(), "pinned before the call");
        Assertions.assertNull(afterFailure.egress().correlationId(), "no request accepted yet");

        job.run(new RequestEgressJobRequest(batch.id(), 1));

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
