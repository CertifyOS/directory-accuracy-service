package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.clients.ApiLayerTokenService;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Clause;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.time.Instant;
import java.time.YearMonth;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The select job body, run directly on the api role against WireMock pages. Outside a JobRunr
 * worker {@code jobContext()} is null, which the log context tolerates; the egress request job it
 * enqueues stays ENQUEUED in storage where the test can see it.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class SelectJobIT {

    static final SelectionCriteria CRITERIA =
            new SelectionCriteria(List.of(new Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));
    static final String FILTER = "{\"data.delegationStatus\":{\"in\":[\"Direct\"]}}";
    static final YearMonth PERIOD = YearMonth.of(2026, 10);

    @Inject
    SelectJob select;

    @Inject
    ExportBatchRepository batches;

    @Inject
    ExportNpiRepository npis;

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

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        Mockito.when(google.idToken("/projects/1/global/backendServices/2")).thenReturn("iap-token");
        WireMockUpstreams.resetRequests();
    }

    private ExportBatch scheduled(String tenantId) {
        ExportBatch batch = ExportBatch.scheduled(tenantId, "candor", PERIOD, 1, CRITERIA, Instant.now());
        transactions.run(session -> {
            batches.insert(session, batch);
            return null;
        });
        return batch;
    }

    /** {@code count} practitioners numbered from {@code first}, as api-layer's page JSON. */
    private static String pageJson(int first, int count, long totalCount) {
        String data = IntStream.range(first, first + count)
                .mapToObj(index -> "{\"id\":\"p-" + index + "\",\"npi\":\""
                        + String.format(Locale.ROOT, "%010d", 1000000000L + index) + "\"}")
                .collect(Collectors.joining(","));
        return "{\"data\":[" + data + "],\"totalCount\":" + totalCount + "}";
    }

    private static com.github.tomakehurst.wiremock.client.MappingBuilder page(String tenantId, int page) {
        return WireMock.get(WireMock.urlPathEqualTo("/practitioners"))
                .withHeader("tenant-id", WireMock.equalTo(tenantId))
                .withQueryParam("filter", WireMock.equalToJson(FILTER))
                .withQueryParam("size", WireMock.equalTo(String.valueOf(SelectJob.PAGE_SIZE)))
                .withQueryParam("page", WireMock.equalTo(String.valueOf(page)));
    }

    private static void verifyPage(String tenantId, int page, int times) {
        WireMockUpstreams.verify(
                times,
                WireMock.getRequestedFor(WireMock.urlPathEqualTo("/practitioners"))
                        .withHeader("tenant-id", WireMock.equalTo(tenantId))
                        .withQueryParam("page", WireMock.equalTo(String.valueOf(page))));
    }

    @Test
    void threePagesRegisterEveryNpiOnceAndMoveTheBatchOn() {
        String tenant = "sel-three";
        WireMockUpstreams.stubFor(page(tenant, 0).willReturn(WireMock.okJson(pageJson(0, 100, 250))));
        WireMockUpstreams.stubFor(page(tenant, 1).willReturn(WireMock.okJson(pageJson(100, 100, 250))));
        WireMockUpstreams.stubFor(page(tenant, 2).willReturn(WireMock.okJson(pageJson(200, 50, 250))));
        ExportBatch batch = scheduled(tenant);

        select.run(new SelectJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.NPIS_SELECTED, after.state());
        Assertions.assertEquals(250, after.selection().practitionersSelected());
        Assertions.assertNull(after.selection().page(), "the cursor is cleared once selection completes");
        Assertions.assertNotNull(after.selection().completedAt());
        Assertions.assertEquals(250, npis.countForBatch(batch.id()));
        Assertions.assertEquals(
                "p-0", npis.listForBatch(batch.id(), null, 1).get(0).certifyPractitionerId());
        verifyPage(tenant, 0, 1);
        verifyPage(tenant, 1, 1);
        verifyPage(tenant, 2, 1);
        verifyPage(tenant, 3, 0);

        List<AuditEvent> trail = audit.findForBatch(batch.id(), 20);
        List<AuditEvent> registered = trail.stream()
                .filter(event -> event.type() == AuditEventType.NPIS_REGISTERED)
                .toList();
        Assertions.assertEquals(3, registered.size(), "one NPIS_REGISTERED per page");
        AuditEvent completed = trail.stream()
                .filter(event -> event.type() == AuditEventType.EXPORT_SELECTION_COMPLETED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(3, completed.detail().get("pages"));
        Assertions.assertEquals(250L, completed.detail().get("practitionersSelected"));
        Assertions.assertNotNull(completed.detail().get("criteria"));
        Assertions.assertNotNull(completed.detail().get("durationMs"));

        UUID egressJob = JobIds.of(RequestEgressJob.NAME, batch.id(), 1);
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(egressJob).getState());
    }

    @Test
    void aFailureOnPageTwoThenARetryFetchesOnlyTheRestAndRegistersNothingTwice() {
        String tenant = "sel-retry";
        WireMockUpstreams.stubFor(page(tenant, 0).willReturn(WireMock.okJson(pageJson(0, 100, 230))));
        WireMockUpstreams.stubFor(page(tenant, 1)
                .inScenario("sel-retry-page-1")
                .whenScenarioStateIs(Scenario.STARTED)
                .willSetStateTo("recovered")
                .willReturn(WireMock.serviceUnavailable()));
        WireMockUpstreams.stubFor(page(tenant, 1)
                .inScenario("sel-retry-page-1")
                .whenScenarioStateIs("recovered")
                .willReturn(WireMock.okJson(pageJson(100, 100, 230))));
        WireMockUpstreams.stubFor(page(tenant, 2).willReturn(WireMock.okJson(pageJson(200, 30, 230))));
        ExportBatch batch = scheduled(tenant);

        WebApplicationException failure = Assertions.assertThrows(
                WebApplicationException.class, () -> select.run(new SelectJobRequest(batch.id(), 1)));
        Assertions.assertEquals(
                503, failure.getResponse().getStatus(), "api-layer errors propagate so JobRunr retries");
        ExportBatch afterFailure = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SCHEDULED, afterFailure.state());
        Assertions.assertEquals(0, afterFailure.selection().page(), "page 0 is saved as the cursor");
        Assertions.assertEquals(100, npis.countForBatch(batch.id()));

        select.run(new SelectJobRequest(batch.id(), 1));

        verifyPage(tenant, 0, 1);
        verifyPage(tenant, 1, 2);
        verifyPage(tenant, 2, 1);
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.NPIS_SELECTED, after.state());
        Assertions.assertEquals(230, after.selection().practitionersSelected());
        Assertions.assertEquals(230, npis.countForBatch(batch.id()));
        AuditEvent completed = audit.findForBatch(batch.id(), 20).stream()
                .filter(event -> event.type() == AuditEventType.EXPORT_SELECTION_COMPLETED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(3, completed.detail().get("pages"), "pages counts the whole selection, not the retry");
    }

    @Test
    void zeroResultsIsEmptyWithItsEventAndNoEgressJob() {
        String tenant = "sel-empty";
        WireMockUpstreams.stubFor(page(tenant, 0).willReturn(WireMock.okJson("{\"data\":[],\"totalCount\":0}")));
        ExportBatch batch = scheduled(tenant);

        select.run(new SelectJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EMPTY, after.state());
        Assertions.assertEquals(0, after.selection().practitionersSelected());
        Assertions.assertEquals(0, npis.countForBatch(batch.id()));
        List<AuditEventType> types = audit.findForBatch(batch.id(), 20).stream()
                .map(AuditEvent::type)
                .toList();
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_BATCH_EMPTY), types.toString());
        Assertions.assertFalse(
                types.contains(AuditEventType.NPIS_REGISTERED), "an empty page writes no NPIS_REGISTERED");
        Assertions.assertThrows(
                org.jobrunr.storage.JobNotFoundException.class,
                () -> storage.getJobById(JobIds.of(RequestEgressJob.NAME, batch.id(), 1)),
                "no egress request job for an empty batch");
    }

    @Test
    void practitionersWithoutATenDigitNpiAreSkippedAndCounted() {
        String tenant = "sel-skip";
        WireMockUpstreams.stubFor(page(tenant, 0)
                .willReturn(WireMock.okJson("{\"data\":[{\"id\":\"p-ok\",\"npi\":\"1234567890\"},"
                        + "{\"id\":\"p-none\"},{\"id\":\"p-short\",\"npi\":\"123\"}],\"totalCount\":3}")));
        ExportBatch batch = scheduled(tenant);

        select.run(new SelectJobRequest(batch.id(), 1));

        Assertions.assertEquals(1, npis.countForBatch(batch.id()));
        Assertions.assertEquals(
                BatchState.NPIS_SELECTED, batches.find(batch.id()).orElseThrow().state());
        AuditEvent registered = audit.findForBatch(batch.id(), 20).stream()
                .filter(event -> event.type() == AuditEventType.NPIS_REGISTERED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(1, registered.detail().get("count"));
        Assertions.assertEquals(2, registered.detail().get("skippedNoNpi"));
    }

    @Test
    void anNpiTwiceOnOnePageIsRegisteredOnce() {
        String tenant = "sel-dup";
        WireMockUpstreams.stubFor(page(tenant, 0)
                .willReturn(WireMock.okJson("{\"data\":[{\"id\":\"p-a\",\"npi\":\"1234567893\"},"
                        + "{\"id\":\"p-b\",\"npi\":\"1234567893\"},"
                        + "{\"id\":\"p-c\",\"npi\":\"2345678918\"}],\"totalCount\":3}")));
        ExportBatch batch = scheduled(tenant);

        select.run(new SelectJobRequest(batch.id(), 1));

        Assertions.assertEquals(
                BatchState.NPIS_SELECTED, batches.find(batch.id()).orElseThrow().state());
        Assertions.assertEquals(2, npis.countForBatch(batch.id()));
    }

    @Test
    void aBatchInAnotherStateIsANoOp() {
        ExportBatch batch = scheduled("sel-noop");
        transactions.run(session -> {
            batches.transition(
                    session, batch.id(), BatchState.SCHEDULED, BatchState.NPIS_SELECTED, Instant.now(), null);
            return null;
        });

        select.run(new SelectJobRequest(batch.id(), 1));

        verifyPage("sel-noop", 0, 0);
        Assertions.assertEquals(2L, batches.find(batch.id()).orElseThrow().version(), "untouched");
    }
}
