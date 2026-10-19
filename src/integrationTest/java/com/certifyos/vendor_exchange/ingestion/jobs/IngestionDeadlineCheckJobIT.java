package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.clients.ApiLayerTokenService;
import com.certifyos.vendor_exchange.clients.WireMockUpstreams;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures.Seeded;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The ingestion deadline check, run directly against WireMock file-ingestion: one status read and
 * one move per outcome. Every tenant here starts with {@code ingdl-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class IngestionDeadlineCheckJobIT {

    static final int TRAIL_LIMIT = 100;

    @Inject
    IngestionDeadlineCheckJob job;

    @Inject
    InboundFileRepository inbound;

    @Inject
    ExportBatchRepository batches;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @InjectMock
    ApiLayerTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        WireMockUpstreams.resetRequests();
    }

    /** An entry in {@code state} whose ingestion id is {@code ing-<tenant>}. */
    private Seeded seed(String tenant, InboundFileState state) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.set("inboundFiles.0.ingestionId", ingestionId(tenant)));
        return seeded;
    }

    private static String ingestionId(String tenant) {
        return "ing-" + tenant;
    }

    private static String statusPath(String tenant) {
        return "/v1/ingestion/" + ingestionId(tenant);
    }

    private static void stubStatus(String tenant, String body) {
        WireMockUpstreams.stubFor(
                WireMock.get(WireMock.urlEqualTo(statusPath(tenant))).willReturn(WireMock.okJson(body)));
    }

    private void run(Seeded seeded) {
        job.run(new IngestionDeadlineCheckJobRequest(
                seeded.batch().id(), seeded.entry().uploadId(), 1));
    }

    private InboundFile entry(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow().entry();
    }

    private List<AuditEvent> since(Seeded seeded, int skip) {
        List<AuditEvent> all = audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT);
        return all.subList(skip, all.size());
    }

    private int events(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT).size();
    }

    private static void verifyOneStatusCall(String tenant) {
        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlEqualTo(statusPath(tenant)))
                        .withHeader("tenant-id", WireMock.equalTo(tenant))
                        .withHeader("identifier", WireMock.equalTo("vendor-ingestion"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token")));
    }

    @Test
    void completedAtDeadlineParsesTheEntry() {
        String tenant = "ingdl-completed";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);
        stubStatus(
                tenant,
                "{\"ingestionId\":\"" + ingestionId(tenant) + "\",\"status\":\"PARTIAL\","
                        + "\"counts\":{\"populatedRows\":1873,\"validRows\":1871,\"invalidRows\":2},"
                        + "\"registeredAt\":\"2026-10-15T09:13:00Z\",\"terminalAt\":\"2026-10-15T09:14:20Z\"}");
        int before = events(seeded);

        run(seeded);

        verifyOneStatusCall(tenant);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.PARSED, after.state());
        Assertions.assertEquals("PARTIAL", after.parse().status());
        Assertions.assertEquals("DEADLINE", after.parse().completionSource());
        Assertions.assertEquals(1871L, after.parse().validRows());
        Assertions.assertEquals(2L, after.parse().invalidRows());
        List<AuditEvent> written = since(seeded, before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_PARSED),
                written.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("DEADLINE", written.get(0).detail().get("completionSource"));
    }

    @Test
    void invalidAtDeadlineFailsIngestion() {
        String tenant = "ingdl-invalid";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);
        stubStatus(
                tenant,
                "{\"ingestionId\":\"" + ingestionId(tenant) + "\",\"status\":\"INVALID\","
                        + "\"invalidReason\":\"HEADER_MISMATCH\"}");
        int before = events(seeded);

        run(seeded);

        verifyOneStatusCall(tenant);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.INGESTION_FAILED, after.state());
        Assertions.assertEquals("INVALID", after.parse().status());
        Assertions.assertEquals("HEADER_MISMATCH", after.parse().reason());
        Assertions.assertEquals("DEADLINE", after.parse().completionSource());
        List<AuditEvent> written = since(seeded, before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_INGESTION_FAILED),
                written.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals("HEADER_MISMATCH", written.get(0).detail().get("reason"));
    }

    @Test
    void stillParsingAtDeadlineFailsEntry() {
        String tenant = "ingdl-parsing";
        Seeded seeded = seed(tenant, InboundFileState.INGESTION_REQUESTED);
        stubStatus(tenant, "{\"ingestionId\":\"" + ingestionId(tenant) + "\",\"status\":\"PARSING\"}");
        int before = events(seeded);

        run(seeded);

        verifyOneStatusCall(tenant);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.FAILED, after.state());
        Assertions.assertEquals("PARSE", after.failedStep());
        Assertions.assertEquals("INGESTION_DID_NOT_FINISH", after.failureReason());
        Assertions.assertEquals("PARSING", after.lastError());
        List<AuditEvent> written = since(seeded, before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_FAILED),
                written.stream().map(AuditEvent::type).toList());
        Assertions.assertEquals(1, ((Number) written.get(0).detail().get("jobAttempts")).intValue());
    }

    @Test
    void entryAlreadyParsedMakesNoStatusCall() {
        String tenant = "ingdl-parsed";
        Seeded seeded = seed(tenant, InboundFileState.PARSED);
        InboundFile before = entry(seeded);
        int eventsBefore = events(seeded);

        run(seeded);

        WireMockUpstreams.verify(0, WireMock.getRequestedFor(WireMock.urlEqualTo(statusPath(tenant))));
        Assertions.assertEquals(before, entry(seeded));
        Assertions.assertEquals(eventsBefore, events(seeded));
    }
}
