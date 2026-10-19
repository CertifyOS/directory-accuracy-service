package com.certifyos.vendor_exchange.ingestion.jobs;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.clients.ApiLayerTokenService;
import com.certifyos.vendor_exchange.clients.ArchiveSourceMissingException;
import com.certifyos.vendor_exchange.clients.ArchiveStore;
import com.certifyos.vendor_exchange.clients.WireMockUpstreams;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.export.jobs.JobIds;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures.Seeded;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundNames;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.quarkus.test.junit.mockito.InjectSpy;
import jakarta.inject.Inject;
import jakarta.ws.rs.WebApplicationException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The request-ingestion job, run directly on the api role: archive copy through a mocked archive
 * store, register against WireMock file-ingestion. Every tenant here starts with {@code reqing-}.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class RequestIngestionJobIT {

    static final String REGISTER = "/v1/ingestion";
    static final String ARCHIVE_BUCKET = "test-archive-bucket";
    static final int TRAIL_LIMIT = 100;

    @Inject
    RequestIngestionJob job;

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

    @Inject
    StorageProvider storage;

    @InjectMock
    ArchiveStore archive;

    @InjectSpy
    InboundLifecycle lifecycle;

    @InjectMock
    ApiLayerTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
        WireMockUpstreams.resetRequests();
    }

    private Seeded seed(String tenant, InboundFileState state) {
        return InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
    }

    private static String fileName(Seeded seeded) {
        String objectName = seeded.entry().objectName();
        return objectName.substring(objectName.lastIndexOf('/') + 1);
    }

    private static String archiveObject(Seeded seeded) {
        return InboundNames.archiveObject(
                seeded.batch().tenantId(), seeded.batch().id(), InboundFixtures.GENERATION, fileName(seeded));
    }

    private static String archivePath(Seeded seeded) {
        return "gs://" + ARCHIVE_BUCKET + "/" + archiveObject(seeded);
    }

    private static String externalRef(Seeded seeded) {
        return Ids.externalRef(seeded.batch().id(), InboundFixtures.GENERATION);
    }

    private void archiveCopies(Seeded seeded, boolean alreadyExisted) {
        Mockito.when(archive.copy(
                        InboundFixtures.VENDOR_BUCKET,
                        seeded.entry().objectName(),
                        Long.parseLong(InboundFixtures.GENERATION),
                        ARCHIVE_BUCKET,
                        archiveObject(seeded)))
                .thenReturn(new ArchiveStore.Copied(archivePath(seeded), 52_000L, alreadyExisted));
    }

    private static void stubRegister(Seeded seeded, int status, String body) {
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo(REGISTER))
                .withRequestBody(WireMock.matchingJsonPath("$.externalRef", WireMock.equalTo(externalRef(seeded))))
                .willReturn(WireMock.aResponse()
                        .withStatus(status)
                        .withHeader("Content-Type", "application/json")
                        .withBody(body)));
    }

    /** The design's step-3 register body for the seeded entry. */
    private static String designBody(Seeded seeded) {
        return "{\"mappingId\":\"map-candor-recs-v1\","
                + "\"filename\":\"" + fileName(seeded) + "\","
                + "\"sourceUri\":\"" + archivePath(seeded) + "\","
                + "\"externalRef\":\"" + externalRef(seeded) + "\","
                + "\"meta\":{\"exportBatchId\":\"" + seeded.batch().id() + "\","
                + "\"uploadId\":\"" + seeded.entry().uploadId() + "\"}}";
    }

    private InboundFile entry(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow().entry();
    }

    private List<AuditEvent> trail(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT);
    }

    /** The batch's events written after the first {@code skip}, in write order. */
    private List<AuditEvent> since(Seeded seeded, int skip) {
        List<AuditEvent> all = trail(seeded);
        return all.subList(skip, all.size());
    }

    private static List<AuditEventType> types(List<AuditEvent> events) {
        return events.stream().map(AuditEvent::type).toList();
    }

    private static UUID deadlineId(Seeded seeded) {
        return JobIds.of(IngestionDeadlineCheckJob.NAME, seeded.entry().uploadId(), 1);
    }

    private void run(Seeded seeded) {
        job.run(new RequestIngestionJobRequest(
                seeded.batch().id(), seeded.entry().uploadId(), 1));
    }

    private void assertRequested(Seeded seeded, String ingestionId) {
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, after.state());
        Assertions.assertEquals(ingestionId, after.ingestionId());
        Assertions.assertEquals(externalRef(seeded), after.externalRef());
        Assertions.assertEquals(archivePath(seeded), after.archivePath());
        Assertions.assertNotNull(after.requestedAt());
        Assertions.assertEquals(deadlineId(seeded).toString(), after.jobId());
    }

    private void assertFailed(Seeded seeded, String step, String reason) {
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.FAILED, after.state());
        Assertions.assertEquals(step, after.failedStep());
        Assertions.assertEquals(reason, after.failureReason());
        Assertions.assertThrows(
                org.jobrunr.storage.JobNotFoundException.class,
                () -> storage.getJobById(deadlineId(seeded)),
                "no deadline check for a failed entry");
    }

    private AuditEvent last(Seeded seeded, AuditEventType type) {
        List<AuditEvent> all = trail(seeded);
        for (int index = all.size() - 1; index >= 0; index--) {
            if (all.get(index).type() == type) {
                return all.get(index);
            }
        }
        throw new AssertionError("no " + type + " for " + seeded.batch().id());
    }

    @Test
    void archivesThenRegistersWithTheDesignBody() {
        Seeded seeded = seed("reqing-first", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 201, "{\"ingestionId\":\"ing-reqing-first\",\"status\":\"RECEIVED\"}");
        int before = trail(seeded).size();
        Instant started = Instant.now();

        run(seeded);

        Mockito.verify(archive)
                .copy(
                        InboundFixtures.VENDOR_BUCKET,
                        seeded.entry().objectName(),
                        1760520000123456L,
                        ARCHIVE_BUCKET,
                        archiveObject(seeded));
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(REGISTER))
                        .withHeader("tenant-id", WireMock.equalTo("reqing-first"))
                        .withHeader("identifier", WireMock.equalTo("vendor-ingestion"))
                        .withHeader("Authorization", WireMock.equalTo("Bearer api-layer-token"))
                        .withRequestBody(WireMock.equalToJson(designBody(seeded))));
        assertRequested(seeded, "ing-reqing-first");
        InboundFile after = entry(seeded);
        Assertions.assertEquals(52_000L, after.archiveBytes());

        org.jobrunr.jobs.Job deadline = storage.getJobById(deadlineId(seeded));
        Assertions.assertEquals(StateName.SCHEDULED, deadline.getState());
        Instant scheduledAt = ((ScheduledState) deadline.getJobState()).getScheduledAt();
        Duration fromExpected =
                Duration.between(started.plus(Duration.ofHours(6)), scheduledAt).abs();
        Assertions.assertTrue(
                fromExpected.compareTo(Duration.ofMinutes(2)) < 0, "deadline at now + 6 h, was " + scheduledAt);

        List<AuditEvent> events = since(seeded, before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_ARCHIVED, AuditEventType.INBOUND_FILE_INGESTION_REQUESTED),
                types(events));
        Assertions.assertEquals("CREATED", events.get(0).detail().get("copyResult"));
        AuditEvent requested = events.get(1);
        Assertions.assertEquals("ing-reqing-first", requested.detail().get("ingestionId"));
        Assertions.assertEquals(externalRef(seeded), requested.detail().get("externalRef"));
        Assertions.assertEquals("map-candor-recs-v1", requested.detail().get("mappingId"));
        Assertions.assertEquals("201", requested.detail().get("registerResponse"));
        Assertions.assertEquals(
                deadlineId(seeded).toString(), requested.detail().get("deadlineJobId"));
    }

    @Test
    void existingArchiveIsSuccess() {
        Seeded seeded = seed("reqing-exists", InboundFileState.RECEIVED);
        archiveCopies(seeded, true);
        stubRegister(seeded, 201, "{\"ingestionId\":\"ing-reqing-exists\",\"status\":\"RECEIVED\"}");

        run(seeded);

        assertRequested(seeded, "ing-reqing-exists");
        Assertions.assertEquals(
                "ALREADY_PRESENT",
                last(seeded, AuditEventType.INBOUND_FILE_ARCHIVED).detail().get("copyResult"));
    }

    @Test
    void retryFromArchivingSkipsTheCopy() {
        Seeded seeded = seed("reqing-retry", InboundFileState.ARCHIVING);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.combine(
                                Updates.set("inboundFiles.0.archivePath", archivePath(seeded)),
                                Updates.set("inboundFiles.0.archiveBytes", 52_000L)));
        stubRegister(seeded, 201, "{\"ingestionId\":\"ing-reqing-retry\",\"status\":\"RECEIVED\"}");
        int before = trail(seeded).size();

        run(seeded);

        Mockito.verifyNoInteractions(archive);
        WireMockUpstreams.verify(
                1,
                WireMock.postRequestedFor(WireMock.urlEqualTo(REGISTER))
                        .withRequestBody(WireMock.equalToJson(designBody(seeded))));
        assertRequested(seeded, "ing-reqing-retry");
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_INGESTION_REQUESTED), types(since(seeded, before)));
    }

    @Test
    void repeatRegisterAnswers200AndProceeds() {
        Seeded seeded = seed("reqing-repeat", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 200, "{\"ingestionId\":\"ing-reqing-seen\",\"status\":\"PARSING\"}");

        run(seeded);

        assertRequested(seeded, "ing-reqing-seen");
        Assertions.assertEquals(
                "200",
                last(seeded, AuditEventType.INBOUND_FILE_INGESTION_REQUESTED)
                        .detail()
                        .get("registerResponse"));
        Assertions.assertEquals(
                StateName.SCHEDULED, storage.getJobById(deadlineId(seeded)).getState());
    }

    @Test
    void mappingUnknownFailsTheEntry() {
        Seeded seeded = seed("reqing-mapping", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 422, "{\"code\":\"MAPPING_UNKNOWN\"}");

        run(seeded);

        assertFailed(seeded, "REGISTER", "MAPPING_UNKNOWN");
        InboundFile after = entry(seeded);
        Assertions.assertTrue(after.lastError().startsWith("HTTP 422"), after.lastError());
        Assertions.assertTrue(after.lastError().contains("MAPPING_UNKNOWN"), after.lastError());
        Assertions.assertEquals(archivePath(seeded), after.archivePath(), "the archive is kept");
        AuditEvent failed = last(seeded, AuditEventType.INBOUND_FILE_FAILED);
        Assertions.assertEquals("REGISTER", failed.detail().get("failedStep"));
        Assertions.assertEquals("MAPPING_UNKNOWN", failed.detail().get("failureReason"));
        Assertions.assertEquals(1, ((Number) failed.detail().get("jobAttempts")).intValue());
    }

    @Test
    void refusedRegisterFailsTheEntry() {
        Seeded seeded = seed("reqing-refused", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 403, "{\"code\":\"CONSUMER_NOT_ALLOWED\"}");

        run(seeded);

        assertFailed(seeded, "REGISTER", "FILE_INGESTION_REFUSED");
        Assertions.assertTrue(
                entry(seeded).lastError().startsWith("HTTP 403"), entry(seeded).lastError());
    }

    @Test
    void sourceMissingFailsTheEntry() {
        Seeded seeded = seed("reqing-missing", InboundFileState.RECEIVED);
        Mockito.when(archive.copy(
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString()))
                .thenThrow(new ArchiveSourceMissingException("source generation 1760520000123456 not found", null));

        run(seeded);

        assertFailed(seeded, "ARCHIVE", "SOURCE_MISSING");
        InboundFile after = entry(seeded);
        Assertions.assertEquals("source generation 1760520000123456 not found", after.lastError());
        Assertions.assertNull(after.archivePath());
        WireMockUpstreams.verify(
                0,
                WireMock.postRequestedFor(WireMock.urlEqualTo(REGISTER))
                        .withRequestBody(
                                WireMock.matchingJsonPath("$.externalRef", WireMock.equalTo(externalRef(seeded)))));
    }

    @Test
    void fileIngestionDownPropagates() {
        Seeded seeded = seed("reqing-down", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 503, "{\"code\":\"SERVICE_DISABLED\"}");

        WebApplicationException down = Assertions.assertThrows(WebApplicationException.class, () -> run(seeded));

        Assertions.assertEquals(503, down.getResponse().getStatus());
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.ARCHIVING, after.state(), "left for the JobRunr retry");
        Assertions.assertEquals(archivePath(seeded), after.archivePath(), "the archive is kept");
        Assertions.assertNull(after.failedStep());
    }

    @Test
    void otherClientErrorPropagates() {
        Seeded seeded = seed("reqing-unreadable", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 400, "{\"code\":\"SOURCE_UNREADABLE\"}");

        WebApplicationException refused = Assertions.assertThrows(WebApplicationException.class, () -> run(seeded));

        Assertions.assertEquals(400, refused.getResponse().getStatus());
        Assertions.assertEquals(InboundFileState.ARCHIVING, entry(seeded).state(), "left for the JobRunr retry");
    }

    @Test
    void crashBetweenScheduleAndCommitLeavesTheCheckAndTheRerunCommitsWithoutRescheduling() {
        Seeded seeded = seed("reqing-crash", InboundFileState.RECEIVED);
        archiveCopies(seeded, false);
        stubRegister(seeded, 201, "{\"ingestionId\":\"ing-reqing-crash\",\"status\":\"RECEIVED\"}");
        Mockito.doThrow(new IllegalStateException("worker died before the commit"))
                .doCallRealMethod()
                .when(lifecycle)
                .ingestionRequested(
                        ArgumentMatchers.any(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any(),
                        ArgumentMatchers.any());

        Assertions.assertThrows(IllegalStateException.class, () -> run(seeded));

        InboundFile crashed = entry(seeded);
        Assertions.assertEquals(InboundFileState.ARCHIVING, crashed.state(), "the commit never happened");
        Assertions.assertEquals(archivePath(seeded), crashed.archivePath());
        org.jobrunr.jobs.Job scheduled = storage.getJobById(deadlineId(seeded));
        Assertions.assertEquals(StateName.SCHEDULED, scheduled.getState(), "the check exists before the commit");
        Instant firstAt = ((ScheduledState) scheduled.getJobState()).getScheduledAt();

        run(seeded);

        assertRequested(seeded, "ing-reqing-crash");
        Mockito.verify(archive, Mockito.times(1))
                .copy(
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString());
        org.jobrunr.jobs.Job after = storage.getJobById(deadlineId(seeded));
        Assertions.assertEquals(StateName.SCHEDULED, after.getState());
        Assertions.assertEquals(1, after.getJobStates().size(), "the rerun did not schedule a second time");
        Assertions.assertEquals(
                firstAt, ((ScheduledState) after.getJobState()).getScheduledAt(), "JobRunr kept the first schedule");
    }

    @Test
    void entryNotReceivedIsNoOp() {
        Seeded seeded = seed("reqing-noop", InboundFileState.INGESTION_REQUESTED);
        InboundFile before = entry(seeded);
        int events = trail(seeded).size();

        run(seeded);

        Mockito.verifyNoInteractions(archive);
        WireMockUpstreams.verify(0, WireMock.postRequestedFor(WireMock.urlEqualTo(REGISTER)));
        Assertions.assertEquals(before, entry(seeded));
        Assertions.assertEquals(events, trail(seeded).size());
    }
}
