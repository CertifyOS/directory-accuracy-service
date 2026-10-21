package com.certifyos.directory_accuracy.ingestion.jobs;

import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.WorkerTestProfile;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.clients.ApiLayerTokenService;
import com.certifyos.directory_accuracy.clients.ArchiveStore;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures.Seeded;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundNames;
import com.certifyos.directory_accuracy.metrics.DirectoryAccuracyMetrics;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.UUID;
import org.awaitility.Awaitility;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The ingestion lane's JobRunr gate on the worker role (one retry in the test profile): a job that
 * keeps failing reaches {@code FAILED}, and the final-failure filter, registered beside JobRunr's
 * retry filter and the export lane's, fails the entry with its event. Every tenant here starts with
 * {@code ingate-}; entries are written fresh so the ten-minute reconciler leaves them alone.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(WorkerTestProfile.class)
class IngestionJobRunrGateIT {

    static final int TRAIL_LIMIT = 100;
    static final String ARCHIVE_BUCKET = "test-archive-bucket";

    @Inject
    IngestionJobEnqueuer enqueuer;

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

    @Inject
    MeterRegistry registry;

    @InjectMock
    ArchiveStore archive;

    @InjectMock
    ApiLayerTokenService tokens;

    @BeforeEach
    void before() {
        Mockito.when(tokens.accessToken()).thenReturn("api-layer-token");
    }

    private Seeded fresh(String tenant, InboundFileState state) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(transactions, batches, collections, tenant, state);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.set("inboundFiles.0.updatedAt", Instant.now().truncatedTo(ChronoUnit.MILLIS)));
        return seeded;
    }

    private InboundFile entry(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow().entry();
    }

    private List<AuditEvent> failedEvents(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT).stream()
                .filter(event -> event.type() == AuditEventType.INBOUND_FILE_FAILED)
                .toList();
    }

    private double counter(String name, String job) {
        Counter found = registry.find(name).tag("job", job).counter();
        return found == null ? 0 : found.count();
    }

    private void awaitJobFailed(UUID jobId) {
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> storage.getJobById(jobId).getState() == StateName.FAILED);
    }

    @Test
    void exhaustedRequestJobFailsTheEntryWithItsEvent() {
        Seeded seeded = fresh("ingate-archive", InboundFileState.RECEIVED);
        Mockito.when(archive.copy(
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString()))
                .thenThrow(new IllegalStateException("archive store unavailable"));
        double retries = counter(DirectoryAccuracyMetrics.INGESTION_JOBS_RETRIES, "RequestIngestionJob");
        double finals = counter(DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES, "RequestIngestionJob");

        UUID jobId =
                enqueuer.requestIngestion(seeded.batch().id(), seeded.entry().uploadId(), 1);

        awaitJobFailed(jobId);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> entry(seeded).state() == InboundFileState.FAILED);
        InboundFile failed = entry(seeded);
        Assertions.assertEquals(RequestIngestionJob.STEP_ARCHIVE, failed.failedStep());
        Assertions.assertEquals(IngestionFinalFailureFilter.JOB_RETRIES_EXHAUSTED, failed.failureReason());
        Assertions.assertTrue(
                failed.lastError().startsWith("java.lang.IllegalStateException: archive store unavailable"),
                failed.lastError());
        List<AuditEvent> events = failedEvents(seeded);
        Assertions.assertEquals(1, events.size());
        AuditEvent event = events.get(0);
        Assertions.assertEquals("ARCHIVE", event.detail().get("failedStep"));
        Assertions.assertEquals("JOB_RETRIES_EXHAUSTED", event.detail().get("failureReason"));
        Assertions.assertEquals(2, ((Number) event.detail().get("jobAttempts")).intValue(), "one run, one retry");
        Assertions.assertEquals(jobId.toString(), event.jobId(), "the exhausted job is named on the event");
        Assertions.assertEquals(
                retries + 1, counter(DirectoryAccuracyMetrics.INGESTION_JOBS_RETRIES, "RequestIngestionJob"));
        Assertions.assertEquals(
                finals + 1, counter(DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES, "RequestIngestionJob"));
    }

    @Test
    void registerClientErrorExhaustedIsSourceUnreadable() {
        Seeded seeded = fresh("ingate-unreadable", InboundFileState.RECEIVED);
        String objectName = seeded.entry().objectName();
        String fileName = objectName.substring(objectName.lastIndexOf('/') + 1);
        String archiveObject = InboundNames.archiveObject(
                seeded.batch().tenantId(), seeded.batch().id(), InboundFixtures.GENERATION, fileName);
        Mockito.when(archive.copy(
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyLong(),
                        ArgumentMatchers.anyString(),
                        ArgumentMatchers.anyString()))
                .thenReturn(new ArchiveStore.Copied("gs://" + ARCHIVE_BUCKET + "/" + archiveObject, 52_000L, false));
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/v1/ingestion"))
                .withRequestBody(WireMock.matchingJsonPath(
                        "$.externalRef",
                        WireMock.equalTo(Ids.externalRef(seeded.batch().id(), InboundFixtures.GENERATION))))
                .willReturn(WireMock.aResponse()
                        .withStatus(400)
                        .withHeader("Content-Type", "application/json")
                        .withBody("{\"code\":\"SOURCE_UNREADABLE\"}")));

        UUID jobId =
                enqueuer.requestIngestion(seeded.batch().id(), seeded.entry().uploadId(), 1);

        awaitJobFailed(jobId);
        Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .until(() -> entry(seeded).state() == InboundFileState.FAILED);
        InboundFile failed = entry(seeded);
        Assertions.assertEquals(RequestIngestionJob.STEP_REGISTER, failed.failedStep());
        Assertions.assertEquals(IngestionFinalFailureFilter.SOURCE_UNREADABLE, failed.failureReason());
        Assertions.assertTrue(
                failed.lastError().startsWith(RegisterClientErrorException.class.getName()), failed.lastError());
        Assertions.assertFalse(failed.lastError().contains("SOURCE_UNREADABLE\""), "no problem body on the entry");
        Assertions.assertEquals(
                "SOURCE_UNREADABLE", failedEvents(seeded).get(0).detail().get("failureReason"));
    }

    @Test
    void exhaustedDeadlineCheckLeavesAnArchivingEntryAlone() {
        Seeded seeded = fresh("ingate-archiving", InboundFileState.ARCHIVING);
        double finals = counter(DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES, "IngestionDeadlineCheckJob");

        UUID jobId = enqueuer.deadlineCheck(seeded.batch().id(), seeded.entry().uploadId(), 1, Instant.now());

        awaitJobFailed(jobId);
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .until(() ->
                        counter(DirectoryAccuracyMetrics.INGESTION_JOBS_FINAL_FAILURES, "IngestionDeadlineCheckJob")
                                >= finals + 1);
        Awaitility.await()
                .during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(10))
                .until(() -> entry(seeded).state() == InboundFileState.ARCHIVING);
        Assertions.assertEquals(List.of(), failedEvents(seeded), "the request job may still commit");
    }
}
