package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.clients.VendorBucket;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * The finish job against a mocked vendor bucket and WireMock egress: present and matching,
 * mismatching, missing, and selection counts read from the status when the batch has none.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class FinishJobIT {

    @Inject
    FinishJob job;

    @Inject
    ExportBatchRepository batches;

    @Inject
    AuditRepository audit;

    @Inject
    Transactions transactions;

    @Inject
    StorageProvider storage;

    @Inject
    JobEnqueuer enqueuer;

    @InjectMock
    VendorBucket bucket;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-egress-iap-client-id")).thenReturn("egress-token");
        WireMockUpstreams.resetRequests();
    }

    private ExportBatch completed(String tenant, int selected) {
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                tenant,
                BatchState.EGRESS_COMPLETED,
                selected,
                Instant.now().minusSeconds(3600));
        enqueuer.deadlineCheck(batch.id(), 1, 1, Instant.now().plusSeconds(3600));
        return batch;
    }

    private void objectAt(ExportBatch batch, long size, Map<String, String> metadata) {
        Mockito.when(bucket.head(
                        batch.egress().destination().bucket(),
                        batch.egress().destination().objectName()))
                .thenReturn(Optional.of(new VendorBucket.ObjectInfo(
                        batch.egress().destination().bucket(),
                        batch.egress().destination().objectName(),
                        size,
                        metadata,
                        "1B2M2Y8AsgTpgAmY7PhCfg==")));
    }

    @Test
    void presentAndMatchingIsDeliveredWithTheFileFactsAndTheProducer() {
        ExportBatch batch = completed("fin-match", 3);
        UUID deadline = EgressFixtures.deadlineId(batch);
        objectAt(
                batch,
                2048,
                Map.of(
                        "complete",
                        "true",
                        "totalRecords",
                        "3",
                        "totalRows",
                        "5",
                        "correlationId",
                        batch.egress().correlationId()));

        job.run(new FinishJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.DELIVERED, after.state());
        Assertions.assertNotNull(after.deliveredAt());
        Assertions.assertTrue(after.reconciliation().match());
        Assertions.assertEquals(3, after.reconciliation().selected());
        Assertions.assertEquals(3, after.reconciliation().inFile());
        Assertions.assertEquals(batch.file().name(), after.file().name());
        Assertions.assertEquals(batch.file().path(), after.file().path());
        Assertions.assertEquals(5L, after.file().rowCount());
        Assertions.assertEquals(2048L, after.file().bytes());
        Assertions.assertEquals("certify-export-v1", after.file().schemaVersion());
        Assertions.assertEquals(
                "1B2M2Y8AsgTpgAmY7PhCfg==", after.file().md5(), "the object's MD5 as the bucket reports it");
        Assertions.assertEquals(batch.egress().correlationId(), after.egress().fileProducedBy());
        var trail = audit.findForBatch(batch.id(), 20);
        AuditEvent reconciled = trail.stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_RECONCILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertTrue((Boolean) reconciled.detail().get("match"));
        Assertions.assertEquals(3L, ((Number) reconciled.detail().get("selected")).longValue());
        WireMockUpstreams.verify(
                0,
                WireMock.getRequestedFor(WireMock.urlPathMatching("/api/v1/egress/status/practitioner/fin-match/.*")));
        AuditEvent delivered = trail.stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_DELIVERED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(batch.file().name(), delivered.detail().get("fileName"));
        Assertions.assertEquals("1B2M2Y8AsgTpgAmY7PhCfg==", delivered.detail().get("md5"));
        Assertions.assertEquals("EVENT", delivered.detail().get("completionSource"));
        Assertions.assertEquals(StateName.DELETED, storage.getJobById(deadline).getState(), "pending deadline deleted");
    }

    @Test
    void presentAndMismatchingIsStillDeliveredWithMatchFalse() {
        ExportBatch batch = completed("fin-mismatch", 4);
        objectAt(
                batch,
                1024,
                Map.of(
                        "complete",
                        "true",
                        "totalRecords",
                        "3",
                        "correlationId",
                        batch.egress().correlationId()));

        job.run(new FinishJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.DELIVERED, after.state());
        Assertions.assertFalse(after.reconciliation().match());
        Assertions.assertEquals(4, after.reconciliation().selected());
        Assertions.assertEquals(3, after.reconciliation().inFile());
        Assertions.assertNull(after.file().rowCount(), "no totalRows metadata, no row count");
        AuditEvent reconciled = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_RECONCILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertFalse((Boolean) reconciled.detail().get("match"));
    }

    @Test
    void missingObjectFailsTheBatchAtEgressWithFileNotFound() {
        ExportBatch batch = completed("fin-missing", 2);
        UUID deadline = EgressFixtures.deadlineId(batch);
        Mockito.when(bucket.head(
                        batch.egress().destination().bucket(),
                        batch.egress().destination().objectName()))
                .thenReturn(Optional.empty());

        job.run(new FinishJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, after.failedStep());
        Assertions.assertTrue(
                after.lastError().contains(batch.egress().destination().objectName()), after.lastError());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("FILE_NOT_FOUND", failed.detail().get("cause"));
        Assertions.assertNull(after.reconciliation());
        Assertions.assertEquals(StateName.DELETED, storage.getJobById(deadline).getState());
    }

    @Test
    void aProducerFromAnEarlierAttemptIsRecordedAsIs() {
        ExportBatch batch = completed("fin-prior", 1);
        objectAt(batch, 512, Map.of("complete", "true", "totalRecords", "1", "correlationId", batch.id() + "-r0"));

        job.run(new FinishJobRequest(batch.id(), 1));

        Assertions.assertEquals(
                batch.id() + "-r0",
                batches.find(batch.id()).orElseThrow().egress().fileProducedBy());
    }

    @Test
    void countsMissingOnTheBatchAreReadOnceFromTheStatusAndRecorded() {
        ExportBatch batch = completed("fin-status", -1);
        Assertions.assertFalse(batch.selection().hasCounts());
        objectAt(
                batch,
                512,
                Map.of(
                        "complete",
                        "true",
                        "totalRecords",
                        "2",
                        "correlationId",
                        batch.egress().correlationId()));
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/"
                        + batch.tenantId() + "/" + batch.egress().correlationId()))
                .willReturn(WireMock.okJson("{\"state\":\"COMPLETED\",\"phase\":\"EXPORT_DONE\",\"gcs_complete\":true,"
                        + "\"selection\":{\"state\":\"RESOLVED\",\"requested\":3,\"distinct\":2,\"matched\":2,"
                        + "\"unmatched\":1,\"parts\":1,\"listUsedUri\":\"gs://egress-list-used/fin-status.csv\","
                        + "\"reusedFrom\":null,\"attempts\":1,\"failureReason\":null}}")));

        job.run(new FinishJobRequest(batch.id(), 1));

        WireMockUpstreams.verify(
                1,
                WireMock.getRequestedFor(WireMock.urlPathMatching("/api/v1/egress/status/practitioner/fin-status/.*")));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.DELIVERED, after.state());
        Assertions.assertTrue(after.reconciliation().match());
        Assertions.assertEquals(2, after.reconciliation().selected());
        Assertions.assertEquals(2L, after.selection().distinct());
        Assertions.assertEquals(1L, after.selection().unmatched());
        Assertions.assertEquals(
                "gs://egress-list-used/fin-status.csv", after.selection().listUsedUri());
        Assertions.assertEquals(EgressFixtures.FILTER_ID, after.selection().filterId(), "the filter id is kept");
    }

    @Test
    void aStatusWithoutSelectionCountsFailsTheBatch() {
        ExportBatch batch = completed("fin-nocounts", -1);
        objectAt(
                batch,
                512,
                Map.of(
                        "complete",
                        "true",
                        "totalRecords",
                        "2",
                        "correlationId",
                        batch.egress().correlationId()));
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/"
                        + batch.tenantId() + "/" + batch.egress().correlationId()))
                .willReturn(
                        WireMock.okJson("{\"state\":\"COMPLETED\",\"phase\":\"EXPORT_DONE\",\"gcs_complete\":true}")));

        job.run(new FinishJobRequest(batch.id(), 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("SELECTION_COUNTS_MISSING", failed.detail().get("cause"));
    }
}
