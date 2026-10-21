package com.certifyos.directory_accuracy.export.jobs;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.clients.WireMockUpstreams;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
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
import org.mockito.Mockito;

/** The four status outcomes across the two checks, against WireMock egress. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@QuarkusTestResource(WireMockUpstreams.class)
@TestProfile(ApiTestProfile.class)
class DeadlineCheckJobIT {

    @Inject
    DeadlineCheckJob job;

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

    @Inject
    JobEnqueuer enqueuer;

    @InjectMock
    GoogleIdTokenService google;

    @BeforeEach
    void before() {
        Mockito.when(google.idToken("test-egress-iap-client-id")).thenReturn("egress-token");
        WireMockUpstreams.resetRequests();
    }

    private ExportBatch requested(String tenant, Duration ago) {
        return EgressFixtures.batchIn(
                transactions,
                batches,
                npis,
                tenant,
                BatchState.EGRESS_REQUESTED,
                2,
                Instant.now().minus(ago));
    }

    private static void stubStatus(ExportBatch batch, String phase, boolean gcsComplete) {
        WireMockUpstreams.stubFor(WireMock.get(WireMock.urlEqualTo("/api/v1/egress/status/practitioner/"
                        + batch.tenantId() + "/" + batch.egress().correlationId()))
                .willReturn(WireMock.okJson("{\"state\":\"RUNNING\",\"phase\":\"" + phase
                        + "\",\"total_rows\":5,\"rows_with_data\":2,"
                        + "\"gcs_uri\":\"gs://test-vendor-bucket/from/x.csv\",\"gcs_complete\":" + gcsComplete + "}")));
    }

    private List<AuditEventType> types(ExportBatch batch) {
        return audit.findForBatch(batch.id(), 20).stream().map(AuditEvent::type).toList();
    }

    @Test
    void completedWithTheFileInPlaceCompletesFromTheDeadlinePath() {
        ExportBatch batch = requested("dl-done", Duration.ofHours(6));
        stubStatus(batch, "COMPLETED", true);

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_COMPLETED, after.state());
        Assertions.assertEquals(
                EgressDetails.CompletionSource.DEADLINE, after.egress().completionSource());
        List<AuditEventType> types = types(batch);
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_EGRESS_COMPLETED), types.toString());
        Assertions.assertTrue(types.contains(AuditEventType.EXPORT_EVENT_MISSED), types.toString());
        AuditEvent missed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EVENT_MISSED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(1, missed.detail().get("check"));
        Assertions.assertEquals("COMPLETED", missed.detail().get("phaseFound"));
        Assertions.assertEquals(
                StateName.ENQUEUED,
                storage.getJobById(JobIds.of(FinishJob.NAME, batch.id(), 1)).getState());
    }

    @Test
    void completedWithoutTheFileCountsAsStillRunning() {
        ExportBatch batch = requested("dl-nofile", Duration.ofHours(6));
        stubStatus(batch, "COMPLETED", false);

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 1));

        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
        Assertions.assertTrue(types(batch).contains(AuditEventType.EXPORT_EGRESS_STALE));
    }

    @Test
    void failedFailsTheBatchAtEgress() {
        ExportBatch batch = requested("dl-fail", Duration.ofHours(6));
        stubStatus(batch, "FAILED", false);
        UUID running = enqueuer.deadlineCheck(batch.id(), 1, 1, Instant.now().plusSeconds(3600));

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 1));

        Assertions.assertEquals(
                StateName.SCHEDULED,
                storage.getJobById(running).getState(),
                "the deadline check does not delete the job it runs as");
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, after.failedStep());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EGRESS_FAILED", failed.detail().get("cause"));
        Assertions.assertTrue(types(batch).contains(AuditEventType.EXPORT_EVENT_MISSED));
    }

    @Test
    void stillRunningAtCheckOneIsStaleAndSchedulesCheckTwoAtRequestPlusAbandonHours() {
        ExportBatch batch = requested("dl-stale", Duration.ofHours(6));
        stubStatus(batch, "EXPORTING_TEMPLATE", false);

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 1));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_REQUESTED, after.state());
        UUID second = JobIds.deadline(batch.id(), 1, 2);
        Assertions.assertEquals(second.toString(), after.egress().deadlineJobId(), "the pending check is now check 2");
        org.jobrunr.jobs.Job scheduled = storage.getJobById(second);
        Assertions.assertEquals(StateName.SCHEDULED, scheduled.getState());
        Instant expected = batch.egress().requestedAt().plus(Duration.ofHours(48));
        Instant at = ((ScheduledState) scheduled.getJobState()).getScheduledAt();
        Assertions.assertTrue(Duration.between(expected, at).abs().compareTo(Duration.ofMinutes(1)) < 0, at.toString());
        AuditEvent stale = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EGRESS_STALE)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(1, stale.detail().get("check"));
        Assertions.assertEquals("EXPORTING_TEMPLATE", stale.detail().get("phaseLastSeen"));
        Assertions.assertEquals(6L, stale.detail().get("hoursWaiting"));
    }

    @Test
    void stillRunningAtCheckTwoCancelsAndFailsTheBatch() {
        ExportBatch batch = requested("dl-abandon", Duration.ofHours(48));
        stubStatus(batch, "EXPORTING_NDJSON", false);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/"
                        + batch.tenantId() + "/" + batch.egress().correlationId() + "/cancel"))
                .willReturn(WireMock.jsonResponse("{\"status\":\"cancellation_requested\"}", 202)));

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 2));

        WireMockUpstreams.verify(1, WireMock.postRequestedFor(WireMock.urlPathMatching(".*/cancel")));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EGRESS_DID_NOT_FINISH", failed.detail().get("cause"));
        Assertions.assertTrue(types(batch).contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCELLED));
    }

    @Test
    void anUnreachableCancelAtCheckTwoStillAbandonsTheBatch() {
        ExportBatch batch = requested("dl-abandon-down", Duration.ofHours(48));
        stubStatus(batch, "EXPORTING_NDJSON", false);
        WireMockUpstreams.stubFor(WireMock.post(WireMock.urlEqualTo("/api/v1/egress/jobs/practitioner/"
                        + batch.tenantId() + "/" + batch.egress().correlationId() + "/cancel"))
                .willReturn(WireMock.aResponse()
                        .withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER)));

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 2));

        Assertions.assertEquals(
                BatchState.FAILED, batches.find(batch.id()).orElseThrow().state());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EGRESS_DID_NOT_FINISH", failed.detail().get("cause"));
        Assertions.assertTrue(types(batch).contains(AuditEventType.EXPORT_PRIOR_ATTEMPT_CANCEL_REJECTED));
    }

    @Test
    void aBatchNoLongerRequestedIsANoOpWithoutAStatusCall() {
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                npis,
                "dl-noop",
                BatchState.EGRESS_COMPLETED,
                1,
                Instant.now().minusSeconds(3600));

        job.run(new DeadlineCheckJobRequest(batch.id(), 1, 1));

        WireMockUpstreams.verify(
                0, WireMock.getRequestedFor(WireMock.urlPathMatching("/api/v1/egress/status/practitioner/dl-noop/.*")));
        Assertions.assertEquals(
                batch.version(), batches.find(batch.id()).orElseThrow().version());
    }
}
