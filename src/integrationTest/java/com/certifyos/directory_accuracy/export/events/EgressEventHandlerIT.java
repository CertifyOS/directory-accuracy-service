package com.certifyos.directory_accuracy.export.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.auth.PushIdentity;
import com.certifyos.directory_accuracy.auth.PushTokenVerifier;
import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.EgressDetails;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.jobs.EgressFixtures;
import com.certifyos.directory_accuracy.export.jobs.FinishJob;
import com.certifyos.directory_accuracy.export.jobs.JobEnqueuer;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Every row of the design's handler table, through the push endpoint with a verified token. */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class EgressEventHandlerIT {

    static final String PATH = "/internal/vendor-exports/egress-events";

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
    JobEnqueuer enqueuer;

    @InjectMock
    PushTokenVerifier verifier;

    @BeforeEach
    void before() {
        Mockito.when(verifier.verify("good")).thenReturn(new PushIdentity("pubsub-push@test.iam.gserviceaccount.com"));
    }

    private ExportBatch requested(String tenant) {
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                tenant,
                BatchState.EGRESS_REQUESTED,
                3,
                Instant.now().minusSeconds(3600));
        enqueuer.deadlineCheck(batch.id(), 1, 1, Instant.now().plusSeconds(3600));
        return batch;
    }

    private static String event(String tenant, String correlationId, String phase, String schema) {
        return "{\"schemaVersion\":\"" + schema + "\",\"type\":\"practitioner\",\"tenantId\":\"" + tenant + "\","
                + "\"correlationId\":\"" + correlationId + "\",\"phase\":\"" + phase + "\","
                + "\"outputUri\":\"gs://test-vendor-bucket/from/" + tenant
                + "/f.csv\",\"totalRecords\":3,\"totalRows\":5,"
                + (phase.equals("FAILED") ? "\"failureMessage\":\"pipeline blew up\"," : "")
                + "\"completedAt\":\"2026-10-01T07:12:00Z\"}";
    }

    private static String envelope(String messageId, String eventJson) {
        String data = Base64.getEncoder().encodeToString(eventJson.getBytes(StandardCharsets.UTF_8));
        return "{\"message\":{\"data\":\"" + data + "\",\"messageId\":\"" + messageId
                + "\",\"publishTime\":\"2026-10-01T07:12:01Z\",\"attributes\":{\"initiator\":\"directory-accuracy-worker\"}},"
                + "\"subscription\":\"projects/p/subscriptions/vendor-export-egress-events\"}";
    }

    private static void push(String messageId, String eventJson) {
        RestAssured.given()
                .header("Authorization", "Bearer good")
                .contentType("application/json")
                .body(envelope(messageId, eventJson))
                .post(PATH)
                .then()
                .statusCode(204);
    }

    private long rejected(String messageId, String reason) {
        return collections
                .events()
                .countDocuments(Filters.and(
                        Filters.eq("type", AuditEventType.EXPORT_EVENT_REJECTED.name()),
                        Filters.eq("detail.messageId", messageId),
                        Filters.eq("detail.reason", reason)));
    }

    private long received(String messageId) {
        return collections
                .events()
                .countDocuments(Filters.and(
                        Filters.eq("type", AuditEventType.EXPORT_EVENT_RECEIVED.name()),
                        Filters.eq("detail.messageId", messageId)));
    }

    @Test
    void unparseableAndUnknownSchemaAreAcknowledgedAndRejected() {
        push("m-bad", "not json");
        Assertions.assertEquals(1, rejected("m-bad", "UNPARSEABLE"));

        push(
                "m-schema",
                event("evt-schema", "evt-schema-candor-2026-10-001-r1", "COMPLETED", "egress-export-event-v9"));
        Assertions.assertEquals(1, rejected("m-schema", "UNKNOWN_SCHEMA"));
    }

    @Test
    void unknownCorrelationIsAcknowledgedWithoutAnEvent() {
        push(
                "m-unknown",
                event(
                        "evt-nobody",
                        "evt-nobody-candor-2026-10-001-r1",
                        "COMPLETED",
                        EgressEventHandler.SCHEMA_VERSION));
        Assertions.assertEquals(0, rejected("m-unknown", "UNPARSEABLE") + received("m-unknown"));
        Assertions.assertEquals(
                0,
                collections.events().countDocuments(Filters.eq("detail.messageId", "m-unknown")),
                "nothing is written for a correlation that is not ours");
    }

    @Test
    void tenantMismatchIsRejectedAndTheBatchUntouched() {
        ExportBatch batch = requested("evt-mismatch");
        push(
                "m-mismatch",
                event("evt-other", batch.egress().correlationId(), "COMPLETED", EgressEventHandler.SCHEMA_VERSION));
        Assertions.assertEquals(1, rejected("m-mismatch", "TENANT_MISMATCH"));
        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
    }

    @Test
    void completedMovesTheBatchWritesBothEventsEnqueuesFinishAndDeletesTheDeadline() {
        ExportBatch batch = requested("evt-done");
        UUID deadline = EgressFixtures.deadlineId(batch);
        Assertions.assertEquals(
                StateName.SCHEDULED, storage.getJobById(deadline).getState());

        push(
                "m-done",
                event("evt-done", batch.egress().correlationId(), "COMPLETED", EgressEventHandler.SCHEMA_VERSION));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_COMPLETED, after.state());
        Assertions.assertEquals(
                EgressDetails.CompletionSource.EVENT, after.egress().completionSource());
        Assertions.assertEquals(
                Instant.parse("2026-10-01T07:12:00Z"), after.egress().completedAt());
        Assertions.assertEquals(1, received("m-done"));
        List<AuditEvent> trail = audit.findForBatch(batch.id(), 20);
        AuditEvent completed = trail.stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_EGRESS_COMPLETED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EVENT", completed.detail().get("completionSource"));
        Assertions.assertEquals(3L, completed.detail().get("totalRecords"));
        Assertions.assertNotNull(completed.detail().get("waitSeconds"));
        Assertions.assertEquals(
                StateName.ENQUEUED,
                storage.getJobById(JobIds.of(FinishJob.NAME, batch.id(), 1)).getState());
        Assertions.assertEquals(StateName.DELETED, storage.getJobById(deadline).getState(), "deadline check deleted");
    }

    @Test
    void aSecondDeliveryOfTheSameMessageWritesNothing() {
        ExportBatch batch = requested("evt-twice");
        String json =
                event("evt-twice", batch.egress().correlationId(), "COMPLETED", EgressEventHandler.SCHEMA_VERSION);
        push("m-twice", json);
        long eventsAfterFirst = collections.events().countDocuments(Filters.eq("exportBatchId", batch.id()));
        long version = batches.find(batch.id()).orElseThrow().version();

        push("m-twice", json);

        Assertions.assertEquals(1, received("m-twice"));
        Assertions.assertEquals(
                eventsAfterFirst, collections.events().countDocuments(Filters.eq("exportBatchId", batch.id())));
        Assertions.assertEquals(version, batches.find(batch.id()).orElseThrow().version(), "no second write");
    }

    @Test
    void aMessageForABatchPastEgressRequestedHasNoEffect() {
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                "evt-late",
                BatchState.EGRESS_COMPLETED,
                1,
                Instant.now().minusSeconds(3600));
        push("m-late", event("evt-late", batch.egress().correlationId(), "FAILED", EgressEventHandler.SCHEMA_VERSION));
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.EGRESS_COMPLETED, after.state());
        Assertions.assertEquals(batch.version(), after.version());
        Assertions.assertEquals(0, received("m-late"));
    }

    @Test
    void failedMovesTheBatchToFailedAtEgressAndDeletesTheDeadline() {
        ExportBatch batch = requested("evt-fail");
        UUID deadline = EgressFixtures.deadlineId(batch);

        push("m-fail", event("evt-fail", batch.egress().correlationId(), "FAILED", EgressEventHandler.SCHEMA_VERSION));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(BatchState.FAILED, after.state());
        Assertions.assertEquals(ExportBatch.FailedStep.EGRESS, after.failedStep());
        Assertions.assertEquals("pipeline blew up", after.lastError());
        AuditEvent failed = audit.findForBatch(batch.id(), 20).stream()
                .filter(found -> found.type() == AuditEventType.EXPORT_BATCH_FAILED)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals("EGRESS_FAILED", failed.detail().get("cause"));
        Assertions.assertEquals(1, received("m-fail"));
        Assertions.assertEquals(StateName.DELETED, storage.getJobById(deadline).getState());
    }

    @Test
    void intermediatePhasesAreAcknowledgedWithoutEffect() {
        ExportBatch batch = requested("evt-mid");
        push(
                "m-mid",
                event(
                        "evt-mid",
                        batch.egress().correlationId(),
                        "EXPORTING_TEMPLATE",
                        EgressEventHandler.SCHEMA_VERSION));
        Assertions.assertEquals(
                BatchState.EGRESS_REQUESTED,
                batches.find(batch.id()).orElseThrow().state());
        Assertions.assertEquals(0, received("m-mid"));
    }
}
