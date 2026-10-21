package com.certifyos.directory_accuracy.ingestion.events;

import com.certifyos.directory_accuracy.ApiTestProfile;
import com.certifyos.directory_accuracy.MongoResource;
import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.events.PubSubPushMessage;
import com.certifyos.directory_accuracy.export.jobs.JobIds;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures;
import com.certifyos.directory_accuracy.ingestion.InboundFixtures.Seeded;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileRepository;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundLifecycle;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionDeadlineCheckJob;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.directory_accuracy.ingestion.jobs.StageRowsJob;
import com.certifyos.directory_accuracy.ingestion.staging.StagingLifecycle;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

/**
 * The completion handler table (design step 4), row by row, on the api role: file-ingestion's
 * event body as the design quotes it, base64 in the message data. Every tenant here starts with
 * {@code compl-} and is used by one test only.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class CompletionHandlerIT {

    static final int TRAIL_LIMIT = 100;

    @Inject
    CompletionHandler handler;

    @Inject
    InboundFileRepository inbound;

    @Inject
    InboundLifecycle lifecycle;

    @Inject
    StagingLifecycle stagingLifecycle;

    @Inject
    IngestionJobEnqueuer enqueuer;

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

    @Inject
    ObjectMapper mapper;

    @Inject
    ManagedExecutor executor;

    @Inject
    Clock clock;

    /**
     * An {@code INGESTION_REQUESTED} entry whose job is a real deadline check scheduled six hours out,
     * as the request job leaves it.
     */
    private Seeded requested(String tenant) {
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.INGESTION_REQUESTED);
        UUID deadline = enqueuer.deadlineCheck(
                seeded.batch().id(), seeded.entry().uploadId(), 1, Instant.now().plus(Duration.ofHours(6)));
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.combine(
                                Updates.set("inboundFiles.0.jobId", deadline.toString()),
                                Updates.set("inboundFiles.0.ingestionId", ingestionId(tenant))));
        return seeded;
    }

    static String ingestionId(String tenant) {
        return "ing-" + tenant;
    }

    static String externalRef(Seeded seeded) {
        return Ids.externalRef(seeded.batch().id(), InboundFixtures.GENERATION);
    }

    /** The design's success body: {@code COMPLETED} or {@code PARTIAL} with counts. */
    static String parsedBody(String tenant, String externalRef, Seeded seeded, String status) {
        return "{\"ingestionId\":\"" + ingestionId(tenant) + "\","
                + "\"tenantId\":\"" + tenant + "\","
                + "\"identifier\":\"vendor-ingestion\","
                + "\"status\":\"" + status + "\","
                + "\"counts\":{\"populatedRows\":1000,\"validRows\":998,\"invalidRows\":2},"
                + "\"externalRef\":\"" + externalRef + "\","
                + "\"meta\":{\"exportBatchId\":\"" + seeded.batch().id() + "\","
                + "\"uploadId\":\"" + seeded.entry().uploadId() + "\"},"
                + "\"rows\":\"/v1/ingestion-rows/" + ingestionId(tenant) + "\","
                + "\"publishedBy\":\"a field this consumer does not know\"}";
    }

    /** The design's refusal body: {@code INVALID} with a reason and no counts. */
    static String invalidBody(String tenant, String externalRef, String reason) {
        return "{\"ingestionId\":\"" + ingestionId(tenant) + "\","
                + "\"tenantId\":\"" + tenant + "\","
                + "\"identifier\":\"vendor-ingestion\","
                + "\"status\":\"INVALID\","
                + (reason == null ? "" : "\"reason\":\"" + reason + "\",")
                + "\"externalRef\":\"" + externalRef + "\","
                + "\"meta\":{},"
                + "\"rows\":\"/v1/ingestion-rows/" + ingestionId(tenant) + "\"}";
    }

    static PubSubPushMessage.Message message(String messageId, String body) {
        String data = body == null ? null : Base64.getEncoder().encodeToString(body.getBytes(StandardCharsets.UTF_8));
        return new PubSubPushMessage.Message(
                data,
                messageId,
                "2026-10-15T09:14:21Z",
                Map.of("status", "COMPLETED", "identifier", "vendor-ingestion"));
    }

    private InboundFile entry(Seeded seeded) {
        return inbound.findByUploadId(seeded.entry().uploadId()).orElseThrow().entry();
    }

    private List<AuditEvent> trail(Seeded seeded) {
        return audit.trailForBatch(seeded.batch().id(), TRAIL_LIMIT);
    }

    private List<AuditEvent> since(Seeded seeded, int skip) {
        List<AuditEvent> all = trail(seeded);
        return all.subList(skip, all.size());
    }

    private static List<AuditEventType> types(List<AuditEvent> events) {
        return events.stream().map(AuditEvent::type).toList();
    }

    private static UUID stageJobId(Seeded seeded) {
        return JobIds.of(StageRowsJob.NAME, seeded.entry().uploadId(), 1);
    }

    private static UUID deadlineId(Seeded seeded) {
        return JobIds.of(IngestionDeadlineCheckJob.NAME, seeded.entry().uploadId(), 1);
    }

    private boolean jobExists(UUID id) {
        try {
            storage.getJobById(id);
            return true;
        } catch (org.jobrunr.storage.JobNotFoundException missing) {
            return false;
        }
    }

    private double counter(String name, String tag, String value) {
        Counter found = registry.find(name).tag(tag, value).counter();
        return found == null ? 0 : found.count();
    }

    @Test
    void completedParsesEnqueuesAndDeletesDeadline() {
        String tenant = "compl-completed";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();
        double fromEvent = counter(VendorExchangeMetrics.INGESTION_COMPLETION_SOURCE, "source", "EVENT");
        double completed = counter(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS, "status", "COMPLETED");

        CompletionHandler.Outcome outcome =
                handler.handle(message("m-c1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED")));

        Assertions.assertEquals(CompletionHandler.Outcome.PARSED, outcome);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.PARSED, after.state());
        Assertions.assertEquals("COMPLETED", after.parse().status());
        Assertions.assertEquals("EVENT", after.parse().completionSource());
        Assertions.assertEquals(1000L, after.parse().populatedRows());
        Assertions.assertEquals(998L, after.parse().validRows());
        Assertions.assertEquals(2L, after.parse().invalidRows());
        Assertions.assertNotNull(after.parsedAt());
        Assertions.assertEquals(
                StateName.DELETED, storage.getJobById(deadlineId(seeded)).getState(), "deadline check deleted");
        Job stage = storage.getJobById(stageJobId(seeded));
        Assertions.assertEquals(StateName.ENQUEUED, stage.getState());
        Assertions.assertEquals(
                StageRowsJob.class.getName(), stage.getJobDetails().getClassName());
        Assertions.assertEquals(stageJobId(seeded).toString(), after.jobId(), "the stage job now owns the entry");
        List<AuditEvent> written = since(seeded, before);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_PARSED), types(written));
        Assertions.assertEquals("EVENT", written.get(0).detail().get("completionSource"));
        Assertions.assertEquals("COMPLETED", written.get(0).detail().get("status"));
        Assertions.assertEquals(
                fromEvent + 1, counter(VendorExchangeMetrics.INGESTION_COMPLETION_SOURCE, "source", "EVENT"));
        Assertions.assertEquals(
                completed + 1, counter(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS, "status", "COMPLETED"));
    }

    @Test
    void partialIsParsed() {
        String tenant = "compl-partial";
        Seeded seeded = requested(tenant);

        CompletionHandler.Outcome outcome =
                handler.handle(message("m-p1", parsedBody(tenant, externalRef(seeded), seeded, "PARTIAL")));

        Assertions.assertEquals(CompletionHandler.Outcome.PARSED, outcome);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.PARSED, after.state());
        Assertions.assertEquals("PARTIAL", after.parse().status());
        Assertions.assertEquals(2L, after.parse().invalidRows());
        Assertions.assertEquals(stageJobId(seeded).toString(), after.jobId());
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(stageJobId(seeded)).getState());
    }

    @Test
    void invalidFailsIngestion() {
        String tenant = "compl-invalid";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();
        double invalid = counter(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS, "status", "INVALID");

        CompletionHandler.Outcome outcome =
                handler.handle(message("m-i1", invalidBody(tenant, externalRef(seeded), "STRUCTURE_REJECTED")));

        Assertions.assertEquals(CompletionHandler.Outcome.INGESTION_FAILED, outcome);
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.INGESTION_FAILED, after.state());
        Assertions.assertEquals("INVALID", after.parse().status());
        Assertions.assertEquals("STRUCTURE_REJECTED", after.parse().reason());
        Assertions.assertEquals("EVENT", after.parse().completionSource());
        Assertions.assertEquals(
                StateName.DELETED, storage.getJobById(deadlineId(seeded)).getState(), "deadline check deleted");
        Assertions.assertFalse(jobExists(stageJobId(seeded)), "nothing to stage");
        List<AuditEvent> written = since(seeded, before);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_INGESTION_FAILED), types(written));
        Assertions.assertEquals("STRUCTURE_REJECTED", written.get(0).detail().get("reason"));
        Assertions.assertEquals("EVENT", written.get(0).detail().get("completionSource"));
        Assertions.assertEquals(
                invalid + 1, counter(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS, "status", "INVALID"));
    }

    @Test
    void invalidWithoutReasonRecordsInvalid() {
        String tenant = "compl-invalid-bare";
        Seeded seeded = requested(tenant);

        CompletionHandler.Outcome outcome =
                handler.handle(message("m-i2", invalidBody(tenant, externalRef(seeded), null)));

        Assertions.assertEquals(CompletionHandler.Outcome.INGESTION_FAILED, outcome);
        Assertions.assertEquals("INVALID", entry(seeded).parse().reason());
    }

    @Test
    void unknownRefIsAcknowledged() {
        String tenant = "compl-unknown";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();

        Assertions.assertEquals(
                CompletionHandler.Outcome.UNKNOWN_REF,
                handler.handle(message("m-u1", parsedBody(tenant, "not-ours|1", seeded, "COMPLETED"))));
        String withoutRef = parsedBody(tenant, "x", seeded, "COMPLETED").replace("\"externalRef\":\"x\",", "");
        Assertions.assertEquals(CompletionHandler.Outcome.UNKNOWN_REF, handler.handle(message("m-u2", withoutRef)));

        Assertions.assertEquals(
                InboundFileState.INGESTION_REQUESTED, entry(seeded).state());
        Assertions.assertEquals(before, trail(seeded).size());
    }

    @Test
    void tenantMismatchIsRejected() {
        String tenant = "compl-tenant";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();
        String otherTenant = parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED")
                .replace("\"tenantId\":\"" + tenant + "\"", "\"tenantId\":\"compl-intruder\"");
        String noTenant = parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED")
                .replace("\"tenantId\":\"" + tenant + "\",", "");

        Assertions.assertEquals(
                CompletionHandler.Outcome.REJECTED_TENANT_MISMATCH, handler.handle(message("m-t1", otherTenant)));
        Assertions.assertEquals(
                CompletionHandler.Outcome.REJECTED_TENANT_MISMATCH, handler.handle(message("m-t2", noTenant)));

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, after.state());
        Assertions.assertNull(after.parse());
        Assertions.assertEquals(before, trail(seeded).size());
        Assertions.assertEquals(
                StateName.SCHEDULED, storage.getJobById(deadlineId(seeded)).getState());
        Assertions.assertFalse(jobExists(stageJobId(seeded)));
    }

    @Test
    void duplicateEventIsNoOp() {
        String tenant = "compl-dup";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();
        PubSubPushMessage.Message message =
                message("m-d1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"));
        Assertions.assertEquals(CompletionHandler.Outcome.PARSED, handler.handle(message));
        InboundFile first = entry(seeded);

        Assertions.assertEquals(CompletionHandler.Outcome.STATE_PASSED, handler.handle(message));
        Assertions.assertEquals(
                CompletionHandler.Outcome.STATE_PASSED,
                handler.handle(message("m-d2", invalidBody(tenant, externalRef(seeded), "STRUCTURE_REJECTED"))));

        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.PARSED, after.state());
        Assertions.assertEquals(first.parsedAt(), after.parsedAt());
        Assertions.assertEquals(first.updatedAt(), after.updatedAt(), "nothing written by the redelivery");
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_PARSED), types(since(seeded, before)));
    }

    @Test
    void redeliveryOfAParsedEntryWithoutItsStageJobHandsOverAgain() {
        String tenant = "compl-handover";
        Seeded seeded = requested(tenant);
        // the first delivery committed PARSED, then died before the stage job was enqueued
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.set("inboundFiles.0.state", InboundFileState.PARSED.name()));
        int before = trail(seeded).size();

        Assertions.assertEquals(
                CompletionHandler.Outcome.STATE_PASSED,
                handler.handle(message("m-h1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"))));

        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(stageJobId(seeded)).getState());
        Assertions.assertEquals(stageJobId(seeded).toString(), entry(seeded).jobId());
        Assertions.assertEquals(InboundFileState.PARSED, entry(seeded).state());
        Assertions.assertTrue(since(seeded, before).isEmpty(), "no second INBOUND_FILE_PARSED");
    }

    @Test
    void redeliveryAfterTheReconcilerHandedOverEnqueuesNoSecondStageJob() {
        String tenant = "compl-reconciled";
        Seeded seeded = requested(tenant);
        UUID reconciled = IngestionJobEnqueuer.reconciledId(
                StageRowsJob.NAME, seeded.entry().uploadId(), 1, 1);
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.combine(
                                Updates.set("inboundFiles.0.state", InboundFileState.PARSED.name()),
                                Updates.set("inboundFiles.0.reconciliations", 1),
                                Updates.set("inboundFiles.0.jobId", reconciled.toString())));

        Assertions.assertEquals(
                CompletionHandler.Outcome.STATE_PASSED,
                handler.handle(message("m-rc1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"))));

        Assertions.assertFalse(jobExists(stageJobId(seeded)), "the reconciled stage job owns the entry");
        Assertions.assertEquals(reconciled.toString(), entry(seeded).jobId());
    }

    @Test
    void archivingEntryIsRetried() {
        String tenant = "compl-archiving";
        Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.ARCHIVING);
        // RequestIngestionJob writes the external ref with RECEIVED -> ARCHIVING, before register.
        collections
                .batches()
                .updateOne(
                        Filters.eq("_id", seeded.batch().id()),
                        Updates.set("inboundFiles.0.externalRef", externalRef(seeded)));
        InboundFile before = entry(seeded);
        int events = trail(seeded).size();

        ProblemException retry = Assertions.assertThrows(
                ProblemException.class,
                () -> handler.handle(message("m-a1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"))));

        Assertions.assertEquals(503, retry.status());
        Assertions.assertEquals("ARCHIVING_NOT_COMMITTED", retry.code());
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.ARCHIVING, after.state());
        Assertions.assertNull(after.parse());
        Assertions.assertEquals(before.updatedAt(), after.updatedAt());
        Assertions.assertEquals(events, trail(seeded).size());
        Assertions.assertFalse(jobExists(stageJobId(seeded)));
    }

    @Test
    void unparseableIsAcknowledged() {
        Assertions.assertEquals(
                CompletionHandler.Outcome.REJECTED_UNPARSEABLE, handler.handle(message("m-x1", "{not json")));
        Assertions.assertEquals(CompletionHandler.Outcome.REJECTED_UNPARSEABLE, handler.handle(message("m-x2", null)));
        Assertions.assertEquals(
                CompletionHandler.Outcome.REJECTED_UNPARSEABLE,
                handler.handle(new PubSubPushMessage.Message("%%% not base64", "m-x3", null, Map.of())));
        Assertions.assertEquals(CompletionHandler.Outcome.REJECTED_UNPARSEABLE, handler.handle(message("m-x4", "[]")));
    }

    @Test
    void otherStatusIsIgnored() {
        String tenant = "compl-other";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();

        Assertions.assertEquals(
                CompletionHandler.Outcome.IGNORED_STATUS,
                handler.handle(message("m-o1", parsedBody(tenant, externalRef(seeded), seeded, "PARSING"))));

        Assertions.assertEquals(
                InboundFileState.INGESTION_REQUESTED, entry(seeded).state());
        Assertions.assertEquals(before, trail(seeded).size());
        Assertions.assertEquals(
                StateName.SCHEDULED, storage.getJobById(deadlineId(seeded)).getState());
    }

    @Test
    void disabledServiceAcknowledgesWithoutWriting() {
        String tenant = "compl-disabled";
        Seeded seeded = requested(tenant);
        VendorExchangeConfig disabled = Mockito.mock(VendorExchangeConfig.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(disabled.enabled()).thenReturn(false);
        CompletionHandler off =
                new CompletionHandler(mapper, inbound, lifecycle, stagingLifecycle, enqueuer, disabled, clock);

        Assertions.assertEquals(
                CompletionHandler.Outcome.DISABLED,
                off.handle(message("m-off", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"))));

        Assertions.assertEquals(
                InboundFileState.INGESTION_REQUESTED, entry(seeded).state());
        Assertions.assertEquals(
                StateName.SCHEDULED, storage.getJobById(deadlineId(seeded)).getState());
    }

    @Test
    void concurrentCompletionsParseOnce() throws Exception {
        String tenant = "compl-race";
        Seeded seeded = requested(tenant);
        int before = trail(seeded).size();
        ExportBatch batchBefore = batches.find(seeded.batch().id()).orElseThrow();
        PubSubPushMessage.Message message =
                message("m-r1", parsedBody(tenant, externalRef(seeded), seeded, "COMPLETED"));
        CyclicBarrier start = new CyclicBarrier(2);
        List<CompletableFuture<CompletionHandler.Outcome>> calls = new ArrayList<>();
        for (int call = 0; call < 2; call++) {
            calls.add(executor.supplyAsync(() -> {
                awaitTogether(start);
                return handler.handle(message);
            }));
        }
        CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);
        List<CompletionHandler.Outcome> outcomes = new ArrayList<>();
        for (CompletableFuture<CompletionHandler.Outcome> call : calls) {
            outcomes.add(call.get());
        }

        Assertions.assertTrue(
                Set.of(CompletionHandler.Outcome.PARSED, CompletionHandler.Outcome.STATE_PASSED)
                        .containsAll(outcomes),
                "both deliveries acknowledged: " + outcomes);
        Assertions.assertEquals(
                1,
                outcomes.stream()
                        .filter(CompletionHandler.Outcome.PARSED::equals)
                        .count(),
                "one parse: " + outcomes);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_PARSED), types(since(seeded, before)));
        InboundFile after = entry(seeded);
        Assertions.assertEquals(InboundFileState.PARSED, after.state());
        Assertions.assertEquals(stageJobId(seeded).toString(), after.jobId());
        Assertions.assertEquals(
                StateName.ENQUEUED, storage.getJobById(stageJobId(seeded)).getState());
        Assertions.assertEquals(
                batchBefore.version(),
                batches.find(seeded.batch().id()).orElseThrow().version(),
                "entry writes never bump the batch version");
    }

    private static void awaitTogether(CyclicBarrier start) {
        try {
            start.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted before the race", interrupted);
        } catch (BrokenBarrierException | TimeoutException notTogether) {
            throw new IllegalStateException("the deliveries never ran at the same time", notTogether);
        }
    }
}
