package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.batch.BatchState;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.export.batch.ExportNpiRepository;
import com.certifyos.vendor_exchange.export.events.PubSubPushMessage;
import com.certifyos.vendor_exchange.export.jobs.EgressFixtures;
import com.certifyos.vendor_exchange.export.jobs.JobIds;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileState;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundLifecycle;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundNames;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedOperations;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedUpload;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedUploadRepository;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.vendor_exchange.ingestion.jobs.RequestIngestionJob;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Ids;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.bson.Document;
import org.eclipse.microprofile.context.ManagedExecutor;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.StorageProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mockito;

/**
 * The arrival handler table, row by row, on the api role. Every tenant here starts with {@code
 * arrival-} and is used by one test only, so per-tenant counters and batch ids belong to that test.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
class ArrivalHandlerIT {

    private static final String FINALIZE = "OBJECT_FINALIZE";

    @Inject
    ArrivalHandler handler;

    @Inject
    ExportBatchRepository batches;

    @Inject
    InboundFileRepository inbound;

    @Inject
    InboundLifecycle lifecycle;

    @Inject
    UnmatchedOperations unmatchedOperations;

    @Inject
    UnmatchedUploadRepository unmatched;

    @Inject
    IngestionJobEnqueuer enqueuer;

    @Inject
    Transactions transactions;

    @Inject
    Collections collections;

    @Inject
    StorageProvider storage;

    @Inject
    MeterRegistry registry;

    @Inject
    ManagedExecutor executor;

    @Inject
    Clock clock;

    @Test
    void finalizeUnderToClaimsAndEnqueues() {
        String tenant = "arrival-claim";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        PubSubPushMessage.Message message = finalize(tenant, batch.id(), "7001");
        String uploadId = uploadId(tenant, batch.id(), "7001");

        Assertions.assertEquals(ArrivalHandler.Outcome.RECEIVED, handler.handle(message));

        InboundFile entry = inbound.findByUploadId(uploadId).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.RECEIVED, entry.state());
        UUID jobId = JobIds.of(RequestIngestionJob.NAME, uploadId, 1);
        Assertions.assertEquals(jobId.toString(), entry.jobId(), "the job id is recorded after the enqueue");
        Assertions.assertEquals(StateName.ENQUEUED, storage.getJobById(jobId).getState());
        ExportBatch acknowledged = batches.find(batch.id()).orElseThrow();
        Assertions.assertNotNull(acknowledged.acknowledgedAt());
        Assertions.assertEquals(uploadId, acknowledged.inboundBatchId());
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant));
    }

    @Test
    void secondGenerationIsDuplicateLogged() {
        String tenant = "arrival-dup";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        String firstUpload = uploadId(tenant, batch.id(), "7101");
        String secondUpload = uploadId(tenant, batch.id(), "7102");
        Assertions.assertEquals(ArrivalHandler.Outcome.RECEIVED, handler.handle(finalize(tenant, batch.id(), "7101")));

        Assertions.assertEquals(
                ArrivalHandler.Outcome.DUPLICATE_LOGGED, handler.handle(finalize(tenant, batch.id(), "7102")));

        InboundFile duplicate =
                inbound.findByUploadId(secondUpload).orElseThrow().entry();
        Assertions.assertEquals(InboundFileState.DUPLICATE_LOGGED, duplicate.state());
        Assertions.assertNull(duplicate.jobId(), "a duplicate is never processed");
        Assertions.assertThrows(
                org.jobrunr.storage.JobNotFoundException.class,
                () -> storage.getJobById(JobIds.of(RequestIngestionJob.NAME, secondUpload, 1)));
        Document event = collections
                .events()
                .find(Filters.and(
                        Filters.eq("type", AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED.name()),
                        Filters.eq("uploadId", secondUpload)))
                .first();
        Assertions.assertNotNull(event);
        Assertions.assertEquals(firstUpload, event.get("detail", Document.class).getString("firstUploadId"));
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED, tenant));
    }

    @Test
    void redeliveryOfClaimedEventIsNoOp() {
        String tenant = "arrival-redeliver";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        PubSubPushMessage.Message message = finalize(tenant, batch.id(), "7201");
        String uploadId = uploadId(tenant, batch.id(), "7201");
        Assertions.assertEquals(ArrivalHandler.Outcome.RECEIVED, handler.handle(message));
        ExportBatch afterFirst = batches.find(batch.id()).orElseThrow();
        InboundFile entryAfterFirst =
                inbound.findByUploadId(uploadId).orElseThrow().entry();
        long events = eventsFor(batch.id());

        Assertions.assertEquals(ArrivalHandler.Outcome.ALREADY_CLAIMED, handler.handle(message));

        ExportBatch afterSecond = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(afterFirst.version(), afterSecond.version(), "the batch was not written");
        Assertions.assertEquals(1, entries(batch.id()));
        Assertions.assertEquals(
                entryAfterFirst, inbound.findByUploadId(uploadId).orElseThrow().entry(), "the entry was not written");
        Assertions.assertEquals(events, eventsFor(batch.id()), "no event for a redelivery");
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant));
    }

    @Test
    void redeliveryOfReceivedWithoutJobEnqueues() {
        String tenant = "arrival-crashed";
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.RECEIVED);
        Assertions.assertNull(seeded.entry().jobId(), "the crash left no job on the entry");
        String uploadId = seeded.entry().uploadId();
        long events = eventsFor(seeded.batch().id());

        Assertions.assertEquals(
                ArrivalHandler.Outcome.ALREADY_CLAIMED,
                handler.handle(message(
                        "m-crashed",
                        attributes(
                                InboundFixtures.objectName(
                                        tenant, seeded.batch().id()),
                                InboundFixtures.GENERATION,
                                FINALIZE))));

        UUID jobId = JobIds.of(RequestIngestionJob.NAME, uploadId, 1);
        Assertions.assertEquals(StateName.ENQUEUED, storage.getJobById(jobId).getState());
        Assertions.assertEquals(
                jobId.toString(),
                inbound.findByUploadId(uploadId).orElseThrow().entry().jobId());
        Assertions.assertEquals(events, eventsFor(seeded.batch().id()), "recording the job writes no event");
    }

    @Test
    void fromPrefixIsIgnored() {
        String objectName = "from/arrival-from/arrival-from_x_y_20261015.csv";
        PubSubPushMessage.Message message = message("m-from", attributes(objectName, "7301", FINALIZE));

        Assertions.assertEquals(ArrivalHandler.Outcome.IGNORED_PREFIX, handler.handle(message));

        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7301");
        Assertions.assertTrue(unmatched.find(uploadId).isEmpty());
        Assertions.assertEquals(0, collections.events().countDocuments(Filters.eq("uploadId", uploadId)));
    }

    @Test
    void deleteEventIsIgnored() {
        String tenant = "arrival-delete";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        String objectName = InboundFixtures.objectName(tenant, batch.id());
        double before = ignored("OBJECT_DELETE");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.IGNORED_EVENT_TYPE,
                handler.handle(message("m-delete", attributes(objectName, "7401", "OBJECT_DELETE"))));

        Assertions.assertEquals(0, entries(batch.id()));
        Assertions.assertEquals(before + 1, ignored("OBJECT_DELETE"));
    }

    @Test
    void unreadableAttributesAreIgnored() {
        double before = ignored("UNREADABLE");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.IGNORED_EVENT_TYPE,
                handler.handle(
                        new PubSubPushMessage.Message(null, "m-unreadable", null, Map.of("eventType", FINALIZE))));

        Assertions.assertEquals(before + 1, ignored("UNREADABLE"));
    }

    @Test
    void badNameIsUnmatched() {
        String objectName = "to/arrival-bad/notes.txt";
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7501");
        PubSubPushMessage.Message message = message("m-bad", attributes(objectName, "7501", FINALIZE));

        Assertions.assertEquals(ArrivalHandler.Outcome.UNMATCHED, handler.handle(message));

        UnmatchedUpload row = unmatched.find(uploadId).orElseThrow();
        Assertions.assertEquals(InboundNames.UNPARSEABLE_NAME, row.reason());
        Assertions.assertEquals("arrival-bad", row.folderTenant(), "the folder leg is read even when the name fails");
        Assertions.assertNull(row.parsed());
        Document event = unmatchedEvent(uploadId);
        Assertions.assertEquals(Actors.INGESTION, event.getString("actor"));
        Assertions.assertNull(event.getString("tenantId"), "a service event, as EXPORT_EVENT_REJECTED");
        Document detail = event.get("detail", Document.class);
        Assertions.assertEquals(InboundNames.UNPARSEABLE_NAME, detail.getString("reason"));
        Assertions.assertEquals(objectName, detail.getString("objectName"));
        Assertions.assertEquals("7501", detail.getString("generation"));
        Assertions.assertEquals(
                1.0, unmatchedCount("arrival-bad", InboundNames.UNPARSEABLE_NAME), "counted under the folder tenant");

        Assertions.assertEquals(ArrivalHandler.Outcome.UNMATCHED, handler.handle(message));
        Assertions.assertEquals(1, unmatchedEvents(uploadId), "a redelivery records nothing new");
        Assertions.assertEquals(1.0, unmatchedCount("arrival-bad", InboundNames.UNPARSEABLE_NAME));
    }

    @Test
    void unknownBatchIsUnmatched() {
        String tenant = "arrival-unknown";
        String objectName = InboundFixtures.objectName(tenant, tenant + "-candor-2026-10-001-r1");
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7601");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.UNMATCHED,
                handler.handle(message("m-unknown", attributes(objectName, "7601", FINALIZE))));

        UnmatchedUpload row = unmatched.find(uploadId).orElseThrow();
        Assertions.assertEquals(InboundNames.UNKNOWN_BATCH, row.reason());
        Assertions.assertEquals(tenant, row.folderTenant());
        Assertions.assertNotNull(row.parsed());
        Assertions.assertEquals(
                InboundFixtures.VENDOR_BATCH_ID, row.parsed().vendorBatchId(), "the parsed legs are kept");
        Document detail = unmatchedEvent(uploadId).get("detail", Document.class);
        Assertions.assertNotNull(detail.getString("parsedLegs"), "a parsed name records its legs");
        Assertions.assertEquals(1.0, unmatchedCount(tenant, InboundNames.UNKNOWN_BATCH));
    }

    @Test
    void tenantMismatchIsUnmatched() {
        String fileTenant = "arrival-mismatch-a";
        String folderTenant = "arrival-mismatch-b";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, fileTenant);
        String objectName = InboundNames.TO_PREFIX + folderTenant + "/" + fileTenant + "_" + batch.id() + "_"
                + InboundFixtures.VENDOR_BATCH_ID + "_20261015.csv";
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7701");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.UNMATCHED,
                handler.handle(message("m-mismatch", attributes(objectName, "7701", FINALIZE))));

        Assertions.assertEquals(
                InboundNames.TENANT_MISMATCH,
                unmatched.find(uploadId).orElseThrow().reason());
        Assertions.assertEquals(0, entries(batch.id()), "nothing claimed");
        Assertions.assertEquals(1.0, unmatchedCount(folderTenant, InboundNames.TENANT_MISMATCH));
    }

    @Test
    void folderTenantCannotClaimAnotherTenantsBatch() {
        String owner = "arrival-xt-a";
        String intruder = "arrival-xt-b";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, owner);
        String objectName = InboundNames.TO_PREFIX + intruder + "/" + intruder + "_" + batch.id() + "_"
                + InboundFixtures.VENDOR_BATCH_ID + "_20261015.csv";
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7751");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.UNMATCHED,
                handler.handle(message("m-cross", attributes(objectName, "7751", FINALIZE))));

        Assertions.assertEquals(
                InboundNames.TENANT_MISMATCH,
                unmatched.find(uploadId).orElseThrow().reason());
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(0, entries(batch.id()), "the owner's batch is untouched");
        Assertions.assertEquals(batch.version(), after.version());
        Assertions.assertNull(after.acknowledgedAt());
        Assertions.assertEquals(1.0, unmatchedCount(intruder, InboundNames.TENANT_MISMATCH));
    }

    @Test
    void deliveredBatchWithoutFileIsUnmatched() {
        String tenant = "arrival-nofile";
        ExportBatch delivered = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        collections.batches().updateOne(Filters.eq("_id", delivered.id()), Updates.unset("file"));
        ExportBatch batch = batches.find(delivered.id()).orElseThrow();
        Assertions.assertNull(batch.file());
        String objectName = InboundFixtures.objectName(tenant, batch.id());
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7851");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.UNMATCHED,
                handler.handle(message("m-nofile", attributes(objectName, "7851", FINALIZE))));

        Assertions.assertEquals(
                InboundNames.BATCH_NOT_DELIVERED,
                unmatched.find(uploadId).orElseThrow().reason());
        Assertions.assertEquals(0, entries(batch.id()));
    }

    @Test
    void batchNotDeliveredIsUnmatched() {
        String tenant = "arrival-early";
        ExportBatch batch = EgressFixtures.batchIn(
                transactions,
                batches,
                new ExportNpiRepository(collections),
                tenant,
                BatchState.EGRESS_REQUESTED,
                1,
                InboundFixtures.DELIVERED_AT);
        String objectName = InboundFixtures.objectName(tenant, batch.id());
        String uploadId = Ids.uploadId(InboundFixtures.VENDOR_BUCKET, objectName, "7801");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.UNMATCHED,
                handler.handle(message("m-early", attributes(objectName, "7801", FINALIZE))));

        Assertions.assertEquals(
                InboundNames.BATCH_NOT_DELIVERED,
                unmatched.find(uploadId).orElseThrow().reason());
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(0, entries(batch.id()));
        Assertions.assertEquals(batch.version(), after.version());
    }

    @Test
    void supersededBatchIsMatched() {
        String tenant = "arrival-superseded";
        ExportBatch delivered = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        transactions.run(session -> batches.transition(
                session,
                delivered.id(),
                BatchState.DELIVERED,
                BatchState.SUPERSEDED,
                InboundFixtures.DELIVERED_AT.plusSeconds(3600),
                null));
        String uploadId = uploadId(tenant, delivered.id(), "7901");

        Assertions.assertEquals(
                ArrivalHandler.Outcome.RECEIVED, handler.handle(finalize(tenant, delivered.id(), "7901")));

        ExportBatch after = batches.find(delivered.id()).orElseThrow();
        Assertions.assertEquals(
                BatchState.SUPERSEDED, after.state(), "the echo binds the file to the cycle that asked");
        Assertions.assertEquals(uploadId, after.inboundBatchId());
        Assertions.assertEquals(
                InboundFileState.RECEIVED,
                inbound.findByUploadId(uploadId).orElseThrow().entry().state());
    }

    @Test
    void disabledServiceAcknowledgesWithoutWriting() {
        String tenant = "arrival-disabled";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        VendorExchangeConfig disabled = Mockito.mock(VendorExchangeConfig.class, Answers.RETURNS_DEEP_STUBS);
        Mockito.when(disabled.enabled()).thenReturn(false);
        Mockito.when(disabled.ingestion().completenessSignal()).thenReturn("DATA_FILE");
        ArrivalHandler off =
                new ArrivalHandler(batches, inbound, lifecycle, unmatchedOperations, enqueuer, disabled, clock);
        String badName = "to/" + tenant + "/notes.txt";

        Assertions.assertEquals(ArrivalHandler.Outcome.DISABLED, off.handle(finalize(tenant, batch.id(), "8001")));
        Assertions.assertEquals(
                ArrivalHandler.Outcome.DISABLED, off.handle(message("m-off", attributes(badName, "8002", FINALIZE))));

        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(0, entries(batch.id()));
        Assertions.assertEquals(batch.version(), after.version());
        Assertions.assertTrue(unmatched
                .find(Ids.uploadId(InboundFixtures.VENDOR_BUCKET, badName, "8002"))
                .isEmpty());
        Assertions.assertEquals(0.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant));
    }

    @Test
    void concurrentArrivalsAcknowledgeOnce() throws Exception {
        String tenant = "arrival-race";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        PubSubPushMessage.Message message = finalize(tenant, batch.id(), "8101");
        String uploadId = uploadId(tenant, batch.id(), "8101");
        CyclicBarrier start = new CyclicBarrier(2);
        List<CompletableFuture<ArrivalHandler.Outcome>> calls = new ArrayList<>();
        for (int call = 0; call < 2; call++) {
            calls.add(executor.supplyAsync(() -> {
                awaitTogether(start);
                return handler.handle(message);
            }));
        }
        CompletableFuture.allOf(calls.toArray(CompletableFuture[]::new)).get(120, TimeUnit.SECONDS);
        List<ArrivalHandler.Outcome> outcomes = new ArrayList<>();
        for (CompletableFuture<ArrivalHandler.Outcome> call : calls) {
            outcomes.add(call.get());
        }

        Assertions.assertTrue(
                Set.of(ArrivalHandler.Outcome.RECEIVED, ArrivalHandler.Outcome.ALREADY_CLAIMED)
                        .containsAll(outcomes),
                "both deliveries acknowledged: " + outcomes);
        Assertions.assertEquals(
                1,
                outcomes.stream()
                        .filter(ArrivalHandler.Outcome.RECEIVED::equals)
                        .count(),
                "one claim: " + outcomes);
        ExportBatch after = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(1, entries(batch.id()));
        Assertions.assertEquals(batch.version() + 1, after.version(), "the acknowledgement bumped the version once");
        Assertions.assertEquals(
                1,
                collections
                        .events()
                        .countDocuments(Filters.and(
                                Filters.eq("type", AuditEventType.EXPORT_BATCH_ACKNOWLEDGED.name()),
                                Filters.eq("exportBatchId", batch.id()))));
        UUID jobId = JobIds.of(RequestIngestionJob.NAME, uploadId, 1);
        Assertions.assertEquals(
                jobId.toString(),
                inbound.findByUploadId(uploadId).orElseThrow().entry().jobId());
        Assertions.assertEquals(StateName.ENQUEUED, storage.getJobById(jobId).getState());
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

    private static PubSubPushMessage.Message finalize(String tenant, String exportBatchId, String generation) {
        return message(
                "m-" + generation, attributes(InboundFixtures.objectName(tenant, exportBatchId), generation, FINALIZE));
    }

    private static String uploadId(String tenant, String exportBatchId, String generation) {
        return Ids.uploadId(
                InboundFixtures.VENDOR_BUCKET, InboundFixtures.objectName(tenant, exportBatchId), generation);
    }

    static Map<String, String> attributes(String objectName, String generation, String eventType) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("bucketId", InboundFixtures.VENDOR_BUCKET);
        attributes.put("objectId", objectName);
        attributes.put("objectGeneration", generation);
        attributes.put("eventType", eventType);
        attributes.put("eventTime", InboundFixtures.ARRIVED_AT.toString());
        return attributes;
    }

    private static PubSubPushMessage.Message message(String messageId, Map<String, String> attributes) {
        return new PubSubPushMessage.Message(null, messageId, Instant.now().toString(), attributes);
    }

    private int entries(String exportBatchId) {
        Document doc =
                collections.batches().find(Filters.eq("_id", exportBatchId)).first();
        List<Document> files = doc == null ? null : doc.getList("inboundFiles", Document.class);
        return files == null ? 0 : files.size();
    }

    private long eventsFor(String exportBatchId) {
        return collections.events().countDocuments(Filters.eq("exportBatchId", exportBatchId));
    }

    private Document unmatchedEvent(String uploadId) {
        Document event = collections
                .events()
                .find(Filters.and(
                        Filters.eq("type", AuditEventType.INBOUND_FILE_UNMATCHED.name()),
                        Filters.eq("uploadId", uploadId)))
                .first();
        Assertions.assertNotNull(event, "INBOUND_FILE_UNMATCHED for " + uploadId);
        return event;
    }

    private long unmatchedEvents(String uploadId) {
        return collections
                .events()
                .countDocuments(Filters.and(
                        Filters.eq("type", AuditEventType.INBOUND_FILE_UNMATCHED.name()),
                        Filters.eq("uploadId", uploadId)));
    }

    private double counter(String name, String tenant) {
        Counter found = registry.find(name).tag("tenant", tenant).counter();
        return found == null ? 0 : found.count();
    }

    private double unmatchedCount(String tenant, String reason) {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_ARRIVALS_UNMATCHED)
                .tag("tenant", tenant)
                .tag("reason", reason)
                .counter();
        return found == null ? 0 : found.count();
    }

    private double ignored(String eventType) {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_ARRIVALS_IGNORED_EVENT_TYPE)
                .tag("eventType", eventType)
                .counter();
        return found == null ? 0 : found.count();
    }
}
