package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ApiTestProfile;
import com.certifyos.vendor_exchange.MongoResource;
import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.clients.ArchiveStore;
import com.certifyos.vendor_exchange.clients.FileIngestionCounts;
import com.certifyos.vendor_exchange.export.batch.BatchState;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.export.batch.ExportBatchRepository;
import com.certifyos.vendor_exchange.ingestion.InboundFixtures;
import com.certifyos.vendor_exchange.ingestion.events.ArrivalEvent;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository.Located;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionLogContext;
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
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.jobrunr.jobs.context.JobContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * The inbound lifecycle: each method one transaction that moves an entry (or claims one) and writes
 * its audit event. Every tenant here starts with {@code inlife-}, so the per-tenant arrival counters
 * belong to this class alone.
 */
@QuarkusTest
@QuarkusTestResource(MongoResource.class)
@TestProfile(ApiTestProfile.class)
public class InboundLifecycleIT {

    private static final Instant NOW = Instant.parse("2026-10-15T10:00:00Z");
    private static final int TRAIL_LIMIT = 100;

    @Inject
    InboundLifecycle lifecycle;

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
    MeterRegistry registry;

    @Test
    void receivedAcknowledgesTheBatchOnce() {
        String tenant = "inlife-ack";
        ExportBatch delivered = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        Instant deliveredAt = Instant.parse("2026-10-13T06:00:00Z");
        collections.batches().updateOne(Filters.eq("_id", delivered.id()), Updates.set("deliveredAt", deliveredAt));
        ExportBatch batch = batches.find(delivered.id()).orElseThrow();
        InboundFile first = upload(tenant, batch.id(), "5001", "4999");
        InboundFile second = upload(tenant, batch.id(), "5002", null);
        int before = trail(batch.id()).size();

        Assertions.assertEquals(InboundLifecycle.Outcome.RECEIVED, lifecycle.received(batch, first, NOW));

        ExportBatch acknowledged = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(first.eventTime(), acknowledged.acknowledgedAt());
        Assertions.assertEquals(first.uploadId(), acknowledged.inboundBatchId());
        Assertions.assertEquals(
                InboundFileState.RECEIVED,
                inbound.findByUploadId(first.uploadId()).orElseThrow().entry().state());
        List<AuditEvent> firstEvents = since(batch.id(), before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_RECEIVED, AuditEventType.EXPORT_BATCH_ACKNOWLEDGED),
                types(firstEvents),
                "received, then acknowledged, in insertion order");
        AuditEvent received = firstEvents.get(0);
        assertEnvelope(received, tenant, batch.id(), first.uploadId());
        Assertions.assertEquals(first.objectName(), received.detail().get("objectName"));
        Assertions.assertEquals("5001", received.detail().get("generation"));
        Assertions.assertEquals(
                InboundFixtures.VENDOR_BATCH_ID, received.detail().get("vendorBatchId"));
        Assertions.assertEquals(first.eventTime(), toInstant(received.detail().get("eventTime")));
        Assertions.assertEquals("4999", received.detail().get("overwroteGeneration"));
        AuditEvent acknowledgement = firstEvents.get(1);
        assertEnvelope(acknowledgement, tenant, batch.id(), first.uploadId());
        Assertions.assertEquals(first.uploadId(), acknowledgement.detail().get("uploadId"));
        Assertions.assertEquals(
                InboundFixtures.VENDOR_BATCH_ID, acknowledgement.detail().get("vendorBatchId"));
        Assertions.assertEquals(
                2L,
                ((Number) acknowledgement.detail().get("daysToResponse")).longValue(),
                "delivered 2026-10-13T06:00Z, arrived 2026-10-15T09:12Z: two whole days");

        Assertions.assertEquals(
                InboundLifecycle.Outcome.DUPLICATE_LOGGED,
                lifecycle.received(acknowledged, second, NOW.plusSeconds(60)));

        ExportBatch afterDuplicate = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(acknowledged.acknowledgedAt(), afterDuplicate.acknowledgedAt());
        Assertions.assertEquals(acknowledged.inboundBatchId(), afterDuplicate.inboundBatchId());
        Assertions.assertEquals(acknowledged.version(), afterDuplicate.version(), "the batch was not written");
        List<AuditEvent> duplicateEvents = since(batch.id(), before + 2);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED), types(duplicateEvents));
        AuditEvent duplicate = duplicateEvents.get(0);
        assertEnvelope(duplicate, tenant, batch.id(), second.uploadId());
        Assertions.assertEquals(second.objectName(), duplicate.detail().get("objectName"));
        Assertions.assertEquals("5002", duplicate.detail().get("generation"));
        Assertions.assertEquals(first.uploadId(), duplicate.detail().get("firstUploadId"));
        Assertions.assertEquals("RECEIVED", duplicate.detail().get("firstUploadState"));

        Assertions.assertEquals(
                InboundLifecycle.Outcome.ALREADY_CLAIMED,
                lifecycle.received(afterDuplicate, first, NOW.plusSeconds(120)),
                "a redelivered event writes nothing");
        Assertions.assertEquals(before + 3, trail(batch.id()).size());
        Assertions.assertEquals(
                afterDuplicate.version(), batches.find(batch.id()).orElseThrow().version());
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant));
        Assertions.assertEquals(1.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_DUPLICATE_LOGGED, tenant));
    }

    @Test
    void receivedRollsBackTheClaimWhenTheBatchLeftTheStateRead() {
        String tenant = "inlife-rollback";
        ExportBatch delivered = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        transactions.run(session -> batches.transition(
                session, delivered.id(), BatchState.DELIVERED, BatchState.SUPERSEDED, NOW.minusSeconds(60), null));
        ExportBatch superseded = batches.find(delivered.id()).orElseThrow();
        Assertions.assertEquals(BatchState.SUPERSEDED, superseded.state());
        InboundFile first = upload(tenant, delivered.id(), "5401", null);
        int before = trail(delivered.id()).size();

        Assertions.assertThrows(
                IllegalStateException.class,
                () -> lifecycle.received(delivered, first, NOW),
                "the snapshot says DELIVERED, the stored batch is SUPERSEDED");

        Assertions.assertTrue(
                inbound.findByUploadId(first.uploadId()).isEmpty(), "the claim rolled back with the acknowledgement");
        Assertions.assertEquals(before, trail(delivered.id()).size(), "no event");
        ExportBatch after = batches.find(delivered.id()).orElseThrow();
        Assertions.assertEquals(superseded.version(), after.version(), "the batch was not written");
        Assertions.assertNull(after.acknowledgedAt());
        Assertions.assertNull(after.inboundBatchId());
        Assertions.assertEquals(0.0, counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant));
    }

    @Test
    void jobWrittenEventsCarryTheJobIdAndHttpPathEventsDoNot() {
        String tenant = "inlife-jobid";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        InboundFile first = upload(tenant, batch.id(), "5501", null);
        int before = trail(batch.id()).size();
        UUID jobId = UUID.fromString("44444444-4444-4444-4444-444444444444");
        JobContext running = new JobContext() {
            @Override
            public UUID getJobId() {
                return jobId;
            }
        };

        Assertions.assertEquals(InboundLifecycle.Outcome.RECEIVED, lifecycle.received(batch, first, NOW));
        try (IngestionLogContext ignored = IngestionLogContext.open(batch.id(), first.uploadId(), running)) {
            Assertions.assertTrue(
                    lifecycle.archiving(inbound.findByUploadId(first.uploadId()).orElseThrow(), NOW.plusSeconds(1)));
            Assertions.assertTrue(lifecycle.archived(
                    inbound.findByUploadId(first.uploadId()).orElseThrow(),
                    new ArchiveStore.Copied("gs://vendor-ingestion-archive/inlife-jobid/5501.csv", 1L, false),
                    NOW.plusSeconds(2)));
        }

        List<AuditEvent> events = since(batch.id(), before);
        Assertions.assertEquals(
                List.of(
                        AuditEventType.INBOUND_FILE_RECEIVED,
                        AuditEventType.EXPORT_BATCH_ACKNOWLEDGED,
                        AuditEventType.INBOUND_FILE_ARCHIVED),
                types(events));
        Assertions.assertNull(events.get(0).jobId(), "the arrival is an HTTP path: no job");
        Assertions.assertNull(events.get(1).jobId(), "the arrival is an HTTP path: no job");
        Assertions.assertEquals(jobId.toString(), events.get(2).jobId(), "the job that wrote the event");
    }

    @Test
    void claimInWritesTheEventsInTheCallersTransactionWithTheCallersActor() {
        String tenant = "inlife-operator";
        String operator = "user:operator@example.com";
        ExportBatch batch = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        InboundFile first = upload(tenant, batch.id(), "5201", null);
        InboundFile second = upload(tenant, batch.id(), "5202", null);
        int before = trail(batch.id()).size();

        Assertions.assertEquals(
                InboundLifecycle.Outcome.RECEIVED,
                transactions.run(session -> lifecycle.claimIn(session, batch, first, operator, NOW)));
        ExportBatch acknowledged = batches.find(batch.id()).orElseThrow();
        Assertions.assertEquals(first.uploadId(), acknowledged.inboundBatchId());
        Assertions.assertEquals(
                InboundLifecycle.Outcome.DUPLICATE_LOGGED,
                transactions.run(session -> lifecycle.claimIn(session, acknowledged, second, operator, NOW)));

        List<AuditEvent> events = since(batch.id(), before);
        Assertions.assertEquals(
                List.of(
                        AuditEventType.INBOUND_FILE_RECEIVED,
                        AuditEventType.EXPORT_BATCH_ACKNOWLEDGED,
                        AuditEventType.INBOUND_FILE_DUPLICATE_LOGGED),
                types(events));
        for (AuditEvent event : events) {
            Assertions.assertEquals(operator, event.actor(), event.type() + " names the caller");
        }
        Assertions.assertEquals(
                0.0,
                counter(VendorExchangeMetrics.INGESTION_ARRIVALS_RECEIVED, tenant),
                "claimIn records no metric; its caller does after the commit");
    }

    @Test
    void jobRecordedSetsTheJobIdOnlyInReceived() {
        InboundFixtures.Seeded received = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, "inlife-jobrec", InboundFileState.RECEIVED);
        InboundFixtures.Seeded archiving = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, "inlife-jobrec-moved", InboundFileState.ARCHIVING);
        UUID jobId = UUID.fromString("33333333-3333-3333-3333-333333333333");
        int before = trail(received.batch().id()).size();

        Assertions.assertTrue(lifecycle.jobRecorded(
                inbound.findByUploadId(received.entry().uploadId()).orElseThrow(), jobId, NOW));
        InboundFile recorded = entry(received.entry().uploadId());
        Assertions.assertEquals(jobId.toString(), recorded.jobId());
        Assertions.assertEquals(InboundFileState.RECEIVED, recorded.state());
        Assertions.assertEquals(before, trail(received.batch().id()).size(), "no event for a job id");

        Assertions.assertFalse(lifecycle.jobRecorded(
                inbound.findByUploadId(archiving.entry().uploadId()).orElseThrow(), jobId, NOW));
        Assertions.assertEquals(
                archiving.entry().jobId(),
                entry(archiving.entry().uploadId()).jobId(),
                "an entry past RECEIVED keeps its job");
    }

    @Test
    void acknowledgementCarriesTheBatchAttemptAndTheFileEventTheEntryAttempt() {
        String tenant = "inlife-attempt";
        ExportBatch delivered = InboundFixtures.deliveredBatch(transactions, batches, collections, tenant);
        collections.batches().updateOne(Filters.eq("_id", delivered.id()), Updates.set("attempt", 2));
        ExportBatch batch = batches.find(delivered.id()).orElseThrow();
        Assertions.assertEquals(2, batch.attempt());
        InboundFile first = upload(tenant, batch.id(), "5101", null);
        int before = trail(batch.id()).size();

        Assertions.assertEquals(InboundLifecycle.Outcome.RECEIVED, lifecycle.received(batch, first, NOW));

        List<AuditEvent> events = since(batch.id(), before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_RECEIVED, AuditEventType.EXPORT_BATCH_ACKNOWLEDGED), types(events));
        Assertions.assertEquals(1, events.get(0).attempt(), "a file event carries the entry's ingestion attempt");
        Assertions.assertEquals(2, events.get(1).attempt(), "an export event carries the batch's attempt");
    }

    @Test
    void archivingThenArchivedThenRequestedWriteTheirEvents() {
        String tenant = "inlife-path";
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.RECEIVED);
        String batchId = seeded.batch().id();
        String uploadId = seeded.entry().uploadId();
        Located received = inbound.findByUploadId(uploadId).orElseThrow();
        int before = trail(batchId).size();

        Assertions.assertTrue(lifecycle.archiving(received, NOW));
        Assertions.assertEquals(InboundFileState.ARCHIVING, entry(uploadId).state());
        Assertions.assertEquals(before, trail(batchId).size(), "archiving writes no event: nothing was copied yet");
        Assertions.assertFalse(lifecycle.archiving(received, NOW.plusSeconds(1)), "a second job finds it moved");
        Assertions.assertEquals(
                uploadId,
                inbound.findByExternalRef(Ids.externalRef(batchId, InboundFixtures.GENERATION))
                        .orElseThrow()
                        .entry()
                        .uploadId(),
                "a completion event finds the entry from the first job write");

        Located archiving = inbound.findByUploadId(uploadId).orElseThrow();
        String archivePath = "gs://vendor-ingestion-archive/inlife-path/" + InboundFixtures.GENERATION + ".csv";
        Assertions.assertTrue(
                lifecycle.archived(archiving, new ArchiveStore.Copied(archivePath, 52_000L, true), NOW.plusSeconds(5)));
        InboundFile archived = entry(uploadId);
        Assertions.assertEquals(InboundFileState.ARCHIVING, archived.state(), "archived records the copy in place");
        Assertions.assertEquals(archivePath, archived.archivePath());
        Assertions.assertEquals(52_000L, archived.archiveBytes());

        UUID deadlineJobId = UUID.fromString("22222222-2222-2222-2222-222222222222");
        Instant deadlineAt = NOW.plusSeconds(6 * 3600);
        String externalRef = Ids.externalRef(batchId, InboundFixtures.GENERATION);
        Assertions.assertTrue(lifecycle.ingestionRequested(
                archiving,
                "ing_01HX",
                externalRef,
                "mapping-candor",
                "201",
                deadlineJobId,
                deadlineAt,
                NOW.plusSeconds(10)));
        InboundFile requested = entry(uploadId);
        Assertions.assertEquals(InboundFileState.INGESTION_REQUESTED, requested.state());
        Assertions.assertEquals(externalRef, requested.externalRef());
        Assertions.assertEquals("ing_01HX", requested.ingestionId());
        Assertions.assertEquals(NOW.plusSeconds(10), requested.requestedAt());
        Assertions.assertEquals(deadlineJobId.toString(), requested.jobId(), "the deadline check now owns the entry");

        List<AuditEvent> events = since(batchId, before);
        Assertions.assertEquals(
                List.of(AuditEventType.INBOUND_FILE_ARCHIVED, AuditEventType.INBOUND_FILE_INGESTION_REQUESTED),
                types(events));
        AuditEvent archivedEvent = events.get(0);
        assertEnvelope(archivedEvent, tenant, batchId, uploadId);
        Assertions.assertEquals(archivePath, archivedEvent.detail().get("archivePath"));
        Assertions.assertEquals(52_000L, ((Number) archivedEvent.detail().get("bytes")).longValue());
        Assertions.assertEquals("ALREADY_PRESENT", archivedEvent.detail().get("copyResult"));
        AuditEvent requestedEvent = events.get(1);
        assertEnvelope(requestedEvent, tenant, batchId, uploadId);
        Assertions.assertEquals("ing_01HX", requestedEvent.detail().get("ingestionId"));
        Assertions.assertEquals(externalRef, requestedEvent.detail().get("externalRef"));
        Assertions.assertEquals("mapping-candor", requestedEvent.detail().get("mappingId"));
        Assertions.assertEquals("201", requestedEvent.detail().get("registerResponse"));
        Assertions.assertEquals(deadlineAt, toInstant(requestedEvent.detail().get("deadlineAt")));

        Assertions.assertFalse(
                lifecycle.archived(archiving, new ArchiveStore.Copied(archivePath, 1L, false), NOW.plusSeconds(20)),
                "the entry left ARCHIVING; a late archived write is a no-op");
        Assertions.assertEquals(before + 2, trail(batchId).size());
    }

    @Test
    void parsedRecordsTheCountsAndTheWait() {
        String tenant = "inlife-parsed";
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.INGESTION_REQUESTED);
        String batchId = seeded.batch().id();
        String uploadId = seeded.entry().uploadId();
        Located requested = inbound.findByUploadId(uploadId).orElseThrow();
        Instant parsedAt = requested.entry().requestedAt().plusSeconds(60);
        int before = trail(batchId).size();
        double partialBefore = statusCount("PARTIAL");

        Assertions.assertTrue(
                lifecycle.parsed(requested, new FileIngestionCounts(1873L, 1871L, 2L), "PARTIAL", "EVENT", parsedAt));

        InboundFile parsed = entry(uploadId);
        Assertions.assertEquals(InboundFileState.PARSED, parsed.state());
        Assertions.assertEquals(parsedAt, parsed.parsedAt());
        Assertions.assertEquals(new InboundFile.Parse("PARTIAL", 1873L, 1871L, 2L, "EVENT", null), parsed.parse());
        List<AuditEvent> events = since(batchId, before);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_PARSED), types(events));
        AuditEvent event = events.get(0);
        assertEnvelope(event, tenant, batchId, uploadId);
        Assertions.assertEquals("PARTIAL", event.detail().get("status"));
        Assertions.assertEquals(1873L, ((Number) event.detail().get("populatedRows")).longValue());
        Assertions.assertEquals(1871L, ((Number) event.detail().get("validRows")).longValue());
        Assertions.assertEquals(2L, ((Number) event.detail().get("invalidRows")).longValue());
        Assertions.assertEquals("EVENT", event.detail().get("completionSource"));
        Assertions.assertEquals(60L, ((Number) event.detail().get("waitSeconds")).longValue());
        Assertions.assertEquals(partialBefore + 1, statusCount("PARTIAL"));
    }

    @Test
    void parsedOnMovedEntryReturnsFalseAndWritesNothing() {
        String tenant = "inlife-moved";
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.INGESTION_REQUESTED);
        String batchId = seeded.batch().id();
        String uploadId = seeded.entry().uploadId();
        Located requested = inbound.findByUploadId(uploadId).orElseThrow();
        int before = trail(batchId).size();

        Assertions.assertTrue(lifecycle.ingestionFailed(requested, "MAPPING_REJECTED", "DEADLINE", NOW));
        InboundFile failed = entry(uploadId);
        Assertions.assertEquals(InboundFileState.INGESTION_FAILED, failed.state());
        Assertions.assertEquals(
                new InboundFile.Parse("INVALID", null, null, null, "DEADLINE", "MAPPING_REJECTED"), failed.parse());
        List<AuditEvent> failedEvents = since(batchId, before);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_INGESTION_FAILED), types(failedEvents));
        assertEnvelope(failedEvents.get(0), tenant, batchId, uploadId);
        Assertions.assertEquals("MAPPING_REJECTED", failedEvents.get(0).detail().get("reason"));
        Assertions.assertEquals("DEADLINE", failedEvents.get(0).detail().get("completionSource"));
        double completedBefore = statusCount("COMPLETED");

        Assertions.assertFalse(
                lifecycle.parsed(requested, new FileIngestionCounts(10L, 10L, 0L), "COMPLETED", "EVENT", NOW),
                "the event arrived after the deadline check failed the entry");

        InboundFile after = entry(uploadId);
        Assertions.assertEquals(InboundFileState.INGESTION_FAILED, after.state());
        Assertions.assertNull(after.parsedAt());
        Assertions.assertEquals(failed.parse(), after.parse());
        Assertions.assertEquals(failed.updatedAt(), after.updatedAt());
        Assertions.assertEquals(before + 1, trail(batchId).size(), "no PARSED event");
        Assertions.assertEquals(completedBefore, statusCount("COMPLETED"), "no completion metric for a no-op");
        Assertions.assertFalse(lifecycle.ingestionFailed(requested, "MAPPING_REJECTED", "EVENT", NOW));
    }

    @Test
    void failedRecordsStepReasonAndError() {
        String tenant = "inlife-failed";
        InboundFixtures.Seeded seeded = InboundFixtures.deliveredBatchWithEntry(
                transactions, batches, collections, tenant, InboundFileState.ARCHIVING);
        String batchId = seeded.batch().id();
        String uploadId = seeded.entry().uploadId();
        Located archiving = inbound.findByUploadId(uploadId).orElseThrow();
        String lastError = "source generation not found; ".repeat(30);
        int before = trail(batchId).size();

        Assertions.assertTrue(lifecycle.failed(
                archiving, InboundFileState.ARCHIVING, "ARCHIVE", "SOURCE_MISSING", lastError, 3, NOW));

        InboundFile failed = entry(uploadId);
        Assertions.assertEquals(InboundFileState.FAILED, failed.state());
        Assertions.assertEquals("ARCHIVE", failed.failedStep());
        Assertions.assertEquals("SOURCE_MISSING", failed.failureReason());
        Assertions.assertEquals(lastError.substring(0, 500), failed.lastError(), "lastError is cut to 500 characters");
        List<AuditEvent> events = since(batchId, before);
        Assertions.assertEquals(List.of(AuditEventType.INBOUND_FILE_FAILED), types(events));
        AuditEvent event = events.get(0);
        assertEnvelope(event, tenant, batchId, uploadId);
        Assertions.assertEquals("ARCHIVE", event.detail().get("failedStep"));
        Assertions.assertEquals("SOURCE_MISSING", event.detail().get("failureReason"));
        Assertions.assertEquals(lastError.substring(0, 500), event.detail().get("lastError"));
        Assertions.assertEquals(3, ((Number) event.detail().get("jobAttempts")).intValue());

        Assertions.assertFalse(
                lifecycle.failed(archiving, InboundFileState.ARCHIVING, "ARCHIVE", "SOURCE_MISSING", "again", 4, NOW),
                "a second failure of the same read is a no-op");
        Assertions.assertEquals(before + 1, trail(batchId).size());
    }

    private static void assertEnvelope(AuditEvent event, String tenant, String batchId, String uploadId) {
        Assertions.assertEquals(tenant, event.tenantId());
        Assertions.assertEquals("candor", event.vendor());
        Assertions.assertEquals(batchId, event.exportBatchId());
        Assertions.assertEquals(1, event.attempt());
        Assertions.assertEquals(uploadId, event.uploadId());
        Assertions.assertEquals(Actors.INGESTION, event.actor());
    }

    /** An upload of the vendor's object for {@code exportBatchId}, as the arrival handler would build it. */
    private static InboundFile upload(String tenant, String exportBatchId, String generation, String overwrote) {
        String objectName = InboundFixtures.objectName(tenant, exportBatchId);
        ArrivalEvent event = new ArrivalEvent(
                InboundFixtures.VENDOR_BUCKET,
                objectName,
                generation,
                "OBJECT_FINALIZE",
                InboundFixtures.ARRIVED_AT.minusSeconds(1),
                overwrote);
        return InboundFile.received(event, InboundNames.parse(objectName).orElseThrow(), InboundFixtures.ARRIVED_AT);
    }

    private InboundFile entry(String uploadId) {
        return inbound.findByUploadId(uploadId).orElseThrow().entry();
    }

    private List<AuditEvent> trail(String batchId) {
        return audit.trailForBatch(batchId, TRAIL_LIMIT);
    }

    /** The batch's events written after the first {@code skip}, in write order. */
    private List<AuditEvent> since(String batchId, int skip) {
        List<AuditEvent> all = trail(batchId);
        return all.subList(skip, all.size());
    }

    private static List<AuditEventType> types(List<AuditEvent> events) {
        return events.stream().map(AuditEvent::type).toList();
    }

    private static Instant toInstant(Object value) {
        return value instanceof java.util.Date date ? date.toInstant() : (Instant) value;
    }

    private double counter(String name, String tenant) {
        Counter found = registry.find(name).tag("tenant", tenant).counter();
        return found == null ? 0 : found.count();
    }

    private double statusCount(String status) {
        Counter found = registry.find(VendorExchangeMetrics.INGESTION_COMPLETION_STATUS)
                .tag("status", status)
                .counter();
        return found == null ? 0 : found.count();
    }
}
