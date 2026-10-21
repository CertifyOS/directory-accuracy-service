package com.certifyos.directory_accuracy.ingestion;

import com.certifyos.directory_accuracy.export.batch.BatchState;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.batch.ExportBatchRepository;
import com.certifyos.directory_accuracy.export.batch.ExportNpiRepository;
import com.certifyos.directory_accuracy.export.jobs.EgressFixtures;
import com.certifyos.directory_accuracy.ingestion.events.ArrivalEvent;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFileState;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundNames;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import java.time.Instant;
import org.bson.Document;

/** A {@code DELIVERED} batch with one inbound entry, as the stages before {@code state} would have left it. */
public final class InboundFixtures {

    public static final String VENDOR_BUCKET = "test-vendor-bucket";
    public static final String VENDOR_BATCH_ID = "CB-2026-10-001";
    public static final String GENERATION = "1760520000123456";
    public static final Instant DELIVERED_AT = Instant.parse("2026-10-14T12:00:00Z");
    public static final Instant ARRIVED_AT = Instant.parse("2026-10-15T09:12:41Z");

    private InboundFixtures() {}

    /** The batch and the entry that was pushed onto it. */
    public record Seeded(ExportBatch batch, InboundFile entry) {}

    /** The object name the vendor would drop for {@code batch}. */
    public static String objectName(String tenant, String exportBatchId) {
        return InboundNames.TO_PREFIX + tenant + "/" + tenant + "_" + exportBatchId + "_" + VENDOR_BATCH_ID
                + "_20261015.csv";
    }

    /**
     * Inserts a batch, walks it to {@code DELIVERED} and pushes one entry in {@code state} with the
     * fields the earlier states would have written. The push goes straight to the collection, so it
     * does not depend on the repository code under test.
     */
    public static Seeded deliveredBatchWithEntry(
            Transactions transactions,
            ExportBatchRepository batches,
            Collections collections,
            String tenant,
            InboundFileState state) {
        ExportBatch batch = deliveredBatch(transactions, batches, collections, tenant);
        InboundFile entry = entryIn(state, tenant, batch.id());
        collections
                .batches()
                .updateOne(Filters.eq("_id", batch.id()), Updates.push("inboundFiles", entry.toDocument()));
        return new Seeded(batch, entry);
    }

    /** Inserts a batch and walks it to {@code DELIVERED}, with no inbound entry yet. */
    public static ExportBatch deliveredBatch(
            Transactions transactions, ExportBatchRepository batches, Collections collections, String tenant) {
        ExportBatch egressed = EgressFixtures.batchIn(
                transactions,
                batches,
                new ExportNpiRepository(collections),
                tenant,
                BatchState.EGRESS_COMPLETED,
                1,
                DELIVERED_AT);
        transactions.run(session -> {
            batches.transition(
                    session,
                    egressed.id(),
                    BatchState.EGRESS_COMPLETED,
                    BatchState.DELIVERED,
                    DELIVERED_AT.plusSeconds(60),
                    null);
            return null;
        });
        return batches.find(egressed.id()).orElseThrow();
    }

    /** The entry for {@code exportBatchId} in {@code state}, as the earlier states would have left it. */
    public static InboundFile entryIn(InboundFileState state, String tenant, String exportBatchId) {
        String objectName = objectName(tenant, exportBatchId);
        ArrivalEvent event = new ArrivalEvent(
                VENDOR_BUCKET, objectName, GENERATION, "OBJECT_FINALIZE", ARRIVED_AT.minusSeconds(1), null);
        InboundNames.Parsed name = InboundNames.parse(objectName).orElseThrow();
        if (state == InboundFileState.DUPLICATE_LOGGED) {
            return InboundFile.duplicateLogged(event, name, ARRIVED_AT);
        }
        Document doc = InboundFile.received(event, name, ARRIVED_AT).toDocument();
        doc.put("state", state.name());
        addJob(doc, state);
        if (reachedRequested(state)) {
            addRequested(doc, tenant, exportBatchId, name.fileName());
        }
        if (reachedParsed(state)) {
            addParsed(doc);
        }
        addStaging(doc, state);
        addFailure(doc, state);
        return InboundFile.fromDocument(doc);
    }

    private static void addJob(Document doc, InboundFileState state) {
        if (state != InboundFileState.RECEIVED && state != InboundFileState.FAILED) {
            doc.append("jobId", "11111111-1111-1111-1111-111111111111");
        }
    }

    private static void addStaging(Document doc, InboundFileState state) {
        if (state == InboundFileState.STAGING || state == InboundFileState.STAGED) {
            doc.append("checkpoint", new Document("chunks", 0L));
        }
        if (state == InboundFileState.STAGED) {
            addStaged(doc);
        }
    }

    private static void addFailure(Document doc, InboundFileState state) {
        if (state == InboundFileState.FAILED || state == InboundFileState.INGESTION_FAILED) {
            doc.append("failedStep", "ARCHIVE").append("failureReason", "TEST_FAILURE");
        }
    }

    private static void addRequested(Document doc, String tenant, String exportBatchId, String fileName) {
        doc.append(
                        "archivePath",
                        "gs://vendor-ingestion-archive/"
                                + InboundNames.archiveObject(tenant, exportBatchId, GENERATION, fileName))
                .append("archiveBytes", 52_000L)
                .append("externalRef", Ids.externalRef(exportBatchId, GENERATION))
                .append("requestedAt", ARRIVED_AT.plusSeconds(11));
    }

    private static void addParsed(Document doc) {
        doc.append("ingestionId", "ing_01HX")
                .append(
                        "parse",
                        new Document("status", "PARTIAL")
                                .append("populatedRows", 1873L)
                                .append("validRows", 1871L)
                                .append("invalidRows", 2L)
                                .append("completionSource", "EVENT"))
                .append("parsedAt", ARRIVED_AT.plusSeconds(81));
    }

    private static void addStaged(Document doc) {
        doc.append("stagedAt", ARRIVED_AT.plusSeconds(169))
                .append(
                        "dispositions",
                        new Document("STAGED", 1830L)
                                .append("DUPLICATE", 0L)
                                .append("SCHEMA_INVALID", 2L)
                                .append("MISSING_IDS", 41L)
                                .append("UNSUPPORTED_ENTITY", 0L))
                .append(
                        "written",
                        new Document("fieldRecommendations", 1960L)
                                .append("confirmedFields", 12250L)
                                .append("unverifiedFields", 40L));
    }

    private static boolean reachedRequested(InboundFileState state) {
        return switch (state) {
            case INGESTION_REQUESTED, PARSED, STAGING, STAGED, INGESTION_FAILED -> true;
            default -> false;
        };
    }

    private static boolean reachedParsed(InboundFileState state) {
        return switch (state) {
            case PARSED, STAGING, STAGED -> true;
            default -> false;
        };
    }
}
