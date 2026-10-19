package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.ingestion.events.ArrivalEvent;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile.Checkpoint;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile.Parse;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile.Written;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class InboundFileTest {

    private static final String OBJECT = "to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv";
    private static final Instant NOW = Instant.parse("2026-10-15T09:12:41Z");

    private static ArrivalEvent event(String generation, String overwrote) {
        return new ArrivalEvent(
                "vendor-sftp", OBJECT, generation, "OBJECT_FINALIZE", Instant.parse("2026-10-15T09:12:40Z"), overwrote);
    }

    private static InboundFile stagedSample() {
        Map<String, Long> dispositions = new LinkedHashMap<>();
        dispositions.put("STAGED", 1830L);
        dispositions.put("DUPLICATE", 0L);
        dispositions.put("SCHEMA_INVALID", 2L);
        dispositions.put("MISSING_IDS", 41L);
        dispositions.put("UNSUPPORTED_ENTITY", 0L);
        return new InboundFile(
                "vendor-sftp|" + OBJECT + "|1760520000123456",
                "1760520000123456",
                "CB-2026-10-001",
                "20261015",
                InboundFileState.STAGED,
                2,
                "4b1e0000-0000-0000-0000-000000000000",
                "gs://vendor-ingestion-archive/org-xyz/org-xyz-candor-2026-10-001/1760520000123456/f.csv",
                52_000L,
                "org-xyz-candor-2026-10-001|1760520000123456",
                "ing_01HX",
                new Parse("PARTIAL", 1873L, 1871L, 2L, "EVENT", "two rows invalid"),
                new Checkpoint("row-1800", 4L),
                dispositions,
                new Written(1960L, 12250L, 40L, 3L),
                "STAGING",
                "boom",
                "last error",
                "1760519000000000",
                Instant.parse("2026-10-15T09:12:40Z"),
                Instant.parse("2026-10-15T09:12:41Z"),
                Instant.parse("2026-10-15T09:12:52Z"),
                Instant.parse("2026-10-15T09:14:02Z"),
                Instant.parse("2026-10-15T09:15:30Z"),
                Instant.parse("2026-10-15T09:15:31Z"));
    }

    @Test
    void roundTripsThroughBson() {
        InboundFile entry = stagedSample();
        Document stored = entry.toDocument();

        Assertions.assertEquals(entry, InboundFile.fromDocument(stored));
        Assertions.assertEquals("STAGED", stored.getString("state"));
        Assertions.assertEquals(
                1830L, stored.get("dispositions", Document.class).getLong("STAGED"));
    }

    @Test
    void theDesignPostStagingSampleReadsBackFieldForField() {
        Document sample = new Document("uploadId", "vendor-sftp|" + OBJECT + "|1760520000123456")
                .append("generation", "1760520000123456")
                .append("vendorBatchId", "CB-2026-10-001")
                .append("state", "STAGED")
                .append("jobId", "4b1e")
                .append("archivePath", "gs://vendor-ingestion-archive/org-xyz/ref/1760520000123456/f.csv")
                .append("externalRef", "org-xyz-candor-2026-10-001|1760520000123456")
                .append("ingestionId", "ing_01HX")
                .append(
                        "parse",
                        new Document("status", "PARTIAL")
                                .append("populatedRows", 1873)
                                .append("validRows", 1871)
                                .append("invalidRows", 2)
                                .append("completionSource", "EVENT"))
                .append("checkpoint", new Document())
                .append(
                        "dispositions",
                        new Document("STAGED", 1830)
                                .append("DUPLICATE", 0)
                                .append("SCHEMA_INVALID", 2)
                                .append("MISSING_IDS", 41)
                                .append("UNSUPPORTED_ENTITY", 0))
                .append(
                        "written",
                        new Document("fieldRecommendations", 1960)
                                .append("confirmedFields", 12250)
                                .append("unverifiedFields", 40))
                .append("receivedAt", Instant.parse("2026-10-15T09:12:41Z"))
                .append("requestedAt", Instant.parse("2026-10-15T09:12:52Z"))
                .append("parsedAt", Instant.parse("2026-10-15T09:14:02Z"))
                .append("stagedAt", Instant.parse("2026-10-15T09:15:30Z"));

        InboundFile read = InboundFile.fromDocument(sample);

        Assertions.assertEquals(InboundFileState.STAGED, read.state());
        Assertions.assertEquals("CB-2026-10-001", read.vendorBatchId());
        Assertions.assertEquals("4b1e", read.jobId());
        Assertions.assertEquals("ing_01HX", read.ingestionId());
        Assertions.assertEquals(new Parse("PARTIAL", 1873L, 1871L, 2L, "EVENT", null), read.parse());
        Assertions.assertEquals(new Checkpoint(null, null), read.checkpoint());
        Assertions.assertEquals(
                Map.of(
                        "STAGED",
                        1830L,
                        "DUPLICATE",
                        0L,
                        "SCHEMA_INVALID",
                        2L,
                        "MISSING_IDS",
                        41L,
                        "UNSUPPORTED_ENTITY",
                        0L),
                read.dispositions());
        Assertions.assertEquals(new Written(1960L, 12250L, 40L, null), read.written());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:12:41Z"), read.receivedAt());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:12:52Z"), read.requestedAt());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:14:02Z"), read.parsedAt());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:15:30Z"), read.stagedAt());
        Assertions.assertNull(read.overwroteGeneration());
        Assertions.assertNull(read.failureReason());
        Assertions.assertEquals("vendor-sftp", read.bucket());
        Assertions.assertEquals(OBJECT, read.objectName());
    }

    @Test
    void absentFieldsAreLeftOut() {
        InboundFile received = InboundFile.received(
                event("1760520000123456", null), InboundNames.parse(OBJECT).orElseThrow(), NOW);
        Document stored = received.toDocument();

        Assertions.assertEquals(
                Set.of(
                        "uploadId",
                        "generation",
                        "vendorBatchId",
                        "fileDate",
                        "state",
                        "attempt",
                        "eventTime",
                        "receivedAt",
                        "updatedAt"),
                stored.keySet());
        Assertions.assertEquals(received, InboundFile.fromDocument(stored));
    }

    @Test
    void receivedAndDuplicateLoggedStartTheEntry() {
        InboundNames.Parsed name = InboundNames.parse(OBJECT).orElseThrow();
        InboundFile received = InboundFile.received(event("200", null), name, NOW);
        InboundFile duplicate = InboundFile.duplicateLogged(event("300", "200"), name, NOW);

        Assertions.assertEquals(InboundFileState.RECEIVED, received.state());
        Assertions.assertEquals(1, received.attempt());
        Assertions.assertEquals("200", received.generation());
        Assertions.assertEquals("CB-2026-10-001", received.vendorBatchId());
        Assertions.assertEquals("20261015", received.fileDate());
        Assertions.assertEquals(NOW, received.receivedAt());
        Assertions.assertEquals(NOW, received.updatedAt());
        Assertions.assertEquals(Instant.parse("2026-10-15T09:12:40Z"), received.eventTime());
        Assertions.assertNull(received.jobId());
        Assertions.assertNull(received.requestedAt());
        Assertions.assertTrue(received.dispositions().isEmpty());

        Assertions.assertEquals(InboundFileState.DUPLICATE_LOGGED, duplicate.state());
        Assertions.assertEquals("200", duplicate.overwroteGeneration());
        Assertions.assertEquals("vendor-sftp|" + OBJECT + "|300", duplicate.uploadId());
    }

    @Test
    void bucketAndObjectComeFromUploadId() {
        InboundFile entry = InboundFile.received(
                event("1760520000123456", null), InboundNames.parse(OBJECT).orElseThrow(), NOW);

        Assertions.assertEquals("vendor-sftp", entry.bucket());
        Assertions.assertEquals(OBJECT, entry.objectName());
    }

    @Test
    void dispositionsAreCopied() {
        Map<String, Long> source = new LinkedHashMap<>();
        source.put("STAGED", 1L);
        InboundFile base = stagedSample();
        InboundFile entry = new InboundFile(
                base.uploadId(),
                base.generation(),
                null,
                null,
                base.state(),
                1,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                source,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        source.put("DUPLICATE", 9L);

        Assertions.assertEquals(Map.of("STAGED", 1L), entry.dispositions());
    }
}
