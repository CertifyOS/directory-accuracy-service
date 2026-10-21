package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.clients.FileIngestionCell;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.HistoryEntry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FieldRecommendationTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-10-15T09:15:10Z");
    private static final String UPLOAD_ID = "vendor-sftp|to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001"
            + "_20261015.csv|1760520000123456";

    private static FieldRecommendation pending(String field) {
        WideRow row = WideRow.from(DesignRows.row());
        return FieldRecommendation.pending(
                row, row.cells().get(field), DesignRows.batch(), DesignRows.entry(), MAPPER, NOW);
    }

    @Test
    void pendingFromTheDesignRowMatchesTheSample() {
        FieldRecommendation recommendation = pending("practice_name");

        Assertions.assertEquals(
                "org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|loc-7781|practice_name", recommendation.id());
        Assertions.assertEquals(FieldLevel.LOCATION, recommendation.level());
        Document doc = recommendation.toDocument();
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("reason", "direct_outreach");
        evidence.put("evidence", "date");
        evidence.put("value", "2026-06-17");
        Document sample = new Document("_id", recommendation.id())
                .append("tenantId", "org-xyz")
                .append("sourceType", "vendor")
                .append("source", "candor")
                .append("exportBatchId", "org-xyz-candor-2026-10-001")
                .append(
                        "practitioner",
                        new Document("certifyPractitionerId", "cert-000123").append("npi", "1003002890"))
                .append("location", new Document("certifyLocationId", "loc-7781"))
                .append("field", "practice_name")
                .append("sentValue", "AdventHealth")
                .append("vendorValue", "Greenwood Village")
                .append("vendorStatus", "INVALID")
                .append("vendorReason", List.of("direct_outreach"))
                .append("vendorConfidence", "VERY HIGH")
                .append("vendorEvidence", List.of(evidence))
                .append("reviewStatus", "PENDING")
                .append("reviewHistory", List.of())
                .append("version", 1L)
                .append(
                        "sourceFile",
                        new Document("uploadId", UPLOAD_ID)
                                .append("sheet", "Physicians Directory Accuracy")
                                .append("row", 913))
                .append("stagedAt", NOW)
                .append("updatedAt", NOW);
        Assertions.assertEquals(json(sample), json(doc), "the design's sample, field for field, in order");
        Assertions.assertNull(recommendation.goldenRecordUpdate());
        Assertions.assertFalse(doc.containsKey("goldenRecordUpdate"), "absent until a release, so a dotted $set works");
        Assertions.assertEquals(1L, recommendation.version());
        Assertions.assertEquals(List.of(), recommendation.reviewHistory());
        Assertions.assertEquals(913, recommendation.sourceFile().row());
        Assertions.assertEquals(List.of(evidence), recommendation.vendorEvidence());
    }

    @Test
    void practitionerLevelFieldHasNullLocation() {
        FieldRecommendation firstName = pending("first_name");

        Assertions.assertEquals("org-xyz|candor|org-xyz-candor-2026-10-001|cert-000123|-|first_name", firstName.id());
        Assertions.assertNull(firstName.location());
        Assertions.assertEquals(FieldLevel.PRACTITIONER, firstName.level());
        Assertions.assertFalse(firstName.toDocument().containsKey("location"));
        Assertions.assertEquals("Dana", firstName.sentValue());
        Assertions.assertEquals("Danielle", firstName.vendorValue());
        Assertions.assertEquals(List.of("direct_outreach", "nppes_update"), firstName.vendorReason());
        Assertions.assertEquals(
                List.of(Map.of("evidence", "called the office")),
                firstName.vendorEvidence(),
                "evidence that is not a JSON array of objects is kept as text");
    }

    @Test
    void evidenceThatIsBlankOrNotObjectsIsKeptAsText() {
        Assertions.assertEquals(List.of(), evidenceOf(null));
        Assertions.assertEquals(List.of(), evidenceOf("  "));
        Assertions.assertEquals(List.of(), evidenceOf("[]"));
        Assertions.assertEquals(List.of(Map.of("evidence", "[1, 2]")), evidenceOf("[1, 2]"));
        Assertions.assertEquals(List.of(Map.of("evidence", "{\"a\":1}")), evidenceOf("{\"a\":1}"));
        Assertions.assertEquals(List.of(Map.of("evidence", "[null]")), evidenceOf("[null]"));
    }

    @Test
    void locationFieldOfARowWithoutALocationIdIsRefused() {
        Map<String, FileIngestionCell> overrides = Map.of("certify_location_id", DesignRows.valid(""));
        WideRow row = WideRow.from(DesignRows.row("VALID", overrides));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> FieldRecommendation.pending(
                        row, row.cells().get("phone"), DesignRows.batch(), DesignRows.entry(), MAPPER, NOW));
    }

    @Test
    void roundTripsThroughBson() {
        FieldRecommendation staged = pending("practice_name");
        GoldenRecordUpdate update = new GoldenRecordUpdate(
                "rel-0001",
                "reviewer@example.com",
                NOW.plusSeconds(60),
                "SLICE",
                "location",
                "cw-1",
                "slice-1",
                NOW.plusSeconds(65),
                NOW.plusSeconds(70),
                "job-1",
                "WON",
                "candor",
                NOW.plusSeconds(80),
                null);
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("reason", "health_system_verified");
        nested.put("evidence", "https://example.org/x");
        nested.put("value", null);
        nested.put("extra", Map.of("depth", List.of("a", "b")));
        FieldRecommendation released = new FieldRecommendation(
                staged.id(),
                staged.tenantId(),
                staged.sourceType(),
                staged.source(),
                staged.exportBatchId(),
                staged.practitioner(),
                staged.location(),
                staged.field(),
                staged.sentValue(),
                staged.vendorValue(),
                staged.vendorStatus(),
                staged.vendorReason(),
                staged.vendorConfidence(),
                List.of(nested),
                ReviewStatus.RELEASED,
                List.of(
                        new HistoryEntry("APPROVE", "reviewer@example.com", NOW.plusSeconds(30), "looks right"),
                        new HistoryEntry("RELEASE", "reviewer@example.com", NOW.plusSeconds(60), null)),
                3L,
                update,
                staged.sourceFile(),
                staged.stagedAt(),
                NOW.plusSeconds(80));

        for (FieldRecommendation recommendation : List.of(staged, released, pending("first_name"))) {
            Document stored = throughBson(recommendation.toDocument());
            Assertions.assertInstanceOf(Date.class, stored.get("stagedAt"), "a real BSON round trip");
            Assertions.assertEquals(recommendation, FieldRecommendation.fromDocument(stored), recommendation.field());
        }
        Document goldenRecordUpdate = released.toDocument().get("goldenRecordUpdate", Document.class);
        Assertions.assertFalse(goldenRecordUpdate.containsKey("error"), "only set components are written");
    }

    @Test
    void evidenceNumbersBeyondLongStayEncodable() {
        Map<String, FileIngestionCell> overrides = Map.of(
                CandorRecsMapping.column("practice_name", CandorRecsMapping.Suffix.EVIDENCE),
                DesignRows.valid("[{\"reason\":\"direct_outreach\",\"value\":1234567890123456789012345,"
                        + "\"score\":0.5,\"nested\":[98765432109876543210987]}]"));
        WideRow row = WideRow.from(DesignRows.row("VALID", overrides));

        FieldRecommendation recommendation = FieldRecommendation.pending(
                row, row.cells().get("practice_name"), DesignRows.batch(), DesignRows.entry(), MAPPER, NOW);

        Map<String, Object> evidence = recommendation.vendorEvidence().get(0);
        Assertions.assertEquals("1234567890123456789012345", evidence.get("value"), "kept as its text");
        Assertions.assertEquals(List.of("98765432109876543210987"), evidence.get("nested"));
        Assertions.assertEquals(0.5, evidence.get("score"), "a number BSON can encode stays a number");
        Assertions.assertEquals(
                recommendation, FieldRecommendation.fromDocument(throughBson(recommendation.toDocument())));
    }

    @Test
    void unknownReviewStatusIsRefusedOnRead() {
        Document doc = pending("phone").toDocument().append("reviewStatus", "PARKED");

        Assertions.assertThrows(IllegalStateException.class, () -> FieldRecommendation.fromDocument(doc));
    }

    private static List<Map<String, Object>> evidenceOf(String text) {
        Map<String, FileIngestionCell> overrides = new LinkedHashMap<>();
        overrides.put(
                CandorRecsMapping.column("practice_name", CandorRecsMapping.Suffix.EVIDENCE),
                text == null ? null : DesignRows.valid(text));
        WideRow row = WideRow.from(DesignRows.row("VALID", overrides));
        return FieldRecommendation.pending(
                        row, row.cells().get("practice_name"), DesignRows.batch(), DesignRows.entry(), MAPPER, NOW)
                .vendorEvidence();
    }

    private static String json(Document doc) {
        return doc.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson();
    }

    private static Document throughBson(Document doc) {
        BsonDocument bson = doc.toBsonDocument(BsonDocument.class, MongoClientSettings.getDefaultCodecRegistry());
        return new DocumentCodec(MongoClientSettings.getDefaultCodecRegistry())
                .decode(new BsonDocumentReader(bson), DecoderContext.builder().build());
    }
}
