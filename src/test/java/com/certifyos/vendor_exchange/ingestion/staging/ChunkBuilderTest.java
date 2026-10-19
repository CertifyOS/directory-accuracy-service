package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.clients.FileIngestionCell;
import com.certifyos.vendor_exchange.clients.FileIngestionRow;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFile;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping.Suffix;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** The chunk builder's row and cell rules (design step 5), all local to the chunk. */
class ChunkBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Instant NOW = Instant.parse("2026-10-15T09:15:00Z");
    private static final ExportBatch BATCH = DesignRows.batch();
    private static final InboundFile ENTRY = DesignRows.entry();
    private static final String FIRST_NAME_VALUE = CandorRecsMapping.column("first_name", Suffix.VALUE);

    private static ChunkBuilder.Built build(FileIngestionRow... rows) {
        List<WideRow> wide = java.util.Arrays.stream(rows).map(WideRow::from).toList();
        return ChunkBuilder.build(wide, BATCH, ENTRY, MAPPER, NOW);
    }

    private static Map<RowDisposition, Long> counts(Map<RowDisposition, Long> nonZero) {
        Map<RowDisposition, Long> all = new EnumMap<>(RowDisposition.class);
        for (RowDisposition disposition : RowDisposition.values()) {
            all.put(disposition, nonZero.getOrDefault(disposition, 0L));
        }
        return all;
    }

    private static String id(String practitionerId, String locationId, String field) {
        return DesignRows.idOf(BATCH, practitionerId, locationId, field);
    }

    private static Map<String, FieldRecommendation> byId(ChunkBuilder.Built built) {
        return built.recommendations().stream()
                .collect(Collectors.toMap(FieldRecommendation::id, recommendation -> recommendation));
    }

    private static Map<String, FileIngestionCell> firstName(String value) {
        Map<String, FileIngestionCell> cells = new HashMap<>();
        cells.put(FIRST_NAME_VALUE, DesignRows.valid(value));
        return cells;
    }

    @Test
    void onePractitionerThreeLocationsWritesPractitionerFieldsOnce() {
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "loc-1"),
                DesignRows.row(2, "cert-1", "1003002890", "loc-2"),
                DesignRows.row(3, "cert-1", "1003002890", "loc-3"));

        String firstName = id("cert-1", null, "first_name");
        Assertions.assertEquals(
                Set.of(
                        firstName,
                        id("cert-1", "loc-1", "practice_name"),
                        id("cert-1", "loc-2", "practice_name"),
                        id("cert-1", "loc-3", "practice_name")),
                byId(built).keySet());
        Assertions.assertEquals(4, built.recommendations().size(), "one write per id");
        FieldRecommendation practitionerLevel = byId(built).get(firstName);
        Assertions.assertNull(practitionerLevel.location());
        Assertions.assertEquals(1, practitionerLevel.sourceFile().row(), "the first row that saw it wrote it");
        Assertions.assertEquals(ReviewStatus.PENDING, practitionerLevel.reviewStatus());
        Assertions.assertEquals(counts(Map.of(RowDisposition.STAGED, 3L)), built.dispositions());
        Assertions.assertEquals(3L * DesignRows.VALID_PER_ROW, built.confirmedFields());
        Assertions.assertEquals(3L * DesignRows.UNVERIFIED_PER_ROW, built.unverifiedFields());
        Assertions.assertEquals(0L, built.practitionerFieldDisagreements());
        Assertions.assertEquals(3, built.rowsSeen());
        Assertions.assertEquals(DesignRows.SHEET + ":3", built.lastCursor());
        for (int row = 1; row <= 3; row++) {
            Assertions.assertEquals(
                    Set.of(firstName, id("cert-1", "loc-" + row, "practice_name")),
                    built.documentIdsByRow().get(DesignRows.SHEET + ":" + row),
                    "a row names the collapsed practitioner document too");
        }
    }

    @Test
    void disagreeingRowsCountOnce() {
        // Row 2 disagrees with row 1 on first_name, row 3 agrees: one disagreement, the first value kept.
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "loc-1", firstName("Danielle")),
                DesignRows.row(2, "cert-1", "1003002890", "loc-2", firstName("Dani")),
                DesignRows.row(3, "cert-1", "1003002890", "loc-3", firstName("Danielle")));

        FieldRecommendation kept = byId(built).get(id("cert-1", null, "first_name"));
        Assertions.assertEquals("Danielle", kept.vendorValue());
        Assertions.assertEquals(1, kept.sourceFile().row());
        Assertions.assertEquals(1L, built.practitionerFieldDisagreements());
        Assertions.assertEquals(
                1L,
                built.recommendations().stream()
                        .filter(recommendation -> recommendation.field().equals("first_name"))
                        .count());
    }

    @Test
    void blankLocationIdStagesPractitionerCellsOnly() {
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "   "), DesignRows.row(2, "cert-2", "1003002891", null));

        Assertions.assertEquals(
                Set.of(id("cert-1", null, "first_name"), id("cert-2", null, "first_name")),
                byId(built).keySet());
        Assertions.assertEquals(counts(Map.of(RowDisposition.MISSING_IDS, 2L)), built.dispositions());
        // Practitioner-level cells are processed: npi, last_name and specialties have no status
        // (unverified); first_name is written. The location-level cells (phone VALID, fax UNKNOWN,
        // four blanks, practice_name INVALID) are neither written nor counted.
        Assertions.assertEquals(0L, built.confirmedFields());
        Assertions.assertEquals(2L * 3, built.unverifiedFields());
        Assertions.assertTrue(built.documentIdsByRow().isEmpty(), "only STAGED rows can settle to DUPLICATE");
    }

    @Test
    void idsAreTrimmedAndUnusableIdsAreMissingIds() {
        ChunkBuilder.Built built = build(
                DesignRows.row(1, " cert-1 ", " 1003002890 ", " loc-1 "),
                DesignRows.row(2, "cert|2", "1003002891", "loc-2"),
                DesignRows.row(3, "cert 3", "1003002892", "loc-3"),
                DesignRows.row(4, "cert-4", "  ", "loc-4"),
                DesignRows.row(5, "cert-5", "1003002895", "-"),
                DesignRows.row(6, "cert-6", "1003002896", "loc|6"),
                DesignRows.row(7, null, "1003002897", "loc-7"));

        Assertions.assertEquals(
                Set.of(
                        id("cert-1", null, "first_name"),
                        id("cert-1", "loc-1", "practice_name"),
                        id("cert-5", null, "first_name"),
                        id("cert-6", null, "first_name")),
                byId(built).keySet());
        FieldRecommendation trimmed = byId(built).get(id("cert-1", "loc-1", "practice_name"));
        Assertions.assertEquals("cert-1", trimmed.practitioner().certifyPractitionerId());
        Assertions.assertEquals("1003002890", trimmed.practitioner().npi());
        Assertions.assertEquals("loc-1", trimmed.location().certifyLocationId());
        Assertions.assertEquals(
                counts(Map.of(RowDisposition.STAGED, 1L, RowDisposition.MISSING_IDS, 6L)), built.dispositions());
    }

    @Test
    void repeatedLocationRowWritesOneDocument() {
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "loc-1"), DesignRows.row(2, "cert-1", "1003002890", "loc-1"));

        Assertions.assertEquals(2, built.recommendations().size());
        Assertions.assertEquals(
                Set.of(id("cert-1", null, "first_name"), id("cert-1", "loc-1", "practice_name")),
                byId(built).keySet());
        Assertions.assertEquals(0L, built.practitionerFieldDisagreements(), "location fields are not counted");
        Assertions.assertEquals(counts(Map.of(RowDisposition.STAGED, 2L)), built.dispositions());
    }

    @Test
    void nonPractitionerRowIsUnsupported() {
        ChunkBuilder.Built built =
                build(DesignRows.row(1, "VALID", Map.of(CandorRecsMapping.ENTITY_TYPE, DesignRows.valid("FACILITY"))));

        Assertions.assertTrue(built.recommendations().isEmpty());
        Assertions.assertEquals(counts(Map.of(RowDisposition.UNSUPPORTED_ENTITY, 1L)), built.dispositions());
        Assertions.assertEquals(0L, built.confirmedFields());
        Assertions.assertEquals(0L, built.unverifiedFields());
    }

    @Test
    void schemaInvalidRowIsCounted() {
        // Schema first: an invalid row that is also not a practitioner and has no ids is SCHEMA_INVALID.
        Map<String, FileIngestionCell> broken = new HashMap<>();
        broken.put(CandorRecsMapping.ENTITY_TYPE, DesignRows.valid("FACILITY"));
        broken.put(CandorRecsMapping.PRACTITIONER_ID, null);
        ChunkBuilder.Built built = build(DesignRows.row(1, "INVALID", broken));

        Assertions.assertTrue(built.recommendations().isEmpty());
        Assertions.assertEquals(counts(Map.of(RowDisposition.SCHEMA_INVALID, 1L)), built.dispositions());
        Assertions.assertEquals(0L, built.confirmedFields());
        Assertions.assertEquals(0L, built.unverifiedFields());
        Assertions.assertEquals(DesignRows.SHEET + ":1", built.lastCursor());
    }

    @Test
    void validAndUnknownAreCountedNotWritten() {
        Map<String, FileIngestionCell> cells = new HashMap<>();
        cells.put(CandorRecsMapping.column("first_name", Suffix.STATUS), DesignRows.valid("VALID"));
        cells.put(CandorRecsMapping.column("practice_name", Suffix.STATUS), DesignRows.valid("INCONCLUSIVE"));
        cells.put(CandorRecsMapping.column("npi", Suffix.STATUS), DesignRows.valid("UNKNOWN"));
        cells.put(CandorRecsMapping.column("last_name", Suffix.STATUS), DesignRows.valid("  "));
        ChunkBuilder.Built built = build(DesignRows.row(1, "cert-1", "1003002890", "loc-1", cells));

        Assertions.assertTrue(built.recommendations().isEmpty());
        Assertions.assertEquals(2L, built.confirmedFields(), "first_name and phone");
        Assertions.assertEquals(8L, built.unverifiedFields());
        Assertions.assertEquals(counts(Map.of(RowDisposition.STAGED, 1L)), built.dispositions());
        Assertions.assertTrue(built.documentIdsByRow().isEmpty());
    }

    @Test
    void settleMarksRowsWhoseDocumentsAllExistedAsDuplicate() {
        Map<String, FileIngestionCell> allValid = new HashMap<>();
        allValid.put(CandorRecsMapping.column("first_name", Suffix.STATUS), DesignRows.valid("VALID"));
        allValid.put(CandorRecsMapping.column("practice_name", Suffix.STATUS), DesignRows.valid("VALID"));
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "loc-1"),
                DesignRows.row(2, "cert-1", "1003002890", "loc-2"),
                DesignRows.row(3, "cert-2", "1003002891", "loc-3"),
                DesignRows.row(4, "cert-3", "1003002892", "loc-4", allValid),
                DesignRows.row(5, "cert-4", "1003002893", null));
        Set<String> rowOne = Set.of(id("cert-1", null, "first_name"), id("cert-1", "loc-1", "practice_name"));

        // Row 1 wrote its documents; row 2 shares one of them, so it is ours; row 3 found all of its
        // documents in place; row 4 writes nothing and keeps STAGED; row 5 stays MISSING_IDS.
        Assertions.assertEquals(
                counts(Map.of(RowDisposition.STAGED, 3L, RowDisposition.DUPLICATE, 1L, RowDisposition.MISSING_IDS, 1L)),
                ChunkBuilder.settle(built, rowOne));
        Set<String> every = new HashSet<>(byId(built).keySet());
        Assertions.assertEquals(
                counts(Map.of(RowDisposition.STAGED, 4L, RowDisposition.MISSING_IDS, 1L)),
                ChunkBuilder.settle(built, every));
        Assertions.assertEquals(
                counts(Map.of(RowDisposition.STAGED, 1L, RowDisposition.DUPLICATE, 3L, RowDisposition.MISSING_IDS, 1L)),
                ChunkBuilder.settle(built, Set.of()));
        Assertions.assertEquals(
                counts(Map.of(RowDisposition.STAGED, 4L, RowDisposition.MISSING_IDS, 1L)),
                built.dispositions(),
                "settle leaves the built counts as they were");
    }

    @Test
    void dispositionsSumToRowsSeen() {
        Map<String, FileIngestionCell> facility = Map.of(CandorRecsMapping.ENTITY_TYPE, DesignRows.valid("FACILITY"));
        ChunkBuilder.Built built = build(
                DesignRows.row(1, "cert-1", "1003002890", "loc-1"),
                DesignRows.row(2, "INVALID", Map.of()),
                DesignRows.row(3, "VALID", facility),
                DesignRows.row(4, "cert-4", "", "loc-4"),
                DesignRows.row(5, "cert-5", "1003002895", ""),
                DesignRows.row(6, "PARTIAL", Map.of()),
                DesignRows.row(7, "VALID", facility));

        Assertions.assertEquals(7, built.rowsSeen());
        Assertions.assertEquals(
                RowDisposition.values().length, built.dispositions().size(), "every disposition is a key");
        Assertions.assertEquals(
                built.rowsSeen(),
                built.dispositions().values().stream()
                        .mapToLong(Long::longValue)
                        .sum());
        Assertions.assertEquals(
                built.rowsSeen(),
                ChunkBuilder.settle(built, Set.of()).values().stream()
                        .mapToLong(Long::longValue)
                        .sum());
        Assertions.assertEquals(
                counts(Map.of(
                        RowDisposition.STAGED, 2L,
                        RowDisposition.SCHEMA_INVALID, 1L,
                        RowDisposition.UNSUPPORTED_ENTITY, 2L,
                        RowDisposition.MISSING_IDS, 2L)),
                built.dispositions());
        Assertions.assertEquals(DesignRows.SHEET + ":7", built.lastCursor(), "the last row, whatever its disposition");
    }

    @Test
    void emptyChunkHasNoCursor() {
        ChunkBuilder.Built built = ChunkBuilder.build(List.of(), BATCH, ENTRY, MAPPER, NOW);

        Assertions.assertEquals(0, built.rowsSeen());
        Assertions.assertNull(built.lastCursor());
        Assertions.assertEquals(counts(Map.of()), built.dispositions());
    }
}
