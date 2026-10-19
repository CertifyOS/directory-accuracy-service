package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping.Suffix;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class CandorRecsMappingTest {

    /**
     * Columns the mapping file lists: 4 ids, 5 address legs, 8 sent values and 10 fields of 5 suffixed
     * columns. The design says the September 2026 sample has 59; the gate settles the difference.
     */
    private static final int COLUMN_COUNT = 67;

    @Test
    void levelsAndColumns() throws Exception {
        for (String field : CandorRecsMapping.PRACTITIONER_FIELDS) {
            Assertions.assertEquals(FieldLevel.PRACTITIONER, CandorRecsMapping.levelOf(field), field);
        }
        for (String field : CandorRecsMapping.LOCATION_FIELDS) {
            Assertions.assertEquals(FieldLevel.LOCATION, CandorRecsMapping.levelOf(field), field);
        }
        Assertions.assertEquals(
                10, CandorRecsMapping.PRACTITIONER_FIELDS.size() + CandorRecsMapping.LOCATION_FIELDS.size());
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandorRecsMapping.levelOf("degrees"));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandorRecsMapping.levelOf(null));

        Assertions.assertEquals("phone_verification_status", CandorRecsMapping.column("phone", Suffix.STATUS));
        Assertions.assertEquals("location_candor_value", CandorRecsMapping.column("location", Suffix.VALUE));
        Assertions.assertEquals("npi_verification_reason", CandorRecsMapping.column("npi", Suffix.REASON));
        Assertions.assertEquals(
                "pcp_at_location_verification_confidence",
                CandorRecsMapping.column("pcp_at_location", Suffix.CONFIDENCE));
        Assertions.assertEquals(
                "first_name_verification_evidence", CandorRecsMapping.column("first_name", Suffix.EVIDENCE));
        Assertions.assertThrows(IllegalArgumentException.class, () -> CandorRecsMapping.column("zip", Suffix.STATUS));

        JsonNode mapping = mappingFile();
        Assertions.assertEquals(
                CandorRecsMapping.MAPPING_NAME, mapping.get("name").asText());
        Assertions.assertEquals(
                "Physicians Directory Accuracy", mapping.get("sheet").asText());
        List<String> names = new ArrayList<>();
        List<String> required = new ArrayList<>();
        for (JsonNode column : mapping.get("columns")) {
            String name = column.get("name").asText();
            names.add(name);
            for (JsonNode rule : column.get("rules")) {
                if ("required".equals(rule.asText())) {
                    required.add(name);
                }
            }
        }
        Assertions.assertEquals(COLUMN_COUNT, names.size());
        Assertions.assertEquals(expectedColumns(), names, "every column WideRow reads, in the file's order");
        Assertions.assertEquals(
                List.of(CandorRecsMapping.ENTITY_TYPE, CandorRecsMapping.PRACTITIONER_ID, CandorRecsMapping.NPI),
                required);
    }

    /** The column list the constants imply, built independently of the file. */
    private static List<String> expectedColumns() {
        List<String> columns = new ArrayList<>(List.of(
                CandorRecsMapping.ENTITY_TYPE,
                CandorRecsMapping.PRACTITIONER_ID,
                CandorRecsMapping.NPI,
                CandorRecsMapping.LOCATION_ID));
        columns.addAll(CandorRecsMapping.LOCATION_LEGS);
        List<String> fields = new ArrayList<>(CandorRecsMapping.PRACTITIONER_FIELDS);
        fields.addAll(CandorRecsMapping.LOCATION_FIELDS);
        for (String field : fields) {
            if (!field.equals(CandorRecsMapping.NPI) && !field.equals("location")) {
                columns.add(field);
            }
        }
        for (String field : fields) {
            for (Suffix suffix : Suffix.values()) {
                columns.add(CandorRecsMapping.column(field, suffix));
            }
        }
        return columns;
    }

    private static JsonNode mappingFile() throws Exception {
        try (InputStream in = CandorRecsMappingTest.class.getResourceAsStream("/mappings/candor-recs-v1.json")) {
            Assertions.assertNotNull(in, "the mapping file is on the classpath");
            return new ObjectMapper().readTree(in);
        }
    }
}
