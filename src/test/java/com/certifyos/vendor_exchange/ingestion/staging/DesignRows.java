package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.clients.FileIngestionCell;
import com.certifyos.vendor_exchange.clients.FileIngestionRow;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping.Suffix;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The design's sample row (step 5, row 913 of the September 2026 sheet), as file-ingestion streams it. */
final class DesignRows {

    static final String SHEET = "Physicians Directory Accuracy";
    static final int ROW = 913;
    static final String EVIDENCE = "[{\"reason\":\"direct_outreach\",\"evidence\":\"date\",\"value\":\"2026-06-17\"}]";

    private DesignRows() {}

    /** The sample row's cells, in the file's column order, every cell {@code VALID} at file-ingestion. */
    static Map<String, FileIngestionCell> values() {
        Map<String, FileIngestionCell> values = new LinkedHashMap<>();
        put(values, "entity_type", "PRACTITIONER");
        put(values, "certify_practitioner_id", "cert-000123");
        put(values, "npi", "1003002890");
        put(values, "certify_location_id", "loc-7781");
        put(values, "address_line1", "9100 E Mineral Cir");
        put(values, "address_line2", "Suite 200");
        put(values, "city", "Centennial");
        put(values, "state", "CO");
        put(values, "zip", "80112");
        put(values, "first_name", "Dana");
        put(values, "last_name", "Whitfield");
        put(values, "specialties", "[\"Family Medicine\"]");
        put(values, "practice_name", "AdventHealth");
        put(values, "phone", "3036790177");
        put(values, "fax", "3036790178");
        put(values, "accepting_new_patients", "true");
        put(values, "pcp_at_location", "");
        put(values, CandorRecsMapping.column("practice_name", Suffix.STATUS), "INVALID");
        put(values, CandorRecsMapping.column("practice_name", Suffix.VALUE), "Greenwood Village");
        put(values, CandorRecsMapping.column("practice_name", Suffix.REASON), "direct_outreach");
        put(values, CandorRecsMapping.column("practice_name", Suffix.CONFIDENCE), "VERY HIGH");
        put(values, CandorRecsMapping.column("practice_name", Suffix.EVIDENCE), EVIDENCE);
        put(values, CandorRecsMapping.column("phone", Suffix.STATUS), "VALID");
        put(values, CandorRecsMapping.column("phone", Suffix.CONFIDENCE), "HIGH");
        put(values, CandorRecsMapping.column("phone", Suffix.EVIDENCE), "[]");
        put(values, CandorRecsMapping.column("first_name", Suffix.STATUS), "INVALID");
        put(values, CandorRecsMapping.column("first_name", Suffix.VALUE), "Danielle");
        put(values, CandorRecsMapping.column("first_name", Suffix.REASON), "direct_outreach; nppes_update ;");
        put(values, CandorRecsMapping.column("first_name", Suffix.CONFIDENCE), "HIGH");
        put(values, CandorRecsMapping.column("first_name", Suffix.EVIDENCE), "called the office");
        put(values, CandorRecsMapping.column("fax", Suffix.STATUS), "UNKNOWN");
        return values;
    }

    /** The sample row with the given cells replaced; a null value removes the cell. */
    static FileIngestionRow row(String status, Map<String, FileIngestionCell> overrides) {
        Map<String, FileIngestionCell> values = values();
        overrides.forEach((column, cell) -> {
            if (cell == null) {
                values.remove(column);
            } else {
                values.put(column, cell);
            }
        });
        return new FileIngestionRow(SHEET, ROW, status, values, List.of());
    }

    /** The sample row as file-ingestion streams it. */
    static FileIngestionRow row() {
        return row("VALID", Map.of());
    }

    static FileIngestionCell valid(String value) {
        return new FileIngestionCell(value, "VALID", List.of());
    }

    private static void put(Map<String, FileIngestionCell> values, String column, String value) {
        values.put(column, valid(value));
    }
}
