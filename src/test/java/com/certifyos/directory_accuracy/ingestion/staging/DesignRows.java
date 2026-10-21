package com.certifyos.directory_accuracy.ingestion.staging;

import com.certifyos.directory_accuracy.clients.FileIngestionCell;
import com.certifyos.directory_accuracy.clients.FileIngestionRow;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria;
import com.certifyos.directory_accuracy.export.schedule.SelectionCriteria.Operator;
import com.certifyos.directory_accuracy.ingestion.events.ArrivalEvent;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundFile;
import com.certifyos.directory_accuracy.ingestion.inbound.InboundNames;
import com.certifyos.directory_accuracy.ingestion.staging.CandorRecsMapping.Suffix;
import com.certifyos.directory_accuracy.persistence.Ids;
import java.time.Instant;
import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The design's sample row (step 5, row 913 of the September 2026 sheet), as file-ingestion streams it.
 * Public for the staging job's integration test, which streams these rows as NDJSON.
 */
public final class DesignRows {

    public static final String SHEET = "Physicians Directory Accuracy";
    public static final int ROW = 913;
    public static final String EVIDENCE =
            "[{\"reason\":\"direct_outreach\",\"evidence\":\"date\",\"value\":\"2026-06-17\"}]";

    /** {@code INVALID} cells per sample row: {@code first_name} and {@code practice_name}. */
    public static final int INVALID_PER_ROW = 2;

    /** {@code VALID} cells per sample row: {@code phone}. */
    public static final int VALID_PER_ROW = 1;

    /** Unverified cells per sample row: {@code fax} is {@code UNKNOWN}, six more are blank. */
    public static final int UNVERIFIED_PER_ROW = 7;

    private DesignRows() {}

    /**
     * The sample row's cells, in the file's column order, every cell {@code VALID} at file-ingestion.
     * Per row: {@code first_name} (practitioner level) and {@code practice_name} (location level) are
     * {@code INVALID}, {@code phone} is {@code VALID}, the other seven fields are unverified.
     */
    public static Map<String, FileIngestionCell> values() {
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

    /** The sample row as file-ingestion streams it. */
    public static FileIngestionRow row() {
        return row("VALID", Map.of());
    }

    /** The sample row with the given cells replaced; a null value removes the cell. */
    public static FileIngestionRow row(String status, Map<String, FileIngestionCell> overrides) {
        return row(ROW, status, overrides);
    }

    /** The sample row numbered {@code number} with the given cells replaced; a null value removes the cell. */
    public static FileIngestionRow row(int number, String status, Map<String, FileIngestionCell> overrides) {
        Map<String, FileIngestionCell> values = values();
        overrides.forEach((column, cell) -> {
            if (cell == null) {
                values.remove(column);
            } else {
                values.put(column, cell);
            }
        });
        return new FileIngestionRow(SHEET, number, status, values, List.of());
    }

    /**
     * A {@code VALID} sample row numbered {@code number} for one practitioner at one location, with
     * further cells replaced.
     *
     * @param number the row number
     * @param practitionerId {@code certify_practitioner_id}
     * @param npi {@code npi}
     * @param locationId {@code certify_location_id}, null to leave the cell out
     * @param overrides further cells; a null value removes the cell
     * @return the row
     */
    public static FileIngestionRow row(
            int number,
            String practitionerId,
            String npi,
            String locationId,
            Map<String, FileIngestionCell> overrides) {
        Map<String, FileIngestionCell> cells = new LinkedHashMap<>();
        cells.put("certify_practitioner_id", valid(practitionerId));
        cells.put("npi", valid(npi));
        cells.put("certify_location_id", locationId == null ? null : valid(locationId));
        cells.putAll(overrides);
        return row(number, "VALID", cells);
    }

    /** A {@code VALID} sample row numbered {@code number} for one practitioner at one location. */
    public static FileIngestionRow row(int number, String practitionerId, String npi, String locationId) {
        return row(number, practitionerId, npi, locationId, Map.of());
    }

    /** The {@code _id} one field of {@code batch} is written under. */
    public static String idOf(ExportBatch batch, String practitionerId, String locationId, String field) {
        return Ids.fieldRecommendationId(
                batch.tenantId(), batch.vendor(), batch.id(), practitionerId, locationId, field);
    }

    /** The design's batch, {@code org-xyz-candor-2026-10-001}. */
    public static ExportBatch batch() {
        return batch("org-xyz", YearMonth.of(2026, 10));
    }

    /** A candor batch of {@code tenantId} for {@code period}, sequence 1. */
    public static ExportBatch batch(String tenantId, YearMonth period) {
        SelectionCriteria criteria = new SelectionCriteria(
                List.of(new SelectionCriteria.Clause("data.delegationStatus", Operator.IN, List.of("Direct"))));
        return ExportBatch.scheduled(tenantId, "candor", period, 1, criteria, Instant.parse("2026-10-01T06:00:12Z"));
    }

    /** The upload the design's row came from. */
    public static InboundFile entry() {
        String objectName = "to/org-xyz/org-xyz_org-xyz-candor-2026-10-001_CB-2026-10-001_20261015.csv";
        ArrivalEvent event = new ArrivalEvent(
                "vendor-sftp",
                objectName,
                "1760520000123456",
                "OBJECT_FINALIZE",
                Instant.parse("2026-10-15T09:12:40Z"),
                null);
        return InboundFile.received(
                event, InboundNames.parse(objectName).orElseThrow(), Instant.parse("2026-10-15T09:12:41Z"));
    }

    /** A cell file-ingestion found valid, holding {@code value}. */
    public static FileIngestionCell valid(String value) {
        return new FileIngestionCell(value, "VALID", List.of());
    }

    private static void put(Map<String, FileIngestionCell> values, String column, String value) {
        values.put(column, valid(value));
    }
}
