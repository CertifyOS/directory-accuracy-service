package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.clients.FileIngestionCell;
import com.certifyos.vendor_exchange.clients.FileIngestionRow;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping.Suffix;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * One {@code candor-recs-v1} row read back from file-ingestion: the ids, whether file-ingestion found
 * the row valid, and one {@link FieldCell} per field. A missing or null cell reads as a null value.
 *
 * @param sheet the sheet the row came from
 * @param row the row number in the sheet
 * @param schemaValid false when file-ingestion marked the row {@code INVALID}
 * @param problems every cell's problems, in column order
 * @param entityType the {@code entity_type} column
 * @param certifyPractitionerId our practitioner id, as the vendor echoed it
 * @param npi the NPI we sent
 * @param certifyLocationId our location id, as the vendor echoed it, or null
 * @param cells one cell per field, practitioner fields first, in {@link CandorRecsMapping} order
 */
public record WideRow(
        String sheet,
        int row,
        boolean schemaValid,
        List<String> problems,
        String entityType,
        String certifyPractitionerId,
        String npi,
        String certifyLocationId,
        Map<String, FieldCell> cells) {

    private static final String INVALID = "INVALID";
    private static final String REASON_SEPARATOR = ";";
    private static final String LEG_SEPARATOR = "|";

    /**
     * What the vendor said about one field of the row.
     *
     * @param field the field name
     * @param level its level
     * @param vendorStatus {@code INVALID}, {@code VALID}, {@code UNKNOWN}, {@code INCONCLUSIVE}, or
     *     {@code ""} when blank or absent
     * @param sentValue the value we sent; for {@code location} the five address legs joined with
     *     {@code |}
     * @param vendorValue the vendor's value, or null
     * @param reasons the reason cell split on {@code ;}, trimmed, blanks dropped
     * @param confidence the confidence cell, or null
     * @param evidenceJson the evidence cell as sent, or null
     */
    public record FieldCell(
            String field,
            FieldLevel level,
            String vendorStatus,
            String sentValue,
            String vendorValue,
            List<String> reasons,
            String confidence,
            String evidenceJson) {
        public FieldCell {
            reasons = reasons == null ? List.of() : List.copyOf(reasons);
        }
    }

    public WideRow {
        problems = problems == null ? List.of() : List.copyOf(problems);
        cells = cells == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(cells));
    }

    /**
     * Reads one streamed row.
     *
     * @param row the row as file-ingestion streams it
     * @return the wide row
     * @throws NullPointerException when the row carries no row number
     */
    public static WideRow from(FileIngestionRow row) {
        Map<String, FileIngestionCell> values = row.values();
        List<String> problems = new ArrayList<>();
        for (FileIngestionCell cell : values.values()) {
            if (cell != null) {
                problems.addAll(cell.problems());
            }
        }
        Map<String, FieldCell> cells = new LinkedHashMap<>();
        for (String field : CandorRecsMapping.PRACTITIONER_FIELDS) {
            cells.put(field, cellOf(values, field));
        }
        for (String field : CandorRecsMapping.LOCATION_FIELDS) {
            cells.put(field, cellOf(values, field));
        }
        return new WideRow(
                row.sheet(),
                Objects.requireNonNull(row.row(), "row number"),
                !INVALID.equals(row.status()),
                problems,
                valueOf(values, CandorRecsMapping.ENTITY_TYPE),
                valueOf(values, CandorRecsMapping.PRACTITIONER_ID),
                valueOf(values, CandorRecsMapping.NPI),
                valueOf(values, CandorRecsMapping.LOCATION_ID),
                cells);
    }

    /**
     * Whether the row names a location.
     *
     * @return true when {@code certify_location_id} is present and not blank
     */
    public boolean hasLocationId() {
        return certifyLocationId != null && !certifyLocationId.isBlank();
    }

    /**
     * Whether the row is about a practitioner, the only entity this mapping stages.
     *
     * @return true when {@code entity_type} is {@code PRACTITIONER} in any case, trimmed; the export
     *     sends {@code practitioner} and a vendor may echo it as sent
     */
    public boolean isPractitioner() {
        return entityType != null && CandorRecsMapping.ENTITY_PRACTITIONER.equalsIgnoreCase(entityType.strip());
    }

    private static FieldCell cellOf(Map<String, FileIngestionCell> values, String field) {
        String status = valueOf(values, CandorRecsMapping.column(field, Suffix.STATUS));
        return new FieldCell(
                field,
                CandorRecsMapping.levelOf(field),
                // VALID and INVALID in any case: the chunk builder matches the canonical form.
                status == null ? "" : status.strip().toUpperCase(Locale.ROOT),
                sentValueOf(values, field),
                valueOf(values, CandorRecsMapping.column(field, Suffix.VALUE)),
                reasonsOf(valueOf(values, CandorRecsMapping.column(field, Suffix.REASON))),
                valueOf(values, CandorRecsMapping.column(field, Suffix.CONFIDENCE)),
                valueOf(values, CandorRecsMapping.column(field, Suffix.EVIDENCE)));
    }

    private static String sentValueOf(Map<String, FileIngestionCell> values, String field) {
        if (!"location".equals(field)) {
            return valueOf(values, field);
        }
        List<String> legs = new ArrayList<>();
        for (String leg : CandorRecsMapping.LOCATION_LEGS) {
            String value = valueOf(values, leg);
            legs.add(value == null ? "" : value);
        }
        return String.join(LEG_SEPARATOR, legs);
    }

    private static List<String> reasonsOf(String cell) {
        if (cell == null) {
            return List.of();
        }
        return Arrays.stream(cell.split(REASON_SEPARATOR))
                .map(String::strip)
                .filter(reason -> !reason.isEmpty())
                .toList();
    }

    private static String valueOf(Map<String, FileIngestionCell> values, String column) {
        FileIngestionCell cell = values.get(column);
        return cell == null ? null : cell.value();
    }
}
