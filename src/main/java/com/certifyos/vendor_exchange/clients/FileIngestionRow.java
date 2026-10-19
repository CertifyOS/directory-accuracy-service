package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/**
 * One NDJSON line of the row stream, file-ingestion's {@code RowView.of}: the sheet, the row number,
 * the row status and one cell per mapped column. The raw cells and header it also sends are not read.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionRow(
        String sheet, Integer row, String status, Map<String, FileIngestionCell> values, List<String> warnings) {
    public FileIngestionRow {
        values = values == null ? Map.of() : Map.copyOf(values);
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }
}
