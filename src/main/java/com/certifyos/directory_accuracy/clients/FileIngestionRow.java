package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One NDJSON line of the row stream, file-ingestion's {@code RowView.of}: the sheet, the row number,
 * the row status and one cell per mapped column. The raw cells and header it also sends are not read.
 * The cell map keeps the file's column order and tolerates a null cell.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionRow(
        String sheet, Integer row, String status, Map<String, FileIngestionCell> values, List<String> warnings) {
    public FileIngestionRow {
        values = values == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(values));
        warnings = warnings == null ? List.of() : List.copyOf(warnings);
    }
}
