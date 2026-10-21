package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** One mapped cell of a row: the value, its validation status and the problems found, if any. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionCell(String value, String status, List<String> problems) {
    public FileIngestionCell {
        problems = problems == null ? List.of() : List.copyOf(problems);
    }
}
