package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/**
 * The fields of api-layer's egress template this service pins onto a batch before the export
 * request: the version-specific mappings CSV, separator, output format and row expansion keys.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EgressTemplate(
        String id,
        String tenantId,
        String templateName,
        String entityType,
        String status,
        Integer version,
        String mappingsCsvUrl,
        String separator,
        String outputFormat,
        List<String> rowExpansionKeys) {
    public EgressTemplate {
        rowExpansionKeys = rowExpansionKeys == null ? List.of() : List.copyOf(rowExpansionKeys);
    }
}
