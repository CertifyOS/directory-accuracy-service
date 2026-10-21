package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * The egress export request from the design's contracts (step 3). Field names are egress's own
 * {@code ExportRequest}. {@code rowExpansionKeys} is a string on the wire because egress declares
 * it so and parses it itself (its pipeline options take a string); the design's example shows an
 * array and is corrected in the design-doc edits. {@code destination} is the field egress adds for
 * this service (egress ask): the create-only copy of the finished file.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EgressExportRequest(
        String type,
        String tenantId,
        String correlationId,
        String templateId,
        String mappingsCsvUrl,
        String separator,
        String outputFormat,
        String rowExpansionKeys,
        List<String> npiFilter,
        String userId,
        Destination destination) {

    /** The entity type this service exports. */
    public static final String PRACTITIONER = "practitioner";

    public EgressExportRequest {
        npiFilter = npiFilter == null ? List.of() : List.copyOf(npiFilter);
    }

    /** Where egress must place the finished file. */
    public record Destination(String bucket, String objectName) {}
}
