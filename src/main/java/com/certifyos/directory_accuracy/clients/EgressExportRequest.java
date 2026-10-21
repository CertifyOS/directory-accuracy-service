package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;

/**
 * The egress export request from the design's contracts (step 3). Field names are egress's own
 * {@code ExportRequest}. {@code rowExpansionKeys} is a string on the wire because egress declares
 * it so and parses it itself (its pipeline options take a string); the design's example shows an
 * array and is corrected in the design-doc edits. {@code selection} is egress's saved-filter
 * selection ({@code SelectionRequest}): the first attempt names the filter, a retry names the earlier
 * attempt whose frozen list it reuses. {@code destination} is the field egress adds for this service
 * (egress ask): the create-only copy of the finished file.
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
        Selection selection,
        String userId,
        Destination destination) {

    /** The entity type this service exports. */
    public static final String PRACTITIONER = "practitioner";

    public EgressExportRequest {
        npiFilter = npiFilter == null ? List.of() : List.copyOf(npiFilter);
    }

    /**
     * The saved-filter selection. {@code listUsed} is always true: egress writes the list it
     * exported, which operators diff against the file on a count mismatch.
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Selection(String source, String filterId, Boolean listUsed, String reuseSelectionFrom) {

        /** The only source egress accepts. */
        public static final String SAVED_FILTER = "SAVED_FILTER";

        /** A first attempt, or a retry whose reuse egress refused: egress evaluates the filter. */
        public static Selection savedFilter(String filterId) {
            return new Selection(SAVED_FILTER, filterId, true, null);
        }

        /** A retry: egress copies the frozen list of the attempt with {@code correlationId}. */
        public static Selection reuse(String correlationId) {
            return new Selection(null, null, true, correlationId);
        }
    }

    /** Where egress must place the finished file. */
    public record Destination(String bucket, String objectName) {}
}
