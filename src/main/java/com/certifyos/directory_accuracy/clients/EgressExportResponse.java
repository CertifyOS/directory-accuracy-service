package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Egress's answer to an export request, 202 in both of its shapes: a new job comes back flat with
 * {@code state: PENDING}; a repeated correlation id comes back wrapped in {@code job}. {@link
 * #jobReference()} reads whichever is present. Keys are snake case on the wire.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record EgressExportResponse(
        Job job, String type, String tenantId, String correlationId, String table, Integer ttlDays, String state) {

    /** The job reference egress returns for an already-known correlation id. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
    public record Job(String type, String tenantId, String correlationId, String table, Integer ttlDays) {}

    /** The job reference, from the wrapper when present, else from the flat fields. */
    public Job jobReference() {
        return job != null ? job : new Job(type, tenantId, correlationId, table, ttlDays);
    }
}
