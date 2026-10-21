package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * Egress's job status, as {@code GET /api/v1/egress/status/practitioner/{tenantId}/{correlationId}} returns it.
 * {@code selection} is present only for a saved-filter export.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy.class)
public record EgressStatusResponse(
        String type,
        String tenantId,
        String correlationId,
        String table,
        Long totalRows,
        Long rowsWithData,
        Long successfulRows,
        Long unsuccessfulRows,
        String state,
        String phase,
        String gcsUri,
        Boolean gcsComplete,
        EgressSelection selection) {}
