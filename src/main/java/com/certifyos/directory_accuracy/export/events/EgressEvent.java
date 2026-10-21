package com.certifyos.directory_accuracy.export.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/**
 * The egress completion event body from the design's contracts: what egress publishes when an
 * export job finishes or fails. {@code correlationId} is this service's {@code <batchId>-r<attempt>}
 * and is how the event finds its batch.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EgressEvent(
        String schemaVersion,
        String type,
        String tenantId,
        String correlationId,
        String phase,
        String outputUri,
        Long totalRecords,
        Long totalRows,
        String failureMessage,
        Instant completedAt) {}
