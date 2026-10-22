package com.certifyos.directory_accuracy.export.events;

import com.certifyos.directory_accuracy.clients.EgressSelection;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/**
 * The egress completion event body from the design's contracts: what egress publishes when an
 * export job finishes or fails. {@code correlationId} is this service's {@code <batchId>-r<attempt>}
 * and is how the event finds its batch. {@code failureReason} (a reason code, null on success) and
 * {@code selection} are egress ask 5's extension and may be absent until egress sends them.
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
        String failureReason,
        EgressSelection selection,
        Instant completedAt) {}
