package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/** An ingestion's status, as {@code GET /v1/ingestion/{id}} returns it. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionStatus(
        String ingestionId,
        String status,
        FileIngestionCounts counts,
        Instant registeredAt,
        Instant terminalAt,
        String invalidReason) {}
