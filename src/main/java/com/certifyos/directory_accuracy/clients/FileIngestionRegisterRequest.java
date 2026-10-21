package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

/**
 * The file-ingestion register request from the design's contracts (step 3). {@code sourceUri} is
 * the archive path file-ingestion copies from; today's file-ingestion {@code RegisterRequest} has no
 * such field (rollout ask 9). {@code meta} comes back unchanged on the completion event.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionRegisterRequest(
        String mappingId, String filename, String sourceUri, String externalRef, Map<String, String> meta) {
    public FileIngestionRegisterRequest {
        meta = meta == null ? Map.of() : Map.copyOf(meta);
    }
}
