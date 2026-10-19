package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.clients.FileIngestionCounts;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * file-ingestion's completion event body (design step 4, confirmed by its team 2026-10-16), read
 * from the base64 message data. The handler keys on {@code externalRef}; {@code meta} is our
 * register request echoed back and {@code identifier} is always ours, because the topic is ours.
 * Fields this consumer does not know are ignored, so file-ingestion may add to the body.
 *
 * @param ingestionId file-ingestion's id for the ingestion
 * @param tenantId the tenant the ingestion ran for
 * @param identifier the consumer identifier, {@code vendor-ingestion}
 * @param status {@code COMPLETED}, {@code PARTIAL} or {@code INVALID}
 * @param counts the row counts on success, null on a refusal
 * @param reason why file-ingestion refused the file, on {@code INVALID} only
 * @param externalRef {@code <exportBatchId>|<generation>}, how the entry is found
 * @param meta the register request's {@code meta}, echoed back
 * @param rows the path of the parsed rows
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CompletionEvent(
        String ingestionId,
        String tenantId,
        String identifier,
        String status,
        FileIngestionCounts counts,
        String reason,
        String externalRef,
        Map<String, String> meta,
        String rows) {

    public CompletionEvent {
        // A copy that keeps a null value: a stray null in the echo must not make the event unreadable.
        meta = meta == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(meta));
    }
}
