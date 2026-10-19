package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One source's slice of an entity. The data copy keeps null values, which a slice carries. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Slice(
        String certifyId, String sourceType, String crosswalkId, Map<String, Object> data, Instant updatedAt) {
    public Slice {
        data = data == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(data));
    }
}
