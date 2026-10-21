package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One source's slice of an entity. The data copy keeps null values, which a slice carries. The DAL
 * names the slice id {@code certifyId} on practitioner and entity-address slices and {@code
 * certifyLocationId} on location slices; both read into {@code certifyId}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Slice(
        @JsonAlias({"certifyId", "certifyLocationId"}) String certifyId,
        String sourceType,
        String crosswalkId,
        Map<String, Object> data,
        Instant updatedAt) {
    public Slice {
        data = data == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(data));
    }
}
