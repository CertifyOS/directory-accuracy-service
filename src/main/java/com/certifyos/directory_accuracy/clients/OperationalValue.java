package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An entity's operational value: the merged data, the contributing slices and which source won
 * each field. The map copies keep null values. The DAL names the id {@code certifyId} on a
 * practitioner and {@code certifyLocationId} on a location; both read into {@code certifyId}.
 * {@code contributionMap} is empty unless the read asked for lineage. {@code contributingSlices}
 * holds the contributing slices' ids, as the DAL sends them.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OperationalValue(
        @JsonAlias({"certifyId", "certifyLocationId"}) String certifyId,
        String tenantId,
        Map<String, Object> data,
        List<String> contributingSlices,
        Map<String, Object> contributionMap) {
    public OperationalValue {
        data = data == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(data));
        contributingSlices = contributingSlices == null ? List.of() : List.copyOf(contributingSlices);
        contributionMap =
                contributionMap == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(contributionMap));
    }
}
