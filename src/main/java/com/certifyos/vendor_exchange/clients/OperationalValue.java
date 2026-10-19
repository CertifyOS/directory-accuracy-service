package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * An entity's operational value: the merged data, the contributing slices and which source won
 * each field. The map copies keep null values.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OperationalValue(
        String certifyId,
        String tenantId,
        Map<String, Object> data,
        List<Map<String, Object>> contributingSlices,
        Map<String, Object> contributionMap) {
    public OperationalValue {
        data = data == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(data));
        contributingSlices = contributingSlices == null ? List.of() : List.copyOf(contributingSlices);
        contributionMap =
                contributionMap == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(contributionMap));
    }
}
