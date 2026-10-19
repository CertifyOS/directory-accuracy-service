package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** One operation's outcome in a DAL batch answer. The result copy keeps null values. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DalOperationResult(String alias, Boolean success, Map<String, Object> result, String error) {
    public DalOperationResult {
        result = result == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(result));
    }
}
