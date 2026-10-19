package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The DAL's {@code OperationRequest}: one operation on one resource. {@code body} and {@code
 * filters} are the DAL's free-form maps; their copies keep null values, which a slice body carries.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DalOperation(
        String resource,
        String alias,
        String operation,
        Map<String, Object> body,
        Map<String, Object> filters,
        List<String> returning) {
    public DalOperation {
        body = body == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(body));
        filters = filters == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(filters));
        returning = returning == null ? List.of() : List.copyOf(returning);
    }
}
