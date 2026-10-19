package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import java.util.Map;

/** One page of a tenant's configuration entries and the total count. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record TenantConfigurationPage(List<Map<String, Object>> data, Long totalCount) {
    public TenantConfigurationPage {
        data = data == null ? List.of() : List.copyOf(data);
    }
}
