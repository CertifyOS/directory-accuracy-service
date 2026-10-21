package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** One page of api-layer's practitioner list: {@code data} and {@code totalCount}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PagedPractitioners(List<PractitionerRef> data, Long totalCount) {
    public PagedPractitioners {
        data = data == null ? List.of() : List.copyOf(data);
    }
}
