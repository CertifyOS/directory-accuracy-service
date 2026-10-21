package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** One page of api-layer's template list: {@code templates}, {@code total}, {@code page}, {@code size}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EgressTemplateList(List<EgressTemplate> templates, int total, int page, int size) {
    public EgressTemplateList {
        templates = templates == null ? List.of() : List.copyOf(templates);
    }
}
