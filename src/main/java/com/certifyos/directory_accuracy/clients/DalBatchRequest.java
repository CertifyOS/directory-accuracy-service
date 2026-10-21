package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;

/** The DAL's {@code BatchRequest}: the operations and whether they run in one transaction. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DalBatchRequest(Boolean transaction, List<DalOperation> operations) {
    public DalBatchRequest {
        operations = operations == null ? List.of() : List.copyOf(operations);
    }
}
