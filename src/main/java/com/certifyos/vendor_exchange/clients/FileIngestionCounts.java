package com.certifyos.vendor_exchange.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** An ingestion's row counts, on the status read and the completion event. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionCounts(Long populatedRows, Long validRows, Long invalidRows) {}
