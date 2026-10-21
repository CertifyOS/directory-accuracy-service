package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** The register answer: the ingestion id and its status ({@code RECEIVED} when new). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FileIngestionRegisterResponse(String ingestionId, String status) {}
