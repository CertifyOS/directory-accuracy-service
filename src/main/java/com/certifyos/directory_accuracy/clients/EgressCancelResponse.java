package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/** Egress's 202 answer to a cancel: {@code {"status": "cancellation_requested"}}. A 409 (already terminal) is thrown, not returned. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EgressCancelResponse(String status) {}
