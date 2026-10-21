package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * The {@code selection} block egress reports for a saved-filter export: in the status response
 * (egress's {@code SelectionStatusView}) and in the completion event. Keys are camel case on the
 * wire even inside the snake-case status response, so the naming is stated here rather than
 * inherited. Egress also sends {@code source}, {@code ref}, {@code cursor}, {@code expectedCount},
 * {@code duplicates} and {@code window}; this service reads none of them. {@code failureReason} is
 * egress's stored text, {@code "REASON"} or {@code "REASON (detail)"}; {@link EgressFailureReason}
 * reads the code from it.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonNaming(PropertyNamingStrategies.LowerCamelCaseStrategy.class)
public record EgressSelection(
        String state,
        Long requested,
        Long distinct,
        Long matched,
        Long unmatched,
        Long parts,
        String listUsedUri,
        String reusedFrom,
        Long attempts,
        String failureReason) {}
