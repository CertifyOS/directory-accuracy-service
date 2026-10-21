package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Egress's error body ({@code ErrorResponse}): a stable code in {@code error}, a message, and
 * whether egress thinks the call may succeed later.
 *
 * @param error the code, for example {@code INVALID_REQUEST} or {@code SELECTION_QUOTA_EXCEEDED}
 * @param message the human text; for a refused reuse it names why
 * @param retryable egress's own retry hint
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EgressError(String error, String message, Boolean retryable) {

    /** The code egress returns with {@code 403} when saved-filter selection is switched off. */
    public static final String SELECTION_SOURCE_DISABLED = "SELECTION_SOURCE_DISABLED";

    /** The code egress returns with {@code 429} when the tenant's selection exports are at the limit. */
    public static final String SELECTION_QUOTA_EXCEEDED = "SELECTION_QUOTA_EXCEEDED";

    private static final String REUSE_PREFIX = "selection.reuse_selection_from ";

    /**
     * The three request-time messages that mean the earlier attempt's list cannot be reused
     * ({@code ExportOrchestrationService.checkSelectionCanStart}). Other messages with the same
     * prefix are request-form errors and are not a reason to evaluate the filter again.
     */
    private static final java.util.List<String> UNUSABLE_SOURCE = java.util.List.of(
            "does not name a selection export of this tenant and type",
            "names a selection that is not finished",
            "names a selection whose list has expired");

    /** True when this {@code 400} says the reuse source cannot be used. */
    public boolean reuseSourceUnusable() {
        return message != null
                && message.startsWith(REUSE_PREFIX)
                && UNUSABLE_SOURCE.contains(message.substring(REUSE_PREFIX.length()));
    }

    /** {@code error: message}, for {@code lastError}. */
    public String describe() {
        return error + ": " + message;
    }
}
