package com.certifyos.directory_accuracy.clients;

/**
 * The selection failure reasons this service acts on, and the one way to read a reason code from
 * egress's text. Egress stores a reason as {@code "REASON"} or {@code "REASON (detail)"} (its
 * {@code SelectionLauncher.reasonName}); every reason check compares {@link #code(String)}, never
 * the raw text.
 */
public final class EgressFailureReason {

    /** The saved filter matched nobody: the batch ends {@code EMPTY}. */
    public static final String NO_MATCHING_PRACTITIONERS = "NO_MATCHING_PRACTITIONERS";

    /** A retry's reuse source was gone or empty: the retry is sent again with the filter id. */
    public static final String REUSE_SOURCE_UNUSABLE = "REUSE_SOURCE_UNUSABLE";

    private static final String DETAIL_SEPARATOR = " (";

    private EgressFailureReason() {}

    /** The reason code, without any detail; {@code null} for a missing or blank text. */
    public static String code(String text) {
        if (text == null || text.isBlank()) {
            return null;
        }
        int detail = text.indexOf(DETAIL_SEPARATOR);
        return (detail > 0 ? text.substring(0, detail) : text).strip();
    }

    /** True when {@code text} carries the reason {@code code}. */
    public static boolean is(String text, String code) {
        return code.equals(code(text));
    }
}
