package com.certifyos.vendor_exchange.ingestion.release;

import java.util.Locale;
import org.apache.commons.codec.language.Soundex;

/**
 * The name-sound guard (design: name corrections need the matching team). Practitioner matching joins
 * a slice to the Golden record by NPI plus the Soundex of the first and last names; a corrected name
 * that sounds different ({@code Bob} to {@code Robert}) would create a second practitioner, so until
 * Rollout ask 13 lands such a release is refused.
 */
public final class NameSound {

    private NameSound() {}

    /**
     * Whether two names sound the same, by US English Soundex over their letters.
     *
     * @param first one name
     * @param second the other
     * @return true when both have no letters, or both have letters and the same Soundex code
     */
    public static boolean same(String first, String second) {
        String left = letters(first);
        String right = letters(second);
        if (left.isEmpty() || right.isEmpty()) {
            return left.isEmpty() && right.isEmpty();
        }
        return Soundex.US_ENGLISH.encode(left).equals(Soundex.US_ENGLISH.encode(right));
    }

    private static String letters(String name) {
        return name == null ? "" : name.replaceAll("[^A-Za-z]", "").toUpperCase(Locale.ROOT);
    }
}
