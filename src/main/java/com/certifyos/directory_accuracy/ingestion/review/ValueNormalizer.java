package com.certifyos.directory_accuracy.ingestion.review;

import com.certifyos.directory_accuracy.ingestion.staging.CandorRecsMapping;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

/**
 * The rejected-value memory's normaliser, version 1 (design step 7): two spellings of one value
 * normalise to one string, so a value rejected in October and again in December is one row. The
 * version is stored on every row; a rule change bumps it and never rewrites an old row.
 *
 * <ul>
 *   <li>{@code phone}, {@code fax}: digits only; a leading {@code 1} dropped from 11 digits
 *   <li>{@code first_name}, {@code last_name}, {@code practice_name}: trimmed, upper-cased,
 *       punctuation removed, spaces collapsed
 *   <li>{@code specialties}: entries split on {@code ;} or {@code ,}, each trimmed, upper-cased and
 *       single-spaced, blanks and repeats dropped, sorted, joined with {@code ;}
 *   <li>{@code accepting_new_patients}, {@code pcp_at_location}: {@code true} for {@code Y},
 *       {@code YES}, {@code TRUE} or {@code 1} (any case), else {@code false}
 *   <li>{@code location}: {@code address_line1|city|state|zip} from the five legs staging joins with
 *       {@code |} ({@code address_line2} is dropped, as the design's table does; four legs read as
 *       line 1, city, state, zip); line and city as names, then {@code SUITE→STE},
 *       {@code STREET→ST}, {@code AVENUE→AVE}; state its letters, upper-cased; zip its first five
 *       digits. A value without {@code |} (the vendor's single location cell) is one line under the
 *       line rule.
 *   <li>{@code npi}: digits only
 * </ul>
 *
 * A null or blank value, or one with nothing left after its rule, is {@code ""}.
 */
public final class ValueNormalizer {

    /** Stamped on every rejected-value row as {@code normalizationVersion}. */
    public static final int VERSION = 1;

    private static final Pattern NOT_DIGIT = Pattern.compile("\\D");
    private static final Pattern NOT_LETTER = Pattern.compile("[^A-Z]");
    private static final Pattern PUNCTUATION = Pattern.compile("[\\p{P}\\p{S}]");
    private static final Pattern SPACES = Pattern.compile("\\s+");
    private static final Pattern LIST_SEPARATOR = Pattern.compile("[;,]");
    private static final Set<String> YES = Set.of("Y", "YES", "TRUE", "1");
    private static final Map<String, String> STREET_WORDS = Map.of("SUITE", "STE", "STREET", "ST", "AVENUE", "AVE");
    private static final String LEG_SEPARATOR = "|";
    private static final int PHONE_WITH_COUNTRY = 11;
    private static final int ZIP_DIGITS = 5;

    private ValueNormalizer() {}

    /**
     * Normalises one value of one field.
     *
     * @param field a {@code candor-recs-v1} field name
     * @param raw the value as received, may be null
     * @return the normal form, {@code ""} for no value
     * @throws IllegalArgumentException for a field that is not one of the ten
     */
    public static String normalize(String field, String raw) {
        CandorRecsMapping.levelOf(field);
        if (raw == null || raw.isBlank()) {
            return "";
        }
        return switch (field) {
            case "phone", "fax" -> phone(raw);
            case "first_name", "last_name", "practice_name" -> name(raw);
            case "specialties" -> specialties(raw);
            case "accepting_new_patients", "pcp_at_location" -> flag(raw);
            case "location" -> location(raw);
            case "npi" -> digits(raw);
            default -> throw new IllegalArgumentException("no normalisation rule for " + field);
        };
    }

    private static String phone(String raw) {
        String digits = digits(raw);
        return digits.length() == PHONE_WITH_COUNTRY && digits.charAt(0) == '1' ? digits.substring(1) : digits;
    }

    private static String digits(String raw) {
        return NOT_DIGIT.matcher(raw).replaceAll("");
    }

    private static String name(String raw) {
        String upper = raw.toUpperCase(Locale.ROOT);
        return SPACES.matcher(PUNCTUATION.matcher(upper).replaceAll(""))
                .replaceAll(" ")
                .strip();
    }

    private static String specialties(String raw) {
        Set<String> entries = new TreeSet<>();
        for (String entry : LIST_SEPARATOR.split(raw)) {
            String clean =
                    SPACES.matcher(entry.strip().toUpperCase(Locale.ROOT)).replaceAll(" ");
            if (!clean.isEmpty()) {
                entries.add(clean);
            }
        }
        return String.join(";", entries);
    }

    private static String flag(String raw) {
        return YES.contains(raw.strip().toUpperCase(Locale.ROOT)) ? "true" : "false";
    }

    private static String location(String raw) {
        if (!raw.contains(LEG_SEPARATOR)) {
            return line(raw);
        }
        String[] legs = raw.split(Pattern.quote(LEG_SEPARATOR), -1);
        if (legs.length != 4 && legs.length != 5) {
            return line(String.join(" ", legs));
        }
        int city = legs.length - 3;
        List<String> parts = new ArrayList<>(4);
        parts.add(line(legs[0]));
        parts.add(line(legs[city]));
        parts.add(NOT_LETTER.matcher(legs[city + 1].toUpperCase(Locale.ROOT)).replaceAll(""));
        String zip = digits(legs[city + 2]);
        parts.add(zip.length() > ZIP_DIGITS ? zip.substring(0, ZIP_DIGITS) : zip);
        return parts.stream().allMatch(String::isEmpty) ? "" : String.join(LEG_SEPARATOR, parts);
    }

    private static String line(String raw) {
        List<String> words = new ArrayList<>();
        for (String word : SPACES.split(name(raw))) {
            if (!word.isEmpty()) {
                words.add(STREET_WORDS.getOrDefault(word, word));
            }
        }
        return String.join(" ", words);
    }
}
