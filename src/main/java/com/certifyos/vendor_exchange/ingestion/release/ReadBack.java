package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.clients.OperationalValue;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The read-back verdicts (design D19, plan P9): what the operational value shows for each released
 * field. Our slice's certify id absent from {@code contributingSlices} means survivorship did not use
 * it at all, so every field is {@code NOT_APPLIED}; otherwise the {@code contributionMap} winner at the
 * field's data path is ours ({@code WON}), another source ({@code OUTRANKED}), or absent ({@code
 * UNKNOWN}).
 *
 * <p>Provisional until Gate 7 pins one key shape: a path is looked up as {@code firstName}, {@code
 * data.firstName}, the snake-case {@code first_name} and {@code data.first_name}, and a contact path
 * ({@code contacts.phone}) also as the whole {@code contacts} array; a winner is a source-type string
 * or an object carrying {@code sourceType}. A winner of {@code certify-cleanser:<our source>} is our
 * cleansed slice and counts as {@code WON} (ask 12).
 */
public final class ReadBack {

    static final String CLEANSER_PREFIX = "certify-cleanser:";

    private static final Map<String, String> SNAKE = Map.of("firstName", "first_name", "lastName", "last_name");

    private ReadBack() {}

    /** A released field's outcome in the Golden record. */
    public enum Outcome {
        WON,
        OUTRANKED,
        NOT_APPLIED,
        UNKNOWN
    }

    /**
     * One field's verdict.
     *
     * @param outcome the outcome
     * @param winningSource the source the operational value shows for the field, or null when unknown
     */
    public record Verdict(Outcome outcome, String winningSource) {}

    /**
     * The verdict for each released field of one entity.
     *
     * @param ov the entity's operational value, read with lineage
     * @param ownSliceId the certify id of the slice we wrote, or null when it is not known
     * @param sourceType our source type, {@code candor:<tenantId>}
     * @param fieldRecommendationIdToField each released field recommendation and its field
     * @return field recommendation id to verdict, in the given order
     */
    public static Map<String, Verdict> evaluate(
            OperationalValue ov,
            String ownSliceId,
            String sourceType,
            Map<String, String> fieldRecommendationIdToField) {
        Map<String, Verdict> verdicts = new LinkedHashMap<>();
        boolean contributed = ownSliceId != null && ov.contributingSlices().contains(ownSliceId);
        fieldRecommendationIdToField.forEach((id, field) -> verdicts.put(
                id,
                contributed
                        ? verdict(ov.contributionMap(), field, sourceType)
                        : new Verdict(Outcome.NOT_APPLIED, null)));
        return Collections.unmodifiableMap(verdicts);
    }

    private static Verdict verdict(Map<String, Object> contributionMap, String field, String sourceType) {
        String path = SliceMerge.fieldToDataPath().get(field);
        if (path == null) {
            return new Verdict(Outcome.UNKNOWN, null);
        }
        String winner = null;
        for (String key : keysFor(path)) {
            if (contributionMap.containsKey(key)) {
                winner = sourceOf(contributionMap.get(key));
                break;
            }
        }
        if (winner == null) {
            return new Verdict(Outcome.UNKNOWN, null);
        }
        boolean ours = winner.equals(sourceType) || winner.equals(CLEANSER_PREFIX + sourceType);
        return new Verdict(ours ? Outcome.WON : Outcome.OUTRANKED, winner);
    }

    /** The contribution-map keys a data path may appear under, most specific first. */
    static List<String> keysFor(String path) {
        List<String> keys = new ArrayList<>();
        keys.add(path);
        keys.add("data." + path);
        String snake = SNAKE.get(path);
        if (snake != null) {
            keys.add(snake);
            keys.add("data." + snake);
        }
        int dot = path.indexOf('.');
        if (dot > 0) {
            String array = path.substring(0, dot);
            keys.add(array);
            keys.add("data." + array);
        }
        return keys;
    }

    private static String sourceOf(Object value) {
        if (value instanceof String text && !text.isBlank()) {
            return text;
        }
        if (value instanceof Map<?, ?> map && map.get("sourceType") instanceof String text && !text.isBlank()) {
            return text;
        }
        return null;
    }
}
