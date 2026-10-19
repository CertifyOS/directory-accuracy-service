package com.certifyos.vendor_exchange.persistence;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.impl.TimeBasedEpochGenerator;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.YearMonth;
import java.util.HexFormat;
import java.util.Locale;
import java.util.regex.Pattern;

/**
 * Every identifier this service mints, in one place, so the shapes the design doc fixes are written
 * once. Batch ids use hyphens only: the vendor filename uses underscores as separators, so a tenant
 * id containing an underscore is refused at the schedule boundary and again here.
 */
public final class Ids {

    private static final TimeBasedEpochGenerator EVENT_IDS = Generators.timeBasedEpochGenerator();
    private static final Pattern NO_UNDERSCORE = Pattern.compile("^[A-Za-z0-9-]+$");
    private static final Pattern VENDOR = Pattern.compile("^[a-z0-9-]+$");
    private static final Pattern NPI = Pattern.compile("^\\d{10}$");
    private static final String NO_VALUE = "-";

    private Ids() {}

    /**
     * Audit event id: {@code ev-<uuid v7>}. Version 7 carries a millisecond timestamp in its high
     * bits, so new ids sort after old ones at the right edge of every index they start.
     *
     * @return a new event id
     */
    public static String eventId() {
        return "ev-" + EVENT_IDS.generate();
    }

    /**
     * Schedule id: {@code <tenantId>|<vendor>}; one schedule per pair.
     *
     * @param tenantId the tenant
     * @param vendor the vendor code, lower case
     * @return the schedule id
     */
    public static String scheduleId(String tenantId, String vendor) {
        return requireTenant(tenantId) + "|" + requireVendor(vendor);
    }

    /**
     * Batch id: {@code <tenantId>-<vendor>-<yyyy-MM>-<seq>} with a three-digit sequence.
     *
     * @param tenantId the tenant, without underscores
     * @param vendor the vendor code
     * @param period the month the batch is for
     * @param seq 1 for the first batch of a period, incremented by supersede
     * @return the batch id
     */
    public static String batchId(String tenantId, String vendor, YearMonth period, int seq) {
        if (seq < 1 || seq > 999) {
            throw new IllegalArgumentException("seq must be 1..999, got " + seq);
        }
        return requireTenant(tenantId) + "-" + requireVendor(vendor) + "-" + period + "-"
                + String.format(Locale.ROOT, "%03d", seq);
    }

    /**
     * NPI registry id: {@code <exportBatchId>|<npi>}, so a repeated upsert is a no-op.
     *
     * @param exportBatchId the batch
     * @param npi the ten-digit NPI
     * @return the registry id
     */
    public static String npiId(String exportBatchId, String npi) {
        if (!NPI.matcher(npi).matches()) {
            throw new IllegalArgumentException("npi must be ten digits, got " + npi);
        }
        return exportBatchId + "|" + npi;
    }

    /**
     * Egress correlation id: the batch id plus an attempt suffix, so a retry gives egress a new id
     * while the vendor keeps seeing one batch id.
     *
     * @param exportBatchId the batch
     * @param attempt 1 for the first attempt
     * @return the correlation id, for example {@code org-xyz-candor-2026-10-001-r1}
     */
    public static String correlationId(String exportBatchId, int attempt) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be at least 1, got " + attempt);
        }
        return exportBatchId + "-r" + attempt;
    }

    /**
     * The batch id a correlation id belongs to.
     *
     * @param correlationId a value produced by {@link #correlationId(String, int)}
     * @return the batch id
     */
    public static String batchIdOf(String correlationId) {
        int suffix = correlationId.lastIndexOf("-r");
        if (suffix < 1) {
            throw new IllegalArgumentException("not a correlation id: " + correlationId);
        }
        return correlationId.substring(0, suffix);
    }

    /**
     * Release id: {@code rel-<uuid v7>}.
     *
     * @return a new release id
     */
    public static String releaseId() {
        return "rel-" + EVENT_IDS.generate();
    }

    /**
     * Inbound file id: {@code <bucket>|<objectName>|<generation>}. Cloud Storage allows whitespace
     * and {@code |} in an object name, and a vendor file named that way must still reach the
     * unmatched uploads, so the object name leg is escaped rather than refused: {@code %}, {@code |}
     * and every whitespace character become {@code %XX} of their UTF-8 bytes. A name without them is
     * unchanged.
     *
     * @param bucket the storage bucket
     * @param objectName the object name, which may contain whitespace or {@code |}
     * @param generation the object generation
     * @return the upload id
     */
    public static String uploadId(String bucket, String objectName, String generation) {
        return join(leg("bucket", bucket), escapedLeg("objectName", objectName), leg("generation", generation));
    }

    /**
     * External reference of an inbound file: {@code <exportBatchId>|<generation>}.
     *
     * @param exportBatchId the batch
     * @param generation the object generation
     * @return the external reference
     */
    public static String externalRef(String exportBatchId, String generation) {
        return join(leg("exportBatchId", exportBatchId), leg("generation", generation));
    }

    /**
     * Field recommendation id: tenant, source, batch, practitioner, location and field joined with
     * {@code |}, with {@code -} for a practitioner-level field that has no location.
     *
     * @param tenantId the tenant
     * @param source the vendor source
     * @param exportBatchId the batch
     * @param certifyPractitionerId the practitioner
     * @param certifyLocationId the location, or null for a practitioner-level field
     * @param field the field name
     * @return the recommendation id
     */
    public static String fieldRecommendationId(
            String tenantId,
            String source,
            String exportBatchId,
            String certifyPractitionerId,
            String certifyLocationId,
            String field) {
        return join(
                leg("tenantId", tenantId),
                leg("source", source),
                leg("exportBatchId", exportBatchId),
                leg("certifyPractitionerId", certifyPractitionerId),
                optionalLeg("certifyLocationId", certifyLocationId),
                leg("field", field));
    }

    /**
     * Rejected value id: four readable legs, then the SHA-256 of the normalised value, so the raw
     * value never appears in an id or a log line. The four readable legs are refused when null,
     * blank or containing {@code |} or whitespace, and a location of {@code -} is refused because
     * {@code -} means none. The normalised value is refused only when null or blank; it may contain
     * {@code |} or whitespace (a location composite, a specialty list) because it is hashed.
     *
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param certifyLocationId the location, or null for a practitioner-level field
     * @param field the field name
     * @param normalizedValue the normalised value that was rejected
     * @return the rejected value id
     */
    public static String rejectedValueId(
            String tenantId,
            String certifyPractitionerId,
            String certifyLocationId,
            String field,
            String normalizedValue) {
        return join(
                leg("tenantId", tenantId),
                leg("certifyPractitionerId", certifyPractitionerId),
                optionalLeg("certifyLocationId", certifyLocationId),
                leg("field", field),
                sha256Hex(requireValue(normalizedValue)));
    }

    private static String join(String... legs) {
        return String.join("|", legs);
    }

    /** A leg that may be absent, written {@code -}; a real value of {@code -} would collide with it. */
    private static String optionalLeg(String name, String value) {
        if (value == null) {
            return NO_VALUE;
        }
        if (NO_VALUE.equals(value)) {
            throw new IllegalArgumentException(name + " must not be '" + NO_VALUE + "', which means absent");
        }
        return leg(name, value);
    }

    /**
     * A readable leg: not blank, no {@code |} (the separator) and no whitespace, so an id printed on
     * the {@code AUDIT} line keeps that line's {@code key=value} grammar.
     */
    private static String leg(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
        if (value.indexOf('|') >= 0) {
            throw new IllegalArgumentException(name + " must not contain '|'");
        }
        if (value.codePoints().anyMatch(Ids::isWhitespace)) {
            throw new IllegalArgumentException(name + " must not contain whitespace");
        }
        return value;
    }

    /** A leg that may hold any text: not blank, with {@code %}, {@code |} and whitespace escaped. */
    private static String escapedLeg(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
        StringBuilder escaped = new StringBuilder(value.length());
        value.codePoints().forEach(codePoint -> {
            if (codePoint == '%' || codePoint == '|' || isWhitespace(codePoint)) {
                for (byte part : new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8)) {
                    escaped.append(String.format(Locale.ROOT, "%%%02X", part & 0xFF));
                }
            } else {
                escaped.appendCodePoint(codePoint);
            }
        });
        return escaped.toString();
    }

    private static boolean isWhitespace(int codePoint) {
        return Character.isWhitespace(codePoint) || Character.isSpaceChar(codePoint);
    }

    private static String requireValue(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("normalizedValue must not be null or blank");
        }
        return value;
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required of every Java platform", exception);
        }
    }

    private static String requireTenant(String tenantId) {
        if (tenantId == null || !NO_UNDERSCORE.matcher(tenantId).matches()) {
            throw new IllegalArgumentException(
                    "tenant id must be letters, digits and hyphens only (the vendor filename uses underscores): "
                            + tenantId);
        }
        return tenantId;
    }

    private static String requireVendor(String vendor) {
        if (vendor == null || !VENDOR.matcher(vendor).matches()) {
            throw new IllegalArgumentException("vendor must be lower-case letters, digits and hyphens: " + vendor);
        }
        return vendor;
    }
}
