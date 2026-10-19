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
     * Inbound file id: {@code <bucket>|<objectName>|<generation>}.
     *
     * @param bucket the storage bucket
     * @param objectName the object name
     * @param generation the object generation
     * @return the upload id
     */
    public static String uploadId(String bucket, String objectName, String generation) {
        return join(leg("bucket", bucket), leg("objectName", objectName), leg("generation", generation));
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
     * blank or containing {@code |}. The normalised value is refused only when null or blank; it
     * may contain {@code |} (a location composite, a specialty list) because it is hashed.
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

    private static String optionalLeg(String name, String value) {
        return value == null ? "-" : leg(name, value);
    }

    private static String leg(String name, String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be null or blank");
        }
        if (value.indexOf('|') >= 0) {
            throw new IllegalArgumentException(name + " must not contain '|'");
        }
        return value;
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
