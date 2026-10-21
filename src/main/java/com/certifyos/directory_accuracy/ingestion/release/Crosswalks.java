package com.certifyos.directory_accuracy.ingestion.release;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The platform's own crosswalk recipes (design D21), so a candor slice joins the Golden record the way
 * a roster or tenant UI slice does: practitioner = the NPI ({@code EntityCrosswalkGenerator
 * .generateCorePractitionerCrosswalk}); address = SHA-256 of the normalised address legs ({@code
 * LocationHashService.createAddressHash}); location = the URL-encoded normalised name, {@code _}, the
 * address hash ({@code EntityCrosswalkGenerator.formatLocationCrosswalkId}). {@code CrosswalksTest}
 * pins the output against values computed from the api-layer's code.
 */
public final class Crosswalks {

    /** The prefix of every slice this service writes: one source name per vendor. */
    public static final String SOURCE_PREFIX = "candor:";

    /** The address legs the hash reads, in the api-layer's order. */
    static final List<String> ADDRESS_LEGS =
            List.of("addressLine1", "addressLine2", "city", "state", "zip", "addressType");

    static final String UNKNOWN_LOCATION = "UnknownLocation";
    static final String NO_ADDRESS = "no_address";

    private Crosswalks() {}

    /**
     * The source type of this service's slices for a tenant.
     *
     * @param tenantId the tenant
     * @return {@code candor:<tenantId>}
     */
    public static String sourceType(String tenantId) {
        return SOURCE_PREFIX + tenantId;
    }

    /**
     * The practitioner slice's crosswalk id.
     *
     * @param npi the practitioner's NPI
     * @return the NPI
     * @throws IllegalArgumentException when the NPI is null or blank
     */
    public static String practitioner(String npi) {
        if (npi == null || npi.isBlank()) {
            throw new IllegalArgumentException("a practitioner crosswalk needs an NPI");
        }
        return npi;
    }

    /**
     * The api-layer's {@code normalizeString}: every character outside {@code [a-zA-Z0-9]} becomes
     * {@code _}, runs of {@code _} collapse, leading and trailing {@code _} go, upper case.
     *
     * @param value the text
     * @return the normalised text, or null for null
     */
    public static String normalize(String value) {
        if (value == null) {
            return null;
        }
        return value.replaceAll("[^a-zA-Z0-9]", "_")
                .replaceAll("_+", "_")
                .replaceAll("^_|_$", "")
                .toUpperCase(Locale.ROOT);
    }

    /**
     * The address slice's crosswalk id: SHA-256 hex of {@code normalize(value) + "|"} for each
     * non-blank leg of {@link #ADDRESS_LEGS}, in that order.
     *
     * @param address the address legs
     * @return the hash, or null when the address is null or has no non-blank leg
     */
    public static String addressHash(Map<String, Object> address) {
        if (address == null) {
            return null;
        }
        StringBuilder legs = new StringBuilder();
        for (String leg : ADDRESS_LEGS) {
            Object value = address.get(leg);
            if (value != null && !value.toString().trim().isEmpty()) {
                legs.append(normalize(value.toString())).append('|');
            }
        }
        if (legs.isEmpty()) {
            return null;
        }
        return sha256Hex(legs.toString());
    }

    /**
     * The location slice's crosswalk id.
     *
     * @param name the location name; blank reads as {@code UnknownLocation}
     * @param address the service address, or null
     * @return {@code URLEncode(normalize(name))} then {@code _} then the address hash, or {@code no_address}
     */
    public static String location(String name, Map<String, Object> address) {
        String locationName = name == null || name.trim().isEmpty() ? UNKNOWN_LOCATION : name;
        String hash = addressHash(address);
        return URLEncoder.encode(normalize(locationName), StandardCharsets.UTF_8) + "_"
                + (hash == null ? NO_ADDRESS : hash);
    }

    private static String sha256Hex(String value) {
        try {
            return HexFormat.of()
                    .formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is required of every Java platform", exception);
        }
    }
}
