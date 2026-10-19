package com.certifyos.vendor_exchange.ingestion.inbound;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The vendor-facing names of the inbound contract: the vendor drops
 * {@code to/<tenant>/<tenant>_<exportBatchRef>_<vendorBatchId>_<yyyyMMdd>.csv}, and the service
 * archives it as {@code <tenantId>/<exportBatchId>/<generation>/<fileName>}. Tenant ids and export
 * batch ids never contain an underscore, so those legs split exactly; the vendor batch id is
 * whatever lies between the batch ref and the date. The ref ends at the first underscore after the
 * tenant (export batch ids carry hyphens only), and every leg is limited to letters, digits, {@code .} and {@code -} (the vendor id also allows
 * {@code _}), with {@code .} and {@code ..} refused.
 */
public final class InboundNames {

    public static final String TO_PREFIX = "to/";
    public static final String UNPARSEABLE_NAME = "UNPARSEABLE_NAME";
    public static final String UNKNOWN_BATCH = "UNKNOWN_BATCH";
    public static final String TENANT_MISMATCH = "TENANT_MISMATCH";
    public static final String BATCH_NOT_DELIVERED = "BATCH_NOT_DELIVERED";

    private static final Pattern NAME = Pattern.compile(
            "^to/([A-Za-z0-9.-]+)/([A-Za-z0-9.-]+)_([A-Za-z0-9.-]+)_([A-Za-z0-9._-]+)_(\\d{8})\\.(?i:csv)$");
    private static final DateTimeFormatter DAY =
            DateTimeFormatter.ofPattern("uuuuMMdd", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);

    private InboundNames() {}

    /**
     * What the object name says.
     *
     * @param folderTenant the tenant leg of the folder
     * @param fileTenant the tenant leg of the file name
     * @param exportBatchRef the export batch id the vendor echoes
     * @param vendorBatchId the vendor's own batch id
     * @param fileDate the {@code yyyyMMdd} leg
     * @param fileName the last path segment
     */
    public record Parsed(
            String folderTenant,
            String fileTenant,
            String exportBatchRef,
            String vendorBatchId,
            String fileDate,
            String fileName) {}

    /**
     * Parses an object name.
     *
     * @param objectName the object name in the vendor bucket
     * @return the legs, or empty on any mismatch (wrong prefix, shape, date or extension)
     */
    public static Optional<Parsed> parse(String objectName) {
        if (objectName == null) {
            return Optional.empty();
        }
        Matcher matcher = NAME.matcher(objectName);
        if (!matcher.matches() || !validDate(matcher.group(5))) {
            return Optional.empty();
        }
        for (int group = 1; group <= 4; group++) {
            if (isDots(matcher.group(group))) {
                return Optional.empty();
            }
        }
        return Optional.of(new Parsed(
                matcher.group(1),
                matcher.group(2),
                matcher.group(3),
                matcher.group(4),
                matcher.group(5),
                objectName.substring(objectName.lastIndexOf('/') + 1)));
    }

    /**
     * The archive object name.
     *
     * @param tenantId the tenant
     * @param exportBatchId the batch
     * @param generation the object generation
     * @param fileName the original file name
     * @return {@code <tenantId>/<exportBatchId>/<generation>/<fileName>}
     */
    public static String archiveObject(String tenantId, String exportBatchId, String generation, String fileName) {
        requireSegment("tenantId", tenantId);
        requireSegment("exportBatchId", exportBatchId);
        requireSegment("generation", generation);
        requireSegment("fileName", fileName);
        return tenantId + "/" + exportBatchId + "/" + generation + "/" + fileName;
    }

    private static void requireSegment(String name, String value) {
        if (value == null || value.isBlank() || value.indexOf('/') >= 0 || isDots(value)) {
            throw new IllegalArgumentException(name + " is not a safe path segment: " + value);
        }
    }

    private static boolean isDots(String value) {
        return ".".equals(value) || "..".equals(value);
    }

    private static boolean validDate(String digits) {
        try {
            LocalDate.parse(digits, DAY);
            return true;
        } catch (DateTimeParseException exception) {
            return false;
        }
    }
}
