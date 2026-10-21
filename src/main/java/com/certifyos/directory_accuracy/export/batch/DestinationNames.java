package com.certifyos.directory_accuracy.export.batch;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * The vendor-facing names from the outbound file contract: {@code <tenantId>_<exportBatchId>_<yyyyMMdd>.csv}
 * under {@code from/<tenantId>/}. Underscores separate the legs, which is why tenant ids with
 * underscores are refused at schedule creation and batch ids use hyphens.
 */
public final class DestinationNames {

    static final String INBOUND_PREFIX = "from/";
    private static final DateTimeFormatter DAY = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.ROOT);

    private DestinationNames() {}

    /**
     * The file name.
     *
     * @param tenantId the tenant
     * @param exportBatchId the batch
     * @param date the day the egress request was made, in the schedule's timezone
     * @return {@code <tenantId>_<exportBatchId>_<yyyyMMdd>.csv}
     */
    public static String fileName(String tenantId, String exportBatchId, LocalDate date) {
        return tenantId + "_" + exportBatchId + "_" + DAY.format(date) + ".csv";
    }

    /**
     * The object name in the vendor bucket.
     *
     * @param tenantId the tenant
     * @param fileName from {@link #fileName}
     * @return {@code from/<tenantId>/<fileName>}
     */
    public static String objectName(String tenantId, String fileName) {
        return INBOUND_PREFIX + tenantId + "/" + fileName;
    }

    /**
     * The full path as the batch records it.
     *
     * @param bucket the vendor bucket
     * @param objectName from {@link #objectName}
     * @return {@code gs://<bucket>/<objectName>}
     */
    public static String gsPath(String bucket, String objectName) {
        return "gs://" + bucket + "/" + objectName;
    }
}
