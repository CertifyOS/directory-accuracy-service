package com.certifyos.directory_accuracy.clients;

import java.util.Map;
import java.util.Optional;

/**
 * The vendor's bucket as this service reads it: object metadata only, never content. Egress writes
 * the file; this service checks that it is there and what egress stamped on it.
 */
public interface VendorBucket {

    /** Metadata key egress sets to {@code true} once the file is whole. */
    String COMPLETE = "complete";

    /** Metadata key carrying the egress correlation id of the attempt that produced the file. */
    String CORRELATION_ID = "correlationId";

    /**
     * One object's facts.
     *
     * @param bucket the bucket
     * @param objectName the object name
     * @param size bytes
     * @param metadata user metadata, never null
     * @param md5 the object's MD5 as the bucket reports it (base64), or null
     */
    record ObjectInfo(String bucket, String objectName, long size, Map<String, String> metadata, String md5) {
        public ObjectInfo {
            metadata = metadata == null ? Map.of() : Map.copyOf(metadata);
        }

        /**
         * A numeric metadata value.
         *
         * @param key the metadata key
         * @return the number, or null when absent or not a number
         */
        public Long metadataNumber(String key) {
            String value = metadata.get(key);
            if (value == null) {
                return null;
            }
            try {
                return Long.parseLong(value.trim());
            } catch (NumberFormatException notANumber) {
                return null;
            }
        }

        /** Whether egress marked the object complete. */
        public boolean isComplete() {
            return "true".equalsIgnoreCase(metadata.get(COMPLETE));
        }

        /** The producing attempt's correlation id, or null. */
        public String producedBy() {
            return metadata.get(CORRELATION_ID);
        }
    }

    /**
     * Reads an object's metadata.
     *
     * @param bucket the bucket
     * @param objectName the object name
     * @return the object, or empty when it does not exist
     */
    Optional<ObjectInfo> head(String bucket, String objectName);
}
