package com.certifyos.vendor_exchange.ingestion.events;

import com.certifyos.vendor_exchange.persistence.Ids;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

/**
 * A storage notification for an object in the vendor bucket, read from the Pub/Sub message
 * attributes (the object body is never needed). The bucket, object and generation identify one
 * upload; the event type says whether the object arrived or went away.
 *
 * @param bucketId the bucket
 * @param objectId the object name
 * @param objectGeneration the object generation
 * @param eventType for example {@code OBJECT_FINALIZE}
 * @param eventTime when storage finalized the object, null when absent or unparseable
 * @param overwroteGeneration the generation this upload replaced, null for a first upload
 */
public record ArrivalEvent(
        String bucketId,
        String objectId,
        String objectGeneration,
        String eventType,
        Instant eventTime,
        String overwroteGeneration) {

    private static final String FINALIZE = "OBJECT_FINALIZE";

    /**
     * Reads the attributes.
     *
     * @param attributes the push message attributes, may be null
     * @return the event, or empty when the bucket, object or generation is missing or blank
     */
    public static Optional<ArrivalEvent> fromAttributes(Map<String, String> attributes) {
        if (attributes == null) {
            return Optional.empty();
        }
        String bucket = attributes.get("bucketId");
        String object = attributes.get("objectId");
        String generation = attributes.get("objectGeneration");
        if (isBlank(bucket) || isBlank(object) || isBlank(generation)) {
            return Optional.empty();
        }
        return Optional.of(new ArrivalEvent(
                bucket,
                object,
                generation,
                attributes.get("eventType"),
                instant(attributes.get("eventTime")),
                attributes.get("overwroteGeneration")));
    }

    /** Whether the object arrived, as opposed to being deleted, archived or having its metadata updated. */
    public boolean isFinalize() {
        return FINALIZE.equals(eventType);
    }

    /** The upload id: bucket, object and generation. */
    public String uploadId() {
        return Ids.uploadId(bucketId, objectId, objectGeneration);
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static Instant instant(String value) {
        if (value == null) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException exception) {
            return null;
        }
    }
}
