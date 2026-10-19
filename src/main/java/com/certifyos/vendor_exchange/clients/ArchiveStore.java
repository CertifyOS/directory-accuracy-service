package com.certifyos.vendor_exchange.clients;

/**
 * The archive as this service writes it: a create-only server-side copy of one exact generation of
 * a received object. Nothing is overwritten and no bytes pass through this service.
 */
public interface ArchiveStore {

    /**
     * The archived object.
     *
     * @param path {@code gs://<targetBucket>/<targetObject>}
     * @param bytes the archived object's size
     * @param alreadyExisted true when the target was already there and the copy was refused
     */
    record Copied(String path, long bytes, boolean alreadyExisted) {}

    /**
     * Copies one generation of the source to a target that must not exist yet. A target that already
     * exists is success with {@code alreadyExisted}.
     *
     * @param sourceBucket the source bucket
     * @param sourceObject the source object name
     * @param sourceGeneration the source generation to copy
     * @param targetBucket the archive bucket
     * @param targetObject the archive object name
     * @return the archived object
     * @throws ArchiveSourceMissingException when the source generation does not exist
     */
    Copied copy(
            String sourceBucket, String sourceObject, long sourceGeneration, String targetBucket, String targetObject);
}
