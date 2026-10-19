package com.certifyos.vendor_exchange.clients;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import jakarta.enterprise.context.ApplicationScoped;

/**
 * {@link ArchiveStore} over Cloud Storage with Application Default Credentials: a server-side copy
 * of the source read by generation, with {@code ifSourceGenerationMatch} on the source and {@code
 * ifGenerationMatch=0} on the target. The client is built on first use so a deployment without
 * credentials (or a test) boots; the first copy is where a missing grant shows.
 */
@ApplicationScoped
public class GcsArchiveStore implements ArchiveStore {

    private static final int NOT_FOUND = 404;
    private static final int PRECONDITION_FAILED = 412;

    private volatile Storage storage;

    /** The bean: the client is built on first use. */
    public GcsArchiveStore() {}

    /** A store over a given client, for tests. */
    GcsArchiveStore(Storage storage) {
        this.storage = storage;
    }

    @Override
    public Copied copy(
            String sourceBucket, String sourceObject, long sourceGeneration, String targetBucket, String targetObject) {
        BlobId source = BlobId.of(sourceBucket, sourceObject, sourceGeneration);
        BlobId target = BlobId.of(targetBucket, targetObject);
        String path = "gs://" + targetBucket + "/" + targetObject;
        Storage.CopyRequest request = Storage.CopyRequest.newBuilder()
                .setSource(source)
                .setSourceOptions(Storage.BlobSourceOption.generationMatch())
                .setTarget(target, Storage.BlobTargetOption.doesNotExist())
                .build();
        try {
            return new Copied(path, size(storage().copy(request).getResult(), path), false);
        } catch (StorageException failure) {
            if (failure.getCode() == NOT_FOUND) {
                // A 404 also answers a missing or misnamed archive bucket, which is a configuration
                // error to retry, not a terminal missing source; only an absent source is the latter.
                if (storage().get(source) == null) {
                    throw new ArchiveSourceMissingException(
                            "archive source gs://" + sourceBucket + "/" + sourceObject + "#" + sourceGeneration
                                    + " is missing",
                            failure);
                }
                throw failure;
            }
            if (failure.getCode() != PRECONDITION_FAILED) {
                throw failure;
            }
            Blob existing = storage().get(target);
            if (existing == null) {
                // The refused precondition was the source's, not the target's: nothing is archived.
                throw failure;
            }
            return new Copied(path, size(existing, path), true);
        }
    }

    private static long size(Blob blob, String path) {
        Long size = blob.getSize();
        if (size == null) {
            throw new IllegalStateException("Cloud Storage reported no size for " + path);
        }
        return size;
    }

    private Storage storage() {
        Storage current = storage;
        if (current == null) {
            synchronized (this) {
                if (storage == null) {
                    storage = StorageOptions.getDefaultInstance().getService();
                }
                current = storage;
            }
        }
        return current;
    }
}
