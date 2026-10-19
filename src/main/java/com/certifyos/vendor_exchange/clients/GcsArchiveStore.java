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

    @Override
    public Copied copy(
            String sourceBucket, String sourceObject, long sourceGeneration, String targetBucket, String targetObject) {
        BlobId target = BlobId.of(targetBucket, targetObject);
        String path = "gs://" + targetBucket + "/" + targetObject;
        Storage.CopyRequest request = Storage.CopyRequest.newBuilder()
                .setSource(BlobId.of(sourceBucket, sourceObject, sourceGeneration))
                .setSourceOptions(Storage.BlobSourceOption.generationMatch())
                .setTarget(target, Storage.BlobTargetOption.doesNotExist())
                .build();
        try {
            return new Copied(path, size(storage().copy(request).getResult()), false);
        } catch (StorageException failure) {
            if (failure.getCode() == NOT_FOUND) {
                throw new ArchiveSourceMissingException(
                        "archive source gs://" + sourceBucket + "/" + sourceObject + "#" + sourceGeneration
                                + " is missing",
                        failure);
            }
            if (failure.getCode() != PRECONDITION_FAILED) {
                throw failure;
            }
            Blob existing = storage().get(target);
            if (existing == null) {
                // The refused precondition was the source's, not the target's: nothing is archived.
                throw failure;
            }
            return new Copied(path, size(existing), true);
        }
    }

    private static long size(Blob blob) {
        return blob.getSize() == null ? 0 : blob.getSize();
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
