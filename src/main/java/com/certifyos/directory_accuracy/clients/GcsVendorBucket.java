package com.certifyos.directory_accuracy.clients;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Optional;

/**
 * {@link VendorBucket} over Cloud Storage with Application Default Credentials. The client is built
 * on first use so a deployment without credentials (or a test) boots; the first metadata read is
 * where a missing grant shows.
 */
@ApplicationScoped
public class GcsVendorBucket implements VendorBucket {

    private volatile Storage storage;

    @Override
    public Optional<ObjectInfo> head(String bucket, String objectName) {
        Blob blob = storage()
                .get(
                        BlobId.of(bucket, objectName),
                        Storage.BlobGetOption.fields(
                                Storage.BlobField.METADATA, Storage.BlobField.SIZE, Storage.BlobField.MD5HASH));
        if (blob == null) {
            return Optional.empty();
        }
        return Optional.of(new ObjectInfo(
                bucket, objectName, blob.getSize() == null ? 0 : blob.getSize(), blob.getMetadata(), blob.getMd5()));
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
