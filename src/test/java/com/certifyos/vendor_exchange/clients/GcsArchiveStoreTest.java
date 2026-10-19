package com.certifyos.vendor_exchange.clients;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

/**
 * The archive copy's branching over a mocked Cloud Storage client: which failure is a missing
 * source (terminal), which is a retryable error, and which is an archive already in place.
 */
class GcsArchiveStoreTest {

    static final String SOURCE_BUCKET = "vendor-sftp";
    static final String SOURCE_OBJECT = "to/org-a/org-a_org-a-candor-2026-10-001_CB-1_20261015.csv";
    static final long GENERATION = 1760520000123456L;
    static final String ARCHIVE_BUCKET = "vendor-ingestion-archive";
    static final String ARCHIVE_OBJECT = "org-a/org-a-candor-2026-10-001/1760520000123456/file.csv";
    static final BlobId SOURCE = BlobId.of(SOURCE_BUCKET, SOURCE_OBJECT, GENERATION);
    static final BlobId TARGET = BlobId.of(ARCHIVE_BUCKET, ARCHIVE_OBJECT);

    private final Storage storage = Mockito.mock(Storage.class);
    private final GcsArchiveStore store = new GcsArchiveStore(storage);

    private ArchiveStore.Copied copy() {
        return store.copy(SOURCE_BUCKET, SOURCE_OBJECT, GENERATION, ARCHIVE_BUCKET, ARCHIVE_OBJECT);
    }

    private static Blob blobOfSize(Long size) {
        Blob blob = Mockito.mock(Blob.class);
        Mockito.when(blob.getSize()).thenReturn(size);
        return blob;
    }

    private void copyFailsWith(int code) {
        Mockito.when(storage.copy(ArgumentMatchers.any(Storage.CopyRequest.class)))
                .thenThrow(new StorageException(code, "refused"));
    }

    @Test
    void successReturnsTheArchivePathAndSendsOnlyTheTargetPrecondition() {
        CopyWriter writer = Mockito.mock(CopyWriter.class);
        Blob copied = blobOfSize(4096L);
        Mockito.when(writer.getResult()).thenReturn(copied);
        Mockito.when(storage.copy(ArgumentMatchers.any(Storage.CopyRequest.class)))
                .thenReturn(writer);

        ArchiveStore.Copied result = copy();

        Assertions.assertEquals(
                new ArchiveStore.Copied("gs://" + ARCHIVE_BUCKET + "/" + ARCHIVE_OBJECT, 4096L, false), result);
        ArgumentCaptor<Storage.CopyRequest> sent = ArgumentCaptor.forClass(Storage.CopyRequest.class);
        Mockito.verify(storage).copy(sent.capture());
        Assertions.assertEquals(SOURCE, sent.getValue().getSource());
        Assertions.assertEquals(
                List.of(),
                sent.getValue().getSourceOptions(),
                "the pinned generation is the guard; a live-generation check would refuse an overwritten source");
        Assertions.assertEquals(TARGET, sent.getValue().getTarget().getBlobId());
        Assertions.assertEquals(
                List.of(Storage.BlobTargetOption.doesNotExist()),
                sent.getValue().getTargetOptions());
    }

    @Test
    void preconditionFailedWithTargetPresentIsAlreadyExisted() {
        copyFailsWith(412);
        Blob existing = blobOfSize(2048L);
        Mockito.when(storage.get(TARGET)).thenReturn(existing);

        ArchiveStore.Copied result = copy();

        Assertions.assertEquals(
                new ArchiveStore.Copied("gs://" + ARCHIVE_BUCKET + "/" + ARCHIVE_OBJECT, 2048L, true), result);
    }

    @Test
    void preconditionFailedWithTargetAbsentRethrows() {
        copyFailsWith(412);
        Mockito.when(storage.get(TARGET)).thenReturn(null);

        StorageException refused = Assertions.assertThrows(StorageException.class, this::copy);

        Assertions.assertEquals(412, refused.getCode());
    }

    @Test
    void notFoundWithSourceAbsentIsSourceMissing() {
        copyFailsWith(404);
        Mockito.when(storage.get(SOURCE)).thenReturn(null);

        Assertions.assertThrows(ArchiveSourceMissingException.class, this::copy);
    }

    @Test
    void notFoundWithSourcePresentRethrowsForARetry() {
        copyFailsWith(404);
        Blob source = blobOfSize(4096L);
        Mockito.when(storage.get(SOURCE)).thenReturn(source);

        StorageException refused = Assertions.assertThrows(StorageException.class, this::copy);

        Assertions.assertEquals(404, refused.getCode(), "a missing archive bucket is not a missing source");
    }

    @Test
    void missingSizeIsRefused() {
        copyFailsWith(412);
        Blob existing = blobOfSize(null);
        Mockito.when(storage.get(TARGET)).thenReturn(existing);

        IllegalStateException refused = Assertions.assertThrows(IllegalStateException.class, this::copy);

        Assertions.assertTrue(refused.getMessage().contains(ARCHIVE_OBJECT), refused.getMessage());
    }
}
