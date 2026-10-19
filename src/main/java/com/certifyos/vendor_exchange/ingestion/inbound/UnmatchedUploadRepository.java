package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.persistence.Collections;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * Uploads the service could not attach to a batch, in {@code vendor_unmatched_uploads}. The id is
 * the upload id, so a redelivered storage event inserts nothing the second time. A row is open until
 * an operator resolves it, once.
 */
@ApplicationScoped
public class UnmatchedUploadRepository {

    private static final String RESOLUTION = "resolution";
    private static final String RESOLUTION_KIND = RESOLUTION + ".kind";

    private final Collections collections;

    public UnmatchedUploadRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * Records an unmatched upload.
     *
     * @param session the transaction the insert joins
     * @param upload the upload, open
     * @return true when this call inserted it, false when the upload id was already recorded
     */
    public boolean insertIfAbsent(ClientSession session, UnmatchedUpload upload) {
        // An upsert, not insertOne: a duplicate key error aborts the surrounding transaction, so it
        // could not be caught and answered with false.
        Document fields = upload.toDocument();
        fields.remove("_id");
        UpdateResult result = collections
                .unmatchedUploads()
                .updateOne(
                        session,
                        Filters.eq("_id", upload.id()),
                        Updates.setOnInsert(fields),
                        new UpdateOptions().upsert(true));
        return result.getUpsertedId() != null;
    }

    /**
     * Reads one upload by its id.
     *
     * @param uploadId {@code <bucket>|<objectName>|<generation>}
     * @return the upload, or empty
     */
    public Optional<UnmatchedUpload> find(String uploadId) {
        Document doc =
                collections.unmatchedUploads().find(Filters.eq("_id", uploadId)).first();
        return Optional.ofNullable(doc).map(UnmatchedUpload::fromDocument);
    }

    /**
     * Lists open uploads, oldest first.
     *
     * @param folderTenant the folder tenant to list, null for every folder
     * @param limit the most rows to return, at least 1
     * @return the open uploads, at most {@code limit}
     * @throws IllegalArgumentException when {@code limit} is below 1: the driver reads 0 as no limit
     */
    public List<UnmatchedUpload> listOpen(String folderTenant, int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1, got " + limit);
        }
        // resolution.kind null matches a row with no resolution: one equality, so the tenant list runs
        // on unmatched_open_by_tenant and the service-wide one on unmatched_resolution.
        Bson open = Filters.eq(RESOLUTION_KIND, null);
        Bson filter = folderTenant == null ? open : Filters.and(Filters.eq("folderTenant", folderTenant), open);
        List<UnmatchedUpload> found = new ArrayList<>();
        for (Document doc : collections
                .unmatchedUploads()
                .find(filter)
                .sort(Sorts.ascending("receivedAt", "_id"))
                .limit(limit)) {
            found.add(UnmatchedUpload.fromDocument(doc));
        }
        return List.copyOf(found);
    }

    /**
     * Closes an open upload.
     *
     * @param session the transaction the update joins
     * @param uploadId the upload
     * @param resolution how it was closed
     * @return true when this call closed it, false when it was already resolved or does not exist
     */
    public boolean resolve(ClientSession session, String uploadId, UnmatchedUpload.Resolution resolution) {
        return collections
                        .unmatchedUploads()
                        .updateOne(
                                session,
                                Filters.and(Filters.eq("_id", uploadId), Filters.exists(RESOLUTION, false)),
                                Updates.set(RESOLUTION, resolution.toDocument()))
                        .getModifiedCount()
                == 1;
    }

    /**
     * Counts open uploads across all folders, for a gauge.
     *
     * @return the number of unresolved rows
     */
    public long countOpen() {
        return collections.unmatchedUploads().countDocuments(Filters.eq(RESOLUTION_KIND, null));
    }
}
