package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.Collections;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The NPI registry. Rows are upserted by id with {@code $setOnInsert}, so a retried page is a no-op
 * and a row is never rewritten. Reads are by batch, with a limit and a keyset cursor on the NPI.
 */
@ApplicationScoped
public class ExportNpiRepository {

    /** Rows per bulk write; the design's figure for a 1,000,000-row scenario. */
    public static final int BULK_SIZE = 500;

    private final Collections collections;

    public ExportNpiRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * Registers a page of practitioners. Existing rows are left untouched.
     *
     * @param session the transaction
     * @param rows the rows to register
     * @return how many rows were new
     */
    public long upsertAll(ClientSession session, List<ExportNpi> rows) {
        long inserted = 0;
        for (int start = 0; start < rows.size(); start += BULK_SIZE) {
            List<ExportNpi> chunk = rows.subList(start, Math.min(start + BULK_SIZE, rows.size()));
            List<WriteModel<Document>> writes = new ArrayList<>(chunk.size());
            for (ExportNpi row : chunk) {
                Document doc = row.toDocument();
                doc.remove("_id");
                Bson setOnInsert = Updates.setOnInsert(doc);
                writes.add(new UpdateOneModel<>(
                        Filters.eq("_id", row.id()), setOnInsert, new UpdateOptions().upsert(true)));
            }
            inserted += collections
                    .npis()
                    .bulkWrite(session, writes, new BulkWriteOptions().ordered(false))
                    .getUpserts()
                    .size();
        }
        return inserted;
    }

    /**
     * How many practitioners a batch registered; the number reconciliation compares with the file.
     *
     * @param exportBatchId the batch
     * @return the count
     */
    public long countForBatch(String exportBatchId) {
        return collections.npis().countDocuments(Filters.eq("exportBatchId", exportBatchId));
    }

    /**
     * A page of a batch's rows, by NPI, after the given NPI (exclusive).
     *
     * @param exportBatchId the batch
     * @param afterNpi the last NPI of the previous page, or null for the first page
     * @param limit the most rows to return
     * @return the rows, NPI ascending
     */
    public List<ExportNpi> listForBatch(String exportBatchId, String afterNpi, int limit) {
        Bson filter = afterNpi == null
                ? Filters.eq("exportBatchId", exportBatchId)
                : Filters.and(Filters.eq("exportBatchId", exportBatchId), Filters.gt("npi", afterNpi));
        List<ExportNpi> rows = new ArrayList<>();
        for (Document doc :
                collections.npis().find(filter).sort(Sorts.ascending("npi")).limit(limit)) {
            rows.add(ExportNpi.fromDocument(doc));
        }
        return rows;
    }

    /**
     * The distinct NPIs of a batch, for the egress request.
     *
     * @param exportBatchId the batch
     * @return every NPI, ascending
     */
    public List<String> npisForBatch(String exportBatchId) {
        List<String> npis = new ArrayList<>();
        collections
                .npis()
                .distinct("npi", Filters.eq("exportBatchId", exportBatchId), String.class)
                .into(npis);
        npis.sort(null);
        return npis;
    }
}
