package com.certifyos.directory_accuracy.export.batch;

import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.mongodb.MongoWriteException;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The batches collection. The batch id embeds the tenant, so reads by id are tenant-scoped by
 * construction. {@link #transition} is the only way a state changes: compare-and-set on the current
 * state, with {@code version} incremented and {@code updatedAt} set in the same write, so a stale job
 * or a duplicate event is a no-op and the caller sees {@code false}.
 */
@ApplicationScoped
public class ExportBatchRepository {

    private final Collections collections;

    public ExportBatchRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * Inserts a new batch.
     *
     * @param session the transaction
     * @param batch the batch in {@code SCHEDULED}
     * @throws AlreadyExistsException when the tenant, vendor, period and sequence already exist
     */
    public void insert(ClientSession session, ExportBatch batch) {
        try {
            collections.batches().insertOne(session, batch.toDocument());
        } catch (MongoWriteException failure) {
            if (Documents.isDuplicateKey(failure)) {
                throw new AlreadyExistsException("vendor_export_batches", batch.id());
            }
            throw failure;
        }
    }

    /**
     * One batch by id.
     *
     * @param exportBatchId the batch
     * @return the batch, if any
     */
    public Optional<ExportBatch> find(String exportBatchId) {
        Document doc =
                collections.batches().find(Filters.eq("_id", exportBatchId)).first();
        return Optional.ofNullable(doc).map(ExportBatch::fromDocument);
    }

    /**
     * One batch by id, inside a transaction (reads the row the transaction will write).
     *
     * @param session the transaction
     * @param exportBatchId the batch
     * @return the batch, if any
     */
    public Optional<ExportBatch> find(ClientSession session, String exportBatchId) {
        Document doc = collections
                .batches()
                .find(session, Filters.eq("_id", exportBatchId))
                .first();
        return Optional.ofNullable(doc).map(ExportBatch::fromDocument);
    }

    /**
     * The batch an egress correlation id belongs to, for the completion event handler.
     *
     * @param correlationId the egress correlation id
     * @return the batch, if any
     */
    public Optional<ExportBatch> findByCorrelationId(String correlationId) {
        Document doc = collections
                .batches()
                .find(Filters.eq("egress.correlationId", correlationId))
                .first();
        return Optional.ofNullable(doc).map(ExportBatch::fromDocument);
    }

    /**
     * A tenant's batches, newest first, optionally for one period.
     *
     * @param tenantId the tenant
     * @param period the month, or null for all
     * @param limit the most rows to return
     * @return the batches
     */
    public List<ExportBatch> findForTenant(String tenantId, String period, int limit) {
        Bson filter = period == null
                ? Filters.eq("tenantId", tenantId)
                : Filters.and(Filters.eq("tenantId", tenantId), Filters.eq("period", period));
        return toList(collections
                .batches()
                .find(filter)
                .sort(Sorts.descending("createdAt"))
                .limit(limit));
    }

    /**
     * The reconciler's query: batches in the given states untouched since {@code olderThan}, on the
     * {@code (state, updatedAt)} index. A cross-tenant, system-scoped read.
     *
     * @param states the states to look at
     * @param olderThan the staleness threshold
     * @param limit the most rows to return
     * @return the stale batches, oldest first
     */
    public List<ExportBatch> findStale(Set<BatchState> states, Instant olderThan, int limit) {
        List<String> names = states.stream().map(Enum::name).toList();
        Bson filter = Filters.and(Filters.in("state", names), Filters.lt("updatedAt", olderThan));
        return toList(collections
                .batches()
                .find(filter)
                .sort(Sorts.ascending("updatedAt"))
                .limit(limit));
    }

    /**
     * Moves a batch from one state to another and applies the given field updates in the same write.
     * Refuses an edge {@link BatchState#canTransitionTo} does not allow.
     *
     * @param session the transaction
     * @param exportBatchId the batch
     * @param from the state the caller read
     * @param to the state to move to
     * @param now the write time
     * @param updates further fields to set, may be null
     * @return true when the row was in {@code from} and is now in {@code to}; false when another
     *     writer moved it first (the caller treats this as a no-op)
     */
    public boolean transition(
            ClientSession session, String exportBatchId, BatchState from, BatchState to, Instant now, Bson updates) {
        if (!from.canTransitionTo(to)) {
            throw new IllegalArgumentException("no transition " + from + " -> " + to + " for " + exportBatchId);
        }
        Bson base = Updates.combine(
                Updates.set("state", to.name()), Updates.set("updatedAt", now), Updates.inc("version", 1L));
        Bson update = updates == null ? base : Updates.combine(base, updates);
        Bson filter = Filters.and(Filters.eq("_id", exportBatchId), Filters.eq("state", from.name()));
        UpdateResult result = collections.batches().updateOne(session, filter, update);
        return result.getMatchedCount() == 1;
    }

    /**
     * Updates fields without changing the state, compare-and-set on the state the caller read (for
     * progress such as the selection page or the pinned template).
     *
     * @param session the transaction
     * @param exportBatchId the batch
     * @param inState the state the caller read
     * @param now the write time
     * @param updates the fields to set
     * @return true when the row was still in {@code inState}
     */
    public boolean updateInState(
            ClientSession session, String exportBatchId, BatchState inState, Instant now, Bson updates) {
        Bson update = Updates.combine(updates, Updates.set("updatedAt", now), Updates.inc("version", 1L));
        Bson filter = Filters.and(Filters.eq("_id", exportBatchId), Filters.eq("state", inState.name()));
        UpdateResult result = collections.batches().updateOne(session, filter, update);
        return result.getMatchedCount() == 1;
    }

    private static List<ExportBatch> toList(Iterable<Document> docs) {
        List<ExportBatch> batches = new ArrayList<>();
        for (Document doc : docs) {
            batches.add(ExportBatch.fromDocument(doc));
        }
        return batches;
    }

    /**
     * Batches per state, for the by-state gauges.
     *
     * @return state name to count; states with no rows are absent
     */
    public Map<String, Long> countByState() {
        Map<String, Long> counts = new java.util.HashMap<>();
        collections
                .batches()
                .aggregate(List.of(Aggregates.group("$state", Accumulators.sum("n", 1))))
                .forEach(doc -> counts.put(doc.getString("_id"), Documents.longValue(doc, "n")));
        return Map.copyOf(counts);
    }
}
