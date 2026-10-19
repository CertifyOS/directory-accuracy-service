package com.certifyos.vendor_exchange.ingestion.inbound;

import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Documents;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The inbound registry: the {@code inboundFiles[]} array on {@code vendor_export_batches}, one element
 * per vendor upload. The batch id embeds the tenant, so writes by batch id are tenant-scoped by
 * construction; the upload id and external ref are ids the storage event and the ingestion event
 * carry. {@link #claim} is the only way an element is added: one conditional push on {@code uploadId}.
 * Every later change is an update on the element whose {@code uploadId} and {@code state} match
 * together ({@code $elemMatch}), written through the positional {@code $}, so a stale job or a
 * duplicate event is a no-op and the caller sees {@code false}.
 */
@ApplicationScoped
public class InboundFileRepository {

    static final String FIELD = "inboundFiles";

    private final Collections collections;

    public InboundFileRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * What a claim did.
     *
     * @param claimed true when this call added the entry
     * @param state the state the entry was added in, null when nothing was added
     * @param uploadCount entries on the batch after the call
     */
    public record Claim(boolean claimed, InboundFileState state, int uploadCount) {}

    /**
     * An entry and the batch that holds it.
     *
     * @param batch the batch
     * @param entry the one entry the read asked for
     */
    public record Located(ExportBatch batch, InboundFile entry) {}

    /**
     * The positional path of an entry field, for the updates a caller passes to {@link #transition}
     * and {@link #updateInState}.
     *
     * @param field a field of {@link InboundFile}, for example {@code externalRef}
     * @return {@code inboundFiles.$.<field>}
     */
    public static String entry(String field) {
        return FIELD + ".$." + field;
    }

    /**
     * Adds an upload to a batch. The batch is read in the session: the first upload of the batch is
     * {@code RECEIVED}, any later one {@code DUPLICATE_LOGGED}, whatever state {@code entry} carries.
     * The push matches only while no element has the same {@code uploadId}, so a redelivered event
     * adds nothing; two concurrent claims in transactions conflict on the batch document and the
     * loser's re-run reads the winner's entry.
     *
     * @param session the transaction
     * @param exportBatchId the batch the object name points at
     * @param entry the upload, as {@link InboundFile#received} builds it
     * @return claimed with the state and the new count; not claimed, with the current count, when the
     *     upload is already on the batch or the batch does not exist (count 0)
     */
    public Claim claim(ClientSession session, String exportBatchId, InboundFile entry) {
        Document batch = collections
                .batches()
                .find(session, Filters.eq("_id", exportBatchId))
                .projection(Projections.include(FIELD + ".uploadId"))
                .first();
        if (batch == null) {
            return new Claim(false, null, 0);
        }
        int count = entriesOf(batch).size();
        InboundFileState state = count == 0 ? InboundFileState.RECEIVED : InboundFileState.DUPLICATE_LOGGED;
        Document element = entry.toDocument();
        element.put("state", state.name());
        Bson filter = Filters.and(Filters.eq("_id", exportBatchId), Filters.ne(FIELD + ".uploadId", entry.uploadId()));
        UpdateResult result = collections.batches().updateOne(session, filter, Updates.push(FIELD, element));
        if (result.getMatchedCount() == 0) {
            return new Claim(false, null, count);
        }
        return new Claim(true, state, count + 1);
    }

    /**
     * The entry for an upload id, for the storage event handler and operator reads.
     *
     * @param uploadId {@code <bucket>|<objectName>|<generation>}
     * @return the entry and its batch, if any
     */
    public Optional<Located> findByUploadId(String uploadId) {
        return locate(
                Filters.eq(FIELD + ".uploadId", uploadId), element -> uploadId.equals(element.getString("uploadId")));
    }

    /**
     * The entry an ingestion event names, on the {@code batches_inbound_external_ref} index.
     *
     * @param externalRef {@code <exportBatchId>|<generation>}
     * @return the entry and its batch, if any
     */
    public Optional<Located> findByExternalRef(String externalRef) {
        return locate(byExternalRef(externalRef), element -> externalRef.equals(element.getString("externalRef")));
    }

    /** The filter {@link #findByExternalRef} runs, shared with the test that explains it. */
    static Bson byExternalRef(String externalRef) {
        return Filters.eq(FIELD + ".externalRef", externalRef);
    }

    /**
     * The reconciler's query: entries in the given states untouched since {@code olderThan}, one
     * {@link Located} per entry (a batch may hold several), oldest entry first. A cross-tenant,
     * system-scoped read. The first stage uses {@code batches_inbound_state_updated} with both
     * predicates bound to one element; the sort runs on the unwound entries, because a sort on the
     * array would key each batch by its oldest entry of any state.
     *
     * @param states the states to look at
     * @param olderThan the staleness threshold
     * @param limit the most entries to return
     * @return the stale entries, oldest first
     */
    public List<Located> findStale(Set<InboundFileState> states, Instant olderThan, int limit) {
        List<String> names = states.stream().map(Enum::name).toList();
        Bson stale = Filters.and(Filters.in("state", names), Filters.lt("updatedAt", olderThan));
        List<Bson> pipeline = List.of(
                Aggregates.match(Filters.elemMatch(FIELD, stale)),
                Aggregates.unwind("$" + FIELD),
                Aggregates.match(
                        Filters.and(Filters.in(FIELD + ".state", names), Filters.lt(FIELD + ".updatedAt", olderThan))),
                Aggregates.sort(Sorts.ascending(FIELD + ".updatedAt", "_id", FIELD + ".uploadId")),
                Aggregates.limit(limit));
        List<Located> located = new ArrayList<>();
        for (Document doc : collections.batches().aggregate(pipeline)) {
            located.add(new Located(
                    ExportBatch.fromDocument(doc), InboundFile.fromDocument(doc.get(FIELD, Document.class))));
        }
        return located;
    }

    /**
     * Moves an entry from one state to another and applies the given entry updates in the same write.
     * Refuses an edge {@link InboundFileState#canTransitionTo} does not allow, before any write.
     *
     * @param session the transaction
     * @param exportBatchId the batch
     * @param uploadId the entry
     * @param from the state the caller read
     * @param to the state to move to
     * @param now the write time, set as the entry's {@code updatedAt}
     * @param entryUpdates further entry fields to set, on {@link #entry} paths, may be null
     * @return true when the entry was in {@code from} and is now in {@code to}; false when another
     *     writer moved it first (the caller treats this as a no-op). No edge is a self loop, so the
     *     write always modifies the entry it matches.
     */
    public boolean transition(
            ClientSession session,
            String exportBatchId,
            String uploadId,
            InboundFileState from,
            InboundFileState to,
            Instant now,
            Bson entryUpdates) {
        if (!from.canTransitionTo(to)) {
            throw new IllegalArgumentException("no transition " + from + " -> " + to + " for " + uploadId);
        }
        Bson base = Updates.combine(Updates.set(entry("state"), to.name()), Updates.set(entry("updatedAt"), now));
        Bson update = entryUpdates == null ? base : Updates.combine(base, entryUpdates);
        UpdateResult result = collections.batches().updateOne(session, entryIn(exportBatchId, uploadId, from), update);
        return result.getModifiedCount() == 1;
    }

    /**
     * Updates entry fields without changing the state, guarded on the state the caller read (for
     * progress such as the job id or the row checkpoint).
     *
     * @param session the transaction
     * @param exportBatchId the batch
     * @param uploadId the entry
     * @param inState the state the caller read
     * @param now the write time, set as the entry's {@code updatedAt}
     * @param entryUpdates the entry fields to set, on {@link #entry} paths, may be null
     * @return true when the entry was still in {@code inState}, even if the values were already there
     */
    public boolean updateInState(
            ClientSession session,
            String exportBatchId,
            String uploadId,
            InboundFileState inState,
            Instant now,
            Bson entryUpdates) {
        Bson base = Updates.set(entry("updatedAt"), now);
        Bson update = entryUpdates == null ? base : Updates.combine(entryUpdates, base);
        UpdateResult result =
                collections.batches().updateOne(session, entryIn(exportBatchId, uploadId, inState), update);
        return result.getMatchedCount() == 1;
    }

    /**
     * Entries per state, for the by-state gauges.
     *
     * @return state name to count; states with no entries are absent
     */
    public Map<String, Long> countByState() {
        Map<String, Long> counts = new HashMap<>();
        collections
                .batches()
                .aggregate(List.of(
                        Aggregates.unwind("$" + FIELD),
                        Aggregates.group("$" + FIELD + ".state", Accumulators.sum("n", 1))))
                .forEach(doc -> counts.put(doc.getString("_id"), Documents.longValue(doc, "n")));
        return Map.copyOf(counts);
    }

    /**
     * The last write of the longest-untouched entry in a state, for the age gauges.
     *
     * @param state the state
     * @return the oldest {@code updatedAt}, or empty when no entry is in {@code state}
     */
    public Optional<Instant> oldestUpdatedAtIn(InboundFileState state) {
        Bson inState = Filters.eq(FIELD + ".state", state.name());
        Document oldest = collections
                .batches()
                .aggregate(List.of(
                        Aggregates.match(inState),
                        Aggregates.unwind("$" + FIELD),
                        Aggregates.match(inState),
                        Aggregates.group(null, Accumulators.min("oldest", "$" + FIELD + ".updatedAt"))))
                .first();
        return Optional.ofNullable(oldest).map(doc -> Documents.instant(doc, "oldest"));
    }

    /** The batch with the entry whose upload id and state match together; the positional {@code $} binds to it. */
    private static Bson entryIn(String exportBatchId, String uploadId, InboundFileState state) {
        return Filters.and(
                Filters.eq("_id", exportBatchId),
                Filters.elemMatch(
                        FIELD, Filters.and(Filters.eq("uploadId", uploadId), Filters.eq("state", state.name()))));
    }

    private Optional<Located> locate(Bson filter, Predicate<Document> wanted) {
        Document doc = collections.batches().find(filter).first();
        if (doc == null) {
            return Optional.empty();
        }
        return entriesOf(doc).stream()
                .filter(wanted)
                .findFirst()
                .map(element -> new Located(ExportBatch.fromDocument(doc), InboundFile.fromDocument(element)));
    }

    private static List<Document> entriesOf(Document batch) {
        List<Document> entries = batch.getList(FIELD, Document.class);
        return entries == null ? List.of() : entries;
    }
}
