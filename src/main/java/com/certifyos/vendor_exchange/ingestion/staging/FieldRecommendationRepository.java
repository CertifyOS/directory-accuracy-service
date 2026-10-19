package com.certifyos.vendor_exchange.ingestion.staging;

import com.certifyos.vendor_exchange.persistence.Collections;
import com.certifyos.vendor_exchange.persistence.Documents;
import com.mongodb.bulk.BulkWriteUpsert;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The {@code field_recommendations} collection. Staging writes by upsert on the deterministic
 * {@code _id} with {@code $setOnInsert}, so a re-stage never overwrites a decision; that bulk write is
 * the one write of the lane outside a transaction (design D10: idempotent by {@code _id}). Every
 * decision and release move is a guarded update on {@code reviewStatus} (and, for a decision,
 * {@code version}) that bumps {@code version}. Every read takes the tenant; every list carries a limit.
 */
@ApplicationScoped
public class FieldRecommendationRepository {

    /** The open list's order, newest staged first; {@code _id} breaks ties. */
    static final Bson LIST_SORT = Sorts.descending("stagedAt", "_id");

    private static final char CURSOR_SEPARATOR = '|';

    private final Collections collections;

    public FieldRecommendationRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * What a staging upsert did.
     *
     * @param attempted documents sent
     * @param insertedIds the ids this call inserted; the rest already existed
     */
    public record Upserted(int attempted, Set<String> insertedIds) {
        public Upserted {
            insertedIds = insertedIds == null ? Set.of() : Set.copyOf(insertedIds);
        }
    }

    /**
     * One page of the review list.
     *
     * @param tenantId the tenant
     * @param field one field, or null for every field
     * @param statuses the review states to list; a subset of {@link ReviewStatus#open()} reads the
     *     partial index
     * @param npi one practitioner's NPI, or null
     * @param exportBatchId one batch, or null
     * @param after {@link #cursorAfter} of the previous page's last document, or null for the first page
     * @param limit the page size; the read fetches one more so the caller can tell whether a next page exists
     */
    public record ListQuery(
            String tenantId,
            String field,
            Set<ReviewStatus> statuses,
            String npi,
            String exportBatchId,
            String after,
            int limit) {
        public ListQuery {
            if (tenantId == null || tenantId.isBlank()) {
                throw new IllegalArgumentException("tenantId is required");
            }
            if (statuses == null || statuses.isEmpty()) {
                throw new IllegalArgumentException("at least one review status is required");
            }
            if (limit < 1) {
                throw new IllegalArgumentException("limit must be at least 1, got " + limit);
            }
            statuses = Set.copyOf(statuses);
        }
    }

    /**
     * Writes a chunk of staged documents. Each is one {@code updateOne} on its {@code _id} with
     * {@code $setOnInsert} of every field but {@code updatedAt} and {@code $set} of {@code updatedAt},
     * in one unordered bulk write without a session. The caller sends each id at most once per call.
     *
     * @param docs the documents, as {@link FieldRecommendation#pending} builds them
     * @return how many were sent and which were new
     */
    public Upserted upsertAll(List<FieldRecommendation> docs) {
        if (docs.isEmpty()) {
            return new Upserted(0, Set.of());
        }
        List<WriteModel<Document>> writes = new ArrayList<>(docs.size());
        for (FieldRecommendation recommendation : docs) {
            Document onInsert = recommendation.toDocument();
            onInsert.remove("_id");
            onInsert.remove("updatedAt");
            Bson update = Updates.combine(
                    Updates.setOnInsert(onInsert), Updates.set("updatedAt", recommendation.updatedAt()));
            writes.add(new UpdateOneModel<>(
                    Filters.eq("_id", recommendation.id()), update, new UpdateOptions().upsert(true)));
        }
        Set<String> inserted = new HashSet<>();
        for (BulkWriteUpsert upsert : recommendations()
                .bulkWrite(writes, new BulkWriteOptions().ordered(false))
                .getUpserts()) {
            inserted.add(upsert.getId().asString().getValue());
        }
        return new Upserted(docs.size(), inserted);
    }

    /**
     * One document of a tenant.
     *
     * @param tenantId the tenant
     * @param id the recommendation id
     * @return the document, or empty when absent or another tenant's
     */
    public Optional<FieldRecommendation> find(String tenantId, String id) {
        return Optional.ofNullable(recommendations()
                        .find(Filters.and(Filters.eq("_id", id), Filters.eq("tenantId", tenantId)))
                        .first())
                .map(FieldRecommendation::fromDocument);
    }

    /**
     * Where existing documents came from, for staging to tell its own earlier writes from another
     * upload's. Reads {@code _id} and {@code sourceFile} only, at most {@code ids.size()} documents.
     *
     * @param tenantId the tenant
     * @param ids recommendation ids
     * @return id to source file, for the ids that exist
     */
    public Map<String, FieldRecommendation.SourceFile> sourceFilesOf(String tenantId, Collection<String> ids) {
        Map<String, FieldRecommendation.SourceFile> sources = new HashMap<>();
        if (!ids.isEmpty()) {
            recommendations()
                    .find(Filters.and(Filters.in("_id", ids), Filters.eq("tenantId", tenantId)))
                    .projection(Projections.include("sourceFile"))
                    .limit(ids.size())
                    .forEach(doc -> Documents.nested(doc, "sourceFile")
                            .ifPresent(source -> sources.put(
                                    doc.getString("_id"), FieldRecommendation.SourceFile.fromDocument(source))));
        }
        return sources;
    }

    /**
     * A page of the review list, newest staged first, by keyset on {@code (stagedAt, _id)}. A document
     * decided between two pages leaves the list without moving any other row across the cursor.
     *
     * @param query the page
     * @return up to {@code limit + 1} documents; the extra one only says a next page exists
     */
    public List<FieldRecommendation> list(ListQuery query) {
        List<FieldRecommendation> page = new ArrayList<>();
        for (Document doc :
                recommendations().find(listFilter(query)).sort(LIST_SORT).limit(query.limit() + 1)) {
            page.add(FieldRecommendation.fromDocument(doc));
        }
        return page;
    }

    /** The filter {@link #list} runs, shared with the test that explains it. */
    static Bson listFilter(ListQuery query) {
        List<Bson> clauses = new ArrayList<>();
        clauses.add(Filters.eq("tenantId", query.tenantId()));
        clauses.add(Filters.in("reviewStatus", names(query.statuses())));
        if (query.field() != null) {
            clauses.add(Filters.eq("field", query.field()));
        }
        if (query.npi() != null) {
            clauses.add(Filters.eq("practitioner.npi", query.npi()));
        }
        if (query.exportBatchId() != null) {
            clauses.add(Filters.eq("exportBatchId", query.exportBatchId()));
        }
        if (query.after() != null) {
            int separator = query.after().indexOf(CURSOR_SEPARATOR);
            Instant stagedAt = cursorTime(query.after(), separator);
            String id = query.after().substring(separator + 1);
            // stagedAt < t OR (stagedAt == t AND _id < id), written as stagedAt <= t AND (stagedAt < t OR
            // _id < id). The planner turns the top-level range into index bounds but evaluates an $or
            // after the fetch, so without the range every page would re-scan every newer row.
            clauses.add(Filters.lte("stagedAt", stagedAt));
            clauses.add(Filters.or(Filters.lt("stagedAt", stagedAt), Filters.lt("_id", id)));
        }
        return Filters.and(clauses);
    }

    /**
     * The cursor that continues the list after a document.
     *
     * @param last the last document of a page
     * @return {@code <stagedAt epoch millis>|<_id>}
     */
    public static String cursorAfter(FieldRecommendation last) {
        return last.stagedAt().toEpochMilli() + String.valueOf(CURSOR_SEPARATOR) + last.id();
    }

    private static Instant cursorTime(String after, int separator) {
        if (separator < 1 || separator == after.length() - 1) {
            throw new IllegalArgumentException("cursor must be <epoch millis>|<id>");
        }
        try {
            return Instant.ofEpochMilli(Long.parseLong(after.substring(0, separator)));
        } catch (NumberFormatException notMillis) {
            throw new IllegalArgumentException("cursor must start with epoch millis", notMillis);
        }
    }

    /**
     * Documents per field and review state, in one {@code $group}, on {@code field_recs_batch_counts}.
     *
     * @param tenantId the tenant
     * @param exportBatchId one batch, or null for every batch of the tenant
     * @return field to state to count, fields in name order; empty when nothing matches
     */
    public Map<String, Map<ReviewStatus, Long>> summary(String tenantId, String exportBatchId) {
        Bson match = exportBatchId == null
                ? Filters.eq("tenantId", tenantId)
                : Filters.and(Filters.eq("tenantId", tenantId), Filters.eq("exportBatchId", exportBatchId));
        Map<String, Map<ReviewStatus, Long>> counts = new TreeMap<>();
        for (Document group : recommendations()
                .aggregate(List.of(
                        Aggregates.match(match),
                        Aggregates.group(
                                new Document("field", "$field").append("status", "$reviewStatus"),
                                Accumulators.sum("n", 1L))))) {
            Document key = group.get("_id", Document.class);
            counts.computeIfAbsent(key.getString("field"), field -> new EnumMap<>(ReviewStatus.class))
                    .put(ReviewStatus.valueOf(key.getString("status")), Documents.longValue(group, "n"));
        }
        Map<String, Map<ReviewStatus, Long>> frozen = new TreeMap<>();
        counts.forEach((field, byStatus) -> frozen.put(field, java.util.Collections.unmodifiableMap(byStatus)));
        return java.util.Collections.unmodifiableMap(frozen);
    }

    /**
     * A practitioner's documents in one batch, on {@code field_recs_practitioner}.
     *
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param exportBatchId the batch, or null for the practitioner's latest (its newest staged document's)
     * @param limit the most documents to return
     * @return the documents by id, empty when the practitioner has none
     */
    public List<FieldRecommendation> forPractitioner(
            String tenantId, String certifyPractitionerId, String exportBatchId, int limit) {
        return forKey(tenantId, "practitioner.certifyPractitionerId", certifyPractitionerId, exportBatchId, limit);
    }

    /**
     * An NPI's documents in one batch, on {@code field_recs_npi}.
     *
     * @param tenantId the tenant
     * @param npi the NPI
     * @param exportBatchId the batch, or null for the NPI's latest (its newest staged document's batch)
     * @param limit the most documents to return
     * @return the documents by id, empty when the NPI has none
     */
    public List<FieldRecommendation> forNpi(String tenantId, String npi, String exportBatchId, int limit) {
        return forKey(tenantId, "practitioner.npi", npi, exportBatchId, limit);
    }

    private List<FieldRecommendation> forKey(
            String tenantId, String path, String value, String exportBatchId, int limit) {
        requireLimit(limit);
        Bson owner = Filters.and(Filters.eq("tenantId", tenantId), Filters.eq(path, value));
        String batch = exportBatchId;
        if (batch == null) {
            Document latest = recommendations()
                    .find(owner)
                    .sort(Sorts.descending("stagedAt", "_id"))
                    .projection(Projections.include("exportBatchId"))
                    .limit(1)
                    .first();
            if (latest == null) {
                return List.of();
            }
            batch = latest.getString("exportBatchId");
        }
        return read(Filters.and(owner, Filters.eq("exportBatchId", batch)), limit);
    }

    /**
     * The documents of one release, on {@code field_recs_release}.
     *
     * @param tenantId the tenant
     * @param releaseId {@code goldenRecordUpdate.releaseId}
     * @param limit the most documents to return
     * @return the documents by id
     */
    public List<FieldRecommendation> forRelease(String tenantId, String releaseId, int limit) {
        requireLimit(limit);
        return read(
                Filters.and(Filters.eq("tenantId", tenantId), Filters.eq("goldenRecordUpdate.releaseId", releaseId)),
                limit);
    }

    /**
     * Records a decision: one guarded update on the document's id, tenant, version and state. The
     * repository sets {@code reviewStatus} and {@code updatedAt} and increments {@code version}; the
     * caller's {@code updates} carry the rest, including the {@code $push} of the
     * {@link FieldRecommendation.HistoryEntry} onto {@code reviewHistory}. A caller's {@code $set} of
     * {@code reviewStatus} or {@code updatedAt} is overridden; {@code version} must not appear in it.
     *
     * @param session the transaction the decision's audit event is written in
     * @param tenantId the tenant
     * @param id the recommendation
     * @param expectedVersion the version the caller read
     * @param from the states the decision may start from
     * @param to the state to move to
     * @param updates further updates, required: at least the {@code $push} of the decision's history
     *     entry, so no decision is recorded without one
     * @param now the write time
     * @return true when the document matched and moved; false when another writer moved it first, the
     *     version is stale, or the document is another tenant's
     * @throws NullPointerException when {@code updates} is null
     */
    public boolean decide(
            ClientSession session,
            String tenantId,
            String id,
            long expectedVersion,
            Set<ReviewStatus> from,
            ReviewStatus to,
            Bson updates,
            Instant now) {
        Objects.requireNonNull(updates, "a decision needs updates that push its HistoryEntry onto reviewHistory");
        Bson filter = Filters.and(
                Filters.eq("_id", id),
                Filters.eq("tenantId", tenantId),
                Filters.eq("version", expectedVersion),
                Filters.in("reviewStatus", names(from)));
        return recommendations()
                        .updateOne(session, filter, withBase(updates, move(to, now)))
                        .getModifiedCount()
                == 1;
    }

    /**
     * Moves every document of one release of one practitioner from one state to another, as
     * {@link #decide} does for one (Step 7: claim, un-claim, outcome).
     *
     * @param session the transaction
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param releaseId {@code goldenRecordUpdate.releaseId}
     * @param from the state the documents must be in
     * @param to the state to move to
     * @param updates further updates, may be null; the {@code $set} and {@code version} rules of
     *     {@link #decide} apply
     * @param now the write time
     * @return how many documents moved
     */
    public long moveAll(
            ClientSession session,
            String tenantId,
            String certifyPractitionerId,
            String releaseId,
            ReviewStatus from,
            ReviewStatus to,
            Bson updates,
            Instant now) {
        Bson filter = Filters.and(
                Filters.eq("tenantId", tenantId),
                Filters.eq("practitioner.certifyPractitionerId", certifyPractitionerId),
                Filters.eq("goldenRecordUpdate.releaseId", releaseId),
                Filters.eq("reviewStatus", from.name()));
        return recommendations()
                .updateMany(session, filter, withBase(updates, move(to, now)))
                .getModifiedCount();
    }

    /**
     * A practitioner's {@code RELEASING} documents under any other release (Step 7, plan P12: a
     * non-zero count means another release of the practitioner is running).
     *
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param exceptReleaseId the caller's own release
     * @return the count
     */
    public long countReleasing(String tenantId, String certifyPractitionerId, String exceptReleaseId) {
        return recommendations()
                .countDocuments(Filters.and(
                        Filters.eq("tenantId", tenantId),
                        Filters.eq("practitioner.certifyPractitionerId", certifyPractitionerId),
                        Filters.eq("reviewStatus", ReviewStatus.RELEASING.name()),
                        Filters.ne("goldenRecordUpdate.releaseId", exceptReleaseId)));
    }

    /**
     * Documents per review state across every tenant, for the by-state gauges. A system-scoped read,
     * like the inbound registry's {@code countByState}; its tags never name a tenant's data.
     *
     * @return state name to count; states with no documents are absent
     */
    public Map<String, Long> countByStatus() {
        return counts(List.of(Aggregates.group("$reviewStatus", Accumulators.sum("n", 1L))));
    }

    /**
     * A tenant's {@code PENDING} documents per field, on the open-list index.
     *
     * @param tenantId the tenant
     * @return field to count; fields with nothing pending are absent
     */
    public Map<String, Long> countPendingByField(String tenantId) {
        return counts(List.of(
                Aggregates.match(Filters.and(
                        Filters.eq("tenantId", tenantId), Filters.eq("reviewStatus", ReviewStatus.PENDING.name()))),
                Aggregates.group("$field", Accumulators.sum("n", 1L))));
    }

    /**
     * When a tenant's oldest {@code PENDING} document was staged, for the review-age gauge.
     *
     * @param tenantId the tenant
     * @return the oldest {@code stagedAt}, or empty when nothing is pending
     */
    public Optional<Instant> oldestPendingStagedAt(String tenantId) {
        Document oldest = recommendations()
                .find(Filters.and(
                        Filters.eq("tenantId", tenantId), Filters.eq("reviewStatus", ReviewStatus.PENDING.name())))
                .sort(Sorts.ascending("stagedAt"))
                .projection(Projections.include("stagedAt"))
                .limit(1)
                .first();
        return Optional.ofNullable(oldest).map(doc -> Documents.instant(doc, "stagedAt"));
    }

    /** A limit of 0 reads without a bound in MongoDB; every list read needs a real one. */
    private static void requireLimit(int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be at least 1, got " + limit);
        }
    }

    private List<FieldRecommendation> read(Bson filter, int limit) {
        List<FieldRecommendation> docs = new ArrayList<>();
        for (Document doc :
                recommendations().find(filter).sort(Sorts.ascending("_id")).limit(limit)) {
            docs.add(FieldRecommendation.fromDocument(doc));
        }
        return docs;
    }

    private Map<String, Long> counts(List<Bson> pipeline) {
        Map<String, Long> counts = new HashMap<>();
        recommendations()
                .aggregate(pipeline)
                .forEach(doc -> counts.put(doc.getString("_id"), Documents.longValue(doc, "n")));
        return Map.copyOf(counts);
    }

    private MongoCollection<Document> recommendations() {
        return collections.fieldRecommendations();
    }

    /** The repository's own part of every move: the state, the write time and the version. */
    private static Bson move(ReviewStatus to, Instant now) {
        return Updates.combine(
                Updates.set("reviewStatus", to.name()), Updates.set("updatedAt", now), Updates.inc("version", 1L));
    }

    /**
     * The caller's updates, then the repository's own. {@code Updates.combine} merges every
     * {@code $set} into one document with a later field replacing an earlier one, so with the base
     * last a caller cannot override {@code reviewStatus} or {@code updatedAt}.
     */
    private static Bson withBase(Bson updates, Bson base) {
        return updates == null ? base : Updates.combine(updates, base);
    }

    private static List<String> names(Set<ReviewStatus> statuses) {
        return statuses.stream().map(Enum::name).sorted().toList();
    }
}
