package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.audit.Actors;
import com.certifyos.directory_accuracy.persistence.AlreadyExistsException;
import com.certifyos.directory_accuracy.persistence.Collections;
import com.certifyos.directory_accuracy.persistence.Documents;
import com.certifyos.directory_accuracy.persistence.Ids;
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
import org.bson.Document;
import org.bson.conversions.Bson;

/**
 * The schedules collection. Every read is scoped by tenant or by the natural id; the one cross-tenant
 * read, {@link #findDue}, is the tick's and carries a limit. Every update is compare-and-set on
 * {@code version}.
 */
@ApplicationScoped
public class ScheduleRepository {

    private final Collections collections;

    public ScheduleRepository(Collections collections) {
        this.collections = collections;
    }

    /**
     * Inserts a new schedule.
     *
     * @param session the transaction
     * @param schedule the schedule at version 1
     * @throws AlreadyExistsException when the tenant and vendor already have a schedule
     */
    public void insert(ClientSession session, Schedule schedule) {
        try {
            collections.schedules().insertOne(session, schedule.toDocument());
        } catch (MongoWriteException failure) {
            if (Documents.isDuplicateKey(failure)) {
                throw new AlreadyExistsException("vendor_export_schedules", schedule.id());
            }
            throw failure;
        }
    }

    /**
     * One schedule by tenant and vendor.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @return the schedule, if any
     */
    public Optional<Schedule> find(String tenantId, String vendor) {
        Document doc = collections
                .schedules()
                .find(Filters.eq("_id", Ids.scheduleId(tenantId, vendor)))
                .first();
        return Optional.ofNullable(doc).map(Schedule::fromDocument);
    }

    /**
     * Every schedule of a tenant.
     *
     * @param tenantId the tenant
     * @param limit the most rows to return
     * @return the schedules, by vendor
     */
    public List<Schedule> findByTenant(String tenantId, int limit) {
        return toList(collections
                .schedules()
                .find(Filters.eq("tenantId", tenantId))
                .sort(Sorts.ascending("vendor"))
                .limit(limit));
    }

    /**
     * The tick's query: enabled schedules whose next due instant has passed, on the
     * {@code (enabled, nextDueAt)} index.
     *
     * @param now the instant to compare against
     * @param limit the most rows to return
     * @return due schedules, earliest first
     */
    public List<Schedule> findDue(Instant now, int limit) {
        Bson due = Filters.and(Filters.eq("enabled", true), Filters.lte("nextDueAt", now));
        return toList(collections
                .schedules()
                .find(due)
                .sort(Sorts.ascending("nextDueAt"))
                .limit(limit));
    }

    /**
     * The tick's write after it created a batch: advance {@code nextDueAt} and record the run, in the
     * same transaction as the batch insert, compare-and-set on the version the tick read.
     *
     * @param session the transaction
     * @param scheduleId the schedule
     * @param expectedVersion the version the caller read
     * @param nextDueAt the next occurrence
     * @param lastBatchId the batch just created
     * @param now the run time
     * @return true when the row was at the expected version and is now advanced
     */
    public boolean advance(
            ClientSession session,
            String scheduleId,
            long expectedVersion,
            Instant nextDueAt,
            String lastBatchId,
            Instant now) {
        Bson update = Updates.combine(
                Updates.set("nextDueAt", nextDueAt),
                Updates.set("lastBatchId", lastBatchId),
                Updates.set("lastRunAt", now),
                Updates.set("updatedBy", Actors.SYSTEM),
                Updates.set("updatedAt", now),
                Updates.inc("version", 1L));
        return update(session, scheduleId, expectedVersion, update);
    }

    /**
     * A compare-and-set update of any fields. The version is incremented and {@code updatedAt} set
     * by this method; callers pass the other fields.
     *
     * @param session the transaction
     * @param scheduleId the schedule
     * @param expectedVersion the version the caller read
     * @param update the fields to set
     * @return true when the row was at the expected version
     */
    public boolean update(ClientSession session, String scheduleId, long expectedVersion, Bson update) {
        Bson filter = Filters.and(Filters.eq("_id", scheduleId), Filters.eq("version", expectedVersion));
        UpdateResult result = collections.schedules().updateOne(session, filter, update);
        return result.getMatchedCount() == 1;
    }

    private static List<Schedule> toList(Iterable<Document> docs) {
        List<Schedule> schedules = new ArrayList<>();
        for (Document doc : docs) {
            schedules.add(Schedule.fromDocument(doc));
        }
        return schedules;
    }

    /**
     * Schedules by enabled flag, for the by-state gauges.
     *
     * @return true and false to their counts; a missing key means zero
     */
    public Map<Boolean, Long> countByEnabled() {
        Map<Boolean, Long> counts = new java.util.HashMap<>();
        collections
                .schedules()
                .aggregate(List.of(Aggregates.group("$enabled", Accumulators.sum("n", 1))))
                .forEach(doc -> counts.put(doc.getBoolean("_id", false), Documents.longValue(doc, "n")));
        return Map.copyOf(counts);
    }
}
