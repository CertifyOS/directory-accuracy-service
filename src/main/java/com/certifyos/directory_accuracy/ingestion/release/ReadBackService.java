package com.certifyos.directory_accuracy.ingestion.release;

import com.certifyos.directory_accuracy.clients.DalOperationalValueClient;
import com.certifyos.directory_accuracy.clients.OperationalValue;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.directory_accuracy.ingestion.staging.FieldLevel;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/**
 * The read-back (design D19, plan P9): read the entity's operational value with lineage and finish
 * its written {@code RELEASING} fields with their verdicts. Driven by the {@code
 * mdm.slice.ov_generated} handler and, for a lost event, by the release's read-back deadline. A read
 * failure propagates to the caller; the deadline then marks the fields {@code UNKNOWN}.
 *
 * <p>A release's deadline job is deleted only once none of the release's fields is still {@code
 * RELEASING}: one release can carry a practitioner's fields and a location's, which arrive as two
 * events.
 */
@ApplicationScoped
public class ReadBackService {

    private static final Logger LOG = Logger.getLogger(ReadBackService.class);

    private final ReleaseFieldRepository releaseFields;
    private final ReleaseLifecycle lifecycle;
    private final DalOperationalValueClient operationalValues;
    private final IngestionJobEnqueuer enqueuer;

    public ReadBackService(
            ReleaseFieldRepository releaseFields,
            ReleaseLifecycle lifecycle,
            @RestClient DalOperationalValueClient operationalValues,
            IngestionJobEnqueuer enqueuer) {
        this.releaseFields = releaseFields;
        this.lifecycle = lifecycle;
        this.operationalValues = operationalValues;
        this.enqueuer = enqueuer;
    }

    /** What a read-back did. */
    public enum Result {
        /** The entity's written fields are {@code RELEASED} with their verdicts. */
        RELEASED,
        /** The entity has no {@code RELEASING} field: not ours, or already finished. */
        NOTHING_RELEASING,
        /**
         * The entity's {@code RELEASING} fields have no write recorded yet: the event outran the job's
         * {@code written} commit; the caller answers so the event is redelivered.
         */
        NOT_WRITTEN,
        /**
         * The entity's written {@code RELEASING} fields are not covered by this read-back: an event for
         * another or an earlier write, or another release's fields at the deadline. Acknowledged.
         */
        NOT_COVERED,
        /** The deadline could not read the operational value; the fields are {@code RELEASED UNKNOWN}. */
        UNKNOWN_MARKED
    }

    /**
     * One slice whose change triggered an operational-value run, as the event names it.
     *
     * @param sliceId the slice's certify id
     * @param crosswalkId the slice's crosswalk id
     * @param changedAt when the slice changed, null when the event did not say or it did not parse
     */
    public record Trigger(String sliceId, String crosswalkId, Instant changedAt) {}

    /**
     * The event path: reads the entity's operational value and finishes the written {@code RELEASING}
     * fields the run covers. A field counts when one of our triggering slices is the slice its write
     * recorded (by slice id or crosswalk) and that slice changed at or after the write was sent, so a
     * redelivered or earlier run, or another practitioner's run on a shared location slice before this
     * write, finishes nothing it does not include. A trigger without a change time matches by slice
     * alone. Fields the run does not cover wait for their own event or their release's deadline.
     *
     * @param tenantId the tenant
     * @param certifyId the practitioner's or the location's certify id
     * @param level which of the two the id is
     * @param triggers our slices that triggered the run
     * @param now the write time
     * @return what happened
     */
    public Result onEvent(String tenantId, String certifyId, FieldLevel level, List<Trigger> triggers, Instant now) {
        return finishWhere(
                tenantId, certifyId, level, field -> coveredBy(field, triggers), ReleaseLifecycle.EVENT, now);
    }

    /**
     * The deadline path: reads the entity's operational value and finishes the written {@code
     * RELEASING} fields of one release only; another release's fields at the same entity keep waiting
     * for their own event or deadline.
     *
     * @param tenantId the tenant
     * @param certifyId the practitioner's or the location's certify id
     * @param level which of the two the id is
     * @param releaseId the release whose deadline ran
     * @param now the write time
     * @return what happened
     */
    public Result atDeadline(String tenantId, String certifyId, FieldLevel level, String releaseId, Instant now) {
        return finishWhere(
                tenantId,
                certifyId,
                level,
                field -> releaseId.equals(field.goldenRecordUpdate().releaseId()),
                ReleaseLifecycle.DEADLINE,
                now);
    }

    private Result finishWhere(
            String tenantId,
            String certifyId,
            FieldLevel level,
            Predicate<FieldRecommendation> eligible,
            String completionSource,
            Instant now) {
        List<FieldRecommendation> releasing = releaseFields.releasingAt(
                tenantId, certifyId, level, lifecycle.config().ingestion().releaseBulkMax());
        if (releasing.isEmpty()) {
            return Result.NOTHING_RELEASING;
        }
        List<FieldRecommendation> written =
                releasing.stream().filter(ReadBackService::isWritten).toList();
        List<FieldRecommendation> covered = written.stream().filter(eligible).toList();
        if (covered.isEmpty()) {
            if (written.size() < releasing.size()) {
                LOG.infof(
                        "tenant %s %s %s: %d field(s) RELEASING, %d with a recorded write, none covered yet",
                        tenantId, level, certifyId, releasing.size(), written.size());
                return Result.NOT_WRITTEN;
            }
            LOG.infof(
                    "tenant %s %s %s: %d written field(s) RELEASING, none covered by this %s read-back",
                    tenantId, level, certifyId, written.size(), completionSource);
            return Result.NOT_COVERED;
        }
        OperationalValue value = level == FieldLevel.LOCATION
                ? operationalValues.location(tenantId, certifyId, true)
                : operationalValues.practitioner(tenantId, certifyId, true);
        Map<String, ReadBack.Verdict> verdicts = verdicts(value, Crosswalks.sourceType(tenantId), covered);
        finish(tenantId, lifecycle.released(tenantId, covered, verdicts, completionSource, now), completionSource);
        if (ReleaseLifecycle.EVENT.equals(completionSource) && written.size() < releasing.size()) {
            // Fields at the entity whose write is not recorded yet may belong to this very run: ask for
            // the event again rather than leave them to the deadline. The finished ones are not RELEASING
            // any more, so the redelivery only looks at the rest.
            LOG.infof(
                    "tenant %s %s %s: %d field(s) finished, %d still without a recorded write; redelivery asked",
                    tenantId, level, certifyId, covered.size(), releasing.size() - written.size());
            return Result.NOT_WRITTEN;
        }
        return Result.RELEASED;
    }

    /** Whether one of the run's triggering slices is this field's write, changed at or after it was sent. */
    static boolean coveredBy(FieldRecommendation field, List<Trigger> triggers) {
        GoldenRecordUpdate update = field.goldenRecordUpdate();
        for (Trigger trigger : triggers) {
            boolean sameSlice = (trigger.sliceId() != null && trigger.sliceId().equals(update.sliceId()))
                    || (trigger.crosswalkId() != null && trigger.crosswalkId().equals(update.crosswalkId()));
            boolean late = trigger.changedAt() == null
                    || update.sentAt() == null
                    || !trigger.changedAt().isBefore(update.sentAt());
            if (sameSlice && late) {
                return true;
            }
        }
        return false;
    }

    /**
     * Finishes fields whose operational value could not be read by the deadline: {@code RELEASED} with
     * outcome {@code UNKNOWN}.
     *
     * @param tenantId the tenant
     * @param fields the fields, {@code RELEASING}
     * @param completionSource the completion source, {@link ReleaseLifecycle#DEADLINE}
     * @param now the write time
     * @return {@link Result#UNKNOWN_MARKED}
     */
    public Result markUnknown(String tenantId, List<FieldRecommendation> fields, String completionSource, Instant now) {
        finish(tenantId, lifecycle.released(tenantId, fields, Map.of(), completionSource, now), completionSource);
        return Result.UNKNOWN_MARKED;
    }

    /** Each written field's verdict, grouped by the slice id its write recorded. */
    static Map<String, ReadBack.Verdict> verdicts(
            OperationalValue value, String sourceType, List<FieldRecommendation> written) {
        Map<String, Map<String, String>> bySlice = new LinkedHashMap<>();
        for (FieldRecommendation field : written) {
            bySlice.computeIfAbsent(field.goldenRecordUpdate().sliceId(), slice -> new LinkedHashMap<>())
                    .put(field.id(), field.field());
        }
        Map<String, ReadBack.Verdict> verdicts = new LinkedHashMap<>();
        bySlice.forEach((sliceId, fields) -> verdicts.putAll(ReadBack.evaluate(value, sliceId, sourceType, fields)));
        return verdicts;
    }

    /**
     * For each release a move finished: once none of its fields is {@code RELEASING}, count it and,
     * on the event path, delete its deadline job (the deadline job does not delete itself).
     */
    private void finish(String tenantId, List<FieldRecommendation> moved, String completionSource) {
        Set<String> releaseIds = new LinkedHashSet<>();
        Map<String, String> deadlineJobs = new LinkedHashMap<>();
        for (FieldRecommendation field : moved) {
            GoldenRecordUpdate update = field.goldenRecordUpdate();
            releaseIds.add(update.releaseId());
            if (update.deadlineJobId() != null) {
                deadlineJobs.put(update.releaseId(), update.deadlineJobId());
            }
        }
        for (String releaseId : releaseIds) {
            if (!lifecycle.releasing(tenantId, releaseId).isEmpty()) {
                continue;
            }
            lifecycle.metrics().releaseJob(tenantId, "RELEASED");
            if (ReleaseLifecycle.EVENT.equals(completionSource)) {
                enqueuer.delete(deadlineJob(releaseId, deadlineJobs.get(releaseId)));
            }
            LOG.infof("release %s: read back, every field RELEASED", releaseId);
        }
    }

    private static UUID deadlineJob(String releaseId, String recorded) {
        try {
            return recorded == null ? IngestionJobEnqueuer.readBackDeadlineId(releaseId) : UUID.fromString(recorded);
        } catch (IllegalArgumentException notAUuid) {
            return IngestionJobEnqueuer.readBackDeadlineId(releaseId);
        }
    }

    private static boolean isWritten(FieldRecommendation field) {
        GoldenRecordUpdate update = field.goldenRecordUpdate();
        return update != null && update.writtenAt() != null && update.sliceId() != null;
    }
}
