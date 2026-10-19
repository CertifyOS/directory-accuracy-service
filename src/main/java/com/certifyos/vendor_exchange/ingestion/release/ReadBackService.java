package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.clients.DalOperationalValueClient;
import com.certifyos.vendor_exchange.clients.OperationalValue;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.vendor_exchange.ingestion.staging.FieldLevel;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
        /** The deadline could not read the operational value; the fields are {@code RELEASED UNKNOWN}. */
        UNKNOWN_MARKED
    }

    /**
     * Reads the entity's operational value and finishes its written {@code RELEASING} fields.
     *
     * @param tenantId the tenant
     * @param certifyId the practitioner's or the location's certify id
     * @param level which of the two the id is
     * @param completionSource {@link ReleaseLifecycle#EVENT} or {@link ReleaseLifecycle#DEADLINE}
     * @param now the write time
     * @return what happened
     */
    public Result readAndFinish(
            String tenantId, String certifyId, FieldLevel level, String completionSource, Instant now) {
        List<FieldRecommendation> releasing = releaseFields.releasingAt(
                tenantId, certifyId, level, lifecycle.config().ingestion().releaseBulkMax());
        if (releasing.isEmpty()) {
            return Result.NOTHING_RELEASING;
        }
        List<FieldRecommendation> written =
                releasing.stream().filter(ReadBackService::isWritten).toList();
        if (written.isEmpty()) {
            LOG.infof(
                    "tenant %s %s %s: %d field(s) RELEASING, none with a recorded write yet",
                    tenantId, level, certifyId, releasing.size());
            return Result.NOT_WRITTEN;
        }
        OperationalValue value = level == FieldLevel.LOCATION
                ? operationalValues.location(tenantId, certifyId, true)
                : operationalValues.practitioner(tenantId, certifyId, true);
        Map<String, ReadBack.Verdict> verdicts = verdicts(value, Crosswalks.sourceType(tenantId), written);
        finish(tenantId, lifecycle.released(tenantId, written, verdicts, completionSource, now), completionSource);
        return Result.RELEASED;
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
