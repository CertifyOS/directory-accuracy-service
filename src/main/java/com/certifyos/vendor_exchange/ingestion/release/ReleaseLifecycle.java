package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.audit.Actors;
import com.certifyos.vendor_exchange.audit.AuditEventType;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.ingestion.jobs.IngestionLogContext;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.metrics.VendorExchangeMetrics;
import com.certifyos.vendor_exchange.persistence.Transactions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;
import org.jboss.logging.MDC;

/**
 * The release side of the field state machine, after the request: claim ({@code RELEASE_REQUESTED →
 * RELEASING}), written, released, failed. Each move is a guarded update inside a transaction with its
 * audit event; nothing outside MongoDB runs inside one.
 */
@ApplicationScoped
public class ReleaseLifecycle {

    /** {@code completionSource} of a read-back on survivorship's event. */
    public static final String EVENT = "EVENT";

    /** {@code completionSource} of a read-back at the deadline. */
    public static final String DEADLINE = "DEADLINE";

    private static final Logger LOG = Logger.getLogger(ReleaseLifecycle.class);
    private static final ReadBack.Verdict UNKNOWN = new ReadBack.Verdict(ReadBack.Outcome.UNKNOWN, null);

    /** The most of a failure's text kept on the field document. */
    static final int ERROR_MAX = 500;

    private static final Set<ReviewStatus> FAILABLE =
            EnumSet.of(ReviewStatus.RELEASE_REQUESTED, ReviewStatus.RELEASING);

    private final FieldRecommendationRepository fields;
    private final ReleaseFieldRepository releaseFields;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final VendorExchangeMetrics metrics;
    private final VendorExchangeConfig cfg;

    public ReleaseLifecycle(
            FieldRecommendationRepository fields,
            ReleaseFieldRepository releaseFields,
            AuditRepository audit,
            Transactions transactions,
            VendorExchangeMetrics metrics,
            VendorExchangeConfig cfg) {
        this.fields = fields;
        this.releaseFields = releaseFields;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
        this.cfg = cfg;
    }

    /**
     * One slice write of a release and the fields it carried.
     *
     * @param fieldRecommendationIds the fields
     * @param resource the DAL resource ({@code CorePractitioner} or {@code CoreLocation})
     * @param crosswalkId the slice's crosswalk id
     * @param sliceId the certify id of the slice written
     */
    public record Write(List<String> fieldRecommendationIds, String resource, String crosswalkId, String sliceId) {
        public Write {
            fieldRecommendationIds = List.copyOf(fieldRecommendationIds);
        }
    }

    /**
     * Records the DAL write on the release's {@code RELEASING} fields: {@code goldenRecordUpdate.resource},
     * {@code crosswalkId}, {@code sliceId}, {@code writtenAt} and {@code deadlineJobId}, in one
     * transaction, no event. The fields stay {@code RELEASING} until the read-back.
     *
     * @param tenantId the tenant
     * @param releaseId the release
     * @param writes each slice write and its fields
     * @param writtenAt when the DAL accepted the batch
     * @param deadlineJobId the read-back deadline the job schedules next
     */
    public void written(String tenantId, String releaseId, List<Write> writes, Instant writtenAt, UUID deadlineJobId) {
        long marked = transactions.run(session -> {
            long count = 0;
            for (Write write : writes) {
                count += releaseFields.markWritten(
                        session,
                        tenantId,
                        releaseId,
                        write.fieldRecommendationIds(),
                        Updates.combine(
                                Updates.set("goldenRecordUpdate.resource", write.resource()),
                                Updates.set("goldenRecordUpdate.crosswalkId", write.crosswalkId()),
                                Updates.set("goldenRecordUpdate.sliceId", write.sliceId()),
                                Updates.set("goldenRecordUpdate.writtenAt", writtenAt),
                                Updates.set("goldenRecordUpdate.deadlineJobId", deadlineJobId.toString())),
                        writtenAt);
            }
            return count;
        });
        LOG.infof("release %s: %d field(s) written in %d slice write(s)", releaseId, marked, writes.size());
    }

    /**
     * Fails a release from the job: see {@link #failed(String, String, String, String, boolean, int,
     * Instant, String)}, with the job id from the log context.
     */
    public int failed(
            String tenantId,
            String releaseId,
            String reason,
            String error,
            boolean backToApproved,
            int jobAttempts,
            Instant now) {
        return failed(tenantId, releaseId, reason, error, backToApproved, jobAttempts, now, currentJobId());
    }

    /**
     * Fails a release: its {@code RELEASE_REQUESTED} and {@code RELEASING} fields go back to {@code
     * APPROVED} (a refusal a reviewer can retry after a fix: {@code RANKING_MISSING}, {@code
     * UNKNOWN_ENTITY}, {@code NAME_SOUND_CHANGED}, {@code DAL_REJECTED}) or to {@code RELEASE_FAILED}
     * ({@code EXHAUSTED}), each with {@code FIELD_RELEASE_FAILED}, in one transaction. The full text is
     * kept on the document as {@code goldenRecordUpdate.error}; the event carries only its first part
     * (the reason and, for a DAL refusal, the HTTP status), since a DAL error body can echo a value.
     *
     * @param tenantId the tenant
     * @param releaseId the release
     * @param reason the stable reason
     * @param error what went wrong, or null
     * @param backToApproved true for a refusal, false for an exhausted job
     * @param jobAttempts how many times the job ran
     * @param now the write time
     * @param jobId the JobRunr job, or null
     * @return how many fields moved
     */
    public int failed(
            String tenantId,
            String releaseId,
            String reason,
            String error,
            boolean backToApproved,
            int jobAttempts,
            Instant now,
            String jobId) {
        List<FieldRecommendation> open =
                fields.forRelease(tenantId, releaseId, cfg.ingestion().releaseBulkMax()).stream()
                        .filter(field -> FAILABLE.contains(field.reviewStatus()))
                        .toList();
        ReviewStatus to = backToApproved ? ReviewStatus.APPROVED : ReviewStatus.RELEASE_FAILED;
        String text = cut(error == null || error.isBlank() ? reason : reason + ": " + error);
        String summary = summary(text);
        int moved = transactions.run(session -> {
            int count = 0;
            for (FieldRecommendation field : open) {
                if (fields.decide(
                        session,
                        tenantId,
                        field.id(),
                        field.version(),
                        FAILABLE,
                        to,
                        Updates.set("goldenRecordUpdate.error", text),
                        now)) {
                    audit.write(
                            session,
                            ReleaseService.event(AuditEventType.FIELD_RELEASE_FAILED, field, Actors.INGESTION, now)
                                    .release(releaseId)
                                    .jobId(jobId)
                                    .detail("reason", reason)
                                    .detail("error", summary)
                                    .detail("jobAttempts", jobAttempts)
                                    .detail("field", field.field())
                                    .build());
                    count++;
                }
            }
            return count;
        });
        if (moved > 0) {
            metrics.releaseJob(tenantId, "RELEASE_FAILED");
            metrics.releaseFailedReason(reason);
        }
        LOG.warnf("release %s: %d field(s) moved to %s, reason %s", releaseId, moved, to, reason);
        return moved;
    }

    /** The text before the second {@code :}, which holds the reason and a status, never a DAL body. */
    static String summary(String text) {
        int first = text.indexOf(':');
        int second = first < 0 ? -1 : text.indexOf(':', first + 1);
        return second < 0 ? text : text.substring(0, second);
    }

    private static String cut(String text) {
        return text.length() <= ERROR_MAX ? text : text.substring(0, ERROR_MAX);
    }

    /**
     * What a claim handed the job.
     *
     * @param fields every {@code RELEASING} field of the release, the ones this claim moved and any an
     *     earlier run of the same job had moved
     */
    public record Claimed(List<FieldRecommendation> fields) {
        public Claimed {
            fields = List.copyOf(fields);
        }

        /** Whether an earlier run already wrote these fields to the DAL (a crash before the deadline was set). */
        public boolean written() {
            return fields.stream()
                    .allMatch(field -> field.goldenRecordUpdate() != null
                            && field.goldenRecordUpdate().writtenAt() != null);
        }
    }

    /**
     * Claims a release for its job (plan P12). The claim commits on its own; only then does the job
     * count the practitioner's {@code RELEASING} fields of other releases, because a count inside the
     * claim's transaction could not see another job's uncommitted claim and both would go on. Each job
     * counts after its own claim committed, so of two concurrent jobs at least one sees the other; a
     * non-zero count puts this release's fields back to {@code RELEASE_REQUESTED} and throws, so
     * JobRunr retries the job later. A release an earlier run already wrote skips the count: its DAL
     * write is done and only its read-back deadline is missing.
     *
     * @param tenantId the tenant
     * @param certifyPractitionerId the practitioner
     * @param releaseId the release
     * @param now the write time
     * @return the release's {@code RELEASING} fields, or empty when it has none (already released,
     *     failed or rolled back)
     * @throws ReleaseInProgressException when another release of the practitioner is running
     */
    public Optional<Claimed> claim(String tenantId, String certifyPractitionerId, String releaseId, Instant now) {
        long moved = transactions.run(session -> fields.moveAll(
                session,
                tenantId,
                certifyPractitionerId,
                releaseId,
                ReviewStatus.RELEASE_REQUESTED,
                ReviewStatus.RELEASING,
                null,
                now));
        List<FieldRecommendation> releasing = releasing(tenantId, releaseId);
        if (releasing.isEmpty()) {
            LOG.infof("release %s: no field to release (moved %d), already finished or rolled back", releaseId, moved);
            return Optional.empty();
        }
        Claimed claimed = new Claimed(releasing);
        if (claimed.written()) {
            return Optional.of(claimed);
        }
        long others = fields.countReleasing(tenantId, certifyPractitionerId, releaseId);
        if (others > 0) {
            long back = transactions.run(session -> fields.moveAll(
                    session,
                    tenantId,
                    certifyPractitionerId,
                    releaseId,
                    ReviewStatus.RELEASING,
                    ReviewStatus.RELEASE_REQUESTED,
                    null,
                    now));
            throw new ReleaseInProgressException("release " + releaseId + ": " + others
                    + " field(s) of another release of the practitioner are RELEASING; " + back
                    + " field(s) put back for a retry");
        }
        LOG.infof("release %s: claimed %d field(s)", releaseId, releasing.size());
        return Optional.of(claimed);
    }

    /**
     * Finishes read-back fields: {@code RELEASING → RELEASED} with the outcome, the winning source and
     * {@code releasedAt}, and {@code FIELD_RELEASED} per field, in one transaction. Each move is guarded
     * by the field's version and state, so the event handler and the deadline job finishing the same
     * field at once write it once; the later one skips it.
     *
     * @param tenantId the tenant
     * @param released the fields as read, {@code RELEASING} with their write recorded
     * @param verdicts field recommendation id to verdict; a field without one is {@code UNKNOWN}
     * @param completionSource {@link #EVENT} or {@link #DEADLINE}
     * @param now the write time
     * @return the fields this call moved
     */
    public List<FieldRecommendation> released(
            String tenantId,
            List<FieldRecommendation> released,
            Map<String, ReadBack.Verdict> verdicts,
            String completionSource,
            Instant now) {
        String jobId = currentJobId();
        List<FieldRecommendation> moved = transactions.run(session -> {
            List<FieldRecommendation> done = new ArrayList<>();
            for (FieldRecommendation field : released) {
                ReadBack.Verdict verdict = verdicts.getOrDefault(field.id(), UNKNOWN);
                if (releasedOne(session, field, verdict, completionSource, jobId, now)) {
                    done.add(field);
                }
            }
            return done;
        });
        for (FieldRecommendation field : moved) {
            metrics.releaseOutcome(
                    tenantId,
                    verdicts.getOrDefault(field.id(), UNKNOWN).outcome().name());
        }
        LOG.infof(
                "tenant %s: %d of %d field(s) released (completion %s)",
                tenantId, moved.size(), released.size(), completionSource);
        return moved;
    }

    private boolean releasedOne(
            ClientSession session,
            FieldRecommendation field,
            ReadBack.Verdict verdict,
            String completionSource,
            String jobId,
            Instant now) {
        List<Bson> sets = new ArrayList<>();
        sets.add(Updates.set("goldenRecordUpdate.outcome", verdict.outcome().name()));
        sets.add(Updates.set("goldenRecordUpdate.releasedAt", now));
        if (verdict.winningSource() != null) {
            sets.add(Updates.set("goldenRecordUpdate.winningSource", verdict.winningSource()));
        }
        boolean moved = fields.decide(
                session,
                field.tenantId(),
                field.id(),
                field.version(),
                EnumSet.of(ReviewStatus.RELEASING),
                ReviewStatus.RELEASED,
                Updates.combine(sets),
                now);
        if (!moved) {
            LOG.infof("field %s: no longer RELEASING at version %d, left as it is", field.id(), field.version());
            return false;
        }
        GoldenRecordUpdate update = field.goldenRecordUpdate();
        Instant writtenAt = update == null ? null : update.writtenAt();
        audit.write(
                session,
                ReleaseService.event(AuditEventType.FIELD_RELEASED, field, Actors.INGESTION, now)
                        .release(update == null ? null : update.releaseId())
                        .jobId(jobId)
                        .detail("outcome", verdict.outcome().name())
                        .detail("winningSource", verdict.winningSource())
                        .detail("writePath", update == null ? null : update.writePath())
                        .detail("resource", update == null ? null : update.resource())
                        .detail("crosswalkId", update == null ? null : update.crosswalkId())
                        .detail("completionSource", completionSource)
                        .detail(
                                "readBackSeconds",
                                writtenAt == null
                                        ? null
                                        : Duration.between(writtenAt, now).toSeconds())
                        .detail("field", field.field())
                        .build());
        return true;
    }

    /**
     * The release's fields still {@code RELEASING}.
     *
     * @param tenantId the tenant
     * @param releaseId the release
     * @return the fields by id
     */
    public List<FieldRecommendation> releasing(String tenantId, String releaseId) {
        return fields.forRelease(tenantId, releaseId, cfg.ingestion().releaseBulkMax()).stream()
                .filter(field -> field.reviewStatus() == ReviewStatus.RELEASING)
                .toList();
    }

    /** The JobRunr job writing, from the log context, or null on an HTTP path. */
    static String currentJobId() {
        Object jobId = MDC.get(IngestionLogContext.JOB_ID);
        return jobId == null ? null : jobId.toString();
    }

    /** The service configuration, for the settings the release job reads. */
    public VendorExchangeConfig config() {
        return cfg;
    }

    /** The service metrics, for the release job's counters. */
    public VendorExchangeMetrics metrics() {
        return metrics;
    }
}
