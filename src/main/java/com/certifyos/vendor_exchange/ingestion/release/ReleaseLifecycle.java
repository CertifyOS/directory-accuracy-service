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

    private final FieldRecommendationRepository fields;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final VendorExchangeMetrics metrics;
    private final VendorExchangeConfig cfg;

    public ReleaseLifecycle(
            FieldRecommendationRepository fields,
            AuditRepository audit,
            Transactions transactions,
            VendorExchangeMetrics metrics,
            VendorExchangeConfig cfg) {
        this.fields = fields;
        this.audit = audit;
        this.transactions = transactions;
        this.metrics = metrics;
        this.cfg = cfg;
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
