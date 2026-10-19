package com.certifyos.vendor_exchange.ingestion.release;

import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import com.certifyos.vendor_exchange.persistence.Transactions;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.jboss.logging.Logger;

/**
 * The release side of the field state machine, after the request: claim ({@code RELEASE_REQUESTED →
 * RELEASING}), written, released, failed. Each move is a guarded update inside a transaction with its
 * audit event; nothing outside MongoDB runs inside one.
 */
@ApplicationScoped
public class ReleaseLifecycle {

    private static final Logger LOG = Logger.getLogger(ReleaseLifecycle.class);

    private final FieldRecommendationRepository fields;
    private final Transactions transactions;
    private final VendorExchangeConfig cfg;

    public ReleaseLifecycle(FieldRecommendationRepository fields, Transactions transactions, VendorExchangeConfig cfg) {
        this.fields = fields;
        this.transactions = transactions;
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

    /** The release's fields still {@code RELEASING}. */
    List<FieldRecommendation> releasing(String tenantId, String releaseId) {
        return fields.forRelease(tenantId, releaseId, cfg.ingestion().releaseBulkMax()).stream()
                .filter(field -> field.reviewStatus() == ReviewStatus.RELEASING)
                .toList();
    }

    /** The service configuration, for the settings the release job reads. */
    public VendorExchangeConfig config() {
        return cfg;
    }
}
