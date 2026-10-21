package com.certifyos.directory_accuracy.ingestion.release;

import com.certifyos.directory_accuracy.audit.AuditEvent;
import com.certifyos.directory_accuracy.audit.AuditEventType;
import com.certifyos.directory_accuracy.audit.AuditRepository;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.jobs.IngestionJobEnqueuer;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation.GoldenRecordUpdate;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation.HistoryEntry;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus;
import com.certifyos.directory_accuracy.persistence.Ids;
import com.certifyos.directory_accuracy.persistence.Transactions;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Updates;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.conversions.Bson;
import org.jboss.logging.Logger;

/**
 * Release requests (design step 8): per field, in bulk by ids, or every approved slice field of a
 * batch; rollback of an approval; the release read. Guards run before any write; the move and its
 * {@code FIELD_*} event commit together; the release job is enqueued after the commit.
 *
 * <p>A practitioner's fields are grouped under one release while it is open (its fields still wait in
 * {@code RELEASE_REQUESTED}). Joining is a guarded write on the open release's fields inside the
 * request's transaction, so a job that claims the release at the same moment conflicts with the
 * request: one of the two retries, and a request that finds the release claimed mints a new id
 * instead of adding a field the job would never see (its re-enqueue under the same id is a no-op).
 */
@ApplicationScoped
public class ReleaseService {

    /** The fields this step releases, as the practitioner's or the location's candor slice (plan P10). */
    public static final List<String> SLICE_FIELDS = List.of("first_name", "last_name", "phone", "fax");

    /** The fields the design releases in place through api-layer (D22), not built yet: refused, and listed as skipped. */
    static final List<String> IN_PLACE_FIELDS =
            List.of("specialties", "location", "practice_name", "accepting_new_patients", "pcp_at_location");

    static final String WRITE_PATH_SLICE = "SLICE";
    static final String CORE_PRACTITIONER = "CorePractitioner";
    static final String CORE_LOCATION = "CoreLocation";
    static final String NPI = "npi";
    static final int UNPROCESSABLE = 422;

    private static final Set<String> PRACTITIONER_SLICE_FIELDS = Set.of("first_name", "last_name");
    private static final Set<ReviewStatus> RELEASABLE = Arrays.stream(ReviewStatus.values())
            .filter(status -> status.allows(ReviewStatus.Action.RELEASE))
            .collect(Collectors.toCollection(() -> EnumSet.noneOf(ReviewStatus.class)));
    private static final Logger LOG = Logger.getLogger(ReleaseService.class);

    private final ReleaseFieldRepository releaseFields;
    private final AuditRepository audit;
    private final Transactions transactions;
    private final IngestionJobEnqueuer enqueuer;
    private final RankingGuard ranking;
    private final VendorExchangeConfig cfg;
    private final Clock clock;

    public ReleaseService(
            ReleaseFieldRepository releaseFields,
            AuditRepository audit,
            Transactions transactions,
            IngestionJobEnqueuer enqueuer,
            RankingGuard ranking,
            VendorExchangeConfig cfg,
            Clock clock) {
        this.releaseFields = releaseFields;
        this.audit = audit;
        this.transactions = transactions;
        this.enqueuer = enqueuer;
        this.ranking = ranking;
        this.cfg = cfg;
        this.clock = clock;
    }

    /**
     * One release a request created or joined.
     *
     * @param releaseId the release
     * @param certifyPractitionerId its practitioner
     * @param fieldRecommendationIds the fields this call put on it, in request order
     */
    public record Requested(String releaseId, String certifyPractitionerId, List<String> fieldRecommendationIds) {
        public Requested {
            fieldRecommendationIds = List.copyOf(fieldRecommendationIds);
        }
    }

    /**
     * A bulk item that was not released.
     *
     * @param id the field recommendation id
     * @param code the stable code a single request would have answered
     */
    public record Skipped(String id, String code) {}

    /**
     * A bulk request's answer.
     *
     * @param releases one entry per release, in the order first touched
     * @param skipped the items not released, in request order
     */
    public record Bulk(List<Requested> releases, List<Skipped> skipped) {
        public Bulk {
            releases = List.copyOf(releases);
            skipped = List.copyOf(skipped);
        }
    }

    /**
     * Requests the release of one field: guards, then one transaction ({@code APPROVED | RELEASE_FAILED
     * → RELEASE_REQUESTED} with a fresh release record, {@code FIELD_RELEASE_REQUESTED}), then the job
     * enqueue.
     *
     * @param tenantId the caller's tenant
     * @param fieldRecommendationId the field
     * @param version the version the caller read
     * @param actor the caller
     * @param bulk true when the request came through the bulk endpoint
     * @return the release the field is on
     * @throws ProblemException 404 {@code FIELD_NOT_FOUND}; 409 {@code RELEASE_NOT_ALLOWED}; 422 {@code
     *     REMOVAL_NOT_RELEASED}, {@code IN_PLACE_RELEASE_UNSUPPORTED} or {@code RANKING_MISSING}; 503
     *     {@code DAL_UNAVAILABLE}
     */
    public Requested request(String tenantId, String fieldRecommendationId, long version, String actor, boolean bulk) {
        FieldRecommendation field = require(tenantId, fieldRecommendationId);
        if (!field.reviewStatus().allows(ReviewStatus.Action.RELEASE) || field.version() != version) {
            throw notAllowed(field, version);
        }
        requireSliceField(field);
        requireRanking(tenantId);
        Instant now = clock.instant();
        String practitionerId = field.practitioner().certifyPractitionerId();
        String releaseId = transactions.run(session -> requestIn(session, field, version, actor, bulk, now));
        enqueuer.release(tenantId, practitionerId, releaseId);
        LOG.infof(
                "release %s: field %s requested (practitioner %s, bulk %s)",
                releaseId, fieldRecommendationId, practitionerId, bulk);
        return new Requested(releaseId, practitionerId, List.of(fieldRecommendationId));
    }

    private String requestIn(
            ClientSession session, FieldRecommendation field, long version, String actor, boolean bulk, Instant now) {
        String tenantId = field.tenantId();
        String practitionerId = field.practitioner().certifyPractitionerId();
        String releaseId =
                joinOpenRelease(session, tenantId, practitionerId, now).orElseGet(Ids::releaseId);
        GoldenRecordUpdate update = new GoldenRecordUpdate(
                releaseId,
                actor,
                now,
                WRITE_PATH_SLICE,
                resourceOf(field.field()),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
        Bson updates = Updates.combine(
                Updates.set("goldenRecordUpdate", update.toDocument()),
                Updates.push(
                        "reviewHistory",
                        new HistoryEntry(ReviewStatus.Action.RELEASE.name(), actor, now, null).toDocument()));
        if (!staged().decide(
                        session,
                        tenantId,
                        field.id(),
                        version,
                        RELEASABLE,
                        ReviewStatus.RELEASE_REQUESTED,
                        updates,
                        now)) {
            throw notAllowed(field, version);
        }
        audit.write(
                session,
                event(AuditEventType.FIELD_RELEASE_REQUESTED, field, actor, now)
                        .release(releaseId)
                        .detail("releaseId", releaseId)
                        .detail("bulk", bulk)
                        .detail("field", field.field())
                        .build());
        return releaseId;
    }

    /**
     * The practitioner's open release, touched inside the request's transaction so a concurrent claim
     * of it conflicts with this request.
     */
    private Optional<String> joinOpenRelease(
            ClientSession session, String tenantId, String practitionerId, Instant now) {
        Optional<String> open = releaseFields.openRelease(session, tenantId, practitionerId);
        if (open.isEmpty()) {
            return open;
        }
        long touched = staged().moveAll(
                        session,
                        tenantId,
                        practitionerId,
                        open.get(),
                        ReviewStatus.RELEASE_REQUESTED,
                        ReviewStatus.RELEASE_REQUESTED,
                        null,
                        now);
        return touched > 0 ? open : Optional.empty();
    }

    /**
     * Requests the release of many fields, grouped per practitioner into one release each. An item that
     * a single request would refuse is skipped with that code; the ranking guard refuses the whole call.
     *
     * @param tenantId the caller's tenant
     * @param ids the fields, at most {@code releaseBulkMax}
     * @param versions each field's version
     * @param actor the caller
     * @return the releases and the skipped items
     * @throws ProblemException 400 {@code INVALID_REQUEST} for too many ids; 422 {@code RANKING_MISSING}
     */
    public Bulk requestAll(String tenantId, List<String> ids, Map<String, Long> versions, String actor) {
        if (ids.size() > cfg.ingestion().releaseBulkMax()) {
            throw ProblemException.badRequest(
                    "INVALID_REQUEST", "at most " + cfg.ingestion().releaseBulkMax() + " ids per call");
        }
        requireRanking(tenantId);
        return requestEach(tenantId, ids, versions == null ? Map.of() : versions, actor);
    }

    /**
     * Requests the release of every {@code APPROVED} slice field of a batch, at most {@code
     * releaseBulkMax} per call.
     *
     * @param tenantId the caller's tenant
     * @param exportBatchId the batch
     * @param actor the caller
     * @return the releases and the skipped items, which list the batch's approved in-place fields with
     *     {@code IN_PLACE_RELEASE_UNSUPPORTED} so a reviewer sees what was left out; empty when the
     *     batch has no approved field
     * @throws ProblemException 422 {@code RANKING_MISSING}
     */
    public Bulk requestBatch(String tenantId, String exportBatchId, String actor) {
        requireRanking(tenantId);
        int max = cfg.ingestion().releaseBulkMax();
        List<FieldRecommendation> approved = releaseFields.approvedInBatch(tenantId, exportBatchId, SLICE_FIELDS, max);
        Map<String, Long> versions = new LinkedHashMap<>();
        approved.forEach(field -> versions.put(field.id(), field.version()));
        Bulk requested = requestEach(tenantId, List.copyOf(versions.keySet()), versions, actor);
        List<Skipped> skipped = new ArrayList<>(requested.skipped());
        releaseFields.approvedInBatch(tenantId, exportBatchId, IN_PLACE_FIELDS, max).stream()
                .map(field -> new Skipped(field.id(), "IN_PLACE_RELEASE_UNSUPPORTED"))
                .forEach(skipped::add);
        return new Bulk(requested.releases(), skipped);
    }

    private Bulk requestEach(String tenantId, List<String> ids, Map<String, Long> versions, String actor) {
        Map<String, Requested> releases = new LinkedHashMap<>();
        List<Skipped> skipped = new ArrayList<>();
        for (String id : new LinkedHashSet<>(ids)) {
            Long version = versions.get(id);
            if (version == null) {
                skipped.add(new Skipped(id, "VERSION_REQUIRED"));
                continue;
            }
            try {
                Requested one = request(tenantId, id, version, actor, true);
                releases.merge(
                        one.releaseId(),
                        one,
                        (before, added) -> new Requested(
                                before.releaseId(),
                                before.certifyPractitionerId(),
                                concat(before.fieldRecommendationIds(), added.fieldRecommendationIds())));
            } catch (ProblemException refused) {
                skipped.add(new Skipped(id, refused.code()));
            }
        }
        return new Bulk(List.copyOf(releases.values()), skipped);
    }

    /**
     * Rolls an approval back: {@code APPROVED → PENDING} with a history entry and {@code
     * FIELD_ROLLED_BACK}. Refused once a release was requested (design: post-release reversal is out of
     * scope).
     *
     * @param tenantId the caller's tenant
     * @param fieldRecommendationId the field
     * @param version the version the caller read
     * @param reason why, required
     * @param actor the caller
     * @return the field after the move
     * @throws ProblemException 400 {@code REASON_REQUIRED}; 404 {@code FIELD_NOT_FOUND}; 409 {@code
     *     ROLLBACK_NOT_ALLOWED}
     */
    public FieldRecommendation rollback(
            String tenantId, String fieldRecommendationId, long version, String reason, String actor) {
        if (reason == null || reason.isBlank()) {
            throw ProblemException.badRequest("REASON_REQUIRED", "a rollback needs a reason");
        }
        FieldRecommendation field = require(tenantId, fieldRecommendationId);
        if (!field.reviewStatus().allows(ReviewStatus.Action.ROLLBACK) || field.version() != version) {
            throw rollbackRefused(field, version);
        }
        Instant now = clock.instant();
        transactions.run(session -> {
            Bson history = Updates.push(
                    "reviewHistory",
                    new HistoryEntry(ReviewStatus.Action.ROLLBACK.name(), actor, now, reason).toDocument());
            if (!staged().decide(
                            session,
                            tenantId,
                            field.id(),
                            version,
                            EnumSet.of(ReviewStatus.APPROVED),
                            ReviewStatus.PENDING,
                            history,
                            now)) {
                throw rollbackRefused(field, version);
            }
            audit.write(
                    session,
                    event(AuditEventType.FIELD_ROLLED_BACK, field, actor, now)
                            .detail("reason", reason)
                            .detail("version", version)
                            .detail("field", field.field())
                            .build());
            return null;
        });
        return require(tenantId, fieldRecommendationId);
    }

    /**
     * The fields of one release.
     *
     * @param tenantId the caller's tenant
     * @param releaseId the release
     * @return the fields by id
     * @throws ProblemException 404 {@code RELEASE_NOT_FOUND} when the tenant has no field on it
     */
    public List<FieldRecommendation> release(String tenantId, String releaseId) {
        List<FieldRecommendation> found =
                staged().forRelease(tenantId, releaseId, cfg.ingestion().releaseBulkMax());
        if (found.isEmpty()) {
            throw ProblemException.notFound("RELEASE_NOT_FOUND", "no such release");
        }
        return found;
    }

    /** The resource a slice field is written to. */
    static String resourceOf(String field) {
        return PRACTITIONER_SLICE_FIELDS.contains(field) ? CORE_PRACTITIONER : CORE_LOCATION;
    }

    /** The common envelope of a field event: tenant, vendor, batch, upload, field and practitioner. */
    static AuditEvent.Builder event(AuditEventType type, FieldRecommendation field, String actor, Instant now) {
        return AuditEvent.of(type, field.tenantId(), field.source())
                .batch(field.exportBatchId())
                .upload(field.sourceFile() == null ? null : field.sourceFile().uploadId())
                .field(field.id(), field.practitioner().certifyPractitionerId())
                .actor(actor)
                .occurredAt(now);
    }

    private FieldRecommendation require(String tenantId, String fieldRecommendationId) {
        return staged().find(tenantId, fieldRecommendationId)
                .orElseThrow(() -> ProblemException.notFound("FIELD_NOT_FOUND", "no such field recommendation"));
    }

    private static void requireSliceField(FieldRecommendation field) {
        if (NPI.equals(field.field())) {
            throw new ProblemException(
                    UNPROCESSABLE, "REMOVAL_NOT_RELEASED", "an NPI correction is a removal and is never released");
        }
        if (!SLICE_FIELDS.contains(field.field())) {
            // IN_PLACE_FIELDS: written in place through api-layer (design D22), a later step.
            throw new ProblemException(
                    UNPROCESSABLE,
                    "IN_PLACE_RELEASE_UNSUPPORTED",
                    field.field() + " is released in place, which is not built yet");
        }
    }

    private void requireRanking(String tenantId) {
        if (!ranking.ranksCandor(tenantId)) {
            throw new ProblemException(
                    UNPROCESSABLE, "RANKING_MISSING", "the tenant's survivorship ranking has no candor entry");
        }
    }

    private static ProblemException notAllowed(FieldRecommendation field, long version) {
        return ProblemException.conflict(
                "RELEASE_NOT_ALLOWED",
                "field is " + field.reviewStatus() + " at version " + field.version() + ", request was for version "
                        + version);
    }

    private static ProblemException rollbackRefused(FieldRecommendation field, long version) {
        return ProblemException.conflict(
                "ROLLBACK_NOT_ALLOWED",
                "field is " + field.reviewStatus() + " at version " + field.version() + ", request was for version "
                        + version);
    }

    private FieldRecommendationRepository staged() {
        return releaseFields.staged();
    }

    private static List<String> concat(List<String> first, List<String> second) {
        List<String> all = new ArrayList<>(first);
        all.addAll(second);
        return all;
    }
}
