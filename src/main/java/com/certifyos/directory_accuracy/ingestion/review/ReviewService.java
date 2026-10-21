package com.certifyos.directory_accuracy.ingestion.review;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.certifyos.directory_accuracy.http.ProblemException;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendation;
import com.certifyos.directory_accuracy.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.directory_accuracy.ingestion.staging.ReviewStatus.Action;
import com.certifyos.directory_accuracy.metrics.VendorExchangeMetrics;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jboss.logging.Logger;

/**
 * Reviewer decisions on field recommendations (design step 6, D12); the states are
 * {@code ReviewStatus}'s. A decision is {@code APPROVE},
 * {@code REJECT} (comment required) or {@code SKIP}, sent with the {@code version} the reviewer read.
 * It is one guarded update on {@code version} and {@code reviewStatus} with its {@code FIELD_*} event
 * in one transaction ({@link DecisionWriter}); a decision the current state does not allow, a stale
 * version, or a lost race with another reviewer is 409 {@code DECISION_CONFLICT}, whose detail names
 * the current state and who decided last, so a retried request sees its own earlier decision. Bulk
 * decisions apply the same rule per item and never fail as a whole for one item.
 */
@ApplicationScoped
public class ReviewService {

    private static final Logger LOG = Logger.getLogger(ReviewService.class);

    /**
     * One decision as the endpoint received it.
     *
     * @param id the field recommendation
     * @param action the action, or null when the request named none or an unknown one
     * @param comment the reviewer's comment, required on {@code REJECT}
     * @param version the version the reviewer read, required
     */
    public record DecisionRequest(String id, Action action, String comment, Long version) {}

    /**
     * One item's outcome in a bulk decision.
     *
     * @param id the field recommendation
     * @param status 200, 400, 404 or 409, as the single decision would have answered (500 when the
     *     item failed for a reason of ours)
     * @param code the problem code when not 200
     * @param document the current document on 200 and 409, else null
     */
    public record DecisionResult(String id, int status, String code, FieldRecommendation document) {}

    private final FieldRecommendationRepository recommendations;
    private final DecisionWriter writer;
    private final VendorExchangeMetrics metrics;
    private final VendorExchangeConfig config;
    private final ObjectMapper mapper;
    private final Clock clock;

    public ReviewService(
            FieldRecommendationRepository recommendations,
            DecisionWriter writer,
            VendorExchangeMetrics metrics,
            VendorExchangeConfig config,
            ObjectMapper mapper,
            Clock clock) {
        this.recommendations = recommendations;
        this.writer = writer;
        this.metrics = metrics;
        this.config = config;
        this.mapper = mapper;
        this.clock = clock;
    }

    /**
     * Records one decision.
     *
     * @param tenantId the caller's tenant
     * @param request the decision
     * @param actor the reviewer, written on the history entry and as the event's actor
     * @return the document after the decision, as stored
     * @throws ProblemException 400 {@code INVALID_REQUEST}, {@code VERSION_REQUIRED} or
     *     {@code COMMENT_REQUIRED}; 404 {@code FIELD_NOT_FOUND} for a missing document or another
     *     tenant's; 409 {@code DECISION_CONFLICT}
     */
    public FieldRecommendation decide(String tenantId, DecisionRequest request, String actor) {
        validate(request);
        FieldRecommendation current = require(tenantId, request.id());
        if (current.reviewStatus() == null
                || !current.reviewStatus().allows(request.action())
                || current.version() != request.version()) {
            throw conflict(current);
        }
        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        String comment = blankToNull(request.comment());
        if (!writer.record(current, request.action(), comment, actor, now)) {
            throw conflict(require(tenantId, request.id()));
        }
        metrics.reviewDecision(tenantId, request.action().name());
        return require(tenantId, request.id());
    }

    /**
     * Records many decisions, each as {@link #decide} would, in request order.
     *
     * @param tenantId the caller's tenant
     * @param requests one to {@code reviewPageLimitMax} decisions
     * @param actor the reviewer
     * @return one result per request, in request order
     * @throws ProblemException 400 {@code INVALID_REQUEST} for no decisions or too many, before any
     *     is written
     */
    public List<DecisionResult> decideAll(String tenantId, List<DecisionRequest> requests, String actor) {
        int max = config.ingestion().reviewPageLimitMax();
        if (requests == null || requests.isEmpty() || requests.size() > max) {
            throw ProblemException.badRequest("INVALID_REQUEST", "decisions must hold 1.." + max + " items");
        }
        List<DecisionResult> results = new ArrayList<>(requests.size());
        for (DecisionRequest request : requests) {
            results.add(decideOne(tenantId, request, actor));
        }
        return results;
    }

    private DecisionResult decideOne(String tenantId, DecisionRequest request, String actor) {
        if (request == null) {
            return new DecisionResult(null, 400, "INVALID_REQUEST", null);
        }
        String id = request.id();
        try {
            return new DecisionResult(id, 200, null, decide(tenantId, request, actor));
        } catch (ProblemException refused) {
            FieldRecommendation current = refused.status() == 409 && id != null
                    ? recommendations.find(tenantId, id).orElse(null)
                    : null;
            return new DecisionResult(id, refused.status(), refused.code(), current);
        } catch (RuntimeException failure) {
            LOG.errorf(failure, "bulk decision on %s in tenant %s failed", id, tenantId);
            return new DecisionResult(id, 500, "INTERNAL", null);
        }
    }

    private static void validate(DecisionRequest request) {
        if (request == null || request.id() == null || request.id().isBlank()) {
            throw ProblemException.badRequest("INVALID_REQUEST", "id is required");
        }
        Action action = request.action();
        if (action != Action.APPROVE && action != Action.REJECT && action != Action.SKIP) {
            throw ProblemException.badRequest("INVALID_REQUEST", "action must be APPROVE, REJECT or SKIP");
        }
        if (request.version() == null) {
            throw ProblemException.badRequest("VERSION_REQUIRED", "version is required");
        }
        if (action == Action.REJECT && blankToNull(request.comment()) == null) {
            throw ProblemException.badRequest("COMMENT_REQUIRED", "a rejection needs a comment");
        }
    }

    private FieldRecommendation require(String tenantId, String id) {
        return recommendations
                .find(tenantId, id)
                .orElseThrow(() -> ProblemException.notFound(
                        "FIELD_NOT_FOUND", "no field recommendation of that id in the caller's tenant"));
    }

    /**
     * 409 whose detail is JSON of {@code {id, reviewStatus, version, lastDecisionBy}}: the UI reads the
     * row and sees whose decision stands, its own included.
     */
    private ProblemException conflict(FieldRecommendation current) {
        metrics.reviewConflict(current.tenantId());
        Map<String, Object> detail = new LinkedHashMap<>();
        detail.put("id", current.id());
        detail.put(
                "reviewStatus",
                current.reviewStatus() == null ? null : current.reviewStatus().name());
        detail.put("version", current.version());
        List<FieldRecommendation.HistoryEntry> history = current.reviewHistory();
        detail.put(
                "lastDecisionBy",
                history.isEmpty() ? null : history.get(history.size() - 1).by());
        try {
            return ProblemException.conflict("DECISION_CONFLICT", mapper.writeValueAsString(detail));
        } catch (JsonProcessingException unwritable) {
            throw new IllegalStateException("conflict detail is not writable as JSON", unwritable);
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.strip();
    }

    /** Parses an action name, null for a missing or unknown one (the decision then answers 400). */
    public static Action actionOf(String name) {
        if (name == null) {
            return null;
        }
        try {
            return Action.valueOf(name.strip());
        } catch (IllegalArgumentException unknown) {
            return null;
        }
    }
}
