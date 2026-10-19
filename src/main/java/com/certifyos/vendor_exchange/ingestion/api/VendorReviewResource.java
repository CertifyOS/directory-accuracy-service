package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.audit.AuditEvent;
import com.certifyos.vendor_exchange.audit.AuditRepository;
import com.certifyos.vendor_exchange.auth.Permission;
import com.certifyos.vendor_exchange.auth.RequiresPermission;
import com.certifyos.vendor_exchange.auth.UserContext;
import com.certifyos.vendor_exchange.config.VendorExchangeConfig;
import com.certifyos.vendor_exchange.export.api.AuditViews.EventView;
import com.certifyos.vendor_exchange.export.api.Items;
import com.certifyos.vendor_exchange.export.batch.BatchOperations;
import com.certifyos.vendor_exchange.http.Problem;
import com.certifyos.vendor_exchange.http.ProblemException;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.BulkDecisionBody;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.BulkDecisionView;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.DecisionBody;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.DecisionResultView;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.FieldView;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.PractitionerFieldsView;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.SummaryView;
import com.certifyos.vendor_exchange.ingestion.review.ReviewService;
import com.certifyos.vendor_exchange.ingestion.review.ReviewService.DecisionRequest;
import com.certifyos.vendor_exchange.ingestion.staging.CandorRecsMapping;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendation;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository;
import com.certifyos.vendor_exchange.ingestion.staging.FieldRecommendationRepository.ListQuery;
import com.certifyos.vendor_exchange.ingestion.staging.ReviewStatus;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * The reviewer's reads of the vendor's field recommendations (the keyset list, one document with
 * its trail, the counts for the filter chips, one practitioner's documents by id or NPI) and the
 * decisions on them, one or many (design step 6). The
 * tenant is the one the {@code tenant-id} header named and the caller holds a role in, never a
 * query parameter; another tenant's document reads as not found. A field recommendation id holds
 * {@code |}, so the client URL-encodes it as one path segment.
 */
@Path("/v1/vendor-review")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-review")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "Invalid request (TENANT_REQUIRED, INVALID_REQUEST, FIELD_UNKNOWN, VERSION_REQUIRED, "
                + "COMMENT_REQUIRED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description = "Caller holds no role in the tenant (TENANT_FORBIDDEN, TENANT_UNRESOLVED) or lacks the "
                + "permission (PERMISSION_DENIED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description = "User lookup failed (DAL_UNAVAILABLE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class VendorReviewResource {

    /** The page size when the request names none. */
    static final int LIST_DEFAULT = 50;

    /** The smallest page a request may ask for (design: {@code limit} 10 to 200). */
    static final int LIST_MIN = 10;

    /** The most events one field trail returns; a field gathers a handful in its life. */
    static final int TRAIL_LIMIT = 500;

    /** The most documents of one practitioner in one batch: four practitioner fields, six per location. */
    static final int VIEW_LIMIT = 500;

    private final UserContext ctx;
    private final FieldRecommendationRepository recommendations;
    private final AuditRepository audit;
    private final BatchOperations batchOperations;
    private final VendorExchangeConfig config;
    private final ReviewService reviews;

    public VendorReviewResource(
            UserContext ctx,
            FieldRecommendationRepository recommendations,
            AuditRepository audit,
            BatchOperations batchOperations,
            VendorExchangeConfig config,
            ReviewService reviews) {
        this.ctx = ctx;
        this.recommendations = recommendations;
        this.audit = audit;
        this.batchOperations = batchOperations;
        this.config = config;
        this.reviews = reviews;
    }

    /**
     * One page of the review list.
     *
     * @param field one field, or none for every field
     * @param reviewStatus the states to list, repeatable; {@code PENDING} when absent
     * @param npi one practitioner's NPI, or none
     * @param exportBatchId one batch, or none
     * @param after the previous page's {@code next}, or none for the first page
     * @param limit the page size, 10 to {@code reviewPageLimitMax}, default 50
     * @return the page, newest staged first, and the cursor of the next page
     */
    @GET
    @Path("/fields")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "List field recommendations",
            description = "One page of the tenant's field recommendations, newest staged first, filtered by field, "
                    + "review status (repeatable, default PENDING), NPI and export batch. Pages by keyset: pass the "
                    + "previous page's next as after; a row decided between two pages never shifts another across "
                    + "the cursor. limit is 10 to the configured maximum (200), default 50.")
    public Page<FieldView> list(
            @QueryParam("field") String field,
            @QueryParam("reviewStatus") List<String> reviewStatus,
            @QueryParam("npi") String npi,
            @QueryParam("exportBatchId") String exportBatchId,
            @QueryParam("after") String after,
            @QueryParam("limit") Integer limit) {
        int size = limit == null ? LIST_DEFAULT : limit;
        int max = config.ingestion().reviewPageLimitMax();
        if (size < LIST_MIN || size > max) {
            throw ProblemException.badRequest("INVALID_REQUEST", "limit must be " + LIST_MIN + ".." + max);
        }
        ListQuery query = new ListQuery(
                ctx.tenantId(),
                knownField(blankToNull(field)),
                statuses(reviewStatus),
                blankToNull(npi),
                blankToNull(exportBatchId),
                decodeCursor(blankToNull(after)),
                size);
        List<FieldRecommendation> fetched = list(query);
        if (fetched.size() <= size) {
            return new Page<>(FieldView.of(fetched), null);
        }
        List<FieldRecommendation> page = fetched.subList(0, size);
        return new Page<>(
                FieldView.of(page), encodeCursor(FieldRecommendationRepository.cursorAfter(page.get(size - 1))));
    }

    private List<FieldRecommendation> list(ListQuery query) {
        try {
            return recommendations.list(query);
        } catch (IllegalArgumentException notACursor) {
            throw ProblemException.badRequest("INVALID_REQUEST", "after is not a cursor this endpoint issued");
        }
    }

    /**
     * Counts per field and review state.
     *
     * @param exportBatchId one batch, or none for every batch of the tenant
     * @return the counts, and the batch's state when one was named
     */
    @GET
    @Path("/fields/summary")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "Count field recommendations by field and status",
            description = "Counts per field and review status for the filter chips. With exportBatchId the answer "
                    + "also carries the batch's state, so a SUPERSEDED cycle shows on the review screen.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (BATCH_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public SummaryView summary(@QueryParam("exportBatchId") String exportBatchId) {
        String batchId = blankToNull(exportBatchId);
        String state = batchId == null
                ? null
                : batchOperations.require(ctx.tenantId(), batchId).state().name();
        return SummaryView.of(recommendations.summary(ctx.tenantId(), batchId), state);
    }

    /**
     * One field recommendation.
     *
     * @param id the field recommendation id, URL-encoded
     * @return the document with its evidence and history
     */
    @GET
    @Path("/fields/{id}")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "Read a field recommendation",
            description = "The stored document with the vendor's evidence, the review history and the version a "
                    + "decision must send back.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (FIELD_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public FieldView field(@PathParam("id") String id) {
        return FieldView.of(require(id));
    }

    /**
     * A field recommendation's audit trail.
     *
     * @param id the field recommendation id, URL-encoded
     * @return its field events in the order they were written
     */
    @GET
    @Path("/fields/{id}/events")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "List a field recommendation's audit events",
            description = "The field's decision and release events from vendor_review_events, oldest first. The "
                    + "trail outlives the document: a trail that exists is returned even when the document is gone.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (FIELD_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public Items<EventView> events(@PathParam("id") String id) {
        List<AuditEvent> trail = audit.trailForField(ctx.tenantId(), id, TRAIL_LIMIT);
        if (trail.isEmpty()) {
            require(id);
        }
        return new Items<>(trail.stream().map(EventView::of).toList());
    }

    /**
     * One practitioner's documents in one batch.
     *
     * @param certifyPractitionerId the practitioner
     * @param exportBatchId the batch, or none for the practitioner's latest
     * @return the practitioner and its documents
     */
    @GET
    @Path("/practitioners/{certifyPractitionerId}")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "Read one practitioner's field recommendations",
            description = "Every document of one practitioner in one batch, practitioner-level fields first, then "
                    + "by location. Without exportBatchId, the batch of the practitioner's newest staged document.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (PRACTITIONER_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public PractitionerFieldsView practitioner(
            @PathParam("certifyPractitionerId") String certifyPractitionerId,
            @QueryParam("exportBatchId") String exportBatchId) {
        return grouped(recommendations.forPractitioner(
                ctx.tenantId(), certifyPractitionerId, blankToNull(exportBatchId), VIEW_LIMIT));
    }

    /**
     * One NPI's documents in one batch.
     *
     * @param npi the NPI
     * @param exportBatchId the batch, or none for the NPI's latest
     * @return the practitioner and its documents
     */
    @GET
    @Path("/npis/{npi}")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "Read one NPI's field recommendations",
            description = "The practitioner view looked up by NPI, for screens that start from an NPI. Without "
                    + "exportBatchId, the batch of the NPI's newest staged document.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (PRACTITIONER_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public PractitionerFieldsView npi(@PathParam("npi") String npi, @QueryParam("exportBatchId") String exportBatchId) {
        return grouped(recommendations.forNpi(ctx.tenantId(), npi, blankToNull(exportBatchId), VIEW_LIMIT));
    }

    /**
     * Decides one field recommendation.
     *
     * @param id the field recommendation id, URL-encoded
     * @param body the action, the comment and the version read
     * @return the document after the decision
     */
    @POST
    @Path("/fields/{id}/decision")
    @RequiresPermission(Permission.REVIEW_DECIDE)
    @Operation(
            summary = "Approve, reject or skip a field recommendation",
            description = "action is APPROVE, REJECT or SKIP; comment is required on REJECT; version is the one the "
                    + "reviewer read. APPROVE and REJECT go from PENDING or SKIPPED, SKIP from PENDING; APPROVED and "
                    + "REJECTED are final. One guarded update with its audit event; a state that does not allow the "
                    + "action, a stale version or a lost race answers 409 with the current state and who decided "
                    + "last.")
    @APIResponse(
            responseCode = "404",
            description = "Nothing of that id in the caller's tenant (FIELD_NOT_FOUND)",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    @APIResponse(
            responseCode = "409",
            description = "The document's state does not allow the decision or its version moved "
                    + "(DECISION_CONFLICT); the detail is JSON of id, reviewStatus, version and lastDecisionBy",
            content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
    public FieldView decide(@PathParam("id") String id, DecisionBody body) {
        DecisionRequest request = body == null
                ? new DecisionRequest(id, null, null, null)
                : new DecisionRequest(id, ReviewService.actionOf(body.action()), body.comment(), body.version());
        return FieldView.of(reviews.decide(ctx.tenantId(), request, ctx.email()));
    }

    /**
     * Decides many field recommendations, each on its own.
     *
     * @param body up to {@code reviewPageLimitMax} decisions
     * @return 207 with one result per decision, in request order
     */
    @POST
    @Path("/decisions")
    @RequiresPermission(Permission.REVIEW_DECIDE)
    @Operation(
            summary = "Decide many field recommendations",
            description = "Up to the configured maximum (200) decisions, each under the single decision's rule and "
                    + "answered on its own: status 200 with the document, 409 with the current document, 400 or 404 "
                    + "with the code. More than the maximum, or none, is 400 before anything is written.")
    @APIResponse(
            responseCode = "207",
            description = "Per-item results",
            content = @Content(schema = @Schema(implementation = BulkDecisionView.class)))
    public Response decideAll(BulkDecisionBody body) {
        List<DecisionRequest> requests = body == null || body.decisions() == null
                ? List.of()
                : body.decisions().stream()
                        .map(item -> item == null
                                ? null
                                : new DecisionRequest(
                                        item.id(),
                                        ReviewService.actionOf(item.action()),
                                        item.comment(),
                                        item.version()))
                        .toList();
        List<DecisionResultView> results = reviews.decideAll(ctx.tenantId(), requests, ctx.email()).stream()
                .map(result -> new DecisionResultView(
                        result.id(), result.status(), result.code(), FieldView.of(result.document())))
                .toList();
        return Response.status(207).entity(new BulkDecisionView(results)).build();
    }

    private static PractitionerFieldsView grouped(List<FieldRecommendation> docs) {
        if (docs.isEmpty()) {
            throw ProblemException.notFound(
                    "PRACTITIONER_NOT_FOUND", "no field recommendations for that practitioner in the caller's tenant");
        }
        // The practitioner as the newest staged document carries it (ties broken by id).
        FieldRecommendation newest = docs.stream()
                .max(Comparator.comparing(
                                FieldRecommendation::stagedAt, Comparator.nullsFirst(Comparator.naturalOrder()))
                        .thenComparing(FieldRecommendation::id))
                .orElseThrow();
        return new PractitionerFieldsView(newest.practitioner(), newest.exportBatchId(), FieldView.of(docs));
    }

    private FieldRecommendation require(String id) {
        return recommendations
                .find(ctx.tenantId(), id)
                .orElseThrow(() -> ProblemException.notFound(
                        "FIELD_NOT_FOUND", "no field recommendation of that id in the caller's tenant"));
    }

    private static String knownField(String field) {
        if (field == null) {
            return null;
        }
        try {
            CandorRecsMapping.levelOf(field);
            return field;
        } catch (IllegalArgumentException unknown) {
            throw ProblemException.badRequest("FIELD_UNKNOWN", "field must be one of " + fieldNames());
        }
    }

    private static List<String> fieldNames() {
        return java.util.stream.Stream.concat(
                        CandorRecsMapping.PRACTITIONER_FIELDS.stream(), CandorRecsMapping.LOCATION_FIELDS.stream())
                .toList();
    }

    private static Set<ReviewStatus> statuses(List<String> names) {
        Set<ReviewStatus> statuses = EnumSet.noneOf(ReviewStatus.class);
        if (names != null) {
            for (String name : names) {
                if (name == null || name.isBlank()) {
                    continue;
                }
                try {
                    statuses.add(ReviewStatus.valueOf(name.strip()));
                } catch (IllegalArgumentException unknown) {
                    throw ProblemException.badRequest(
                            "INVALID_REQUEST", "reviewStatus must be one of " + List.of(ReviewStatus.values()));
                }
            }
        }
        return statuses.isEmpty() ? EnumSet.of(ReviewStatus.PENDING) : statuses;
    }

    /** The repository cursor holds {@code |}; the page hands it out base64url-encoded, opaque to the client. */
    static String encodeCursor(String cursor) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(cursor.getBytes(StandardCharsets.UTF_8));
    }

    static String decodeCursor(String after) {
        if (after == null) {
            return null;
        }
        try {
            return new String(Base64.getUrlDecoder().decode(after), StandardCharsets.UTF_8);
        } catch (IllegalArgumentException notBase64) {
            throw ProblemException.badRequest("INVALID_REQUEST", "after is not a cursor this endpoint issued");
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value.strip();
    }
}
