package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.auth.Permission;
import com.certifyos.vendor_exchange.auth.RequiresPermission;
import com.certifyos.vendor_exchange.auth.UserContext;
import com.certifyos.vendor_exchange.http.Problem;
import com.certifyos.vendor_exchange.http.ProblemException;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.BulkReleaseAccepted;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.BulkReleaseRequest;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.ReleaseAccepted;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.ReleaseRequest;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.ReleaseView;
import com.certifyos.vendor_exchange.ingestion.api.ReleaseViews.RollbackRequest;
import com.certifyos.vendor_exchange.ingestion.api.ReviewViews.FieldView;
import com.certifyos.vendor_exchange.ingestion.release.ReleaseService;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Releasing approved field recommendations to the Golden record (design step 8): per field, in bulk,
 * the release read and rollback. The tenant is the caller's; another tenant's field or release is not
 * found. A field recommendation id holds {@code |}, so the client URL-encodes it as one path segment.
 */
@Path("/v1/vendor-review")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-review")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "Invalid request (TENANT_REQUIRED, VERSION_REQUIRED, REASON_REQUIRED, INVALID_REQUEST)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description = "Caller holds no role in the tenant (TENANT_FORBIDDEN, TENANT_UNRESOLVED) or lacks the "
                + "permission (PERMISSION_DENIED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "404",
        description = "No such field or release in the caller's tenant (FIELD_NOT_FOUND, RELEASE_NOT_FOUND)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "409",
        description = "Not in a state that allows the write, or a stale version (RELEASE_NOT_ALLOWED, "
                + "ROLLBACK_NOT_ALLOWED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "422",
        description = "The field cannot be released (IN_PLACE_RELEASE_UNSUPPORTED, REMOVAL_NOT_RELEASED) or the "
                + "tenant's survivorship ranking has no candor entry (RANKING_MISSING)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description = "User or tenant configuration lookup failed (DAL_UNAVAILABLE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class VendorReleaseResource {

    private final UserContext ctx;
    private final ReleaseService releases;

    public VendorReleaseResource(UserContext ctx, ReleaseService releases) {
        this.ctx = ctx;
        this.releases = releases;
    }

    /**
     * Requests the release of one approved field.
     *
     * @param id the field recommendation, URL-encoded
     * @param request the version the caller read
     * @return 202 with the release the field is on
     */
    @POST
    @Path("/fields/{id}/release")
    @RequiresPermission(Permission.REVIEW_RELEASE)
    @Operation(
            summary = "Release one approved field",
            description = "The field must be APPROVED (or RELEASE_FAILED, for a re-release) at the given version, be "
                    + "first_name, last_name, phone or fax, and the tenant's survivorship ranking must name candor. "
                    + "Joins the practitioner's open release, or starts one, and enqueues its release job.")
    @APIResponse(
            responseCode = "202",
            description = "Release requested",
            content = @Content(schema = @Schema(implementation = ReleaseAccepted.class)))
    public Response releaseField(@PathParam("id") String id, ReleaseRequest request) {
        long version = requireVersion(request == null ? null : request.version());
        ReleaseService.Requested requested = releases.request(ctx.tenantId(), id, version, ctx.email(), false);
        return Response.accepted(new ReleaseAccepted(requested.releaseId())).build();
    }

    /**
     * Requests the release of many approved fields, one release per practitioner.
     *
     * @param request {@code ids} and {@code versions}, or {@code exportBatchId} with {@code all = true}
     * @return 202 with the releases and the items not released
     */
    @POST
    @Path("/releases")
    @RequiresPermission(Permission.REVIEW_RELEASE)
    @Operation(
            summary = "Release many approved fields",
            description = "Either ids with their versions (at most releaseBulkMax), or exportBatchId with all = true "
                    + "for every APPROVED slice field of the batch. Fields are grouped per practitioner into one "
                    + "release each; an item a single request would refuse is listed under skipped with its code.")
    @APIResponse(
            responseCode = "202",
            description = "Releases requested",
            content = @Content(schema = @Schema(implementation = BulkReleaseAccepted.class)))
    public Response releaseMany(BulkReleaseRequest request) {
        ReleaseService.Bulk bulk = byIds(request)
                ? releases.requestAll(ctx.tenantId(), request.ids(), request.versions(), ctx.email())
                : releases.requestBatch(ctx.tenantId(), request.exportBatchId(), ctx.email());
        return Response.accepted(BulkReleaseAccepted.of(bulk)).build();
    }

    /** Which form a bulk body has; refuses a body that is neither, both, or incomplete. */
    private static boolean byIds(BulkReleaseRequest request) {
        if (request == null) {
            throw invalid("a body is required");
        }
        boolean byIds = request.ids() != null;
        boolean byBatch = request.exportBatchId() != null || request.all() != null;
        if (byIds == byBatch) {
            throw invalid("send either ids with versions, or exportBatchId with all = true");
        }
        if (byIds && (request.ids().isEmpty() || request.ids().stream().anyMatch(VendorReleaseResource::isBlank))) {
            throw invalid("ids must be non-empty and every id non-blank");
        }
        if (byBatch && (isBlank(request.exportBatchId()) || !Boolean.TRUE.equals(request.all()))) {
            throw invalid("a batch release needs exportBatchId and all = true");
        }
        return byIds;
    }

    /**
     * The fields of one release.
     *
     * @param releaseId the release
     * @return the release's fields with their state and outcome
     */
    @GET
    @Path("/releases/{releaseId}")
    @RequiresPermission(Permission.REVIEW_READ)
    @Operation(
            summary = "Read a release",
            description = "Every field carrying the release id, with its review state and, once read back, its "
                    + "outcome (WON, OUTRANKED, NOT_APPLIED, UNKNOWN) and winning source.")
    public ReleaseView release(@PathParam("releaseId") String releaseId) {
        return new ReleaseView(
                releaseId,
                releases.release(ctx.tenantId(), releaseId).stream()
                        .map(FieldView::of)
                        .toList());
    }

    /**
     * Rolls an approval back to pending.
     *
     * @param id the field recommendation, URL-encoded
     * @param request the version the caller read and the reason
     * @return the field after the move
     */
    @POST
    @Path("/fields/{id}/rollback")
    @RequiresPermission(Permission.REVIEW_RELEASE)
    @Operation(
            summary = "Roll back an approval",
            description = "APPROVED to PENDING at the given version, with the reason in the history. Refused once a "
                    + "release was requested.")
    public FieldView rollback(@PathParam("id") String id, RollbackRequest request) {
        if (request == null) {
            throw ProblemException.badRequest("VERSION_REQUIRED", "the version the caller read is required");
        }
        long version = requireVersion(request.version());
        return FieldView.of(releases.rollback(ctx.tenantId(), id, version, request.reason(), ctx.email()));
    }

    private static long requireVersion(Long version) {
        if (version == null) {
            throw ProblemException.badRequest("VERSION_REQUIRED", "the version the caller read is required");
        }
        return version;
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private static ProblemException invalid(String detail) {
        return ProblemException.badRequest("INVALID_REQUEST", detail);
    }
}
