package com.certifyos.vendor_exchange.ingestion.api;

import com.certifyos.vendor_exchange.auth.Permission;
import com.certifyos.vendor_exchange.auth.RequiresPermission;
import com.certifyos.vendor_exchange.auth.UserContext;
import com.certifyos.vendor_exchange.export.api.Items;
import com.certifyos.vendor_exchange.export.batch.BatchOperations;
import com.certifyos.vendor_exchange.export.batch.ExportBatch;
import com.certifyos.vendor_exchange.http.Problem;
import com.certifyos.vendor_exchange.http.ProblemException;
import com.certifyos.vendor_exchange.ingestion.api.InboundViews.AttachRequest;
import com.certifyos.vendor_exchange.ingestion.api.InboundViews.AttachResponse;
import com.certifyos.vendor_exchange.ingestion.api.InboundViews.DiscardRequest;
import com.certifyos.vendor_exchange.ingestion.api.InboundViews.InboundFileView;
import com.certifyos.vendor_exchange.ingestion.api.InboundViews.UnmatchedUploadView;
import com.certifyos.vendor_exchange.ingestion.inbound.InboundFileRepository;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedOperations;
import com.certifyos.vendor_exchange.ingestion.inbound.UnmatchedUploadRepository;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.time.Clock;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Operator reads of the ingestion lane and the resolution of unmatched uploads. Every read is scoped
 * to the tenant the {@code tenant-id} header named: another tenant's batch or folder reads as not
 * found. Views leave out the internal job ids. An upload id holds {@code |} and {@code /}, so the
 * client URL-encodes it as one path segment.
 */
@Path("/v1/vendor-ingestion")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-ingestion")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "Invalid request (TENANT_REQUIRED, REASON_REQUIRED, INVALID_REQUEST)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description = "Caller holds no role in the tenant (TENANT_FORBIDDEN, TENANT_UNRESOLVED) or lacks the "
                + "permission (PERMISSION_DENIED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "404",
        description = "No such batch or unmatched upload in the caller's tenant (BATCH_NOT_FOUND, UNMATCHED_NOT_FOUND)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "409",
        description = "Not in a state that allows the write (ALREADY_RESOLVED, ATTACH_NOT_ALLOWED, ALREADY_CLAIMED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description = "User lookup failed (DAL_UNAVAILABLE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class VendorIngestionOpsResource {

    static final int UNMATCHED_DEFAULT = 50;
    static final int UNMATCHED_MAX = 200;

    private final UserContext ctx;
    private final BatchOperations batchOperations;
    private final InboundFileRepository inbound;
    private final UnmatchedUploadRepository unmatched;
    private final UnmatchedOperations operations;
    private final Clock clock;

    public VendorIngestionOpsResource(
            UserContext ctx,
            BatchOperations batchOperations,
            InboundFileRepository inbound,
            UnmatchedUploadRepository unmatched,
            UnmatchedOperations operations,
            Clock clock) {
        this.ctx = ctx;
        this.batchOperations = batchOperations;
        this.inbound = inbound;
        this.unmatched = unmatched;
        this.operations = operations;
        this.clock = clock;
    }

    /**
     * The uploads recorded on one batch.
     *
     * @param exportBatchId the batch
     * @return the entries in claim order
     */
    @GET
    @Path("/batches/{exportBatchId}/inbound-files")
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "List a batch's inbound files",
            description = "Every vendor upload claimed on the batch, the first one first, with what each stage of "
                    + "ingestion wrote; internal job ids are left out.")
    public Items<InboundFileView> inboundFiles(@PathParam("exportBatchId") String exportBatchId) {
        ExportBatch batch = batchOperations.require(ctx.tenantId(), exportBatchId);
        return new Items<>(
                inbound.entriesFor(batch.id()).stream().map(InboundFileView::of).toList());
    }

    /**
     * The tenant's open unmatched uploads.
     *
     * @param limit page size, default 50, at most 200
     * @return the open uploads in the caller's tenant folder, oldest first
     */
    @GET
    @Path("/unmatched-uploads")
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "List unmatched uploads",
            description = "Open uploads in the caller's tenant folder that no batch took, oldest first, with the "
                    + "reason. limit is 1 to 200, default 50.")
    public Items<UnmatchedUploadView> unmatchedUploads(@QueryParam("limit") Integer limit) {
        int size = limit == null ? UNMATCHED_DEFAULT : limit;
        if (size < 1 || size > UNMATCHED_MAX) {
            throw ProblemException.badRequest("INVALID_REQUEST", "limit must be 1.." + UNMATCHED_MAX);
        }
        return new Items<>(unmatched.listOpen(ctx.tenantId(), size).stream()
                .map(UnmatchedUploadView::of)
                .toList());
    }

    /**
     * Attaches an unmatched upload to a delivered batch.
     *
     * @param uploadId the upload, URL-encoded
     * @param request the batch and the reason
     * @return 202 with the entry's state and the request-ingestion job
     */
    @POST
    @Path("/unmatched-uploads/{uploadId}/attach")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Attach an unmatched upload to a batch",
            description = "The batch must be the caller's and DELIVERED or SUPERSEDED. Claims the upload on it as the "
                    + "storage event would have, with the caller as actor, resolves the row ATTACHED and starts "
                    + "ingestion. A batch that already has an upload logs this one as a duplicate.")
    @APIResponse(
            responseCode = "202",
            description = "Attached",
            content = @Content(schema = @Schema(implementation = AttachResponse.class)))
    public Response attach(@PathParam("uploadId") String uploadId, AttachRequest request) {
        UnmatchedOperations.Attached attached = operations.attach(
                ctx.tenantId(),
                uploadId,
                request == null ? null : request.exportBatchId(),
                request == null ? null : request.reason(),
                ctx.email(),
                clock.instant());
        return Response.accepted(AttachResponse.of(attached)).build();
    }

    /**
     * Discards an unmatched upload.
     *
     * @param uploadId the upload, URL-encoded
     * @param request the reason
     * @return the resolved upload
     */
    @POST
    @Path("/unmatched-uploads/{uploadId}/discard")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Discard an unmatched upload",
            description = "Resolves the row DISCARDED with the reason. The object stays in the vendor bucket.")
    public UnmatchedUploadView discard(@PathParam("uploadId") String uploadId, DiscardRequest request) {
        return UnmatchedUploadView.of(operations.discard(
                ctx.tenantId(), uploadId, request == null ? null : request.reason(), ctx.email(), clock.instant()));
    }
}
