package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.auth.Permission;
import com.certifyos.directory_accuracy.auth.RequiresPermission;
import com.certifyos.directory_accuracy.auth.UserContext;
import com.certifyos.directory_accuracy.export.api.AuditViews.EventView;
import com.certifyos.directory_accuracy.export.api.BatchViews.BatchView;
import com.certifyos.directory_accuracy.export.api.BatchViews.ReasonRequest;
import com.certifyos.directory_accuracy.export.api.BatchViews.RetryResponse;
import com.certifyos.directory_accuracy.export.api.BatchViews.SupersedeResponse;
import com.certifyos.directory_accuracy.export.batch.BatchOperations;
import com.certifyos.directory_accuracy.export.batch.ExportBatch;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.http.ProblemException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;

/**
 * Operator batch reads and writes (design steps 7 and 8). Every read is scoped to the tenant the
 * {@code tenant-id} header named: another tenant's batch reads as not found. Views leave out the
 * internal job ids.
 */
@Path("/v1/vendor-exports")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-exports")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "Invalid request (TENANT_REQUIRED, PERIOD_INVALID, REASON_REQUIRED, INVALID_REQUEST)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description = "Caller holds no role in the tenant (TENANT_FORBIDDEN, TENANT_UNRESOLVED), lacks the permission "
                + "(PERMISSION_DENIED) or named another tenant (TENANT_MISMATCH)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "404",
        description = "No such batch in the caller's tenant (BATCH_NOT_FOUND)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "409",
        description =
                "Not in a state that allows the write (RETRY_NOT_ALLOWED, SUPERSEDE_NOT_ALLOWED, BATCH_EXISTS, VERSION_STALE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description = "User lookup failed (DAL_UNAVAILABLE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class VendorExportsResource {

    private final UserContext ctx;
    private final BatchOperations operations;

    public VendorExportsResource(UserContext ctx, BatchOperations operations) {
        this.ctx = ctx;
        this.operations = operations;
    }

    /**
     * Lists the tenant's export batches, newest first.
     *
     * @param tenantId optional; must equal the header tenant when given
     * @param period optional {@code yyyy-MM}
     * @return the batches
     */
    @GET
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "List export batches",
            description = "Export batches of the tenant named by the tenant-id header, newest first, at most 100; "
                    + "period narrows to one month.")
    public Items<BatchView> batches(@QueryParam("tenantId") String tenantId, @QueryParam("period") String period) {
        if (tenantId != null && !tenantId.equals(ctx.tenantId())) {
            throw new ProblemException(
                    403, "TENANT_MISMATCH", "tenantId names a tenant other than the tenant-id header");
        }
        return new Items<>(operations.list(ctx.tenantId(), period).stream()
                .map(BatchView::of)
                .toList());
    }

    /**
     * One batch.
     *
     * @param exportBatchId the batch
     * @return the batch
     */
    @GET
    @Path("/{exportBatchId}")
    @RequiresPermission(Permission.READ)
    @Operation(summary = "Read an export batch", description = "The batch document minus internal job ids.")
    public BatchView batch(@PathParam("exportBatchId") String exportBatchId) {
        return BatchView.of(operations.require(ctx.tenantId(), exportBatchId));
    }

    /**
     * A batch's audit trail.
     *
     * @param exportBatchId the batch
     * @return the events in the order they were written
     */
    @GET
    @Path("/{exportBatchId}/events")
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "Read a batch's audit trail",
            description = "Every audit event written for the batch, oldest first, at most 500: the design's envelope "
                    + "with its detail. jobId links an event to the JobRunr dashboard entry.")
    public Items<EventView> events(@PathParam("exportBatchId") String exportBatchId) {
        ExportBatch batch = operations.require(ctx.tenantId(), exportBatchId);
        return new Items<>(operations.trail(batch).stream().map(EventView::of).toList());
    }

    /**
     * Retries a failed batch.
     *
     * @param exportBatchId the batch
     * @param request the reason
     * @return 202 with the batch id, the new attempt and the job
     */
    @POST
    @Path("/{exportBatchId}/retry")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Retry a failed export batch",
            description = "From FAILED only. attempt + 1, back to SCHEDULED with an egress request job, which cancels "
                    + "the prior attempt first and asks egress to reuse its frozen practitioner list.")
    @APIResponse(
            responseCode = "202",
            description = "Retry accepted",
            content = @Content(schema = @Schema(implementation = RetryResponse.class)))
    public Response retry(@PathParam("exportBatchId") String exportBatchId, ReasonRequest request) {
        ExportBatch batch = operations.require(ctx.tenantId(), exportBatchId);
        BatchOperations.Retried retried =
                operations.retry(batch, request == null ? null : request.reason(), ctx.email());
        return Response.accepted(new RetryResponse(
                        retried.batch().id(),
                        retried.batch().attempt(),
                        retried.jobId().toString()))
                .build();
    }

    /**
     * Supersedes a delivered batch with a new sequence for the same period.
     *
     * @param exportBatchId the batch
     * @param request the reason
     * @return 201 with the new batch id and its egress request job
     */
    @POST
    @Path("/{exportBatchId}/supersede")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Supersede a delivered export batch",
            description = "From DELIVERED only. Creates seq + 1 in SCHEDULED with the schedule's current saved filter, "
                    + "which egress evaluates afresh; "
                    + "marks this batch SUPERSEDED and points the schedule's lastBatchId at the new one. The old "
                    + "file stays in the vendor folder.")
    @APIResponse(
            responseCode = "201",
            description = "New batch created",
            content = @Content(schema = @Schema(implementation = SupersedeResponse.class)))
    public Response supersede(@PathParam("exportBatchId") String exportBatchId, ReasonRequest request) {
        ExportBatch batch = operations.require(ctx.tenantId(), exportBatchId);
        BatchOperations.Superseded superseded =
                operations.supersede(batch, request == null ? null : request.reason(), ctx.email());
        return Response.status(201)
                .entity(new SupersedeResponse(
                        superseded.newBatch().id(), superseded.jobId().toString()))
                .build();
    }
}
