package com.certifyos.directory_accuracy.export.schedule;

import com.certifyos.directory_accuracy.auth.Permission;
import com.certifyos.directory_accuracy.auth.RequiresPermission;
import com.certifyos.directory_accuracy.auth.UserContext;
import com.certifyos.directory_accuracy.export.api.AuditViews.EventView;
import com.certifyos.directory_accuracy.export.api.Items;
import com.certifyos.directory_accuracy.export.batch.BatchLifecycle;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.DisableRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.EnableRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.PreviewRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.PreviewResponse;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.PutRequest;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.RunNowResponse;
import com.certifyos.directory_accuracy.export.schedule.ScheduleRequests.ScheduleView;
import com.certifyos.directory_accuracy.http.Problem;
import com.certifyos.directory_accuracy.http.ProblemException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
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
 * Schedule endpoints (design Contracts step 0). The tenant in the path must be the tenant the
 * caller's {@code tenant-id} header named, so a member of one tenant cannot write another's.
 * Every refusal is problem+json with a stable code.
 */
@Path("/v1/vendor-exports/schedules")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-exports")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "Invalid request (SELECTION_INVALID, CADENCE_INVALID, TIMEZONE_INVALID, INVALID_REQUEST, "
                + "TEMPLATE_NOT_FOUND, TEMPLATE_WRONG_ENTITY, REASON_REQUIRED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description =
                "Caller holds no role in the tenant, lacks the permission, or named another tenant (TENANT_MISMATCH)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "404",
        description = "No such schedule (SCHEDULE_NOT_FOUND)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "409",
        description =
                "Exists, stale version, already enabled or disabled, ambiguous template (SCHEDULE_EXISTS, "
                        + "VERSION_STALE, ALREADY_ENABLED, ALREADY_DISABLED, TEMPLATE_AMBIGUOUS, BATCH_EXISTS, SCHEDULE_DISABLED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description =
                "DAL or api-layer unavailable or not configured (DAL_UNAVAILABLE, API_LAYER_UNAVAILABLE, API_LAYER_NOT_CONFIGURED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class ScheduleResource {

    private final ScheduleService service;
    private final UserContext ctx;

    public ScheduleResource(ScheduleService service, UserContext ctx) {
        this.service = service;
        this.ctx = ctx;
    }

    /**
     * Lists the caller's tenant's schedules.
     *
     * @param tenantId optional; must equal the header tenant when given
     * @return the schedules
     */
    @GET
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "List export schedules",
            description = "Schedules of the tenant named by the tenant-id header.")
    public Items<ScheduleView> list(@QueryParam("tenantId") String tenantId) {
        if (tenantId != null) {
            sameTenant(tenantId);
        }
        return new Items<>(
                service.list(ctx.tenantId()).stream().map(ScheduleView::of).toList());
    }

    /**
     * One schedule.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @return the schedule
     */
    @GET
    @Path("/{tenantId}/{vendor}")
    @RequiresPermission(Permission.READ)
    @Operation(summary = "Read an export schedule")
    public ScheduleView get(@PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor) {
        sameTenant(tenantId);
        return ScheduleView.of(service.require(tenantId, vendor));
    }

    /**
     * A schedule's own audit events.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @return creation, changes, enable, disable and run-now events, newest first
     */
    @GET
    @Path("/{tenantId}/{vendor}/events")
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "Read a schedule's audit trail",
            description = "The schedule-scoped events (no batch id), newest first, at most 200. A batch's own events "
                    + "are under /v1/vendor-exports/{exportBatchId}/events.")
    public Items<EventView> events(@PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor) {
        sameTenant(tenantId);
        return new Items<>(
                service.trail(tenantId, vendor).stream().map(EventView::of).toList());
    }

    /**
     * Creates or replaces a schedule.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param request the settings; without {@code version} creates, with it replaces
     * @return 201 on create, 200 on replace, with the schedule
     */
    @PUT
    @Path("/{tenantId}/{vendor}")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Create or replace an export schedule",
            description = "No version in the body: create (201), or 409 SCHEDULE_EXISTS. With version: replace (200), "
                    + "or 409 VERSION_STALE. Create provisions the tenant's vendor template through api-layer and "
                    + "sets enabled; replace never changes enabled. Cadence or timezone changes recompute nextDueAt.")
    @APIResponse(
            responseCode = "201",
            description = "Created",
            content = @Content(schema = @Schema(implementation = ScheduleView.class)))
    @APIResponse(
            responseCode = "200",
            description = "Replaced",
            content = @Content(schema = @Schema(implementation = ScheduleView.class)))
    public Response put(
            @PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor, PutRequest request) {
        sameTenant(tenantId);
        if (request == null) {
            throw ProblemException.badRequest("INVALID_REQUEST", "a body is required");
        }
        ScheduleService.Written written = service.put(tenantId, vendor, request, ctx.email());
        return Response.status(written.created() ? 201 : 200)
                .entity(ScheduleView.of(written.schedule()))
                .build();
    }

    /**
     * Disables a schedule.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param request the reason
     * @return the schedule
     */
    @POST
    @Path("/{tenantId}/{vendor}/disable")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Disable an export schedule",
            description = "The tick skips the schedule until enable. Reason is mandatory.")
    public ScheduleView disable(
            @PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor, DisableRequest request) {
        sameTenant(tenantId);
        String reason = request == null ? null : request.reason();
        return ScheduleView.of(service.disable(tenantId, vendor, reason, ctx.email()));
    }

    /**
     * Enables a schedule.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param request the reason and whether to catch up the one missed period
     * @return the schedule
     */
    @POST
    @Path("/{tenantId}/{vendor}/enable")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Enable an export schedule",
            description =
                    "catchUp true sets nextDueAt to the most recent occurrence missed while disabled; false to the next future one.")
    public ScheduleView enable(
            @PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor, EnableRequest request) {
        sameTenant(tenantId);
        String reason = request == null ? null : request.reason();
        boolean catchUp = request != null && Boolean.TRUE.equals(request.catchUp());
        return ScheduleView.of(service.enable(tenantId, vendor, reason, catchUp, ctx.email()));
    }

    /**
     * Creates the current period's batch now.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @return 201 with the batch id, its select job id and the schedule's new due instant
     */
    @POST
    @Path("/{tenantId}/{vendor}/run-now")
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Run an export schedule now",
            description = "Creates the current period's batch as the tick would, advances nextDueAt and enqueues the "
                    + "select job. 409 BATCH_EXISTS when the period already has its batch, SCHEDULE_DISABLED when "
                    + "the schedule is disabled.")
    @APIResponse(
            responseCode = "201",
            description = "Batch created",
            content = @Content(schema = @Schema(implementation = RunNowResponse.class)))
    public Response runNow(@PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor) {
        sameTenant(tenantId);
        BatchLifecycle.Scheduled scheduled = service.runNow(tenantId, vendor, ctx.email());
        return Response.status(201)
                .entity(new RunNowResponse(
                        scheduled.batch().id(), scheduled.jobId().toString(), scheduled.nextDueAt()))
                .build();
    }

    /**
     * Counts what a selection matches today.
     *
     * @param tenantId the tenant
     * @param vendor the vendor
     * @param request the selection to test, or empty for the stored one
     * @return the count
     */
    @POST
    @Path("/{tenantId}/{vendor}/preview")
    @RequiresPermission(Permission.READ)
    @Operation(
            summary = "Preview a selection",
            description = "Runs the selection against api-layer with page size 1 and returns totalCount.")
    public PreviewResponse preview(
            @PathParam("tenantId") String tenantId, @PathParam("vendor") String vendor, PreviewRequest request) {
        sameTenant(tenantId);
        return new PreviewResponse(service.preview(tenantId, vendor, request == null ? null : request.selection()));
    }

    private void sameTenant(String tenantId) {
        if (!tenantId.equals(ctx.tenantId())) {
            throw new ProblemException(
                    403, "TENANT_MISMATCH", "the path names a tenant other than the tenant-id header");
        }
    }
}
