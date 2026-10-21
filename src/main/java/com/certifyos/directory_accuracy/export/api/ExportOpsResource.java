package com.certifyos.directory_accuracy.export.api;

import com.certifyos.directory_accuracy.auth.Permission;
import com.certifyos.directory_accuracy.auth.RequiresPermission;
import com.certifyos.directory_accuracy.auth.UserContext;
import com.certifyos.directory_accuracy.export.jobs.TickJob;
import com.certifyos.directory_accuracy.http.Problem;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.jboss.logging.Logger;

/**
 * Operator actions on the export lane. {@code POST /v1/vendor-exports/tick} runs the daily tick
 * now, inline on this API node, and answers with its counts, so a live check never waits for
 * 06:00 UTC. It is a platform call: authenticated and tenant-scoped like every other, but the
 * tick itself looks at every tenant's schedules.
 */
@Path("/v1/vendor-exports/tick")
@Produces(MediaType.APPLICATION_JSON)
@Tag(name = "vendor-exports")
@SecurityRequirement(name = "platform-token")
@APIResponse(
        responseCode = "400",
        description = "tenant-id header missing (TENANT_REQUIRED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(responseCode = "401", description = "No or invalid platform token")
@APIResponse(
        responseCode = "403",
        description =
                "Caller holds no role in the tenant (TENANT_FORBIDDEN, TENANT_UNRESOLVED) or lacks the permission (PERMISSION_DENIED)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
@APIResponse(
        responseCode = "503",
        description = "User lookup failed (DAL_UNAVAILABLE)",
        content = @Content(mediaType = Problem.MEDIA_TYPE, schema = @Schema(implementation = Problem.class)))
public class ExportOpsResource {

    private static final Logger LOG = Logger.getLogger(ExportOpsResource.class);

    private final UserContext ctx;
    private final TickJob tickJob;

    public ExportOpsResource(UserContext ctx, TickJob tickJob) {
        this.ctx = ctx;
        this.tickJob = tickJob;
    }

    /**
     * Runs the tick now.
     *
     * @return what the tick found and created
     */
    @POST
    @RequiresPermission(Permission.MANAGE)
    @Operation(
            summary = "Run the export tick now",
            description = "Runs the daily schedule tick inline and returns its counts. Same code path as "
                    + "the 06:00 UTC recurring job.")
    public TickJob.TickResult tick() {
        LOG.infof("tick requested by an operator of tenant %s", ctx.tenantId());
        return tickJob.run();
    }
}
