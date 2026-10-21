package com.certifyos.directory_accuracy.clients;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * The egress service (repository {@code core-dal-egress-practitioner-async}), reached through IAP
 * with the worker's identity. Every call carries {@code X-Forwarding-Tenant-Id}, the tenant check
 * egress applies to callers. Egress answers a repeated correlation id with 409 and a cancel of a
 * finished job with 409; both surface as a client exception with that status, for the caller to
 * read. Interface and contracts only; the first call is the export lane's.
 */
@RegisterRestClient(configKey = "egress")
@RegisterProvider(EgressAuthFilter.class)
@Path("/api/v1/egress")
@Produces(MediaType.APPLICATION_JSON)
public interface EgressClient {

    /** The tenant header egress checks. */
    String TENANT_HEADER = "X-Forwarding-Tenant-Id";

    /** The identity egress records as the initiator; also the Pub/Sub attribute our subscription filters on. */
    String INITIATOR = "directory-accuracy-worker";

    /**
     * Asks egress to build and place a file.
     *
     * @param callerTenantId the tenant, for the header
     * @param request the export request
     * @return the job reference (202)
     */
    @POST
    @Path("/export")
    @Consumes(MediaType.APPLICATION_JSON)
    EgressExportResponse export(@HeaderParam(TENANT_HEADER) String callerTenantId, EgressExportRequest request);

    /**
     * Reads a job's status.
     *
     * @param callerTenantId the tenant, for the header
     * @param tenantId the tenant in the path
     * @param correlationId the job's correlation id
     * @return the status
     */
    @GET
    @Path("/status/practitioner/{tenantId}/{correlationId}")
    EgressStatusResponse status(
            @HeaderParam(TENANT_HEADER) String callerTenantId,
            @PathParam("tenantId") String tenantId,
            @PathParam("correlationId") String correlationId);

    /**
     * Cancels a running job (cooperative: egress's pipelines stop at their next checkpoint).
     *
     * @param callerTenantId the tenant, for the header
     * @param tenantId the tenant in the path
     * @param correlationId the job's correlation id
     * @return the acknowledgement (202)
     */
    @POST
    @Path("/jobs/practitioner/{tenantId}/{correlationId}/cancel")
    @ClientHeaderParam(name = "x-user-id", value = INITIATOR)
    EgressCancelResponse cancel(
            @HeaderParam(TENANT_HEADER) String callerTenantId,
            @PathParam("tenantId") String tenantId,
            @PathParam("correlationId") String correlationId);
}
