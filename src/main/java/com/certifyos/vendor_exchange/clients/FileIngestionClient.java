package com.certifyos.vendor_exchange.clients;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.InputStream;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * file-ingestion-service, called with this service's Auth0 machine token ({@link ApiLayerAuthFilter})
 * and, when its backend sits behind IAP, a Google ID token ({@link FileIngestionIapFilter}). Every
 * call names the tenant and this service's consumer identifier in headers. A status of 400 or above
 * surfaces as a client exception carrying that status, for the caller to read. Interface and
 * contracts only; the first call is the ingestion lane's.
 */
@RegisterRestClient(configKey = "file-ingestion")
@RegisterProvider(FileIngestionIapFilter.class)
@RegisterProvider(ApiLayerAuthFilter.class)
@Produces(MediaType.APPLICATION_JSON)
public interface FileIngestionClient {

    /** The tenant header file-ingestion requires. */
    String TENANT_HEADER = "tenant-id";

    /** The header naming the registered consumer. */
    String IDENTIFIER_HEADER = "identifier";

    /**
     * Registers a file by its source URI. A repeated {@code externalRef} returns the existing
     * ingestion.
     *
     * @param tenantId the tenant
     * @param identifier this service's consumer identifier
     * @param body the registration
     * @return 201 for a new ingestion, 200 for an existing one; the entity is a {@link
     *     FileIngestionRegisterResponse}
     */
    @POST
    @Path("/v1/ingestion")
    @Consumes(MediaType.APPLICATION_JSON)
    Response register(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @HeaderParam(IDENTIFIER_HEADER) String identifier,
            FileIngestionRegisterRequest body);

    /**
     * Reads an ingestion's status.
     *
     * @param tenantId the tenant
     * @param identifier this service's consumer identifier
     * @param ingestionId the ingestion
     * @return the status
     */
    @GET
    @Path("/v1/ingestion/{id}")
    FileIngestionStatus status(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @HeaderParam(IDENTIFIER_HEADER) String identifier,
            @PathParam("id") String ingestionId);

    /**
     * Streams an ingestion's rows as NDJSON, one {@link FileIngestionRow} per line. A 429 (too many
     * streams) surfaces as a client exception with that status.
     *
     * @param tenantId the tenant
     * @param identifier this service's consumer identifier
     * @param ingestionId the ingestion
     * @param after the cursor to resume after, or null from the start
     * @return the stream; the caller closes it
     */
    @GET
    @Path("/v1/ingestion-rows/{id}")
    @Produces("application/x-ndjson")
    InputStream rows(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @HeaderParam(IDENTIFIER_HEADER) String identifier,
            @PathParam("id") String ingestionId,
            @QueryParam("after") String after);

    /**
     * Marks an ingestion consumed. Idempotent.
     *
     * @param tenantId the tenant
     * @param identifier this service's consumer identifier
     * @param ingestionId the ingestion
     * @return the acknowledgement
     */
    @POST
    @Path("/v1/ingestion-complete/{id}")
    Response markConsumed(
            @HeaderParam(TENANT_HEADER) String tenantId,
            @HeaderParam(IDENTIFIER_HEADER) String identifier,
            @PathParam("id") String ingestionId);
}
