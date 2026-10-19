package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.auth.DalAuthFilter;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * The DAL batch endpoint: several operations, optionally in one transaction. The DAL's header
 * interceptor needs {@code requesting-user-id} (this service's name, as on {@link DalUserClient})
 * and {@code requesting-organization-id} (the tenant passed in); {@link DalAuthFilter} adds the IAP
 * token. A status of 400 or above surfaces as a client exception carrying that status.
 */
@RegisterRestClient(configKey = "dal")
@RegisterProvider(DalAuthFilter.class)
@ClientHeaderParam(name = "requesting-user-id", value = DalUserClient.REQUESTING_USER_ID)
@Produces(MediaType.APPLICATION_JSON)
public interface DalBatchClient {

    /**
     * Runs a batch.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param body the operations
     * @return the response; the caller reads the per-operation {@link DalOperationResult}s
     */
    @POST
    @Path("/batch")
    @Consumes(MediaType.APPLICATION_JSON)
    Response execute(@HeaderParam(DalUserClient.ORGANIZATION_HEADER) String tenantId, DalBatchRequest body);
}
