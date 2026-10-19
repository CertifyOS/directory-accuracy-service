package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.auth.DalAuthFilter;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/** A tenant's configuration entries, one page at a time. Headers and IAP token as on {@link DalBatchClient}. */
@RegisterRestClient(configKey = "dal")
@RegisterProvider(DalAuthFilter.class)
@ClientHeaderParam(name = "requesting-user-id", value = DalUserClient.REQUESTING_USER_ID)
@Produces(MediaType.APPLICATION_JSON)
public interface DalTenantConfigClient {

    /**
     * Lists one page of a tenant's configurations.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param pathTenantId the tenant in the path
     * @param page the page number, from 0
     * @param size the page size
     * @return the page
     */
    @GET
    @Path("/tenants/{tenantId}/configurations")
    TenantConfigurationPage list(
            @HeaderParam("requesting-organization-id") String tenantId,
            @PathParam("tenantId") String pathTenantId,
            @QueryParam("page") int page,
            @QueryParam("size") int size);
}
