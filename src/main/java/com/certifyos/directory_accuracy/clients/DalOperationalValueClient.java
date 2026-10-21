package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.DalAuthFilter;
import com.certifyos.directory_accuracy.auth.DalUserClient;
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

/**
 * The operational value (the Golden record) of a practitioner or location and the slices that
 * contributed to it. Headers and IAP token as on {@link DalBatchClient}. The DAL sends {@code
 * contributionMap} only when {@code includeLineage=true} is asked for; without it the map is absent.
 */
@RegisterRestClient(configKey = "dal")
@RegisterProvider(DalAuthFilter.class)
@ClientHeaderParam(name = "requesting-user-id", value = DalUserClient.REQUESTING_USER_ID)
@Produces(MediaType.APPLICATION_JSON)
public interface DalOperationalValueClient {

    /**
     * Reads a practitioner's operational value.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param certifyId the practitioner's certify id
     * @param includeLineage true to receive {@code contributionMap}, which the read-back needs
     * @return the operational value
     */
    @GET
    @Path("/practitioners/ov/{entityId}")
    OperationalValue practitioner(
            @HeaderParam(DalUserClient.ORGANIZATION_HEADER) String tenantId,
            @PathParam("entityId") String certifyId,
            @QueryParam("includeLineage") boolean includeLineage);

    /**
     * Reads a location's operational value.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param certifyId the location's certify id
     * @param includeLineage true to receive {@code contributionMap}, which the read-back needs
     * @return the operational value
     */
    @GET
    @Path("/locations/ov/{entityId}")
    OperationalValue location(
            @HeaderParam(DalUserClient.ORGANIZATION_HEADER) String tenantId,
            @PathParam("entityId") String certifyId,
            @QueryParam("includeLineage") boolean includeLineage);
}
