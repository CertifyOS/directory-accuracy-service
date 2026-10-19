package com.certifyos.vendor_exchange.clients;

import com.certifyos.vendor_exchange.auth.DalAuthFilter;
import com.certifyos.vendor_exchange.auth.DalUserClient;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * One source's slice of a practitioner, location or entity address, read by source type and
 * crosswalk id. Headers and IAP token as on {@link DalBatchClient}. A missing slice surfaces as a
 * client exception with status 404.
 */
@RegisterRestClient(configKey = "dal")
@RegisterProvider(DalAuthFilter.class)
@ClientHeaderParam(name = "requesting-user-id", value = DalUserClient.REQUESTING_USER_ID)
@Produces(MediaType.APPLICATION_JSON)
public interface DalSliceClient {

    /**
     * Reads a practitioner slice.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param sourceType the slice's source type
     * @param crosswalkId the slice's crosswalk id
     * @return the slice
     */
    @GET
    @Path("/core/practitioner/{sourceType}/{crosswalkId}")
    Slice practitioner(
            @HeaderParam("requesting-organization-id") String tenantId,
            @PathParam("sourceType") String sourceType,
            @PathParam("crosswalkId") String crosswalkId);

    /**
     * Reads a location slice.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param sourceType the slice's source type
     * @param crosswalkId the slice's crosswalk id
     * @return the slice
     */
    @GET
    @Path("/core/locations/{sourceType}/{crosswalkId}")
    Slice location(
            @HeaderParam("requesting-organization-id") String tenantId,
            @PathParam("sourceType") String sourceType,
            @PathParam("crosswalkId") String crosswalkId);

    /**
     * Reads an entity-address slice.
     *
     * @param tenantId the tenant, sent as {@code requesting-organization-id}
     * @param sourceType the slice's source type
     * @param crosswalkId the slice's crosswalk id
     * @return the slice
     */
    @GET
    @Path("/core/entity-addresses/{sourceType}/{crosswalkId}")
    Slice entityAddress(
            @HeaderParam("requesting-organization-id") String tenantId,
            @PathParam("sourceType") String sourceType,
            @PathParam("crosswalkId") String crosswalkId);
}
