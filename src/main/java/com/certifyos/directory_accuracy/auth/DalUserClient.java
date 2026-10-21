package com.certifyos.directory_accuracy.auth;

import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import org.eclipse.microprofile.rest.client.annotation.ClientHeaderParam;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * The DAL user lookup. The DAL's global header interceptor refuses any request that lacks {@code
 * requesting-user-id} or {@code requesting-organization-id} with a 404 "Missing Headers" and only
 * checks presence, so this client sends the service name as the user (there is no user yet; this
 * call is how one is found) and the caller's tenant as the organisation, as api-layer does. {@link
 * DalAuthFilter} adds the Google ID token IAP checks; until the IAP grant for this service's
 * account exists, IAP refuses the call and the operator sees {@code 503 DAL_UNAVAILABLE}.
 */
@RegisterRestClient(configKey = "dal")
@RegisterProvider(DalAuthFilter.class)
@ClientHeaderParam(name = "requesting-user-id", value = DalUserClient.REQUESTING_USER_ID)
public interface DalUserClient {

    /** The fixed {@code requesting-user-id} the DAL interceptor needs to see. */
    String REQUESTING_USER_ID = "directory-accuracy-service";

    /** The header the DAL interceptor reads the caller's tenant from. */
    String ORGANIZATION_HEADER = "requesting-organization-id";

    /**
     * Looks a user up by email.
     *
     * @param email the email claim from the token
     * @param includePermissions whether to return permissions for every tenant
     * @param tenantId the caller's tenant, sent as {@code requesting-organization-id}
     * @return the user, or a 404 wrapped in a client exception
     */
    @GET
    @Path("/users/by-email")
    DalUserResponse byEmail(
            @QueryParam("email") String email,
            @QueryParam("includePermissions") boolean includePermissions,
            @HeaderParam(ORGANIZATION_HEADER) String tenantId);
}
