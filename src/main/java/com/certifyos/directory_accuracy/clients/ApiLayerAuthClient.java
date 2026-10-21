package com.certifyos.directory_accuracy.clients;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import org.eclipse.microprofile.rest.client.annotation.RegisterProvider;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

/**
 * api-layer's own token endpoint, {@code POST /auth/client-credentials}, which mints an Auth0
 * access token for a machine client. Same host and IAP as {@link ApiLayerClient}, so the same
 * {@link ApiLayerIapFilter}; no Auth0 filter, since this call is how the Auth0 token is obtained.
 */
@RegisterRestClient(configKey = "api-layer")
@RegisterProvider(ApiLayerIapFilter.class)
@Path("/auth")
public interface ApiLayerAuthClient {

    /**
     * Exchanges the machine client for an access token.
     *
     * @param request client id and secret
     * @return the token and its lifetime
     */
    @POST
    @Path("/client-credentials")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    TokenResponse clientCredentials(TokenRequest request);

    /** The request body. */
    record TokenRequest(String clientId, String clientSecret) {}

    /** The answer: {@code accessToken}, {@code expiresIn} seconds, {@code tokenType}. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record TokenResponse(String accessToken, Long expiresIn, String tokenType) {}
}
