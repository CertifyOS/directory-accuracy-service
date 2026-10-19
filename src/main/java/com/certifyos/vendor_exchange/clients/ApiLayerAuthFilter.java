package com.certifyos.vendor_exchange.clients;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.core.HttpHeaders;
import java.io.IOException;

/**
 * The Auth0 half of every api-layer business call: the machine token as the bearer. The IAP token
 * is {@link ApiLayerIapFilter}'s. The tenant header is a method parameter on the client.
 */
@ApplicationScoped
public class ApiLayerAuthFilter implements ClientRequestFilter {

    private final ApiLayerTokenService tokens;

    public ApiLayerAuthFilter(ApiLayerTokenService tokens) {
        this.tokens = tokens;
    }

    @Override
    public void filter(ClientRequestContext request) throws IOException {
        try {
            request.getHeaders().putSingle(HttpHeaders.AUTHORIZATION, "Bearer " + tokens.accessToken());
        } catch (RuntimeException failure) {
            throw new IOException("could not obtain an Auth0 machine token", failure);
        }
    }
}
