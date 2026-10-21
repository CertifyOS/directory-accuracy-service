package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.clients.ApiLayerAuthClient.TokenRequest;
import com.certifyos.directory_accuracy.clients.ApiLayerAuthClient.TokenResponse;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.WebApplicationException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.logging.Logger;

/**
 * An Auth0 access token for api-layer, obtained through api-layer's own client-credentials
 * endpoint with this service's machine client and cached until one minute before it expires.
 * Until the client id and secret exist (a swap-in point), {@link #configured()} is false, {@link
 * #accessToken()} throws {@link ApiLayerNotConfiguredException}, and the readiness check reports
 * the gap. The call goes through {@link ApiLayerAuthClient}, which carries the IAP token.
 */
@ApplicationScoped
public class ApiLayerTokenService {

    static final Duration EXPIRY_MARGIN = Duration.ofSeconds(60);
    private static final Logger LOG = Logger.getLogger(ApiLayerTokenService.class);

    private final VendorExchangeConfig cfg;
    private final ApiLayerAuthClient auth;
    private final Clock clock;
    private volatile Cached cached;

    public ApiLayerTokenService(VendorExchangeConfig cfg, @RestClient ApiLayerAuthClient auth, Clock clock) {
        this.cfg = cfg;
        this.auth = auth;
        this.clock = clock;
    }

    /** Whether the machine client exists in configuration. */
    public boolean configured() {
        return cfg.apiLayer().clientId().isPresent()
                && cfg.apiLayer().clientSecret().isPresent();
    }

    /**
     * A valid access token.
     *
     * @return the token value, to send as a bearer
     * @throws ApiLayerNotConfiguredException when the machine client is absent
     * @throws ApiLayerTokenException when api-layer refuses or cannot be reached
     */
    public String accessToken() {
        if (!configured()) {
            throw new ApiLayerNotConfiguredException();
        }
        Cached current = cached;
        if (current != null && clock.instant().isBefore(current.expiresAt())) {
            return current.token();
        }
        return refresh();
    }

    private synchronized String refresh() {
        Cached current = cached;
        if (current != null && clock.instant().isBefore(current.expiresAt())) {
            return current.token();
        }
        TokenResponse response = fetch();
        long seconds = response.expiresIn() == null ? 0 : response.expiresIn();
        Instant expiresAt = clock.instant().plusSeconds(seconds).minus(EXPIRY_MARGIN);
        cached = new Cached(response.accessToken(), expiresAt);
        LOG.infof("api-layer token obtained, valid until %s", expiresAt);
        return response.accessToken();
    }

    private TokenResponse fetch() {
        TokenRequest body = new TokenRequest(
                cfg.apiLayer().clientId().orElseThrow(),
                cfg.apiLayer().clientSecret().orElseThrow());
        TokenResponse token;
        try {
            token = auth.clientCredentials(body);
        } catch (WebApplicationException refused) {
            int status =
                    refused.getResponse() == null ? 0 : refused.getResponse().getStatus();
            throw new ApiLayerTokenException("client-credentials answered " + status, refused);
        } catch (ProcessingException failure) {
            throw new ApiLayerTokenException("client-credentials call failed: " + failure.getMessage(), failure);
        }
        if (token == null || token.accessToken() == null || token.accessToken().isBlank()) {
            throw new ApiLayerTokenException("client-credentials answered without an access token");
        }
        return token;
    }

    record Cached(String token, Instant expiresAt) {}
}
