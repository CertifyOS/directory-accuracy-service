package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.config.DirectoryAccuracyConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import java.io.IOException;
import java.util.Optional;

/**
 * The IAP half of every api-layer call. api-service-internal sits behind IAP with a Google-managed
 * OAuth client, and api-layer itself reads {@code Authorization}, so the Google ID token for the
 * backend travels in {@code Proxy-Authorization}, the header IAP accepts for exactly this case.
 * Absent audience, no header. Registered on both api-layer clients, the token endpoint included:
 * the JDK {@code HttpClient} strips a user-set {@code Proxy-Authorization} on direct connections,
 * which is why the token fetch is a REST client call too (decisions.md finding 68).
 */
@ApplicationScoped
public class ApiLayerIapFilter implements ClientRequestFilter {

    /** The header IAP accepts its token in when Authorization is taken. */
    public static final String PROXY_AUTHORIZATION = "Proxy-Authorization";

    private final GoogleIdTokenService googleTokens;
    private final DirectoryAccuracyConfig cfg;

    public ApiLayerIapFilter(GoogleIdTokenService googleTokens, DirectoryAccuracyConfig cfg) {
        this.googleTokens = googleTokens;
        this.cfg = cfg;
    }

    @Override
    public void filter(ClientRequestContext request) throws IOException {
        Optional<String> iapAudience = cfg.apiLayer().iapAudience();
        if (iapAudience.isEmpty()) {
            return;
        }
        try {
            request.getHeaders().putSingle(PROXY_AUTHORIZATION, "Bearer " + googleTokens.idToken(iapAudience.get()));
        } catch (RuntimeException failure) {
            throw new IOException("could not obtain a Google ID token for api-layer's IAP", failure);
        }
    }
}
