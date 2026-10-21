package com.certifyos.directory_accuracy.clients;

import com.certifyos.directory_accuracy.auth.GoogleIdTokenService;
import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import java.io.IOException;
import java.util.Optional;

/**
 * The IAP half of every file-ingestion call, the same arrangement as {@link ApiLayerIapFilter}:
 * file-ingestion reads {@code Authorization} for the Auth0 token, so the Google ID token for an
 * IAP-fronted backend travels in {@code Proxy-Authorization}. Absent audience, no header.
 */
@ApplicationScoped
public class FileIngestionIapFilter implements ClientRequestFilter {

    private final GoogleIdTokenService googleTokens;
    private final VendorExchangeConfig cfg;

    public FileIngestionIapFilter(GoogleIdTokenService googleTokens, VendorExchangeConfig cfg) {
        this.googleTokens = googleTokens;
        this.cfg = cfg;
    }

    @Override
    public void filter(ClientRequestContext request) throws IOException {
        Optional<String> iapAudience = cfg.fileIngestion().iapAudience();
        if (iapAudience.isEmpty()) {
            return;
        }
        try {
            request.getHeaders()
                    .putSingle(
                            ApiLayerIapFilter.PROXY_AUTHORIZATION, "Bearer " + googleTokens.idToken(iapAudience.get()));
        } catch (RuntimeException failure) {
            throw new IOException("could not obtain a Google ID token for file-ingestion's IAP", failure);
        }
    }
}
