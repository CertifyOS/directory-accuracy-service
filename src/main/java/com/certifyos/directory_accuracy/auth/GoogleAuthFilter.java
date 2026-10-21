package com.certifyos.directory_accuracy.auth;

import jakarta.ws.rs.client.ClientRequestContext;
import jakarta.ws.rs.client.ClientRequestFilter;
import jakarta.ws.rs.core.HttpHeaders;
import java.io.IOException;

/**
 * Adds a Google ID token for one IAP-protected backend to every outbound call. One subclass per
 * backend names the audience; the REST client interface registers that subclass as a provider.
 * Subclasses are {@code @Singleton}, not {@code @ApplicationScoped}: a normal-scoped bean needs a
 * client proxy, and ArC cannot synthesise a no-args constructor for a class whose parent has none.
 */
public abstract class GoogleAuthFilter implements ClientRequestFilter {

    private final GoogleIdTokenService tokens;

    protected GoogleAuthFilter(GoogleIdTokenService tokens) {
        this.tokens = tokens;
    }

    /** The IAP OAuth client id of the backend this filter serves. */
    protected abstract String audience();

    @Override
    public void filter(ClientRequestContext request) throws IOException {
        try {
            request.getHeaders().putSingle(HttpHeaders.AUTHORIZATION, "Bearer " + tokens.idToken(audience()));
        } catch (RuntimeException failure) {
            // An IOException from a client filter surfaces to the caller as a ProcessingException,
            // the same failure class as a connection error, which callers already handle.
            throw new IOException(
                    "could not obtain a Google ID token for " + getClass().getSimpleName(), failure);
        }
    }
}
