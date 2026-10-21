package com.certifyos.directory_accuracy.auth;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Mints and caches one Google ID token per IAP audience (the DAL's and egress's OAuth client ids)
 * from Application Default Credentials: the metadata server on Compute Engine, a user or
 * impersonated credential locally. A token is reused until five minutes before its expiry and then
 * minted again on the calling thread; a mint is one local metadata-server call, so no background
 * refresh thread is kept (the playbook allows managed executors only).
 */
@ApplicationScoped
public class GoogleIdTokenService {

    /**
     * IAP maps the caller to its IAM binding through the token's {@code email} claim and answers 401
     * without it. On Compute Engine the metadata server omits that claim unless asked for {@code
     * format=full}, which {@code FORMAT_FULL} requests; {@code INCLUDE_EMAIL} does the same for
     * impersonated credentials and is ignored on Compute Engine (file-ingestion finding 63).
     */
    static final List<IdTokenProvider.Option> TOKEN_OPTIONS =
            List.of(IdTokenProvider.Option.FORMAT_FULL, IdTokenProvider.Option.INCLUDE_EMAIL);

    static final Duration REFRESH_BUFFER = Duration.ofMinutes(5);

    private final CredentialsSource credentials;
    private final Clock clock;
    private final Map<String, IdToken> cache = new ConcurrentHashMap<>();

    @Inject
    public GoogleIdTokenService(Clock clock) {
        this(GoogleIdTokenService::applicationDefault, clock);
    }

    GoogleIdTokenService(CredentialsSource credentials, Clock clock) {
        this.credentials = credentials;
        this.clock = clock;
    }

    /**
     * A valid ID token for an audience.
     *
     * @param audience the IAP OAuth client id of the backend being called
     * @return the token value, to send as a bearer
     * @throws IllegalStateException when credentials are missing or cannot mint ID tokens
     */
    public String idToken(String audience) {
        IdToken token = cache.get(audience);
        if (token == null || nearExpiry(token)) {
            token = refresh(audience);
        }
        return token.getTokenValue();
    }

    private synchronized IdToken refresh(String audience) {
        IdToken token = cache.get(audience);
        if (token != null && !nearExpiry(token)) {
            return token;
        }
        IdToken minted = mint(credentials.load(), audience);
        cache.put(audience, minted);
        return minted;
    }

    private boolean nearExpiry(IdToken token) {
        return token.getExpirationTime().toInstant().isBefore(clock.instant().plus(REFRESH_BUFFER));
    }

    /**
     * Mints one token. Package-private so a test can check the options passed to the provider.
     *
     * @param loaded the credentials
     * @param audience the audience
     * @return the token
     */
    static IdToken mint(GoogleCredentials loaded, String audience) {
        if (!(loaded instanceof IdTokenProvider provider)) {
            throw new IllegalStateException("credentials are not an IdTokenProvider; cannot mint ID tokens");
        }
        try {
            return provider.idTokenWithAudience(audience, TOKEN_OPTIONS);
        } catch (IOException failure) {
            throw new IllegalStateException("could not mint a Google ID token for " + audience, failure);
        }
    }

    static GoogleCredentials applicationDefault() {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException failure) {
            throw new IllegalStateException("no Application Default Credentials", failure);
        }
    }

    /** Where credentials come from; the production source is Application Default Credentials. */
    @FunctionalInterface
    interface CredentialsSource {
        GoogleCredentials load();
    }
}
