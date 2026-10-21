package com.certifyos.directory_accuracy.auth;

/** Verifies the OIDC identity token Pub/Sub sends with a push delivery. */
public interface PushTokenVerifier {

    /**
     * Verifies a token.
     *
     * @param token the bearer token
     * @return the identity it proves
     * @throws PushTokenRejectedException when any check fails
     */
    PushIdentity verify(String token);
}
