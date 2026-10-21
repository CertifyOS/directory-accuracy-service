package com.certifyos.directory_accuracy.auth;

import com.certifyos.directory_accuracy.config.VendorExchangeConfig;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.auth.oauth2.TokenVerifier;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * The real verifier: Google's {@link TokenVerifier} checks signature (against Google's published
 * certificates), issuer, audience and expiry; this class then requires the {@code email} claim to
 * be the configured push service account and {@code email_verified} to be true. The audience is
 * the push endpoint URL, which Terraform sets on both the subscription and this service.
 */
@ApplicationScoped
public class GooglePushTokenVerifier implements PushTokenVerifier {

    /** The issuer of every Pub/Sub push identity token. */
    public static final String ISSUER = "https://accounts.google.com";

    private final TokenVerifier verifier;
    private final String expectedEmail;

    @Inject
    public GooglePushTokenVerifier(VendorExchangeConfig cfg) {
        this(
                TokenVerifier.newBuilder()
                        .setIssuer(ISSUER)
                        .setAudience(cfg.pubsub().pushAudience().orElse(null))
                        .build(),
                cfg.pubsub().pushServiceAccount().orElse(""));
    }

    GooglePushTokenVerifier(TokenVerifier verifier, String expectedEmail) {
        this.verifier = verifier;
        this.expectedEmail = expectedEmail;
    }

    @Override
    public PushIdentity verify(String token) {
        JsonWebSignature signature;
        try {
            signature = verifier.verify(token);
        } catch (TokenVerifier.VerificationException failure) {
            throw new PushTokenRejectedException("signature, issuer, audience or expiry check failed", failure);
        }
        Object email = signature.getPayload().get("email");
        if (!(email instanceof String address) || !address.equalsIgnoreCase(expectedEmail)) {
            throw new PushTokenRejectedException("token email is not the push service account");
        }
        if (!Boolean.TRUE.equals(signature.getPayload().get("email_verified"))) {
            throw new PushTokenRejectedException("token email is not verified");
        }
        return new PushIdentity(address);
    }
}
