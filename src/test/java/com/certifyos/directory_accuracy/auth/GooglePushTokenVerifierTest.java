package com.certifyos.directory_accuracy.auth;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.auth.oauth2.TokenVerifier;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.time.Instant;
import java.util.function.Consumer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tokens signed here with a throwaway RSA key; the verifier is given the matching public key. */
class GooglePushTokenVerifierTest {

    static final String AUDIENCE = "https://directory-accuracy.test/internal/vendor-exports/egress-events";
    static final String ACCOUNT = "pubsub-push@test.iam.gserviceaccount.com";

    private static KeyPair keys;
    private static GooglePushTokenVerifier verifier;

    @BeforeAll
    static void beforeAll() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keys = generator.generateKeyPair();
        TokenVerifier google = TokenVerifier.newBuilder()
                .setPublicKey(keys.getPublic())
                .setIssuer(GooglePushTokenVerifier.ISSUER)
                .setAudience(AUDIENCE)
                .build();
        verifier = new GooglePushTokenVerifier(google, ACCOUNT);
    }

    private static String token(Consumer<JsonWebToken.Payload> edit) throws GeneralSecurityException {
        long now = Instant.now().getEpochSecond();
        JsonWebToken.Payload payload = new JsonWebToken.Payload()
                .setIssuer(GooglePushTokenVerifier.ISSUER)
                .setAudience(AUDIENCE)
                .setIssuedAtTimeSeconds(now)
                .setExpirationTimeSeconds(now + 300)
                .set("email", ACCOUNT)
                .set("email_verified", true);
        edit.accept(payload);
        JsonWebSignature.Header header =
                new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT");
        try {
            return JsonWebSignature.signUsingRsaSha256(
                    keys.getPrivate(), GsonFactory.getDefaultInstance(), header, payload);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
    }

    private static void assertRejected(String token, String reasonFragment) {
        PushTokenRejectedException rejected =
                Assertions.assertThrows(PushTokenRejectedException.class, () -> verifier.verify(token));
        Assertions.assertTrue(
                rejected.getMessage().contains(reasonFragment),
                "expected '" + reasonFragment + "' in: " + rejected.getMessage());
    }

    @Test
    void aCorrectTokenYieldsThePushIdentity() throws GeneralSecurityException {
        Assertions.assertEquals(new PushIdentity(ACCOUNT), verifier.verify(token(payload -> {})));
    }

    @Test
    void wrongAudienceIsRejected() throws GeneralSecurityException {
        assertRejected(token(payload -> payload.setAudience("https://other.test/")), "audience");
    }

    @Test
    void wrongIssuerIsRejected() throws GeneralSecurityException {
        assertRejected(token(payload -> payload.setIssuer("https://evil.example")), "issuer");
    }

    @Test
    void expiredTokenIsRejected() throws GeneralSecurityException {
        long past = Instant.now().getEpochSecond() - 600;
        assertRejected(token(payload -> payload.setExpirationTimeSeconds(past)), "expiry");
    }

    @Test
    void wrongEmailIsRejected() throws GeneralSecurityException {
        assertRejected(token(payload -> payload.set("email", "someone@else.test")), "push service account");
    }

    @Test
    void unverifiedEmailIsRejected() throws GeneralSecurityException {
        assertRejected(token(payload -> payload.set("email_verified", false)), "not verified");
    }

    @Test
    void tokenSignedByAnotherKeyIsRejected() throws GeneralSecurityException {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair other = generator.generateKeyPair();
        long now = Instant.now().getEpochSecond();
        JsonWebToken.Payload payload = new JsonWebToken.Payload()
                .setIssuer(GooglePushTokenVerifier.ISSUER)
                .setAudience(AUDIENCE)
                .setExpirationTimeSeconds(now + 300)
                .set("email", ACCOUNT)
                .set("email_verified", true);
        String forged;
        try {
            forged = JsonWebSignature.signUsingRsaSha256(
                    other.getPrivate(),
                    GsonFactory.getDefaultInstance(),
                    new JsonWebSignature.Header().setAlgorithm("RS256").setType("JWT"),
                    payload);
        } catch (java.io.IOException failure) {
            throw new IllegalStateException(failure);
        }
        assertRejected(forged, "signature");
    }
}
