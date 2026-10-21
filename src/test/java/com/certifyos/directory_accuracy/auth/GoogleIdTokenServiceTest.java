package com.certifyos.directory_accuracy.auth;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.IdToken;
import com.google.auth.oauth2.IdTokenProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * IAP rejects an identity token that carries no {@code email} claim, and the Compute Engine
 * metadata server omits that claim unless asked for the full format (proven live by
 * file-ingestion on 2026-09-22). These tests pin the options and the per-audience cache.
 */
class GoogleIdTokenServiceTest {

    static final Instant NOW = Instant.parse("2026-10-05T10:00:00Z");
    static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    /** IdToken.create parses the JWT for its expiry; nothing verifies the signature locally. */
    static String fakeJwt(String audience, Instant expiry) {
        Base64.Encoder enc = Base64.getUrlEncoder().withoutPadding();
        String header = enc.encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
        String payload = enc.encodeToString(("{\"aud\":\"" + audience + "\",\"exp\":" + expiry.getEpochSecond() + "}")
                .getBytes(StandardCharsets.UTF_8));
        return header + "." + payload + "." + enc.encodeToString("sig".getBytes(StandardCharsets.UTF_8));
    }

    static final class RecordingCredentials extends GoogleCredentials implements IdTokenProvider {
        private static final long serialVersionUID = 1L;
        final List<String> audiences = new ArrayList<>();
        List<Option> lastOptions;
        Duration lifetime = Duration.ofHours(1);

        @Override
        public IdToken idTokenWithAudience(String targetAudience, List<Option> options) throws IOException {
            audiences.add(targetAudience);
            lastOptions = options;
            return IdToken.create(fakeJwt(targetAudience, NOW.plus(lifetime)));
        }

        // GoogleCredentials defines equals; a recording stub is only ever equal to itself.
        @Override
        public boolean equals(Object other) {
            return this == other;
        }

        @Override
        public int hashCode() {
            return System.identityHashCode(this);
        }
    }

    @Test
    void asksForTheFullFormatTokenIapRequires() {
        RecordingCredentials creds = new RecordingCredentials();
        GoogleIdTokenService.mint(creds, "aud-dal");
        Assertions.assertEquals(List.of("aud-dal"), creds.audiences);
        Assertions.assertTrue(
                creds.lastOptions.contains(IdTokenProvider.Option.FORMAT_FULL), "FORMAT_FULL adds email on GCE");
        Assertions.assertTrue(
                creds.lastOptions.contains(IdTokenProvider.Option.INCLUDE_EMAIL),
                "INCLUDE_EMAIL does so when impersonating");
    }

    @Test
    void cachesPerAudience() {
        RecordingCredentials creds = new RecordingCredentials();
        GoogleIdTokenService service = new GoogleIdTokenService(() -> creds, CLOCK);
        String dal1 = service.idToken("aud-dal");
        String dal2 = service.idToken("aud-dal");
        String egress = service.idToken("aud-egress");
        Assertions.assertEquals(dal1, dal2, "same audience, same cached token");
        Assertions.assertNotEquals(dal1, egress, "another audience is another token");
        Assertions.assertEquals(List.of("aud-dal", "aud-egress"), creds.audiences, "one mint per audience");
    }

    @Test
    void mintsAgainWithinFiveMinutesOfExpiry() {
        RecordingCredentials creds = new RecordingCredentials();
        creds.lifetime = Duration.ofMinutes(4);
        GoogleIdTokenService service = new GoogleIdTokenService(() -> creds, CLOCK);
        service.idToken("aud-dal");
        service.idToken("aud-dal");
        Assertions.assertEquals(2, creds.audiences.size(), "a token inside the refresh buffer is not reused");
    }

    @Test
    void refusesCredentialsThatCannotMintIdTokens() {
        GoogleCredentials plain = GoogleCredentials.create(new AccessToken("t", null));
        IllegalStateException refused =
                Assertions.assertThrows(IllegalStateException.class, () -> GoogleIdTokenService.mint(plain, "aud"));
        Assertions.assertTrue(refused.getMessage().contains("IdTokenProvider"));
    }
}
